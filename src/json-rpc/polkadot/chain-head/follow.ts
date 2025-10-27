import { getLogger } from "@logtape/logtape";
import { filter, map } from "rxjs";

import {
	createConcurrentCreator,
	TooManyWaitersError,
} from "../../../concurrency/creator";
import { jsonRpcError } from "../../errors";
import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCNotification } from "../../types";
import type { SharedSubscription } from "../../upstream";
import { createStateMap } from "./state";

const logger = getLogger("wsmux.chainhead.follow");
const MAX_FOLLOWS_PER_UPSTREAM = 2;

export const chainHead_v1_follow = (): JSONRPCMethodHandler => {
	const methodKey = "chainHead_v1_follow";
	const state = createStateMap();

	const getOrCreateFollow = createConcurrentCreator({
		maxWaiting: 5,
		label: methodKey,
	});

	return {
		async handleRequest(upstream, downstream, request) {
			const clientId = downstream.clientId;

			logger.info((l) => l`Follow request from ${clientId}`);

			const chainHeadSubs = upstream.subscriptions.getOrCreate(methodKey, {
				maxSubscribers: MAX_FOLLOWS_PER_UPSTREAM,
			});
			const selected = chainHeadSubs.getLeastLoaded();

			async function assignToDownstream(
				shared: SharedSubscription,
				followKey: string,
			) {
				logger.info(
					(l) =>
						l`Assigning ${clientId} to ${followKey} (${shared.subscribersCount()} subs)`,
				);

				const localId = downstream.getLocalId(shared.upstreamSubId);
				downstream.send({
					jsonrpc: "2.0",
					id: request.id ?? null,
					result: localId,
				});
				shared.subscribeLocal(localId, downstream);
				upstream.unsubscribers.set(localId, () =>
					shared.unsubscribeLocal(localId),
				);
				await state.getOrCreate(followKey).replay(downstream, localId);
			}

			if (chainHeadSubs.canCreateNew(selected)) {
				const followIndex = chainHeadSubs.size();
				const followKey = `${methodKey}:${followIndex}`;

				try {
					const shared = await getOrCreateFollow(followKey, async () => {
						logger.info(
							(l) =>
								l`[Follow] Creating upstream follow ${followKey} (${followIndex + 1}/${MAX_FOLLOWS_PER_UPSTREAM})`,
						);
						const { result: upstreamSubId } = (await upstream.request({
							...request,
							params: [true],
						})) as { result: string };
						if (!upstreamSubId) throw new Error("No upstreamSubId in response");

						const snapshot = state.getOrCreate(followKey);

						const shared = chainHeadSubs.createShared(
							followKey,
							upstreamSubId,
							snapshot.withUpdate(
								upstream.message$.pipe(
									filter(
										(msg) =>
											"method" in msg &&
											msg.method === "chainHead_v1_followEvent" &&
											msg.params?.subscription === upstreamSubId,
									),
									map((msg) => msg as JSONRPCNotification),
								),
							),
							async () => {
								logger.info(
									(l) => l`[Follow] Unfollowed upstream ${upstreamSubId}`,
								);
								await upstream.request({
									jsonrpc: "2.0",
									method: "chainHead_v1_unfollow",
									params: [upstreamSubId],
								});
								state.remove(followKey);
							},
						);

						return shared;
					});
					await assignToDownstream(shared, followKey);
					return;
				} catch (err) {
					if (err instanceof TooManyWaitersError) {
						downstream.send(
							jsonRpcError({
								kind: "RATE_LIMITED",
								message: "Backpressure: too many concurrent requests",
								req: request,
							}),
						);
					} else {
						logger.error((l) => l`[Follow] Error creating follow: ${err}`);
						downstream.send(
							jsonRpcError({
								kind: "INTERNAL_ERROR",
								message: String(err),
								req: request,
							}),
						);
					}
				}
				return;
			}

			if (!selected) {
				downstream.send(
					jsonRpcError({
						kind: "RATE_LIMITED",
						message: "No available follows",
						req: request,
					}),
				);
				return;
			}

			const [followKey, shared] = selected;
			const localId = downstream.getLocalId(shared.upstreamSubId);
			if (!shared.hasLocalSubscription(localId)) {
				await assignToDownstream(shared, followKey);
			}
		},
	};
};

export const chainHead_v1_unfollow: JSONRPCMethodHandler = {
	async handleRequest(upstream, downstream, req) {
		const localId = req.params?.[0];
		if (!localId) return;
		upstream.unsubscribe(localId);
		logger.debug((l) => l`[Unfollow] Local ${localId} unsubscribed`);
		downstream.send({ jsonrpc: "2.0", id: req.id ?? null, result: null });
	},
};

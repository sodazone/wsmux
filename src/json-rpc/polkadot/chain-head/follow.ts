import { getLogger } from "@logtape/logtape";

import {
	createConcurrentCreator,
	TooManyWaitersError,
} from "../../../concurrent/creator";
import { jsonRpcError } from "../../errors";
import type { JSONRPCMethodHandler } from "../../methods";
import type {
	SharedSubscription,
	SharedSubscriptionPool,
} from "../../upstream";
import { chainHeadStateFrom } from "./state";
import { followNotifyTransform } from "./transform";

const logger = getLogger("wsmux.chainhead.follow");
const MAX_FOLLOWS_PER_UPSTREAM = 4;

export const chainHead_v1_follow = (): JSONRPCMethodHandler => {
	const methodKey = "chainHead_v1_follow";

	const getOrCreateFollow = createConcurrentCreator({
		maxWaiting: 5,
		label: methodKey,
	});

	return {
		async handleRequest(upstream, downstream, request) {
			const { managers, pinnedBlocks } = chainHeadStateFrom(upstream);

			const clientId = downstream.clientId;

			logger.debug((l) => l`Follow request from ${clientId}`);

			const chainHeadSubs: SharedSubscriptionPool =
				upstream.subscriptions.getOrCreate(methodKey, {
					maxSubscribers: MAX_FOLLOWS_PER_UPSTREAM,
				});
			const selected = chainHeadSubs.getLeastLoaded();

			async function assignToDownstream(
				followKey: string,
				shared: SharedSubscription,
			) {
				logger.info(
					(l) =>
						l`[${shared.upstreamSubId}] ${followKey} assigning ${clientId} (${shared.subscribersCount()} subs)`,
				);

				const localId = downstream.getLocalId(shared.upstreamSubId);
				downstream.send({
					jsonrpc: "2.0",
					id: request.id ?? null,
					result: localId,
				});
				shared.subscribeLocal(
					localId,
					downstream,
					followNotifyTransform(request),
				);
				upstream.unsubscribers.set(localId, () => {
					pinnedBlocks.unsubscribeLocal(upstream, localId);
					shared.unsubscribeLocal(localId);
				});
			}

			if (chainHeadSubs.shouldCreateMore(selected)) {
				const followIndex = chainHeadSubs.size();
				const followKey = `${methodKey}:${followIndex}`;

				try {
					const shared = await getOrCreateFollow(followKey, async () => {
						logger.info(
							(l) =>
								l`[${followKey}] creating upstream follow (${followIndex + 1}/${MAX_FOLLOWS_PER_UPSTREAM})`,
						);
						const { result: upstreamSubId } = (await upstream.request({
							...request,
							params: [true],
						})) as { result: string };
						if (!upstreamSubId) throw new Error("No upstreamSubId in response");

						return managers.createSharedSubscription(
							followKey,
							upstream,
							upstreamSubId,
							chainHeadSubs,
						);
					});
					await assignToDownstream(followKey, shared);
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
						logger.error(
							"Error creating follow {clientId} {followKey} {error}",
							{
								error: err,
								clientId,
								followKey,
							},
						);

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
				await assignToDownstream(followKey, shared);
			}
		},
	};
};

import { getLogger } from "@logtape/logtape";

import {
	createConcurrentCreator,
	TooManyWaitersError,
} from "../../../util/concurrent-creator";
import { jsonRpcError, RateLimitedError } from "../../errors";
import type { JSONRPCMethodHandler } from "../../methods";
import type {
	SharedSubscription,
	SharedSubscriptionPool,
} from "../../upstream";
import { observeFollow } from "./metrics/follow.metrics";
import { chainHeadStateFrom } from "./state";
import { followNotifyTransform } from "./transform";

const logger = getLogger("wsmux.chainhead.follow");
const MAX_FOLLOWS_PER_UPSTREAM = 2;

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
			const upstreamId = upstream.url;

			logger.debug((l) => l`[${upstreamId}] Follow request from ${clientId}`);

			const chainHeadSubs: SharedSubscriptionPool =
				upstream.subscriptions.getOrCreate(methodKey, {
					maxSubscribers: MAX_FOLLOWS_PER_UPSTREAM,
				});

			async function assignToDownstream(
				followKey: string,
				shared: SharedSubscription,
			) {
				const localId = downstream.getLocalId(shared.upstreamSubId);

				if (shared.hasLocalSubscription(localId)) {
					logger.debug(
						(l) =>
							l`[${upstreamId}:${shared.upstreamSubId}] ${followKey} already subscribed for ${clientId}`,
					);
					return;
				}

				logger.info(
					(l) =>
						l`[${upstreamId}:${shared.upstreamSubId}] ${followKey} assigning ${clientId} (${shared.subscribersCount()} subs)`,
				);

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

			try {
				const [followKey, shared] = await observeFollow(upstreamId, () =>
					getOrCreateFollow(`${upstreamId}:${methodKey}`, async () => {
						const selected = chainHeadSubs.getLeastLoaded();

						if (selected && !chainHeadSubs.shouldCreateMore(selected)) {
							return selected;
						}

						const followIndex = chainHeadSubs.size();
						const followKey = `${methodKey}:${followIndex}`;

						logger.info(
							(l) =>
								l`[${upstreamId}:${followKey}] creating upstream follow (${followIndex + 1}/${MAX_FOLLOWS_PER_UPSTREAM})`,
						);

						const response = await upstream.request({
							...request,
							params: [true],
						});

						if ("error" in response) {
							const { code, message } = response.error as {
								code: number;
								message: string;
							};

							if (code === -32800) {
								logger.warn(
									(l) =>
										l`[${upstreamId}:${followKey}] upstream follow limit reached (${code}: ${message})`,
								);
								throw new RateLimitedError(
									message ?? "Upstream follow limit reached",
									code,
								);
							}

							throw new Error(`Upstream RPC error ${code}: ${message}`);
						}

						const upstreamSubId = response.result;
						if (!upstreamSubId) {
							throw new Error(
								`[${upstreamId}:${followKey}] No upstreamSubId in response`,
							);
						}
						const shared = managers.createSharedSubscription(
							followKey,
							upstream,
							upstreamSubId,
							chainHeadSubs,
						);

						return [followKey, shared];
					}),
				);
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
				} else if (err instanceof RateLimitedError) {
					downstream.send(
						jsonRpcError({
							message: err.message,
							code: err.code,
							req: request,
						}),
					);
				} else {
					logger.error("Error creating follow {clientId} {error}", {
						error: err,
						clientId,
					});

					downstream.send(
						jsonRpcError({
							kind: "INTERNAL_ERROR",
							message: String(err),
							req: request,
						}),
					);
				}
			}

			downstream.send(
				jsonRpcError({
					kind: "RATE_LIMITED",
					message: "No available follows",
					req: request,
				}),
			);
		},
	};
};

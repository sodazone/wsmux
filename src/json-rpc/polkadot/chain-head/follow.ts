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
import { filterOperationEvents } from "./ops/filter";
import { chainHeadStateFrom } from "./state";
import { followNotifyTransform } from "./transform";

const DEFAULT_MAX_FOLLOWS_PER_UPSTREAM = 2;
const DEFAULT_MAX_WAITERS = 5;

const logger = getLogger(["wsmux", "chainhead", "follow"]);

export const chainHead_v1_follow = (): JSONRPCMethodHandler => {
	const methodKey = "chainHead_v1_follow";

	const getOrCreateFollow = createConcurrentCreator({
		maxWaiting: DEFAULT_MAX_WAITERS,
		label: methodKey,
	});

	const followLimitReached = new Set<string>();

	return {
		async handleRequest(upstream, downstream, request) {
			const { managers, pinnedBlocks } = chainHeadStateFrom(upstream);
			const clientId = downstream.clientId;
			const upstreamId = upstream.url;

			logger.debug((l) => l`[${upstreamId}] Follow request from ${clientId}`);

			const maxSubscribers =
				upstream.config.methods?.chainHead_v1_follow?.max_subscribers ??
				DEFAULT_MAX_FOLLOWS_PER_UPSTREAM;

			const chainHeadSubs: SharedSubscriptionPool =
				upstream.subscriptions.getOrCreate(methodKey, {
					maxSubscribers,
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

				shared.subscribeLocal(localId, downstream, {
					filter: filterOperationEvents,
					transform: followNotifyTransform(request),
				});

				upstream.setUnsubscriber(localId, () => {
					pinnedBlocks.unsubscribeLocal(upstream, localId);
					shared.unsubscribeLocal(localId);

					if (shared.subscribersCount() === 0) {
						followLimitReached.delete(upstreamId);
					}
				});
			}

			try {
				const [followKey, shared] = await observeFollow(upstreamId, () =>
					getOrCreateFollow(`${upstreamId}:${methodKey}`, async () => {
						const selected = chainHeadSubs.getLeastLoaded();

						if (followLimitReached.has(upstreamId)) {
							if (selected) return selected;

							logger.debug(
								(l) =>
									l`[${upstreamId}] resetting follow-limit flag (no reusable subscription)`,
							);

							followLimitReached.delete(upstreamId);
						}

						if (selected && !chainHeadSubs.shouldCreateMore(selected)) {
							return selected;
						}

						const followIndex = chainHeadSubs.size();
						const followKey = `${methodKey}:${followIndex}`;

						logger.info(
							(l) =>
								l`[${upstreamId}:${followKey}] creating upstream follow (${followIndex + 1}/${maxSubscribers})`,
						);

						const response = await upstream.request({
							...request,
							params: [true],
						});

						if ("error" in response) {
							const { code, message } = response.error;

							if (code === -32800) {
								followLimitReached.add(upstreamId);

								if (selected) return selected;

								throw new RateLimitedError(
									message ?? "Upstream follow limit reached",
								);
							}

							throw new Error(`Upstream RPC error ${code}: ${message}`);
						}

						followLimitReached.delete(upstreamId);

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
			} catch (err) {
				if (
					err instanceof RateLimitedError ||
					err instanceof TooManyWaitersError
				) {
					downstream.send(
						jsonRpcError({
							message: err.message,
							code: -32800,
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
		},
	};
};

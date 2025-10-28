import { getLogger } from "@logtape/logtape";
import { identity, map, type Observable, type Subscription } from "rxjs";

import type { DownstreamClient } from "../downstream";
import type { JSONRPCNotification } from "../types";
import type { SharedSubscription } from "./types";

const logger = getLogger(["wsmux", "upstream", "shared"]);

export type SharedSubscriptionPoolOptions = {
	maxSubscribers?: number;
	destroy?: () => void;
};

export type SharedSubscriptionPool = ReturnType<
	ReturnType<typeof createSharedSubscriptionGroup>["getOrCreate"]
>;

/**
 * Creates a shared subscription group that manages multiple shared subscription pools.
 */
export function createSharedSubscriptionGroup() {
	const groups = new Map<
		string,
		ReturnType<typeof createSharedSubscriptionPool>
	>();
	return {
		getOrCreate(key: string, opts: SharedSubscriptionPoolOptions = {}) {
			if (!groups.has(key)) {
				const destroy = () => {
					groups.delete(key);
				};
				groups.set(
					key,
					createSharedSubscriptionPool(key, { ...opts, destroy }),
				);
			}
			return groups.get(key)!;
		},
		get(key: string) {
			return groups.get(key);
		},
	};
}

/**
 * Pool of shared subscriptions per method.
 * Tracks least-loaded subscriptions and localId to SharedSubscription mapping for O(1) lookup.
 */
function createSharedSubscriptionPool(
	key: string,
	{
		maxSubscribers: maxSubscriptions = 10,
		destroy,
	}: SharedSubscriptionPoolOptions,
) {
	const subscriptions = new Map<string, SharedSubscription>();
	const localIdIndex = new Map<string, SharedSubscription>();
	let leastLoaded: [string, SharedSubscription] | undefined;

	return {
		id: key,

		getByLocalId(localId: string) {
			return localIdIndex.get(localId);
		},

		getLeastLoaded(): [string, SharedSubscription] | undefined {
			return leastLoaded;
		},

		size() {
			return subscriptions.size;
		},

		shouldCreateMore(selected?: [string, SharedSubscription]) {
			return (
				(!selected || selected[1].subscribersCount() > 0) &&
				subscriptions.size < maxSubscriptions
			);
		},

		set(key: string, sub: SharedSubscription) {
			const origSubscribe = sub.subscribeLocal.bind(sub);
			// TODO: we are recalculating iterating by all subs, potential performance issue
			// consider using a min heap or priority queue to keep track of the least loaded subscription
			sub.subscribeLocal = (localId: string, downstream: DownstreamClient) => {
				origSubscribe(localId, downstream);
				localIdIndex.set(localId, sub);

				let min: [string, SharedSubscription] | undefined;
				for (const entry of subscriptions) {
					if (!min || entry[1].subscribersCount() < min[1].subscribersCount()) {
						min = entry;
					}
				}
				leastLoaded = min;
			};

			const origUnsubscribe = sub.unsubscribeLocal.bind(sub);
			sub.unsubscribeLocal = (localId: string) => {
				origUnsubscribe(localId);
				localIdIndex.delete(localId);

				let min: [string, SharedSubscription] | undefined;
				for (const entry of subscriptions) {
					if (!min || entry[1].subscribersCount() < min[1].subscribersCount()) {
						min = entry;
					}
				}
				leastLoaded = min;
			};

			subscriptions.set(key, sub);

			const count = sub.subscribersCount();
			if (!leastLoaded || count < leastLoaded[1].subscribersCount()) {
				leastLoaded = [key, sub];
			}
		},

		remove(key: string) {
			const wasLeast = leastLoaded?.[0] === key;
			const sub = subscriptions.get(key);
			if (sub) {
				for (const localId of sub.getLocalIds()) {
					localIdIndex.delete(localId);
				}
			}
			subscriptions.delete(key);

			if (wasLeast) {
				let min: [string, SharedSubscription] | undefined;
				for (const entry of subscriptions) {
					if (!min || entry[1].subscribersCount() < min[1].subscribersCount())
						min = entry;
				}
				leastLoaded = min;
			}

			if (subscriptions.size === 0 && destroy) {
				destroy();
			}
		},

		/**
		 * Factory: create a shared subscription for this pool
		 * - source$ is upstream observable
		 * - destroy is cleanup callback
		 */
		createSharedSubscription(
			key: string,
			upstreamSubId: string,
			source$: Observable<JSONRPCNotification>,
			destroy: () => Promise<void> | void,
		): SharedSubscription {
			const shared = _createSharedSubscription(
				upstreamSubId,
				source$,
				async () => {
					await destroy();
					this.remove(key);
				},
			);
			this.set(key, shared);
			return shared;
		},
	};
}

/**
 * Creates a shared subscription that fans out an upstream observable
 * to multiple downstream clients, maintaining independent local subscriptions.
 *
 * - `upstreamSubId` is the identifier of the upstream subscription.
 * - `source$` is the shared upstream observable stream.
 * - `destroy` is a cleanup callback invoked once the upstream is no longer needed.
 *
 * Local vs. upstream subscriptions:
 * - **Upstream subscription**: Only one exists per `upstreamSubId`, shared among all downstreams.
 * - **Local subscription**: Each downstream client gets its own subscription to the shared upstream.
 */
function _createSharedSubscription(
	upstreamSubId: string,
	source$: Observable<JSONRPCNotification>,
	destroy: () => Promise<void> | void,
): SharedSubscription {
	const localSubs = new Map<
		string,
		{
			subscription: Subscription;
			transform?: (notif: JSONRPCNotification) => JSONRPCNotification;
		}
	>();
	return {
		hasLocalSubscription(localId: string) {
			return localSubs.has(localId);
		},

		getLocalIds() {
			return Array.from(localSubs.keys());
		},

		subscribeLocal(
			localId: string,
			downstream: DownstreamClient,
			transform?: (notif: JSONRPCNotification) => JSONRPCNotification,
		) {
			if (this.hasLocalSubscription(localId)) {
				throw Error(`Subscription with ID ${localId} already exists`);
			}

			downstream.addCloseFn(() => {
				this.unsubscribeLocal(localId);
			});

			const subscription = source$.pipe(map(transform ?? identity)).subscribe({
				next: (notif) => {
					try {
						downstream.send({
							...notif,
							params: {
								...notif.params,
								subscription: localId,
							},
						});
					} catch (err) {
						logger.warn("failed to send downstream for {localId}", {
							localId,
							err,
						});
					}
				},
			});

			localSubs.set(localId, {
				subscription,
				transform,
			});
		},

		unsubscribeLocal(localId: string) {
			const sub = localSubs.get(localId);
			if (sub) sub.subscription.unsubscribe();
			localSubs.delete(localId);

			if (localSubs.size === 0) {
				const maybePromise = destroy();
				if (maybePromise instanceof Promise) {
					void maybePromise
						.then(() => {
							logger.info(
								"destroyed shared subscription pool for {upstreamSubId}",
								{
									upstreamSubId,
								},
							);
						})
						.catch(() => {});
				}
			}
		},

		upstreamSubId,

		subscribersCount() {
			return localSubs.size;
		},

		hasSubscribers() {
			return localSubs.size > 0;
		},
	};
}

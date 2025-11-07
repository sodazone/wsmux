import { getLogger } from "@logtape/logtape";
import { identity, map, type Observable, type Subscription } from "rxjs";

import type { DownstreamClient } from "../downstream";
import type { JSONRPCNotification } from "../types";
import type { SharedSubscription } from "./types";

const logger = getLogger(["wsmux", "upstream", "shared"]);

export type SharedSubscriptionPoolOptions = {
	maxSubscribers: number;
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
		getOrCreate(key: string, opts: SharedSubscriptionPoolOptions) {
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
		abort() {
			groups.forEach((pool) => {
				pool.abort();
			});
			groups.clear();
		},
	};
}

/**
 * Pool of shared subscriptions per method.
 * Tracks least-loaded subscriptions and localId to SharedSubscription mapping for O(1) lookup.
 */
function createSharedSubscriptionPool(
	key: string,
	{ maxSubscribers, destroy }: Required<SharedSubscriptionPoolOptions>,
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
				subscriptions.size < maxSubscribers
			);
		},

		acquire(key: string, sub: SharedSubscription) {
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

		release(key: string) {
			const wasLeast = leastLoaded?.[0] === key;
			const sub = subscriptions.get(key);
			if (sub) {
				for (const localId of sub.getLocalIds()) {
					sub.unsubscribeLocal(localId);
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
		 * Creates a shared subscription that fans out an upstream observable
		 * to multiple downstream clients, maintaining independent local subscriptions.
		 *
		 * - `key` is the identifier of the shared subscription.
		 * - `upstreamSubId` is the identifier of the upstream subscription.
		 * - `source$` is the shared upstream observable stream.
		 * - `destroy` is a cleanup callback invoked once the upstream is no longer needed.
		 *
		 * Local vs. upstream subscriptions:
		 * - **Upstream subscription**: Only one exists per `upstreamSubId`, shared among all downstreams.
		 * - **Local subscription**: Each downstream client gets its own subscription to the shared upstream.
		 */
		createSharedSubscription(
			key: string,
			upstreamSubId: string,
			source$: Observable<JSONRPCNotification>,
			destroy: () => Promise<void> | void,
		): SharedSubscription {
			const shared = _createSharedSubscription(upstreamSubId, source$, destroy);
			this.acquire(key, shared);
			return shared;
		},

		abort() {
			subscriptions.values().forEach((sub) => {
				sub.abort();
			});
			subscriptions.clear();
		},
	};
}

function _createSharedSubscription(
	upstreamSubId: string,
	source$: Observable<JSONRPCNotification>,
	destroy: () => Promise<void> | void,
): SharedSubscription {
	let aborted = false;
	let destroyed = false;

	const localSubs = new Map<
		string,
		{
			subscription: Subscription;
			downstream: DownstreamClient;
			transform?: (notif: JSONRPCNotification) => JSONRPCNotification;
		}
	>();

	const doDestroy = async () => {
		if (destroyed) return;
		destroyed = true;
		await destroy();
		logger.info`[${upstreamSubId}] destroyed shared subscription`;
	};

	const abort = () => {
		aborted = true;
		logger.warn`[${upstreamSubId}] Shared subscription aborted (upstream disconnect)`;
	};

	return {
		hasLocalSubscription(localId: string) {
			return localSubs.has(localId);
		},

		getLocalIds() {
			return Array.from(localSubs.keys());
		},

		hasLocalId(localId: string) {
			return localSubs.has(localId);
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
					if (aborted) return;
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
				downstream,
				transform,
			});
		},

		unsubscribeLocal(localId: string) {
			const sub = localSubs.get(localId);

			if (sub) {
				try {
					sub.subscription.unsubscribe();
				} catch {
					// ignore
				}
			}

			localSubs.delete(localId);

			for (const [id, entry] of localSubs) {
				if (entry.downstream.closed) {
					localSubs.delete(id);
				}
			}

			if (!aborted && localSubs.size === 0) {
				void doDestroy();
			}
		},

		upstreamSubId,

		subscribersCount() {
			for (const [localId, entry] of localSubs) {
				if (entry.downstream.closed) {
					logger.info`[${localId}] downstream closed`;
					this.unsubscribeLocal(localId);
				}
			}

			return localSubs.size;
		},

		hasSubscribers() {
			return localSubs.size > 0;
		},

		abort,
	};
}

import { getLogger } from "@logtape/logtape";

import { finalize, type Observable, type Subscription, share } from "rxjs";
import type { DownstreamClient } from "../downstream";
import type { JSONRPCNotification } from "../types";

const logger = getLogger(["wsmux", "upstream", "shared"]);

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
export function createSharedSubscription(
	upstreamSubId: string,
	source$: Observable<JSONRPCNotification>,
	destroy: () => Promise<void> | void,
) {
	const localSubs = new Map<string, Subscription>();
	const shared$ = source$.pipe(
		finalize(async () => {
			if (localSubs.size === 0) {
				return;
			}

			localSubs.forEach((sub) => {
				sub.unsubscribe();
			});
			localSubs.clear();
			try {
				await destroy();
			} catch {
				// TODO: all awaiting replies must be canceled upon closing.
				// logger.error("upstream destroy failed", { error });
			}
		}),
		share(),
	);

	return {
		hasLocalSubscription(localId: string) {
			return localSubs.has(localId);
		},
		subscribeLocal(localId: string, downstream: DownstreamClient) {
			if (this.hasLocalSubscription(localId)) {
				throw Error(`Subscription with ID ${localId} already exists`);
			}

			downstream.addCloseListener(() => {
				this.unsubscribeLocal(localId);
			});

			const sub = shared$.subscribe({
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
			localSubs.set(localId, sub);
		},

		unsubscribeLocal(localId: string) {
			const sub = localSubs.get(localId);
			if (sub) sub.unsubscribe();
			localSubs.delete(localId);
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

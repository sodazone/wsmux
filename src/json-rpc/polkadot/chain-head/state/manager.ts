import { getLogger } from "@logtape/logtape";
import { concat, EMPTY, from, Observable, of, ReplaySubject } from "rxjs";
import {
	catchError,
	filter,
	map,
	share,
	switchMap,
	take,
	timeout,
} from "rxjs/operators";

import type { JSONRPCNotification } from "../../../types";
import type { UpstreamServer } from "../../../upstream";
import { createBlockTracker } from "./blocks";

type Snapshot = {
	initialized?: JSONRPCNotification;
	finalized?: string;
	runtime?: unknown;
	bestBlockChanged?: JSONRPCNotification;
	newBlocks: JSONRPCNotification[];
};

const logger = getLogger(["wsmux", "chainhead", "state"]);

export type StateManager = ReturnType<typeof createStateManager>;

export function createStateManager() {
	const snapshot: Snapshot = { newBlocks: [] };
	const tracker = createBlockTracker(512, update);

	let initialized$ = new ReplaySubject<JSONRPCNotification>(1);

	function resetState() {
		snapshot.initialized = undefined;
		snapshot.finalized = undefined;
		snapshot.runtime = undefined;
		snapshot.bestBlockChanged = undefined;
		snapshot.newBlocks = [];

		tracker.known.clear();
		tracker.flushPending();

		initialized$.complete();
		initialized$ = new ReplaySubject<JSONRPCNotification>(1);
	}

	function snapshotEvents(upstreamSubId: string) {
		if (!snapshot.initialized) {
			logger.warn`[${upstreamSubId}] snapshot not initialized`;
			return [];
		}

		const ordered = [
			...snapshot.newBlocks,
			...(snapshot.bestBlockChanged ? [snapshot.bestBlockChanged] : []),
		];

		const base = [
			{
				...snapshot.initialized,
				params: {
					...snapshot.initialized.params,
					subscription: upstreamSubId,
				},
			},
			...ordered.map((ev) => ({
				...ev,
				params: { ...ev.params, subscription: upstreamSubId },
			})),
		];

		const filtered = base.filter((ev) => {
			const result = ev.params?.result;
			if (!result) return false;
			if (result.event === "initialized") return true;

			const parent = result.parentBlockHash ?? result.parent?.blockHash;
			if (!parent) return true;

			return tracker.known.has(parent);
		});

		return filtered;
	}

	function update(msg: JSONRPCNotification) {
		const event = msg.params?.result?.event;
		const result = msg.params?.result;
		if (!result) {
			logger.warn("Received malformed notification without params.result");
			return;
		}

		switch (event) {
			case "initialized": {
				snapshot.initialized = msg;
				snapshot.finalized = result.finalizedBlockHashes?.at(-1);
				snapshot.runtime = result.finalizedBlockRuntime;

				for (const h of result.finalizedBlockHashes || []) tracker.remember(h);
				initialized$.next(msg);
				tracker.flushPending();
				break;
			}
			case "newBlock": {
				snapshot.newBlocks.push(msg);
				break;
			}
			case "bestBlockChanged": {
				const hash = result.bestBlockHash;
				if (hash) tracker.remember(hash);
				snapshot.bestBlockChanged = msg;
				break;
			}
			case "finalized": {
				const hash = result.finalizedBlockHashes?.at(-1);
				if (!hash) return;
				snapshot.finalized = hash;

				const pruned = result.prunedBlockHashes ?? [];
				if (pruned.length > 0) {
					for (const h of pruned) {
						try {
							tracker.forget(h);
							logger.debug((l) => l`Forgot pruned hash ${h}`);
						} catch (err) {
							logger.warn("tracker.forget failed for {hash}: {err}", {
								hash: h,
								err,
							});
						}
					}

					for (let i = snapshot.newBlocks.length - 1; i >= 0; i--) {
						const ev = snapshot.newBlocks[i];
						if (ev) {
							const evHash = ev.params?.result?.blockHash;
							const evParent = ev.params?.result?.parentBlockHash;
							if (pruned.includes(evHash) || pruned.includes(evParent)) {
								snapshot.newBlocks.splice(i, 1);
							}
						}
					}
				}

				if (snapshot.bestBlockChanged) {
					const best = snapshot.bestBlockChanged.params?.result?.bestBlockHash;
					if (best === hash || pruned.includes(best)) {
						snapshot.bestBlockChanged = undefined;
					}
				}

				if (snapshot.initialized) {
					snapshot.initialized.params.result.finalizedBlockHashes = [
						...result.finalizedBlockHashes,
					];
				}

				// Remove already-applied events up to finalized hash
				const idx = snapshot.newBlocks.findIndex(
					(e) => e.params.result.blockHash === hash,
				);
				if (idx !== -1) {
					snapshot.newBlocks.splice(0, idx + 1);
				}

				tracker.remember(hash);
				tracker.flushPending();
				break;
			}
		}
	}

	function withUpdater(
		upstreamSubId: string,
		upstream: UpstreamServer,
		cleanup: () => void,
	) {
		logger.debug(
			(l) => l`Subscribing to chainHead_v1_followEvent for ${upstreamSubId}`,
		);

		const live$ = handleUpdates(
			upstream.notification$.pipe(
				filter(
					(msg) =>
						msg.method === "chainHead_v1_followEvent" &&
						msg.params?.subscription === upstreamSubId,
				),
				map((msg) => msg as JSONRPCNotification),
			),
			cleanup,
		).pipe(share({ resetOnRefCountZero: true }));

		// needed to keep up the initial snapshot
		const liveSub = live$.subscribe();

		const waitInit$ = initialized$.pipe(
			take(1),
			timeout(10_000),
			catchError(() => of(null)),
		);

		// Return a new observable that on each subscription:
		// 1. waits for initialization
		// 2. emits snapshot events
		// 3. then continues with the shared live stream
		return new Observable<JSONRPCNotification>((subscriber) => {
			const sub = waitInit$
				.pipe(
					switchMap((init) => {
						if (!init) {
							logger.warn("Initialization failed");
							return EMPTY;
						}

						const replayEvents = snapshotEvents(upstreamSubId);
						logger.debug((l) => l`Replaying ${replayEvents.length} events`);
						return concat(from(replayEvents), live$);
					}),
				)
				.subscribe(subscriber);

			return () => {
				sub.unsubscribe();
				if (!liveSub.closed) liveSub.unsubscribe();
			};
		});
	}

	function handleUpdates(
		source$: Observable<JSONRPCNotification>,
		cleanup: () => void,
	) {
		return new Observable<JSONRPCNotification>((subscriber) => {
			const FOLLOW_TIMEOUT_MS = 60_000;

			const subscription = source$
				.pipe(
					timeout({
						each: FOLLOW_TIMEOUT_MS,
						with: () => {
							logger.warn(
								(l) =>
									l`Watchdog timeout: no chainHead events for ${FOLLOW_TIMEOUT_MS}ms. Resetting state.`,
							);
							// trigger a full restart
							resetState();
							cleanup();
							return EMPTY;
						},
					}),
				)
				.subscribe({
					next(value) {
						const result = value.params?.result;
						if (!result) return;

						if (result.event === "stop") {
							logger.info(
								(l) =>
									l`[${value.params.subscription ?? "?"}] updates completed (stop)`,
							);

							subscriber.next(value);
							subscriber.complete();

							resetState();
							cleanup();

							return;
						}

						if (result.event === "newBlock") {
							// ordering handled here
							if (tracker.handleNewBlock(value)) {
								update(value);
								subscriber.next(value);
							}
							return;
						}

						update(value);
						subscriber.next(value);
					},
					error: (err) => subscriber.error(err),
					complete: () => subscriber.complete(),
				});

			return () => subscription.unsubscribe();
		});
	}

	return {
		snapshot,
		withUpdater,
		stats: () => {
			return {
				snapshot: {
					events: snapshot.newBlocks.length,
				},
			};
		},
	};
}

import { getLogger } from "@logtape/logtape";
import { firstValueFrom, Observable, of, ReplaySubject } from "rxjs";
import { catchError, take, timeout } from "rxjs/operators";

import type { JSONRPCNotification } from "../../types";

type Snapshot = {
	initialized?: JSONRPCNotification;
	finalized?: string;
	runtime?: unknown;
	events: JSONRPCNotification[];
};

const logger = getLogger(["wsmux", "chainhead", "state"]);

export type StateManager = ReturnType<typeof createStateManager>;

function createBlockTracker(
	maxKnown = 512,
	onApply: (msg: JSONRPCNotification) => void,
) {
	const known = new Set<string>();
	const pending: JSONRPCNotification[] = [];

	function remember(hash?: string) {
		if (!hash) {
			return;
		}

		known.add(hash);
		if (known.size > maxKnown) {
			let excess = known.size - maxKnown;
			for (const h of known) {
				known.delete(h);
				if (--excess <= 0) break;
			}
		}
	}

	function flushPending() {
		let applied = true;
		while (applied) {
			applied = false;
			for (let i = 0; i < pending.length; ) {
				const msg = pending[i];
				if (!msg) {
					continue;
				}
				const parent = msg?.params?.result?.parentBlockHash;
				if (!parent || known.has(parent)) {
					pending.splice(i, 1);
					onApply(msg);
					applied = true;
				} else {
					i++;
				}
			}
		}
	}

	function handleNewBlock(msg: JSONRPCNotification) {
		const event = msg.params?.result;
		const parent = event?.parentBlockHash;

		if (parent && !known.has(parent)) {
			const blockHash = event?.blockHash;
			if (pending.some((p) => p.params?.result?.blockHash === blockHash)) {
				logger.debug((l) => l`Duplicate pending block ${blockHash}, skipping`);
				return false;
			}

			pending.push(msg);
			logger.debug(
				(l) =>
					l`Queued block ${blockHash} (unknown parent ${parent}), pending=${pending.length}`,
			);
			return false;
		}

		remember(event?.blockHash);
		flushPending();
		return true;
	}

	return {
		remember,
		handleNewBlock,
		flushPending,
		known,
		get stats() {
			return { known: known.size, pending: pending.length };
		},
	};
}

export function createStateManager() {
	const snapshot: Snapshot = { events: [] };
	const initialized$ = new ReplaySubject<JSONRPCNotification>(1);

	const tracker = createBlockTracker(512, update);

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

				logger.debug(
					(l) =>
						l`Initialized chainHead snapshot: finalized=${snapshot.finalized}`,
				);

				for (const h of result.finalizedBlockHashes || []) tracker.remember(h);
				initialized$.next(msg);
				tracker.flushPending();
				break;
			}

			case "newBlock": {
				if (!tracker.handleNewBlock(msg)) return;
				snapshot.events.push(msg);
				logger.debug(
					(l) => l`New block event appended (total=${snapshot.events.length})`,
				);
				break;
			}

			case "finalized": {
				const hash = result.finalizedBlockHashes.at(-1);
				if (!hash) {
					logger.warn("Finalized event without finalizedBlockHashes");
					break;
				}

				snapshot.finalized = hash;
				if (snapshot.initialized) {
					snapshot.initialized.params.result.finalizedBlockHashes = [
						...result.finalizedBlockHashes,
					];
				}

				const idx = snapshot.events.findIndex(
					(e) => e.params.result.blockHash === hash,
				);
				if (idx !== -1) {
					snapshot.events.splice(0, idx + 1);
					logger.debug(
						(l) => l`Pruned ${idx + 1} event(s) up to finalized block ${hash}`,
					);
				}

				tracker.remember(hash);
				tracker.flushPending();
				break;
			}

			default:
				logger.debug((l) => l`Unhandled event type: ${event}`);
		}
	}

	async function replay(
		client: { send: (msg: JSONRPCNotification) => void },
		clientSubId: string,
	) {
		if (!snapshot.initialized) {
			logger.debug("Replay requested before initialization, waiting...");
			await firstValueFrom(
				initialized$.pipe(
					take(1),
					timeout(10_000),
					catchError(() => of(null)),
				),
			);
			logger.debug("Initialization complete, proceeding with replay");
		}

		if (!snapshot.initialized) {
			logger.error("Initialization failed, cannot replay");
			return;
		}

		const init = snapshot.initialized;
		const replayEvents = [...snapshot.events];

		client.send({
			jsonrpc: "2.0",
			method: "chainHead_v1_followEvent",
			params: {
				...init.params,
				result: {
					...init.params.result,
					finalizedBlockHashes: [
						...(init.params.result.finalizedBlockHashes ?? []),
					],
				},
				subscription: clientSubId,
			},
		});

		logger.debug(
			(l) =>
				l`Replayed initialized state and ${snapshot.events.length} pending events to ${clientSubId}`,
		);

		for (const event of replayEvents) {
			client.send({
				jsonrpc: "2.0",
				method: "chainHead_v1_followEvent",
				params: {
					...event.params,
					result: { ...event.params.result },
					subscription: clientSubId,
				},
			});
		}
	}

	return {
		snapshot,
		withUpdate: (source$: Observable<JSONRPCNotification>) =>
			new Observable<JSONRPCNotification>((subscriber) => {
				const subscription = source$.subscribe({
					next(value) {
						const result = value.params?.result;
						if (!result) {
							return;
						}

						update(value);

						const parent = result.parentBlockHash;
						if (!parent || tracker.known.has(parent)) {
							subscriber.next(value);
						} else {
							logger.debug(
								(l) =>
									l`Deferring emission for block ${result.blockHash} (missing parent ${parent})`,
							);
						}

						if (result.event === "stop") {
							logger.info("Received stop event, completing stream");
							subscriber.complete();
						}
					},
					error: (err) => {
						logger.error("Error in upstream chainHead stream", { err });
						subscriber.error(err);
					},
					complete: () => {
						logger.debug("Upstream chainHead stream completed");
						subscriber.complete();
					},
				});

				return () => {
					logger.debug("Unsubscribed from chainHead stream");
					subscription.unsubscribe();
				};
			}),
		replay,
	};
}

export function createStateMap() {
	const stateManagers = new Map<string, StateManager>();
	return {
		getOrCreate(key: string): StateManager {
			if (!stateManagers.has(key)) {
				const stateManager = createStateManager();
				stateManagers.set(key, stateManager);
			}
			return stateManagers.get(key)!;
		},
		remove(key: string) {
			stateManagers.delete(key);
		},
	};
}

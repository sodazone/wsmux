import { getLogger } from "@logtape/logtape";
import { firstValueFrom, Observable, ReplaySubject } from "rxjs";
import { take } from "rxjs/operators";

import type { JSONRPCNotification } from "../../types";

type Snapshot = {
	initialized?: JSONRPCNotification;
	finalized?: string;
	runtime?: unknown;
	events: JSONRPCNotification[];
};

const logger = getLogger(["wsmux", "chainhead", "state"]);

export type StateManager = ReturnType<typeof createStateManager>;

export function createStateManager() {
	const snapshot: Snapshot = {
		events: [],
	};

	const initialized$ = new ReplaySubject<JSONRPCNotification>(1);

	function update(msg: JSONRPCNotification) {
		const event = msg.params?.result;
		if (!event) {
			logger.warn("Received malformed notification without params.result");
			return;
		}

		switch (event.event) {
			case "initialized": {
				snapshot.initialized = msg;
				snapshot.finalized = event.finalizedBlockHashes?.at(-1);
				snapshot.runtime = event.finalizedBlockRuntime;

				logger.info`Initialized chainHead snapshot: finalized=${snapshot.finalized}`;

				initialized$.next(msg);
				break;
			}
			case "newBlock": {
				snapshot.events.push(msg);
				logger.debug(
					(l) => l`New block event appended (total=${snapshot.events.length})`,
				);
				break;
			}
			case "finalized": {
				const hash = event.finalizedBlockHashes.at(-1);
				if (!hash) {
					logger.warn("Finalized event without finalizedBlockHashes");
					break;
				}

				snapshot.finalized = hash;
				if (snapshot.initialized) {
					snapshot.initialized.params.result.finalizedBlockHashes = [hash];
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
				break;
			}
			default: {
				logger.debug((l) => l`Unhandled event type: ${event.event}`);
			}
		}
	}

	async function replay(
		client: { send: (msg: JSONRPCNotification) => void },
		clientSubId: string,
	) {
		if (!snapshot.initialized) {
			logger.info("Replay requested before initialization, waiting...");
			try {
				await firstValueFrom(initialized$.pipe(take(1)));
			} catch (err) {
				logger.error("Failed waiting for initialization", { err });
				throw err;
			}
		}

		const init = snapshot.initialized!;
		client.send({
			jsonrpc: "2.0",
			method: "chainHead_v1_followEvent",
			params: {
				...init.params,
				subscription: clientSubId,
			},
		});

		logger.info(
			(l) =>
				l`Replayed initialized state and ${snapshot.events.length} pending events to ${clientSubId}`,
		);

		for (const event of snapshot.events) {
			client.send({
				jsonrpc: "2.0",
				method: "chainHead_v1_followEvent",
				params: {
					...event.params,
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
						update(value);
						const event = value.params?.result?.event;
						if (event === "stop") {
							logger.info("Received stop event, completing stream");
							subscriber.next(value);
							subscriber.complete();
							return;
						}
						subscriber.next(value);
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

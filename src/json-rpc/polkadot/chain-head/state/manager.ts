import { getLogger } from "@logtape/logtape";
import { concat, from, Observable, of, ReplaySubject } from "rxjs";
import {
	catchError,
	filter,
	map,
	mergeMap,
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
	events: JSONRPCNotification[];
};

const logger = getLogger(["wsmux", "chainhead", "state"]);

export type StateManager = ReturnType<typeof createStateManager>;

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

				for (const h of result.finalizedBlockHashes || []) tracker.remember(h);
				initialized$.next(msg);
				tracker.flushPending();
				break;
			}
			case "newBlock": {
				if (!tracker.handleNewBlock(msg)) return;
				snapshot.events.push(msg);
				break;
			}
			case "finalized": {
				const hash = result.finalizedBlockHashes.at(-1);
				if (!hash) return;
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
				}

				tracker.remember(hash);
				tracker.flushPending();
				break;
			}
		}
	}

	function withUpdater(upstreamSubId: string, upstream: UpstreamServer) {
		const live$ = handleUpdates(
			upstream.message$.pipe(
				filter(
					(msg) =>
						"method" in msg &&
						msg.method === "chainHead_v1_followEvent" &&
						msg.params?.subscription === upstreamSubId,
				),
				map((msg) => msg as JSONRPCNotification),
			),
		);

		live$.subscribe();

		return new Observable<JSONRPCNotification>((subscriber) => {
			const waitInit = initialized$.pipe(
				take(1),
				timeout(10_000),
				catchError(() => of(null)),
			);

			// TODO: recover from error or timeout
			const replay$ = waitInit.pipe(
				map(() => [
					{
						...snapshot.initialized!,
						params: {
							...snapshot.initialized!.params,
							subscription: upstreamSubId,
						},
					},
					...snapshot.events.map((ev) => ({
						...ev,
						params: { ...ev.params, subscription: upstreamSubId },
					})),
				]),
				mergeMap((arr) => from(arr)),
			);

			const combined$ = concat(replay$, live$);

			const sub = combined$.subscribe(subscriber);
			return () => sub.unsubscribe();
		});
	}

	function handleUpdates(source$: Observable<JSONRPCNotification>) {
		return new Observable<JSONRPCNotification>((subscriber) => {
			const subscription = source$.subscribe({
				next(value) {
					const result = value.params?.result;
					if (!result) return;

					update(value);

					const parent = result.parentBlockHash;
					if (!parent || tracker.known.has(parent)) {
						subscriber.next(value);
					}

					if (result.event === "stop") {
						subscriber.complete();
					}
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
	};
}

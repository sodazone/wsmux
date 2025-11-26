import { getLogger } from "@logtape/logtape";
import { filter, type Observable, Subject, take, takeUntil } from "rxjs";

import type { CacheConfig } from "@/config";
import type { JSONRPCMethodHandler } from "@/json-rpc/methods";
import type {
	JSONRPCNotification,
	JSONRPCRequest,
	JSONRPCResponse,
} from "@/json-rpc/types";
import { isSuccess } from "@/json-rpc/util";
import { createLRUCache } from "@/util/cache";
import { forwardChainHeadHandler } from "../forward";
import { chainHeadCacheMetrics } from "../metrics/cache.metrics";
import { isCacheEnabledFor, isStarted } from "./util";

const logger = getLogger(["wsmux", "chainhead", "ops"]);

const FLUSH_CACHE_EVENTS = new Set([
	"operationInaccessible",
	"operationError",
	"operationWaitingForContinue",
]);

type CacheEntry = {
	started: JSONRPCResponse;
	buffer: JSONRPCNotification[];
	notification$?: Observable<JSONRPCNotification>;
	complete: boolean;
};

export const chainHead_v1_operation = (
	config: CacheConfig,
	method: string,
	terminalEvents: string[],
	keyOf: (r: JSONRPCRequest) => string,
): JSONRPCMethodHandler => {
	if (isCacheEnabledFor(config, method)) {
		return forwardWithCacheHandler({
			maxSize: config.methods?.[method]?.maxSize ?? 25,
			keyOf,
			terminalEvents,
		});
	}
	return forwardHandler({
		terminalEvents,
	});
};

const forwardHandler = ({
	terminalEvents,
}: {
	terminalEvents: string[];
}): JSONRPCMethodHandler => {
	const terminus = new Set([...FLUSH_CACHE_EVENTS, ...terminalEvents]);

	return forwardChainHeadHandler({
		afterResponse: (req, res, { upstream, upstreamSubId, downstream }) => {
			if (!isSuccess(res)) {
				logger.error("Error response for {id} {method}: {res}", {
					id: req.id,
					method: req.method,
					res,
				});
				return;
			}

			const localId = req.params[0];

			if (!isStarted(res)) return;

			const { operationId } = res.result;

			const notifications = new Subject<JSONRPCNotification>();

			const isForOp = (msg: JSONRPCNotification) =>
				msg.params?.subscription === upstreamSubId &&
				msg.params?.result?.operationId === operationId;

			const done$ = upstream.notification$.pipe(
				filter((msg) => isForOp(msg) && terminus.has(msg.params!.result.event)),
				take(1),
			);

			const notification$ = upstream.notification$.pipe(
				filter(
					(msg) => isForOp(msg) && !terminus.has(msg.params!.result.event),
				),
				takeUntil(done$),
			);

			const forward = (msg: JSONRPCNotification) => {
				downstream.send({
					...msg,
					params: {
						...msg.params,
						subscription: localId,
					},
				});
			};

			const onUpstream = (msg: JSONRPCNotification) => {
				if (!msg.params) return;
				forward(msg);
			};

			notification$.subscribe({
				next: (msg) => {
					onUpstream(msg);
					notifications.next(msg);
				},
				error: (err) => {
					logger.error("notification$ upstream error {err}", err);
					notifications.error(err);
				},
			});

			done$.subscribe({
				next: onUpstream,
				complete: () => {
					notifications.complete();
				},
			});
		},
	});
};

const forwardWithCacheHandler = ({
	maxSize,
	terminalEvents,
	keyOf,
}: {
	maxSize?: number;
	terminalEvents: string[];
	keyOf: (r: JSONRPCRequest) => string;
}): JSONRPCMethodHandler => {
	const cache = createLRUCache<CacheEntry>(maxSize);
	const terminus = new Set([...FLUSH_CACHE_EVENTS, ...terminalEvents]);

	return forwardChainHeadHandler({
		beforeRequest: (req, { downstream }) => {
			const key = keyOf(req);
			const localId = req.params[0];
			const cached = cache.get(key);

			if (!cached) {
				logger.debug((l) => l`[${key}] cache miss`);
				chainHeadCacheMetrics.misses.labels(req.method).inc();
				return;
			}

			logger.debug((l) => l`[${localId}:${key}] cache hit`);
			chainHeadCacheMetrics.hits.labels(req.method).inc();

			// Send original "started"
			downstream.send({
				...cached.started,
				id: req.id ?? null,
			});

			// Replay buffer
			for (const msg of cached.buffer) {
				chainHeadCacheMetrics.replays.labels(req.method).inc();
				downstream.send({
					jsonrpc: "2.0",
					method: msg.method,
					params: {
						subscription: localId,
						result: { ...msg.params.result },
					},
				});
			}

			// Subscribe if still ongoing
			if (!cached.complete && cached.notification$) {
				logger.debug((l) => l`[${key}] subscribing to notification$`);

				const sub = cached.notification$.subscribe({
					next: (msg) => {
						if (!msg.params) return;

						downstream.send({
							...msg,
							params: {
								...msg.params,
								subscription: localId,
							},
						});
					},
					error: (err) => {
						logger.error("notification$ error {err}", err);
					},
				});

				downstream.addCloseFn(() => {
					try {
						sub.unsubscribe();
					} catch {
						//
					}
				});
			}

			return "STOP";
		},

		afterResponse: (req, res, { upstream, upstreamSubId, downstream }) => {
			const localId = req.params[0];

			if (!isSuccess(res)) {
				logger.error("Error response for {key} {method}: {res}", {
					key: keyOf(req),
					method: req.method,
					res,
				});
				return;
			}

			if (!isStarted(res)) return;

			const key = keyOf(req);
			const { operationId } = res.result;

			const buffer: JSONRPCNotification[] = [];
			const notifications = new Subject<JSONRPCNotification>();

			const isForOp = (msg: JSONRPCNotification) =>
				msg.params?.subscription === upstreamSubId &&
				msg.params?.result?.operationId === operationId;

			const done$ = upstream.notification$.pipe(
				filter((msg) => isForOp(msg) && terminus.has(msg.params!.result.event)),
				take(1),
			);

			const notification$ = upstream.notification$.pipe(
				filter(
					(msg) => isForOp(msg) && !terminus.has(msg.params!.result.event),
				),
				takeUntil(done$),
			);

			const started = res;
			let errored = false;

			cache.set(key, {
				started,
				buffer,
				complete: false,
				notification$: notifications.asObservable(),
			});

			chainHeadCacheMetrics.items.labels(req.method).set(cache.size);

			const forward = (msg: JSONRPCNotification) => {
				downstream.send({
					...msg,
					params: {
						...msg.params,
						subscription: localId,
					},
				});
			};

			const onUpstream = (msg: JSONRPCNotification) => {
				if (!msg.params) return;

				const event = msg.params.result?.event;

				if (FLUSH_CACHE_EVENTS.has(event)) {
					errored = true;
				} else {
					buffer.push(msg);
				}

				forward(msg);
			};

			notification$.subscribe({
				next: (msg) => {
					onUpstream(msg);
					notifications.next(msg);
				},
				error: (err) => {
					logger.error("notification$ upstream error {err}", err);
					notifications.error(err);
				},
			});

			done$.subscribe({
				next: onUpstream,
				complete: () => {
					notifications.complete();

					if (errored) {
						cache.remove(key);
						chainHeadCacheMetrics.errors.labels(req.method).inc();
					} else {
						cache.set(key, {
							started,
							buffer,
							complete: true,
							notification$: undefined,
						});
					}

					chainHeadCacheMetrics.items.labels(req.method).set(cache.size);

					logger.debug(
						(l) =>
							l`${key} ${req.method} operation cached (${buffer.length} notifications)`,
					);
				},
			});
		},
	});
};

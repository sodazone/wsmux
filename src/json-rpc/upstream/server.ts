import { getLogger } from "@logtape/logtape";
import { BehaviorSubject, firstValueFrom, of, Subject } from "rxjs";
import { catchError, filter, map, take, timeout } from "rxjs/operators";

import type {
	JSONRPCError,
	JSONRPCNotification,
	JSONRPCRequest,
	JSONRPCResponse,
} from "../types";
import { createSharedSubscriptionGroup } from "./shared";
import { collectStats } from "./stats";
import type { UpstreamServer, UpstreamServerConfig } from "./types";

const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
const DEFAULT_CONNECTION_TIMEOUT_MS = 10_000;
const DEFAULT_RETRY_DELAY_MS = 2_000;
const DEFAULT_MAX_CONNECTIONS = 200;
const MAX_BACKOFF_MS = 10 * 60 * 1_000;

const logger = getLogger(["wsmux", "upstream", "server"]);

function createMessageSubject() {
	return new Subject<JSONRPCResponse | JSONRPCNotification>();
}

export function createUpstreamServer({
	url,
	supportedMethods,
	methods,
	requestTimeout = DEFAULT_REQUEST_TIMEOUT_MS,
	connectionTimeout = DEFAULT_CONNECTION_TIMEOUT_MS,
	maxConnections = DEFAULT_MAX_CONNECTIONS,
	retryDelay = DEFAULT_RETRY_DELAY_MS,
}: UpstreamServerConfig): UpstreamServer {
	const connection$ = new BehaviorSubject<WebSocket | null>(null);
	const unsubscribers = new Map<string, () => void>();
	const states = new Map<string, unknown>();
	const subscriptions = createSharedSubscriptionGroup();
	const message$ = createMessageSubject();

	let stopped = false;
	let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
	let unhealthy = false;
	let currentBackoff = retryDelay;
	let _connections = 0;
	let _nextId = 0;

	function cleanup() {
		if (reconnectTimer) clearTimeout(reconnectTimer);
		unsubscribers.clear();
		states.clear();
		subscriptions.abort();
		_connections = 0;
	}

	function scheduleReconnect() {
		if (stopped) return;

		const delay = Math.min(currentBackoff, MAX_BACKOFF_MS);
		logger.debug`[${url}] scheduling reconnect in ${delay}ms`;

		reconnectTimer = setTimeout(() => {
			connect();
			currentBackoff = Math.min(currentBackoff * 2, MAX_BACKOFF_MS);
		}, delay);
	}

	async function connect() {
		if (stopped) return;

		logger.info`[${url}] connecting...`;
		const ws = new WebSocket(url);

		ws.onopen = () => {
			logger.info`[${url}] connected ok`;
			unhealthy = false;
			currentBackoff = retryDelay;
			connection$.next(ws);
		};

		ws.onmessage = ({ data }) => handleMessage(data.toString());

		ws.onclose = (event) => {
			connection$.next(null);
			logger.info`[${url}] disconnected (${event.code})`;

			if (stopped) {
				message$.complete();
			} else {
				cleanup();
				scheduleReconnect();
			}
		};

		ws.onerror = (err) => {
			logger.error("[{url}] websocket error", { url, err });
		};
	}

	const connections = {
		inc() {
			_connections++;
		},
		dec() {
			_connections = Math.max(0, _connections - 1);
		},
	};

	const server: UpstreamServer = {
		url,
		nextId() {
			return _nextId++;
		},
		config: {
			methods,
		},
		supportedMethods,
		subscriptions,
		message$,
		notification$: message$.pipe(
			filter((m): m is JSONRPCNotification => "method" in m),
			map((m) => m as JSONRPCNotification),
		),

		isReady: () => {
			const ws = connection$.value;
			return ws?.readyState === WebSocket.OPEN && !unhealthy;
		},

		hasCapacity: () => {
			return _connections < maxConnections;
		},

		connections,

		send(req: JSONRPCRequest | JSONRPCNotification) {
			const ws = connection$.value;
			if (!ws || ws.readyState !== WebSocket.OPEN) {
				logger.warn("send while upstream not connected");
				return;
			}
			ws.send(JSON.stringify(req));
		},

		async request(
			req: JSONRPCRequest,
		): Promise<JSONRPCResponse | JSONRPCError> {
			const upstreamId = server.nextId();
			const ws = connection$.value;
			if (!ws || ws.readyState !== WebSocket.OPEN) {
				throw new Error("Upstream not connected");
			}

			const resp$ = message$.pipe(
				filter(
					(m): m is JSONRPCResponse | JSONRPCError =>
						"id" in m && m.id === upstreamId,
				),
				take(1),
				map(
					(response) =>
						({ ...response, id: req.id }) as JSONRPCResponse | JSONRPCError,
				),
				timeout(requestTimeout),
				catchError((err) => {
					logger.warn(
						"[{url}] Request {method} stream aborted or timed out {err}",
						{ url, err, method: req.method },
					);

					unhealthy = true;
					connection$.value?.close(4001, "Request timeout");

					return of({
						jsonrpc: "2.0",
						id: req.id,
						error: {
							code: -32000,
							message: `Upstream timeout for method ${req.method}`,
						},
					} as JSONRPCError);
				}),
			);

			ws.send(JSON.stringify({ ...req, id: upstreamId }));

			try {
				return await firstValueFrom(resp$);
			} catch (error) {
				if (stopped) {
					return {
						jsonrpc: "2.0",
						id: null,
						error: { code: -32000, message: "Upstream stopped" },
					};
				}
				throw error;
			}
		},

		unsubscribe(localId: string) {
			logger.info(`[${localId}] unsubscribing (${unsubscribers.size})`);

			try {
				const unsub = unsubscribers.get(localId);
				if (unsub) unsub();
			} catch (error) {
				logger.error(`[${localId}] error calling unsubscribe: {error}`, {
					error,
				});
			}

			unsubscribers.delete(localId);
		},

		async stop() {
			logger.info`stopping upstream ${url}`;

			stopped = true;
			cleanup();
			connection$.value?.close(1000, "Stopped");
		},

		connect,

		async waitForReady(timeoutMs = connectionTimeout) {
			if (this.isReady()) return Promise.resolve();

			return firstValueFrom(
				connection$.pipe(
					filter((ws) => ws?.readyState === WebSocket.OPEN),
					take(1),
					timeout(timeoutMs),
				),
			);
		},

		getOrCreateState<T>(id: string, factory: () => T) {
			if (!states.has(id)) {
				states.set(id, factory());
			}
			return states.get(id)! as T;
		},

		setUnsubscriber(localId: string, unsub: () => void) {
			logger.info(`[${localId}] add unsubscriber (${unsubscribers.size})`);
			unsubscribers.set(localId, unsub);
		},

		removeUnsubscriber(localId: string) {
			logger.info(`[${localId}] remove unsubscriber (${unsubscribers.size})`);
			unsubscribers.delete(localId);
		},

		stats() {
			return {
				states: collectStats(states),
				unsubscribers: unsubscribers.size,
				messageSubscribers: (message$ as any).observers?.length ?? 0,
				connections: _connections,
				subscriptions: subscriptions.stats(),
			};
		},
	};

	function handleMessage(data: string) {
		try {
			const msg = JSON.parse(data) as JSONRPCResponse | JSONRPCNotification;
			message$.next(msg);
		} catch (err) {
			logger.error("[{url}] JSON parse error", { url, err });
		}
	}

	return server;
}

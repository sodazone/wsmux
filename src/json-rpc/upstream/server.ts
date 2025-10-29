import { getLogger } from "@logtape/logtape";
import { BehaviorSubject, firstValueFrom, of, Subject } from "rxjs";
import { catchError, filter, map, take, timeout } from "rxjs/operators";

import type {
	JSONRPCNotification,
	JSONRPCRequest,
	JSONRPCResponse,
} from "../types";
import { createSharedSubscriptionGroup } from "./shared";
import type { UpstreamServer, UpstreamServerConfig } from "./types";

const logger = getLogger(["wsmux", "upstream"]);

function createMessageSubject() {
	return new Subject<JSONRPCResponse | JSONRPCNotification>();
}

export function createUpstreamServer({
	url,
	supportedMethods,
	retryDelay = 2000,
}: UpstreamServerConfig): UpstreamServer {
	const connection$ = new BehaviorSubject<WebSocket | null>(null);
	const unsubscribers = new Map<string, () => void>();
	const states = new Map<string, unknown>();
	const subscriptions = createSharedSubscriptionGroup();
	const message$ = createMessageSubject();

	let stopped = false;
	let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

	function cleanup() {
		if (reconnectTimer) clearTimeout(reconnectTimer);
		unsubscribers.clear();
		states.clear();
		subscriptions.abort();
	}

	async function connect() {
		if (stopped) return;

		logger.info`[${url}] connecting...`;
		const ws = new WebSocket(url);

		ws.onopen = () => {
			logger.info`[${url}] connected ok`;
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

				logger.debug`[${url}] scheduling reconnect in ${retryDelay}ms`;
				reconnectTimer = setTimeout(connect, retryDelay);
			}
		};

		ws.onerror = (err) => {
			logger.error("[{url}] websocket error", { url, err });
		};
	}

	const server: UpstreamServer = {
		url,
		nextId: 0,
		supportedMethods,
		subscriptions,
		unsubscribers,
		message$,

		isReady: () => {
			const ws = connection$.value;
			return ws?.readyState === WebSocket.OPEN;
		},

		send(req: JSONRPCRequest) {
			const ws = connection$.value;
			if (!ws || ws.readyState !== WebSocket.OPEN) {
				logger.warn("send while upstream not connected");
				return;
			}
			ws.send(JSON.stringify(req));
		},

		async request(req: JSONRPCRequest): Promise<JSONRPCResponse> {
			const upstreamId = server.nextId++;
			const ws = connection$.value;
			if (!ws || ws.readyState !== WebSocket.OPEN) {
				throw new Error("Upstream not connected");
			}

			const resp$ = message$.pipe(
				filter((m): m is JSONRPCResponse => "id" in m && m.id === upstreamId),
				take(1),
				map((response) => ({ ...response, id: req.id }) as JSONRPCResponse),
				timeout(10_000),
				catchError((err) => {
					logger.warn(
						"[{url}] Request {method} stream aborted or timed out {err}",
						{ url, err, method: req.method },
					);
					return of(null);
				}),
			);

			ws.send(JSON.stringify({ ...req, id: upstreamId }));

			const response = await firstValueFrom(resp$);

			if (!response) {
				throw new Error("No response from upstream (disconnected or timeout)");
			}

			return response;
		},

		unsubscribe(localId: string) {
			const unsub = unsubscribers.get(localId);
			if (unsub) unsub();
			unsubscribers.delete(localId);
		},

		async stop() {
			logger.info`stopping upstream ${url}`;

			stopped = true;
			cleanup();
			connection$.value?.close(1000, "Stopped");
		},

		connect,

		getOrCreateState<T>(id: string, factory: () => T) {
			if (!states.has(id)) {
				states.set(id, factory());
			}
			return states.get(id)! as T;
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

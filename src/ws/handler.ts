import { getLogger } from "@logtape/logtape";
import type { ServerWebSocket, WebSocketHandler } from "bun";

type WebSocketEventType =
	| "open"
	| "message"
	| "close"
	| "drain"
	| "ping"
	| "pong"
	| "error";

type WebSocketContextMap<Data> = {
	open: { ws: ServerWebSocket<Data> };
	message: { ws: ServerWebSocket<Data>; message: string | Uint8Array };
	close: { ws: ServerWebSocket<Data>; code: number; reason: string };
	drain: { ws: ServerWebSocket<Data> };
	ping: { ws: ServerWebSocket<Data>; data: Uint8Array };
	pong: { ws: ServerWebSocket<Data>; data: Uint8Array };
	error: {
		ws: ServerWebSocket<Data>;
		error: unknown;
		event: WebSocketEventType;
		ctx: unknown;
	};
};

export type WebSocketContext<
	Data,
	K extends WebSocketEventType = WebSocketEventType,
> = WebSocketContextMap<Data>[K];

export type WebSocketMiddleware<Data = unknown> = {
	[K in WebSocketEventType]?: (
		ctx: WebSocketContext<Data, K>,
		next: () => Promise<void>,
	) => Promise<void>;
};

export type WebSocketHandlerOptions<Data> = Pick<
	WebSocketHandler<Data>,
	| "backpressureLimit"
	| "maxPayloadLength"
	| "publishToSelf"
	| "sendPings"
	| "closeOnBackpressureLimit"
	| "perMessageDeflate"
	| "idleTimeout"
> & {
	middlewares?: WebSocketMiddleware<Data>[];
};

const logger = getLogger(["wsmux", "ws-handler"]);

function compose<Data, K extends WebSocketEventType>(
	middlewares: WebSocketMiddleware<Data>[],
	event: K,
) {
	const stack = middlewares
		.map((mw) => mw[event])
		.filter((fn): fn is NonNullable<typeof fn> => !!fn);

	async function handleErrorMiddlewares(
		ctx: WebSocketContext<Data, K>,
		err: unknown,
	) {
		const errorCtx: WebSocketContext<Data, "error"> = {
			ws: ctx.ws,
			error: err,
			event,
			ctx,
		};
		const errorStack = middlewares
			.map((mw) => mw.error)
			.filter((fn): fn is NonNullable<typeof fn> => !!fn);

		for (const errFn of errorStack) {
			try {
				await errFn(errorCtx, async () => {});
			} catch (innerErr) {
				logger.error("Error in error middleware", { innerErr });
			}
		}

		if (errorStack.length === 0) {
			logger.error("WebSocket error on {event}", { event, err });
		}
	}

	return async (
		ctx: WebSocketContext<Data, K>,
		terminal?: () => Promise<void>,
	) => {
		const dispatch = async (index: number) => {
			const fn = stack[index];

			if (!fn) {
				if (terminal) {
					try {
						await terminal();
					} catch (err) {
						await handleErrorMiddlewares(ctx, err);
					}
				}
				return;
			}

			try {
				await fn(ctx, async () => {
					try {
						await dispatch(index + 1);
					} catch (err) {
						await handleErrorMiddlewares(ctx, err);
					}
				});
			} catch (err) {
				await handleErrorMiddlewares(ctx, err);
			}
		};

		await dispatch(0);
	};
}

export function createWebSocketHandler<Data = unknown>(
	options: WebSocketHandlerOptions<Data> = {},
): WebSocketHandler<Data> {
	const { middlewares = [], ...rest } = options;

	const stacks = {
		open: compose(middlewares, "open"),
		message: compose(middlewares, "message"),
		close: compose(middlewares, "close"),
		drain: compose(middlewares, "drain"),
		ping: compose(middlewares, "ping"),
		pong: compose(middlewares, "pong"),
	};

	return {
		data: {} as Data,
		...rest,

		async open(ws) {
			await stacks.open({ ws });
		},

		async message(ws, message) {
			await stacks.message({ ws, message });
		},

		async close(ws, code, reason) {
			await stacks.close({ ws, code, reason });
		},

		async drain(ws) {
			await stacks.drain({ ws });
		},

		async ping(ws, data) {
			await stacks.ping({ ws, data });
		},

		async pong(ws, data) {
			await stacks.pong({ ws, data });
		},
	};
}

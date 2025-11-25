import { getLogger } from "@logtape/logtape";
import type { JsonRpcLimits } from "../config/types";
import { createDownstream } from "./downstream";
import { jsonRpcError } from "./errors";
import { handleRPCMethod, type JSONRPCMethodHandler } from "./methods";
import type { JSONRPCMiddleware } from "./types";
import type { UpstreamRegistry } from "./upstream";
import { isJsonRpcRequest } from "./util";

const DEFAULT_MAX_PENDING_PER_CLIENT = 50;
const DEFAULT_MAX_GLOBAL_PENDING = 500;
const DEFAULT_REQUESTS_PER_SECOND = 100;

const logger = getLogger(["wsmux", "json-rpc"]);

export function jsonRpcMiddleware(
	registry: UpstreamRegistry,
	methodHandlers: Record<string, JSONRPCMethodHandler>,
	config: JsonRpcLimits = {},
): JSONRPCMiddleware {
	const {
		max_pending_requests = DEFAULT_MAX_GLOBAL_PENDING,
		max_pending_requests_per_connection = DEFAULT_MAX_PENDING_PER_CLIENT,
		max_requests_per_second = DEFAULT_REQUESTS_PER_SECOND,
	} = config;

	let globalPending = 0;

	return {
		open: async (ctx, next) => {
			ctx.ws.data.client = createDownstream(ctx.ws, {
				capacity: max_requests_per_second,
				windowMs: 1_000,
			});
			await next();
		},

		close: async (ctx, next) => {
			const client = ctx.ws.data.client;
			if (client) {
				if (client.clientId !== undefined) {
					registry.disconnect(client.clientId);
				}
				client.onClose?.();
			}
			await next();
		},

		message: async (ctx, next) => {
			const raw = ctx.message?.toString();
			if (!raw) return next();

			const client = ctx.ws.data.client;
			if (!client) return;

			let req: unknown;
			try {
				req = JSON.parse(raw);
			} catch {
				client.send(jsonRpcError({ kind: "PARSE_ERROR" }));
				return;
			}

			if (!isJsonRpcRequest(req)) {
				client.send(jsonRpcError({ kind: "INVALID_REQUEST" }));
				return;
			}

			if (
				client.pendingRequests >= max_pending_requests_per_connection ||
				globalPending >= max_pending_requests
			) {
				ctx.ws.terminate();
				return;
			}

			try {
				client.startRequest();
			} catch {
				ctx.ws.terminate();
				return;
			}

			globalPending++;

			try {
				const apex = registry.resolveApexMethod(req.method);
				if (apex) {
					await apex.handleRequest(client, req);
				} else {
					const upstream = registry.resolveUpstream(ctx.ws.data, req.method);
					if (!upstream) {
						client.close(1013, "JSON-RPC Upstream Unavailable");
						return;
					}

					const { server } = upstream;
					if (!server.isReady()) {
						await server.waitForReady();
					}

					await handleRPCMethod(client, server, req, methodHandlers);
				}
			} catch (err) {
				logger.error("Error handling JSON-RPC message: {err}", { err });

				client.send(
					jsonRpcError({
						kind: "INTERNAL_ERROR",
						req,
						message: String(err),
					}),
				);
			} finally {
				client.endRequest();
				globalPending--;
			}

			await next();
		},
	};
}

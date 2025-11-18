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
			ctx.ws.data.client = createDownstream(ctx.ws);
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

			if (client.requestsInPeriod(1_000) >= max_requests_per_second) {
				ctx.ws.close(1013, "JSON-RPC Rate limit exceeded");
				return;
			}

			if (
				client.pendingRequests >= max_pending_requests_per_connection ||
				globalPending >= max_pending_requests
			) {
				ctx.ws.close(1013, "JSON-RPC Too many concurrent requests");
				return;
			}

			client.startRequest();
			globalPending++;

			try {
				const upstream = registry.resolveUpstream(ctx.ws.data, req.method);
				if (!upstream) {
					client.close(1013, "JSON-RPC Upstream Unavailable");
					return;
				}

				await handleRPCMethod(client, upstream, req, methodHandlers);
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

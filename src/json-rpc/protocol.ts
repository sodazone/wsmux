import { createDownstream } from "./downstream";
import { jsonRpcError } from "./errors";
import { handleRPCMethod, type JSONRPCMethodHandler } from "./methods";
import type { JSONRPCMiddleware } from "./types";
import type { UpstreamRegistry } from "./upstream";
import { isJsonRpcRequest } from "./util";

const MAX_PENDING_PER_CLIENT = 50;
const MAX_GLOBAL_PENDING = 500;
const REQUESTS_PER_SECOND = 20;

let globalPending = 0;

export const jsonRpcMiddleware = (
	registry: UpstreamRegistry,
	methodHandlers: Record<string, JSONRPCMethodHandler>,
): JSONRPCMiddleware => ({
	open: async (ctx, next) => {
		ctx.ws.data.client = createDownstream(ctx.ws);
		await next();
	},

	close: async (ctx, next) => {
		ctx.ws.data.client?.close?.();
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

		if (client.requestsInPeriod(1_000) >= REQUESTS_PER_SECOND) {
			client.send(
				jsonRpcError({
					kind: "RATE_LIMITED",
					message: "Rate limit exceeded (wsmux)",
					req,
				}),
			);
			return;
		}

		if (
			client.pendingRequests >= MAX_PENDING_PER_CLIENT ||
			globalPending >= MAX_GLOBAL_PENDING
		) {
			client.send(
				jsonRpcError({
					kind: "RATE_LIMITED",
					message: `Too many concurrent requests (${client.pendingRequests} ${globalPending})`,
					req,
				}),
			);
			return;
		}

		client.startRequest();
		globalPending++;

		try {
			const upstream = registry.resolveUpstream(ctx.ws.data, req.method);
			if (!upstream) {
				client.send(jsonRpcError({ kind: "METHOD_NOT_FOUND", req }));
				return;
			}

			await handleRPCMethod(client, upstream, req, methodHandlers);
		} catch (err) {
			console.error("Error handling JSON-RPC message:", err);
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
});

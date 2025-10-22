import { createDownstream } from "./downstream";
import { jsonRpcError } from "./errors";
import { handleRPCMethod, type JSONRPCMethodHandler } from "./methods";
import type { JSONRPCMiddleware } from "./types";
import type { UpstreamRegistry } from "./upstream";
import { isJsonRpcRequest } from "./util";

export const jsonRpcMiddleware = (
	registry: UpstreamRegistry,
	methodHandlers: Record<string, JSONRPCMethodHandler>,
): JSONRPCMiddleware => ({
	open: async (ctx, next) => {
		ctx.ws.data.client = createDownstream(ctx.ws);
		await next();
	},

	close: async (ctx, next) => {
		ctx.ws.data.client?.close();
		await next();
	},

	message: async (ctx, next) => {
		const msg = ctx.message?.toString();
		if (!msg) return next();

		const client = ctx.ws.data.client;
		if (!client) return;

		let req: unknown;
		try {
			req = JSON.parse(msg);
		} catch (_err) {
			client.send(
				jsonRpcError({
					kind: "PARSE_ERROR",
				}),
			);
			return;
		}

		if (!isJsonRpcRequest(req)) {
			client.send(
				jsonRpcError({
					kind: "INVALID_REQUEST",
				}),
			);
			return;
		}

		try {
			const upstream = registry.resolveUpstream(ctx.ws.data, req.method);
			if (!upstream) {
				client.send(
					jsonRpcError({
						kind: "METHOD_NOT_FOUND",
						req,
					}),
				);
				return;
			}

			if (ctx.ws.data.client) {
				handleRPCMethod(ctx.ws.data.client, upstream, req, methodHandlers);
			}
		} catch (err) {
			console.error("Error handling JSON-RPC message:", err);
			client.send(
				jsonRpcError({
					kind: "INTERNAL_ERROR",
					req,
					message: String(err),
				}),
			);
			return;
		}

		await next();
	},
});

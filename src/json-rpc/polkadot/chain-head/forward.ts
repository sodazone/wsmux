import { jsonRpcError } from "../../errors";
import type { JSONRPCMethodHandler } from "../../methods";

export const forwardChainHeadHandler: JSONRPCMethodHandler = {
	handleRequest: async (upstream, downstream, req) => {
		const localId = req.params?.[0];
		if (!localId) {
			downstream.send(
				jsonRpcError({
					req,
					kind: "INVALID_PARAMS",
				}),
			);
			return;
		}

		const shared = upstream.subscriptions.get("chainHead_v1_follow");
		if (!shared) {
			downstream.send(
				jsonRpcError({
					kind: "INTERNAL_ERROR",
					message: "No active chainHead_v1_follow",
				}),
			);
			return;
		}

		const upstreamReq = {
			...req,
			params: [
				shared.upstreamSubId,
				...(Array.isArray(req.params) ? req.params.slice(1) : []),
			],
		};

		try {
			const response = await upstream.request(upstreamReq);
			downstream.send(response);
		} catch (err) {
			downstream.send(
				jsonRpcError({
					kind: "INTERNAL_ERROR",
					message: String(err),
				}),
			);
		}
	},
};

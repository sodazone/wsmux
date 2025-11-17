import type { DownstreamClient } from "../../downstream";
import { jsonRpcError } from "../../errors";
import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCRequest, JSONRPCResponse } from "../../types";
import type { UpstreamServer } from "../../upstream";

export type ForwardContext = {
	upstreamSubId: string;
	upstream: UpstreamServer;
	downstream: DownstreamClient;
};

export const forwardChainHeadHandler = ({
	beforeRequest,
	afterResponse,
}: {
	beforeRequest?: (
		req: JSONRPCRequest,
		ctx: ForwardContext,
	) => JSONRPCResponse | "STOP" | undefined;
	afterResponse?: (
		req: JSONRPCRequest,
		res: JSONRPCResponse,
		ctx: ForwardContext,
	) => void;
}): JSONRPCMethodHandler => {
	return {
		async handleRequest(upstream, downstream, req) {
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

			const shared = upstream.subscriptions
				.get("chainHead_v1_follow")
				?.getByLocalId(localId);

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

			if (beforeRequest) {
				const res = beforeRequest(upstreamReq, {
					upstreamSubId: shared.upstreamSubId,
					upstream,
					downstream,
				});

				if (res === "STOP") {
					return;
				}

				if (res) {
					downstream.send(res);
					return;
				}
			}

			try {
				const response = await upstream.request(upstreamReq);
				afterResponse?.(req, response, {
					upstreamSubId: shared.upstreamSubId,
					upstream,
					downstream,
				});
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
};

export const chainHead_v1_forward: JSONRPCMethodHandler =
	forwardChainHeadHandler({});

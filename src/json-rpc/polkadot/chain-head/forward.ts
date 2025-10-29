import { createCache } from "../../../util/cache";
import { jsonRpcError } from "../../errors";
import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCRequest, JSONRPCResponse } from "../../types";

const baseForwardChainHeadHandler = ({
	beforeRequest,
	afterResponse,
}: {
	beforeRequest?: (req: JSONRPCRequest) => JSONRPCResponse | undefined;
	afterResponse?: (req: JSONRPCRequest, res: JSONRPCResponse) => void;
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

			if (beforeRequest) {
				const res = beforeRequest(req);
				if (res) {
					downstream.send(res);
					return;
				}
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

				afterResponse?.(req, response);

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

export const forwardChainHeadHandler: JSONRPCMethodHandler =
	baseForwardChainHeadHandler({});

export const cachingForwardChainHeadHandler = (): JSONRPCMethodHandler => {
	const cache = createCache<JSONRPCResponse>(100);

	return baseForwardChainHeadHandler({
		beforeRequest: (req) => {
			return cache.get(req.params[1]);
		},
		afterResponse: (req, res) => {
			cache.set(req.params[1], res);
		},
	});
};

import { createCache } from "../../../util/cache";
import { jsonRpcError } from "../../errors";
import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCRequest, JSONRPCResponse } from "../../types";
import { isSuccess } from "../../util";

const forwardChainHeadHandler = ({
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

			const upstreamReq = {
				...req,
				params: [
					shared.upstreamSubId,
					...(Array.isArray(req.params) ? req.params.slice(1) : []),
				],
			};

			if (beforeRequest) {
				const res = beforeRequest(upstreamReq);
				if (res) {
					downstream.send(res);
					return;
				}
			}

			try {
				const response = await upstream.request(upstreamReq);

				if (isSuccess(response)) {
					afterResponse?.(req, response);
				}

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

export const chainHead_v1_header = (maxSize = 100): JSONRPCMethodHandler => {
	const cache = createCache<JSONRPCResponse>(maxSize);
	const keyOf: (req: JSONRPCRequest) => string = (req) => req.params[1];

	return forwardChainHeadHandler({
		beforeRequest: (req) => {
			const res = cache.get(keyOf(req));
			if (res) {
				return {
					...res,
					id: req.id,
				} as JSONRPCResponse;
			}
		},
		afterResponse: (req, res) => {
			if (res.result != null && typeof res.result === "string") {
				cache.set(keyOf(req), res);
			}
		},
	});
};

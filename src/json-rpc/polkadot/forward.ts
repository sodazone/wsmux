import type { CacheConfig } from "@/config";
import { createLRUCache } from "@/util/cache";
import type { JSONRPCMethodHandler } from "../methods";
import type { JSONRPCRequest, JSONRPCResponse } from "../types";
import { isSuccess } from "../util";
import { isCacheEnabledFor } from "./chain-head/ops/util";

export const forwardRequest: JSONRPCMethodHandler = {
	handleRequest: async (upstream, downstream, req) => {
		const response = await upstream.request(req);
		downstream.send(response);
	},
};

export const forwardRequestWithCache = (
	method: string,
	config: CacheConfig,
	keyOf: (req: JSONRPCRequest) => string,
): JSONRPCMethodHandler => {
	if (isCacheEnabledFor(config, method)) {
		const cache = createLRUCache<JSONRPCResponse>(
			config.methods?.[method]?.maxSize ?? 100,
		);
		return {
			handleRequest: async (upstream, downstream, req) => {
				const key = keyOf(req);
				const cached = cache.get(key);
				if (cached) {
					downstream.send({
						jsonrpc: "2.0",
						id: req.id ?? null,
						result: cached,
					});
				} else {
					const response = await upstream.request(req);
					if (isSuccess(response) && response.result != null) {
						cache.set(key, response.result);
					}
					downstream.send(response);
				}
			},
		};
	}
	return forwardRequest;
};

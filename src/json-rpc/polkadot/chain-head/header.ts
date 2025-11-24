import { getLogger } from "@logtape/logtape";
import type { CacheConfig } from "@/config";
import { createLRUCache } from "@/util/cache";
import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCRequest, JSONRPCResponse } from "../../types";
import { isSuccess } from "../../util";
import { chainHead_v1_forward, forwardChainHeadHandler } from "./forward";
import { chainHeadCacheMetrics } from "./metrics/cache.metrics";
import { isCacheEnabledFor } from "./ops/util";

const logger = getLogger(["wsmux", "chainhead", "header"]);

export const chainHead_v1_header = (
	config: CacheConfig,
): JSONRPCMethodHandler => {
	if (isCacheEnabledFor(config, "chainHead_v1_header")) {
		const cache = createLRUCache<JSONRPCResponse>(
			config.methods?.chainHead_v1_header?.maxSize ?? 100,
		);
		const keyOf: (req: JSONRPCRequest) => string = (req) => req.params[1];

		return forwardChainHeadHandler({
			beforeRequest: (req) => {
				const res = cache.get(keyOf(req));
				if (res) {
					chainHeadCacheMetrics.hits.labels(req.method).inc();
					return {
						...res,
						id: req.id,
					} as JSONRPCResponse;
				}
				chainHeadCacheMetrics.misses.labels(req.method).inc();
			},
			afterResponse: (req, res) => {
				if (isSuccess(res)) {
					if (res.result != null && typeof res.result === "string") {
						cache.set(keyOf(req), res);

						chainHeadCacheMetrics.items.labels(req.method).set(cache.size);
					} else {
						logger.debug(
							(l) =>
								l`Empty response for ${keyOf(req)} [req=${JSON.stringify(req)}, res=${JSON.stringify(res)}]`,
						);
					}
				} else {
					logger.error("Error response for {key} {method}: {res}", {
						key: keyOf(req),
						method: req.method,
						res,
					});
				}
			},
		});
	} else {
		return chainHead_v1_forward;
	}
};

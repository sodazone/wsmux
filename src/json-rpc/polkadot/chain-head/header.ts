import { getLogger } from "@logtape/logtape";
import type { MethodCacheConfig } from "@/config";
import { createCache } from "@/util/cache";
import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCRequest, JSONRPCResponse } from "../../types";
import { isSuccess } from "../../util";
import { forwardChainHeadHandler } from "./forward";

const logger = getLogger(["wsmux", "chainhead", "header"]);

export const chainHead_v1_header = ({
	maxSize = 25,
}: MethodCacheConfig = {}): JSONRPCMethodHandler => {
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
			if (isSuccess(res)) {
				if (res.result != null && typeof res.result === "string") {
					cache.set(keyOf(req), res);
				} else {
					logger.debug(
						(l) =>
							l`Empty response for ${keyOf(req)} [req=${JSON.stringify(req)}, res=${JSON.stringify(res)}]`,
					);
				}
			} else {
				logger.error(`Error response for ${keyOf(req)}`);
			}
		},
	});
};

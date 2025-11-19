import type { CacheConfig } from "@/config";
import type { JSONRPCMethodHandler } from "../../methods";
import { chainHead_v1_operation } from "./ops/handler";

export const chainHead_v1_body = (
	config: CacheConfig,
): JSONRPCMethodHandler => {
	return chainHead_v1_operation(
		config,
		"chainHead_v1_body",
		["operationBodyDone"],
		({ params }) => params[1],
	);
};

import type { CacheConfig } from "@/config";
import type { JSONRPCMethodHandler } from "../../methods";
import { chainHead_v1_operation } from "./ops/handler";

export const chainHead_v1_call = (
	config: CacheConfig,
): JSONRPCMethodHandler => {
	return chainHead_v1_operation(
		config,
		"chainHead_v1_call",
		["operationCallDone"],
		({ params }) => JSON.stringify([params[1], params[2], params[3]]),
	);
};

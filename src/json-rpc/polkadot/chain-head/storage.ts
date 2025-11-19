import type { CacheConfig } from "@/config";
import type { JSONRPCMethodHandler } from "../../methods";
import { chainHead_v1_operation } from "./ops/handler";

export const chainHead_v1_storage = (
	config: CacheConfig,
): JSONRPCMethodHandler => {
	return chainHead_v1_operation(
		config,
		"chainHead_v1_storage",
		["operationStorageDone"],
		({ params }) => JSON.stringify([params[1], params[2], params[3]]),
	);
};

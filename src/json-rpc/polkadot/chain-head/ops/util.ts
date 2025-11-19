import type { CacheConfig } from "@/config";
import type { JSONRPCResponse } from "@/json-rpc/types";

const OP_METHODS = new Set([
	"chainHead_v1_storage",
	"chainHead_v1_body",
	"chainHead_v1_call",
]);

export function isStarted(res: JSONRPCResponse) {
	return res.result?.result === "started";
}

export function isCacheEnabled(config: CacheConfig) {
	return (
		config.enabled &&
		Object.entries(config.methods).some(
			(e) => OP_METHODS.has(e[0]) && e[1].enabled,
		)
	);
}

export function isCacheEnabledFor(config: CacheConfig, methodName: string) {
	return (
		config.enabled &&
		config.methods &&
		config.methods[methodName] &&
		config.methods[methodName].enabled
	);
}

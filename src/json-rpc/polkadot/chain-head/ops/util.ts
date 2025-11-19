import type { CacheConfig } from "@/config";

const METHODS = [
	"chainHead_v1_storage",
	"chainHead_v1_body",
	"chainHead_v1_call",
];

export function hasOpCacheEnabled(config: CacheConfig) {
	return (
		config.enabled &&
		Object.entries(config.methods).some(
			(e) => METHODS.includes(e[0]) && e[1].enabled,
		)
	);
}

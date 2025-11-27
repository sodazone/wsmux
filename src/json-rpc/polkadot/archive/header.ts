import type { CacheConfig } from "@/config";
import { forwardRequestWithCache } from "../forward";

export const archive_v1_header = (config: CacheConfig) => {
	return forwardRequestWithCache(
		"archive_v1_header",
		config,
		(req) => req.params[0],
	);
};

import { Counter, Gauge } from "prom-client";

export const chainHeadCacheMetrics = {
	hits: new Counter({
		name: "wsmux_chainhead_cache_hits_total",
		help: "Total number of cache hits",
		labelNames: ["method"] as const,
	}),
	misses: new Counter({
		name: "wsmux_chainhead_cache_misses_total",
		help: "Total number of cache misses",
		labelNames: ["method"] as const,
	}),
	replays: new Counter({
		name: "wsmux_chainhead_cache_replays_total",
		help: "Number of cached notifications replayed to downstream clients",
		labelNames: ["method"] as const,
	}),
	errors: new Counter({
		name: "wsmux_chainhead_cache_errors_total",
		help: "Number of cache entries evicted due to errors or terminal events",
		labelNames: ["method"] as const,
	}),
	items: new Gauge({
		name: "wsmux_chainhead_cache_items",
		help: "Current number of cached items",
		labelNames: ["method"] as const,
	}),
};

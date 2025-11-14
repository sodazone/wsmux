import type { LogLevel } from "@logtape/logtape";
import type { UpstreamServerConfig } from "../json-rpc/upstream";

export type DurationString = `${number}ms` | `${number}s` | `${number}m`;

type UpstreamMethodLimits = {
	max_connections?: number;
};

export type RawUpstreamServerConfig = {
	name: string;
	url: string;

	request_timeout?: DurationString;
	connection_timeout?: DurationString;
	retry_delay?: DurationString;

	max_connections?: number;
	supported_methods?: "*" | string[];
	presets?: string | string[];
	methods?: Record<string, UpstreamMethodLimits>;
};

type SubscriptionOptions = {
	maxSubscribers?: number;
};

type JsonRpcLimits = {
	max_pending_requests_per_connection?: number;
	max_pending_requests?: number;
	max_requests_per_second?: number;
};

export type ProxyConfig = {
	log_level?: LogLevel;
	rate_limit?: {
		enabled?: boolean;
		max_requests?: number;
		window?: DurationString;
		trusted_networks?: string[];
	};
	upstream: {
		debug?: {
			stats?: {
				enabled: boolean;
				interval?: DurationString;
			};
		};
		servers: RawUpstreamServerConfig[];
	};
	json_rpc?: JsonRpcLimits;
	subscription?: SubscriptionOptions;
};

export type UpstreamConfig = {
	debug: {
		stats: {
			enabled: boolean;
			interval: number;
		};
	};
	servers: UpstreamServerConfig[];
};

export type ProxyRateLimitConfig = {
	enabled: boolean;
	maxRequests: number;
	windowMs: number;
	trustedNetworks: string[];
};

export type NormalizedConfig = {
	logLevel: ProxyConfig["log_level"];
	jsonRpc: JsonRpcLimits;
	rateLimit: ProxyRateLimitConfig;
	upstream: UpstreamConfig;
};

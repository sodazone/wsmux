import type { LogLevel } from "@logtape/logtape";
import type { UpstreamServerConfig } from "../json-rpc/upstream";

export type DurationString = `${number}ms` | `${number}s` | `${number}m`;

type UpstreamMethodLimits = {
	max_connections?: number;
};

export type UpstreamConfig = {
	name: string;
	url: string;

	request_timeout?: DurationString;
	connection_timeout?: DurationString;
	retry_delay?: DurationString;

	max_connections?: number;
	supported_methods?: "*" | string[];
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
	maxOpenSockets?: number;
	upstream: UpstreamConfig[];
	json_rpc?: JsonRpcLimits;
	subscription?: SubscriptionOptions;
};

export type NormalizedConfig = {
	logLevel: ProxyConfig["log_level"];
	maxOpenSockets: number | undefined;
	jsonRpc: ProxyConfig["json_rpc"];
	upstream: UpstreamServerConfig[];
};

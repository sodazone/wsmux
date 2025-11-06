export type UpstreamConfig = {
	name: string;
	url: string;
	maxConnections?: number;
	reconnect?: {
		maxRetries?: number;
		backoffMs?: number;
	};
};

export type SubscriptionOptions = {
	maxSubscribers?: number;
};

export type ProxyConfig = {
	maxOpenSockets?: number;
	logLevel?: "trace" | "debug" | "info" | "warn" | "error";

	upstream: UpstreamConfig[];
	subscription?: SubscriptionOptions;
};

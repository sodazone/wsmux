// DRAFT: to be defined properly
// ---

export type UpstreamConfig = {
	name: string;
	url: string;
	maxConnections?: number;
	reconnect?: {
		maxRetries?: number;
		backoffMs?: number;
	};
};

// TODO: maybe per method configurations
// under polkadot rpc key?
export type SubscriptionOptions = {
	maxSubscribers?: number;
};

export type ProxyConfig = {
	maxOpenSockets?: number;
	logLevel?: "trace" | "debug" | "info" | "warn" | "error";

	upstream: UpstreamConfig[];
	subscription?: SubscriptionOptions;
};

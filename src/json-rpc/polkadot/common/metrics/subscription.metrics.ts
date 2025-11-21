import { Counter, Gauge } from "prom-client";

const commonActiveSubscribers = new Gauge({
	name: "wsmux_common_active_subscribers",
	help: "Active downstream common subscriptions per upstream",
	labelNames: ["upstream", "method"] as const,
});

const commonSubscriptionRequests = new Counter({
	name: "wsmux_common_subscription_requests_total",
	help: "Total common subscription requests",
	labelNames: ["upstream", "method"] as const,
});

export const metrics = {
	subscribe: (upstream: string, method: string) => {
		commonActiveSubscribers.inc({ upstream, method });
		commonSubscriptionRequests.inc({ upstream, method });
	},
	unsubscribe: (upstream: string, method: string) => {
		commonActiveSubscribers.dec({ upstream, method });
		commonSubscriptionRequests.inc({ upstream, method });
	},
};

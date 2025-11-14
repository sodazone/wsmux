import { Counter, Gauge } from "prom-client";

const legacyActiveSubscribers = new Gauge({
	name: "wsmux_legacy_active_subscribers",
	help: "Active downstream legacy subscriptions per upstream",
	labelNames: ["upstream"] as const,
});

const legacySubscriptionRequests = new Counter({
	name: "wsmux_legacy_subscription_requests_total",
	help: "Total legacy subscription requests",
	labelNames: ["upstream", "method"] as const,
});

export const metrics = {
	subscribe: (upstream: string, method: string) => {
		legacyActiveSubscribers.inc({ upstream });
		legacySubscriptionRequests.inc({ upstream, method });
	},
	unsubscribe: (upstream: string, method: string) => {
		legacyActiveSubscribers.dec({ upstream });
		legacySubscriptionRequests.inc({ upstream, method });
	},
};

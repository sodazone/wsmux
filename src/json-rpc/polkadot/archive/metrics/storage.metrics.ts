import { Counter, Gauge } from "prom-client";

const archiveActiveSubscribers = new Gauge({
	name: "wsmux_archive_active_subscribers",
	help: "Active downstream archive subscriptions per upstream",
	labelNames: ["upstream"] as const,
});

const archiveSubscriptionRequests = new Counter({
	name: "wsmux_archive_subscription_requests_total",
	help: "Total archive subscription requests",
	labelNames: ["upstream", "method"] as const,
});

export const metrics = {
	subscribe: (upstream: string, method: string) => {
		archiveActiveSubscribers.inc({ upstream });
		archiveSubscriptionRequests.inc({ upstream, method });
	},
	unsubscribe: (upstream: string, method: string) => {
		archiveActiveSubscribers.dec({ upstream });
		archiveSubscriptionRequests.inc({ upstream, method });
	},
};

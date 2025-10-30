import { Counter, Gauge, Histogram } from "prom-client";

import { TooManyWaitersError } from "../../../../util/concurrent-creator";
import { RateLimitedError } from "../../../errors";

export const chainHeadMetrics = {
	requestsTotal: new Counter({
		name: "wsmux_chainhead_follow_requests_total",
		help: "Total follow requests",
		labelNames: ["upstream", "result"] as const,
	}),
	requestDuration: new Histogram({
		name: "wsmux_chainhead_follow_duration_seconds",
		help: "Follow request duration",
		labelNames: ["upstream"] as const,
	}),
	pendingRequests: new Gauge({
		name: "wsmux_chainhead_follow_pending_requests",
		help: "Current number of active follow requests",
		labelNames: ["upstream"] as const,
	}),
};

export async function observeFollow<T>(
	upstreamId: string,
	fn: () => Promise<T>,
): Promise<T> {
	chainHeadMetrics.pendingRequests.labels(upstreamId).inc();
	const endTimer = chainHeadMetrics.requestDuration
		.labels(upstreamId)
		.startTimer();

	try {
		const res = await fn();
		chainHeadMetrics.requestsTotal.labels(upstreamId, "ok").inc();
		return res;
	} catch (err: any) {
		if (err instanceof RateLimitedError || err instanceof TooManyWaitersError) {
			chainHeadMetrics.requestsTotal.labels(upstreamId, "rate_limited").inc();
		} else {
			chainHeadMetrics.requestsTotal.labels(upstreamId, "error").inc();
		}
		throw err;
	} finally {
		chainHeadMetrics.pendingRequests.labels(upstreamId).dec();
		endTimer();
	}
}

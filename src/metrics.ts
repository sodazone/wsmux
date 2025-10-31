import { Counter, Gauge, Histogram } from "prom-client";

import type { WebSocketMiddleware } from "./ws";

export const wsMetrics = {
	activeConnections: new Gauge({
		name: "wsmux_ws_active_connections",
		help: "Current number of active WebSocket connections",
	}),
	totalConnections: new Counter({
		name: "wsmux_ws_connections_total",
		help: "Total WebSocket connections",
	}),
	totalMessages: new Counter({
		name: "wsmux_ws_messages_total",
		help: "Total messages received",
	}),
	totalErrors: new Counter({
		name: "wsmux_ws_errors_total",
		help: "Total errors encountered",
	}),
	messageDuration: new Histogram({
		name: "wsmux_ws_message_duration_seconds",
		help: "Message processing duration",
		buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5], // seconds
	}),
};

export const metricsMiddleware = (): WebSocketMiddleware => {
	return {
		open: async (_ctx, next) => {
			wsMetrics.activeConnections.inc();
			wsMetrics.totalConnections.inc();
			await next();
		},

		message: async (_ctx, next) => {
			const endTimer = wsMetrics.messageDuration.startTimer();
			try {
				await next();
			} catch (err) {
				wsMetrics.totalErrors.inc();
				throw err;
			} finally {
				wsMetrics.totalMessages.inc();
				endTimer();
			}
		},

		close: async (_ctx, next) => {
			wsMetrics.activeConnections.dec();
			await next();
		},

		error: async (_err, next) => {
			wsMetrics.totalErrors.inc();
			await next();
		},
	};
};

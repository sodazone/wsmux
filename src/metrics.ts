import { getLogger } from "@logtape/logtape";
import type { WebSocketMiddleware } from "./ws";

const logger = getLogger(["wsmux", "metrics"]);

export const metricsMiddleware = (): WebSocketMiddleware => {
	let activeConnections = 0;
	let totalConnections = 0;
	let totalMessages = 0;
	let totalErrors = 0;
	let totalMessageTime = 0;

	setInterval(() => {
		const avgResponseTimeMs =
			totalMessages > 0 ? totalMessageTime / totalMessages : 0;
		const errorRate = totalMessages > 0 ? totalErrors / totalMessages : 0;

		logger.info(
			(l) =>
				l`Metrics:
  active=${activeConnections},
  total=${totalConnections},
  messages=${totalMessages},
  errors=${totalErrors},
  avgResponseTimeMs=${avgResponseTimeMs},
  errorRate=${errorRate}`,
		);
	}, 20_000).unref();

	return {
		open: async (ctx, next) => {
			activeConnections++;
			totalConnections++;
			await next();
			logger.debug(
				(l) =>
					l`Client connected: ${ctx.ws.remoteAddress}, active=${activeConnections}, total=${totalConnections}`,
			);
		},

		message: async (_ctx, next) => {
			const start = performance.now();
			try {
				await next();
			} catch (err) {
				totalErrors++;
				throw err;
			} finally {
				const duration = performance.now() - start;
				totalMessages++;
				totalMessageTime += duration;
			}
		},

		close: async (ctx, next) => {
			activeConnections--;
			await next();
			logger.debug(
				(l) =>
					l`Client disconnected: ${ctx.ws.remoteAddress}, active=${activeConnections}, total=${totalConnections}`,
			);
		},

		error: async (err, next) => {
			totalErrors++;
			logger.warn`Error for client: ${err}`;
			await next();
		},
	};
};

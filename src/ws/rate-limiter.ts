import { getLogger } from "@logtape/logtape";

import type { ProxyRateLimitConfig } from "../config";
import type { JSONRPCContextData } from "../json-rpc";
import { createTrustedIPs } from "../net/trusted-ip";
import type { WebSocketMiddleware } from "./handler";

const DEFAULT_FLUSH_INTERVAL = 60_000; // 60s

const logger = getLogger(["wsmux", "rate-limiter"]);

export const rateLimiterMiddleware = (
	config: ProxyRateLimitConfig,
): WebSocketMiddleware<JSONRPCContextData> => {
	if (!config.enabled) {
		logger.warn("Rate limiting is disabled, avoid installing this middleware.");
		return {
			async open(_, next) {
				next();
			},
		};
	}

	const { windowMs, maxRequests, trustedNetworks } = config;
	const trustedProxies = createTrustedIPs(trustedNetworks);
	const ipBuckets = new Map<string, { count: number; reset: number }>();

	function getClientIp(ws: Bun.ServerWebSocket<JSONRPCContextData>): string {
		const remoteAddress = ws.remoteAddress || "unknown";

		if (
			trustedNetworks.length === 0 ||
			!trustedProxies.isTrusted(remoteAddress)
		) {
			return remoteAddress;
		}

		// only for trusted networks
		const headers = ws.data?.headers;
		if (headers) {
			const xf = headers.get("x-forwarded-for");
			if (xf != null) return xf.split(",", 1)[0]?.trim() ?? "unknown";

			const cf = headers.get("cf-connecting-ip");
			if (cf) return cf;

			const fwd = headers.get("forwarded");
			if (fwd) {
				const forIndex = fwd.indexOf("for=");
				if (forIndex !== -1) {
					const start = forIndex + 4;
					let end = fwd.indexOf(";", start);
					if (end === -1) end = fwd.length;
					return fwd.slice(start, end).replace(/["<>]/g, "").trim();
				}
			}
		}

		return remoteAddress;
	}

	function checkLimit(ip: string): boolean {
		const now = performance.now();
		const bucket = ipBuckets.get(ip);

		if (!bucket || now > bucket.reset) {
			ipBuckets.set(ip, { count: 1, reset: now + windowMs });
			return true;
		}

		if (bucket.count >= maxRequests) return false;
		bucket.count++;
		return true;
	}

	setInterval(() => {
		const now = performance.now();
		for (const [ip, bucket] of ipBuckets) {
			if (now > bucket.reset) ipBuckets.delete(ip);
		}
	}, DEFAULT_FLUSH_INTERVAL).unref();

	return {
		async open({ ws }, next) {
			const ip = getClientIp(ws);
			if (checkLimit(ip)) {
				next();
			} else {
				ws.close(1013, "Rate limit exceeded");
			}
		},
	};
};

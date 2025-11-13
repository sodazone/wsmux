import { getLogger } from "@logtape/logtape";

import type { UpstreamServer } from "./types";

const logger = getLogger(["wsmux", "upstream", "stats"]);

function isCollectable(
	value: any,
): value is { stats: () => Record<string, any> } {
	return value?.stats && typeof value.stats === "function";
}

export function collectStats(states: Map<string, any>) {
	const result: Record<string, any> = {};

	for (const [stateKey, stateValue] of states.entries()) {
		const entry: Record<string, any> = {};

		for (const [prop, value] of Object.entries(stateValue)) {
			if (isCollectable(value)) {
				entry[prop] = value.stats();
			} else if (value instanceof Map || value instanceof Set) {
				entry[prop] = value.size;
			} else if (Array.isArray(value)) {
				entry[prop] = value.length;
			}
		}

		result[stateKey] = entry;
	}

	return result;
}

export function startServerStats(
	servers: UpstreamServer[],
	{ interval }: { interval: number },
) {
	for (const server of servers) {
		logger.warn(
			`[${server.url}] stats enabled, print every ${interval}ms (not intended for production)`,
		);

		setInterval(() => {
			const s = server.stats();
			logger.info`${server.url} stats:\n${Bun.inspect(s, { compact: true })}`;
		}, interval).unref();
	}
}

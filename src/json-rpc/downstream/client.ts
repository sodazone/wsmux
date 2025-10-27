import { getLogger } from "@logtape/logtape";
import type { ServerWebSocket } from "bun";

import type { JSONRPCContextData } from "../types";
import type { DownstreamClient, DownstreamMessage } from "./types";

const logger = getLogger(["wsmux", "downstream"]);

const nextId = (() => {
	let currentId = 0;
	const MAX_ID = Number.MAX_SAFE_INTEGER - 1;
	return () => {
		if (currentId > MAX_ID) currentId = 0;
		return currentId++;
	};
})();

export function createDownstream(
	ws: ServerWebSocket<JSONRPCContextData>,
): DownstreamClient {
	const closeFns = new Set<() => void>();

	return {
		clientId: nextId(),
		pendingRequests: 0,
		lastRequestTimes: [],
		getLocalId(suffix: string): string {
			return `${this.clientId}-${suffix}`;
		},
		send(message: DownstreamMessage): number {
			logger.debug((l) => l`(${this.clientId})=> ${message}`);

			return ws.send(
				typeof message === "string" ? message : JSON.stringify(message),
			);
		},
		addCloseFn: (closeFn: () => void) => {
			closeFns.add(closeFn);
		},
		close: () => {
			closeFns.forEach((closeFn) => {
				closeFn();
			});
		},
	};
}

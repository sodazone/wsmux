import { getLogger } from "@logtape/logtape";
import type { ServerWebSocket } from "bun";
import { ulid } from "ulid";

import type { JSONRPCContextData } from "../types";
import type { DownstreamClient, DownstreamMessage } from "./types";

const logger = getLogger(["wsmux", "downstream"]);

export function createDownstream(
	ws: ServerWebSocket<JSONRPCContextData>,
): DownstreamClient {
	const listeners = new Set<() => void>();
	return {
		clientId: ulid(),
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
		addCloseListener: (listener: () => void) => {
			listeners.add(listener);
		},
		close: () => {
			listeners.forEach((listener) => {
				listener();
			});
		},
	};
}

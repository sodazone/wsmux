import { getLogger } from "@logtape/logtape";
import type { ServerWebSocket } from "bun";
import { createTokenBucket } from "@/util/token-bucket";
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
	rate: { capacity: number; windowMs: number },
): DownstreamClient {
	const closeFns = new Set<() => void>();
	const _rateLimiter = createTokenBucket(rate.capacity, rate.windowMs);
	let _pendingRequests = 0;

	return {
		clientId: nextId(),
		get pendingRequests() {
			return _pendingRequests;
		},
		get closed() {
			return ws.readyState > 1;
		},
		startRequest() {
			if (!_rateLimiter.allow()) {
				throw new Error("Rate limit exceeded");
			}
			_pendingRequests++;
		},
		endRequest() {
			_pendingRequests--;
		},
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
		close: (code = 1000, reason = "Normal Closure") => {
			ws.close(code, reason);
		},
		onClose: () => {
			for (const closeFn of closeFns) {
				closeFn();
			}
		},
	};
}

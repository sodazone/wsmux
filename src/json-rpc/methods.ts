import type { DownstreamClient } from "./downstream";
import type { JSONRPCRequest } from "./types";
import type { UpstreamServer } from "./upstream";

export interface JSONRPCMethodHandler {
	readonly handleRequest: (
		upstream: UpstreamServer,
		downstream: DownstreamClient,
		request: JSONRPCRequest,
	) => void;
}

export function handleRPCMethod(
	downstream: DownstreamClient,
	upstream: UpstreamServer,
	req: JSONRPCRequest,
	handlers: Record<string, JSONRPCMethodHandler>,
): void {
	const handler = handlers[req.method];
	if (!handler) return;

	handler.handleRequest(upstream, downstream, req);
}

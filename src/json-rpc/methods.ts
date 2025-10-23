import type { DownstreamClient } from "./downstream";
import type { JSONRPCRequest } from "./types";
import type { UpstreamServer } from "./upstream";

export interface JSONRPCMethodHandler {
	readonly handleRequest: (
		upstream: UpstreamServer,
		downstream: DownstreamClient,
		request: JSONRPCRequest,
	) => Promise<void>;
}

export async function handleRPCMethod(
	downstream: DownstreamClient,
	upstream: UpstreamServer,
	req: JSONRPCRequest,
	handlers: Record<string, JSONRPCMethodHandler>,
): Promise<void> {
	const handler = handlers[req.method];
	if (!handler) return;

	await handler.handleRequest(upstream, downstream, req);
}

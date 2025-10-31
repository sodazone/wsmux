import type { JSONRPCError, JSONRPCRequest, JSONRPCResponse } from "./types";

export function isJsonRpcRequest(obj: unknown): obj is JSONRPCRequest {
	if (typeof obj !== "object" || obj === null) return false;

	const o = obj as Partial<JSONRPCRequest>;

	return (
		o.jsonrpc === "2.0" &&
		typeof o.method === "string" &&
		"id" in o &&
		(o.id === undefined || typeof o.id === "string" || typeof o.id === "number")
	);
}

export function isSuccess(
	r: JSONRPCResponse | JSONRPCError | null,
): r is JSONRPCResponse {
	return !!r && !("error" in r);
}

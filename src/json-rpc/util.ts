import type { JSONRPCRequest } from "./types";

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

export function deferred<T = void>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: unknown) => void;

	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});

	return { promise, resolve, reject };
}

import type { JSONRPCError } from "./types";

const JSONRPCErrorCodes = {
	PARSE_ERROR: { code: -32700, message: "Parse error" },
	INVALID_REQUEST: { code: -32600, message: "Invalid request" },
	METHOD_NOT_FOUND: { code: -32601, message: "Method not found" },
	INVALID_PARAMS: { code: -32602, message: "Invalid params" },
	INTERNAL_ERROR: { code: -32603, message: "Internal error" },
	SERVER_ERROR: { code: -32000, message: "Server error" },
	RESOURCE_NOT_FOUND: { code: -32001, message: "Resource not found" },
	RESOURCE_UNAVAILABLE: { code: -32002, message: "Resource unavailable" },
	TRANSACTION_REJECTED: { code: -32003, message: "Transaction rejected" },
	RATE_LIMITED: { code: -32004, message: "Rate limited" },
	TIMEOUT: { code: -32005, message: "Timeout" },
	UPSTREAM_ERROR: { code: -32006, message: "Upstream error" },
} as const;

export type JSONRPCErrorCode = keyof typeof JSONRPCErrorCodes;

export function jsonRpcError(
	opts:
		| {
				kind: JSONRPCErrorCode;
				message?: string;
				req?: { id?: string | number | null };
				data?: unknown;
		  }
		| {
				code: number;
				message: string;
				req?: { id?: string | number | null };
				data?: unknown;
		  },
): JSONRPCError {
	if ("kind" in opts) {
		const { code, message: defaultMsg } = JSONRPCErrorCodes[opts.kind];
		return {
			jsonrpc: "2.0",
			id: opts.req?.id ?? null,
			error: {
				code,
				message: opts.message ?? defaultMsg,
				...(opts.data ? { data: opts.data } : {}),
			},
		};
	}

	return {
		jsonrpc: "2.0",
		id: opts.req?.id ?? null,
		error: {
			code: opts.code,
			message: opts.message,
			...(opts.data ? { data: opts.data } : {}),
		},
	};
}

export class RateLimitedError extends Error {
	code: number;

	constructor(message = "Rate limit reached", code = -32004) {
		super(message);
		this.name = "RateLimited";
		this.code = code;
	}
}

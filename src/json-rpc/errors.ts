import type { JSONRPCError } from "./types";

const JSONRPCErrorCodes = {
	PARSE_ERROR: { code: -32700, message: "Parse error" },
	INVALID_REQUEST: { code: -32600, message: "Invalid request" },
	METHOD_NOT_FOUND: { code: -32601, message: "Method not found" },
	INVALID_PARAMS: { code: -32602, message: "Invalid params" },
	INTERNAL_ERROR: { code: -32603, message: "Internal error" },
	SERVER_ERROR: { code: -32000, message: "Server error" },
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
	constructor(message = "Rate limit reached") {
		super(message);
	}
}

import type { WebSocketContext, WebSocketMiddleware } from "../ws";
import type { DownstreamClient } from "./downstream";

export type JSONRPCRequest = {
	jsonrpc: "2.0";
	method: string;
	id?: string | number;
	params?: any;
};

export type JSONRPCResponse = {
	jsonrpc: "2.0";
	id: string | number | null;
	result?: any;
};

export type JSONRPCError = {
	jsonrpc: "2.0";
	id: string | number | null;
	error: {
		code: number;
		message: string;
		data?: any;
	};
};

export type JSONRPCNotification = {
	jsonrpc: "2.0";
	method: string;
	params?: any;
};

export type JSONRPCContextData = {
	headers?: Headers;
	client?: DownstreamClient;
};

export type JSONRPCContext = WebSocketContext<JSONRPCContextData>;

export type JSONRPCMiddleware = WebSocketMiddleware<JSONRPCContextData>;

import type {
	JSONRPCError,
	JSONRPCNotification,
	JSONRPCResponse,
} from "../types";

export type DownstreamMessage =
	| string
	| JSONRPCResponse
	| JSONRPCNotification
	| JSONRPCError;

export type DownstreamClient = {
	clientId: string;
	pendingRequests: number;
	lastRequestTimes: number[];
	getLocalId(suffix: string): string;
	send(message: DownstreamMessage): number;
	addCloseFn(closeFn: () => void): void;
	close(): void;
};

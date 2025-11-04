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
	clientId: number;
	pendingRequests: number;
	closed: boolean;
	startRequest(): void;
	endRequest(): void;
	requestsInPeriod(millis?: number): number;
	getLocalId(suffix: string): string;
	send(message: DownstreamMessage): number;
	addCloseFn(closeFn: () => void): void;
	close(): void;
};

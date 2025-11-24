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
	getLocalId(suffix: string): string;
	send(message: DownstreamMessage): number;
	close(code?: number, reason?: string): void;
	addCloseFn(closeFn: () => void): void;
	onClose(): void;
};

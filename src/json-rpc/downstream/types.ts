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
	getLocalId(suffix: string): string;
	send(message: DownstreamMessage): number;
	addCloseListener(listener: () => void): void;
	close(): void;
};

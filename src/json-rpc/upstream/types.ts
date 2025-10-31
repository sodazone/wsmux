import type { Observable } from "rxjs";
import type { DownstreamClient } from "../downstream";
import type {
	JSONRPCContextData,
	JSONRPCError,
	JSONRPCNotification,
	JSONRPCRequest,
	JSONRPCResponse,
} from "../types";
import type { createSharedSubscriptionGroup } from "./shared";

export type SharedSubscription = {
	subscribeLocal(
		localId: string,
		downstream: DownstreamClient,
		options?: Record<string, any>,
	): void;
	unsubscribeLocal(localId: string): void;
	hasLocalSubscription(localId: string): boolean;
	hasSubscribers(): boolean;
	subscribersCount(): number;
	hasLocalId(localId: string): boolean;
	getLocalIds(): string[];
	upstreamSubId: string;
	abort(): void;
};

export type SharedSubscriptionGroup = ReturnType<
	typeof createSharedSubscriptionGroup
>;

export type UpstreamServer = {
	url: string;
	nextId: number;
	subscriptions: SharedSubscriptionGroup;
	unsubscribers: Map<string, () => void>;
	unsubscribe: (localId: string) => void;
	message$: Observable<JSONRPCResponse | JSONRPCNotification>;
	supportedMethods?: string[];
	request(req: JSONRPCRequest): Promise<JSONRPCResponse | JSONRPCError>;
	send(req: JSONRPCRequest): void;
	isReady(): boolean;
	connect(): Promise<void>;
	stop(): void;
	getOrCreateState<T>(id: string, factory: () => T): T;
};

export type UpstreamServerConfig = {
	url: string;
	supportedMethods?: string[];
	retryDelay?: number;
};

export type UpstreamRegistry = {
	servers: UpstreamServer[];
	resolveUpstream: (
		ctx: JSONRPCContextData,
		method: string,
	) => UpstreamServer | undefined;
	disconnect: (clientId: number) => void;
	destroy: () => void;
	connectAll: () => Promise<void>;
};

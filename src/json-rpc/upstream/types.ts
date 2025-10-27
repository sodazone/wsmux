import type { Observable } from "rxjs";
import type { DownstreamClient } from "../downstream";
import type {
	JSONRPCContextData,
	JSONRPCNotification,
	JSONRPCRequest,
	JSONRPCResponse,
} from "../types";

export type SharedSubscription = {
	subscribeLocal(localId: string, downstream: DownstreamClient): void;
	unsubscribeLocal(localId: string): void;
	hasLocalSubscription(localId: string): boolean;
	hasSubscribers(): boolean;
	subscribersCount(): number;
	upstreamSubId: string;
};

export type UpstreamServer = {
	url: string;
	nextId: number;
	subscriptions: Map<string, SharedSubscription>;
	unsubscribers: Map<string, () => void>;
	unsubscribe: (localId: string) => void;
	message$: Observable<JSONRPCResponse | JSONRPCNotification>;
	supportedMethods?: string[];
	request(req: JSONRPCRequest): Promise<JSONRPCResponse>;
	send(req: JSONRPCRequest): void;
	isReady(): boolean;
	connect(): Promise<void>;
	stop(): void;
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
	destroy: () => void;
	connectAll: () => Promise<void>;
};

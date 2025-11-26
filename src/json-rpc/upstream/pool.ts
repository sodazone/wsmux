import { createMinHeap } from "@/util/min-heap";
import { createUpstreamServer } from "./server";
import type {
	UpstreamServer,
	UpstreamServerConfig,
	UpstreamServerPool,
} from "./types";

export function createUpstreamPool({
	url,
	supportedMethods,
	methods,
	maxConnections = 500,
	minConnections = 1,
	idleCloseMs = 30_000,
	...rest
}: UpstreamServerConfig): UpstreamServerPool {
	let stopped = false;

	const heap = createMinHeap<UpstreamServer>(
		(c) => c?.connectionsCount ?? Infinity,
	);
	const now = () => performance.now();
	const jitter = (n: number) => n * (0.8 + Math.random() * 0.4);
	const lastUsed = new WeakMap<UpstreamServer, number>();

	function createNewConn(): UpstreamServer {
		const conn = createUpstreamServer({
			url,
			supportedMethods,
			methods,
			...rest,
		});
		conn.connect();
		lastUsed.set(conn, now());
		heap.push(conn);
		return conn;
	}

	function start() {
		for (let i = 0; i < minConnections; i++) {
			const conn = createNewConn();
			conn.clients.inc();
			heap.update(conn);
		}
		return heap.items().filter(Boolean);
	}

	function acquireServerConnection(): UpstreamServer | undefined {
		const best = heap.peek();

		if (!best || !best.hasCapacity()) {
			if (heap.size() < maxConnections) {
				const conn = createNewConn();
				conn.clients.inc();
				heap.update(conn);
				lastUsed.set(conn, now());
				return conn;
			}
		}

		if (best) {
			best.clients.inc();
			heap.update(best);
			lastUsed.set(best, now());
		}

		return best;
	}

	function releaseConn(conn: UpstreamServer) {
		conn.clients.dec();
		heap.update(conn);
		lastUsed.set(conn, now());
	}
	function sweep() {
		if (stopped) return;

		const t = now();

		for (const conn of heap.items()) {
			if (!conn) continue;

			const idle = t - (lastUsed.get(conn) ?? t);

			if (
				conn.connectionsCount === 0 &&
				heap.size() > minConnections &&
				idle > jitter(idleCloseMs)
			) {
				conn.stop();
				heap.remove(conn);
				lastUsed.delete(conn);
			}
		}
	}

	const sweepInterval = setInterval(sweep, idleCloseMs / 2);
	sweepInterval.unref();

	return {
		supportsMethod(method: string) {
			return !supportedMethods || supportedMethods.has(method);
		},

		start,

		acquire() {
			return acquireServerConnection();
		},

		release(conn) {
			releaseConn(conn);
		},

		stop() {
			if (stopped) return;
			stopped = true;

			clearInterval(sweepInterval);

			for (const conn of heap.items()) {
				conn?.stop();
			}
			heap.clear();
		},

		stats() {
			return heap
				.items()
				.filter(Boolean)
				.map((c) => c.stats());
		},
	};
}

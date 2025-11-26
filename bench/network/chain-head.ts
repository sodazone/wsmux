import { isSuccess } from "@/json-rpc/util";

type Method = "follow" | "header" | "body";

type LatencyMap = Record<Method, { n: number; total: number; data: number[] }>;

type Stats = {
	host: string;
	open: number;
	close: number;
	errors: number;
	exfiltrated: number;
	msgs: number;
	events: number;
	latency: LatencyMap;
};

type Pending = { method: Method; sent: number };

export type Script = {
	onOpen?: (send: Send) => void;
	onResponse?: (result: any, send: Send) => void;
	onError?: (error: any, send: Send) => void;
	onEvent?: (subId: string | null, ev: any, send: Send) => void;
};

type Send = (
	method: Method,
	rpcMethod: string,
	params: any[],
	inFollow?: boolean,
) => void;

const emptyLatency = (): LatencyMap => ({
	follow: { n: 0, total: 0, data: [] },
	header: { n: 0, total: 0, data: [] },
	body: { n: 0, total: 0, data: [] },
});

const createStats = (host: string): Stats => ({
	host,
	open: 0,
	close: 0,
	errors: 0,
	exfiltrated: 0,
	msgs: 0,
	events: 0,
	latency: emptyLatency(),
});

export function runChainHeadBench(
	endpoint: string,
	{
		iterations,
		warmup = 0,
	}: {
		iterations: number;
		warmup: number;
	},
	mkScript: () => Script,
) {
	const stats = createStats(endpoint);
	const pendingMap = new WeakMap<WebSocket, Map<number, Pending>>();
	const newClient = (isWarm: boolean) => {
		const ws = new WebSocket(endpoint);
		const script = mkScript();
		const pendings = new Map<number, Pending>();
		pendingMap.set(ws, pendings);

		let nextId = 1;
		let subId: string | null = null;

		const send: Send = (method, rpcMethod, params) => {
			const id = nextId++;
			pendings.set(id, { method, sent: performance.now() });
			ws.send(
				JSON.stringify({ jsonrpc: "2.0", id, method: rpcMethod, params }),
			);
		};

		ws.onopen = () => {
			if (!isWarm) stats.open++;
			script.onOpen?.(send);
		};

		ws.onmessage = ({ data }) => {
			const now = performance.now();
			const msg = JSON.parse(data);

			if (!isWarm) stats.msgs++;

			// response
			if (msg.id != null) {
				const p = pendings.get(msg.id);
				if (p) {
					const ok = isSuccess(msg);
					if (!isWarm) {
						const dt = now - p.sent;
						const lat = stats.latency[p.method];
						lat.n++;
						lat.total += dt;
						lat.data.push(dt);
						if (!ok) stats.errors++;
					}
					pendings.delete(msg.id);

					if (ok) {
						if (msg.result) {
							script.onResponse?.(msg.result, send);
						} else {
							console.error(msg, p);
						}
					} else {
						script.onError?.(msg.error, send);
					}
					if (p.method === "follow") subId = msg.result;
				} else {
					stats.exfiltrated++;
				}
				return;
			}

			// subscription event
			if (msg.params?.subscription === subId) {
				if (!isWarm) stats.events++;
				script.onEvent?.(subId, msg.params.result, send);
			} else {
				stats.exfiltrated++;
			}
		};

		ws.onerror = () => {
			if (!isWarm) stats.errors++;
		};

		ws.onclose = () => {
			if (!isWarm) stats.close++;
		};
	};

	(async () => {
		const totalIterations = iterations + warmup;
		for (let i = 0; i < totalIterations; i++) {
			const isWarm = i < warmup;
			newClient(isWarm);
			await new Promise((r) => setTimeout(r, 2));
		}
	})();

	return stats;
}

export const printSummary = (s: Stats) => {
	const quantile = (xs: number[], q: number) => {
		if (!xs.length) return 0;
		const ys = [...xs].sort((a, b) => a - b);
		const pos = (ys.length - 1) * q;
		const base = Math.floor(pos);
		const rest = pos - base;
		return ys[base + 1] !== undefined
			? ys[base]! + (ys[base + 1]! - ys[base]!) * rest
			: (ys[base] ?? 0);
	};

	const summarize = (xs: number[]) => {
		if (!xs.length) return null;
		const ys = [...xs].sort((a, b) => a - b);
		const n = ys.length;

		const p50 = quantile(ys, 0.5);
		const p90 = quantile(ys, 0.9);
		const p99 = quantile(ys, 0.99);
		const mean = ys.reduce((a, b) => a + b, 0) / n;

		let b50 = 0,
			b90 = 0,
			b99 = 0,
			bOut = 0;
		for (const v of ys) {
			if (v < p50) b50++;
			else if (v < p90) b90++;
			else if (v < p99) b99++;
			else bOut++;
		}

		return {
			n,
			min: ys[0] ?? 0,
			max: ys[n - 1] ?? 0,
			mean,
			p50,
			p90,
			p99,
			b50,
			b90,
			b99,
			bOut,
		};
	};

	const fmt = (x: number) => x.toFixed(2);

	console.log(`Summary ${s.host}`);
	console.log(`open: ${s.open} close: ${s.close} errors: ${s.errors}`);
	console.log(
		`msgs: ${s.msgs} events: ${s.events} exfiltrated: ${s.exfiltrated}`,
	);
	console.log("Latency (ms):");

	for (const method of Object.keys(s.latency) as Method[]) {
		const xs = s.latency[method].data;
		const r = summarize(xs);

		if (!r) {
			console.log(`  ${method.padEnd(10)} no data`);
			continue;
		}

		const { n, min, max, mean, p50, p90, p99, b50, b90, b99, bOut } = r;

		console.log(
			`  ${method.padEnd(10)}` +
				` min=${fmt(min)} max=${fmt(max)} avg=${fmt(mean)}` +
				` p50=${fmt(p50)} p90=${fmt(p90)} p99=${fmt(p99)} (${n})`,
		);

		console.log(
			`    distribution:` +
				` fast(<p50)=${b50}, slow(p50–p90)=${b90},` +
				` tail(p90–p99)=${b99}, worst(>=p99)=${bOut}`,
		);

		console.log(
			`    summary: 50% < ${fmt(p50)}ms, next 40% < ${fmt(p90)}ms,` +
				` next 9% < ${fmt(p99)}ms, worst 1% >= ${fmt(p99)}ms`,
		);
	}
};

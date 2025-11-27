import { printSummary, runBench, type Script } from "./_base/benchmark";

const PROVIDERS = [
	"ws://localhost:8181",
	"wss://polkadot-public-rpc.blockops.network/ws",
	"wss://polkadot.public.curie.radiumblock.co/ws",
	"wss://rockx-dot.w3node.com/polka-public-dot/ws",
	"wss://rpc-polkadot.luckyfriday.io",
	"wss://polkadot.api.onfinality.io/public-ws",
	"wss://rpc.ibp.network/polkadot",
];

const ext = {
	limitReached: 0,
	invalidSubscription: 0,
	exfiltratedOps: 0,
};

export const simpleFollow = (): Script => {
	const ops = new Set<string>();
	let subId: string | null = null;

	return {
		onOpen: (send) => {
			send("chainHead_v1_follow", [true]);
		},

		onResponse: ({ result }) => {
			if (result != null) {
				if (subId === null && typeof result === "string") {
					subId = result;
					return;
				}
				if (result.result === "started") {
					ops.add(result.operationId);
				}
				if (result.result === "limitReached") {
					ext.limitReached++;
				}
			} else {
				ext.invalidSubscription++;
			}
		},

		onEvent: (msg, send) => {
			if (subId === null) {
				console.warn("No subId", msg, subId);
				return;
			}

			if (msg.params?.subscription !== subId) {
				console.warn("Unexpected event", msg, subId);
			}

			const ev = msg.params?.result;
			if (ev.event === "newBlock") {
				send("chainHead_v1_body", [subId, ev.blockHash]);
				send("chainHead_v1_header", [subId, ev.blockHash]);
			}

			if (ev.operationId != null) {
				if (
					[
						"operationBodyDone",
						"operationInaccessible",
						"operationError",
					].includes(ev.event)
				) {
					if (!ops.delete(ev.operationId)) {
						ext.exfiltratedOps++;
					}
				}
			}
		},
		onError: (error) => {
			console.error(`Error: ${error.message}`);
		},
	};
};

(async () => {
	const provider = PROVIDERS[0]!;
	const opts = {
		iterations: 1_000,
		warmup: 0,
	};
	const durationMs = 10_000;
	console.log(
		`Benchmarking ${provider} (iters=${opts.iterations}, duration=${durationMs / 1_000}s)`,
	);

	const stats = runBench(provider, opts, simpleFollow);

	setTimeout(() => {
		printSummary(stats);
		console.log(`Limit reached: ${ext.limitReached}`);
		console.log(`Invalid subscription: ${ext.invalidSubscription}`);
		console.log(`Exfiltrated operations: ${ext.exfiltratedOps}`);
		process.exit(0);
	}, durationMs);
})();

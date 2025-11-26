import { printSummary, runChainHeadBench, type Script } from "./chain-head";

const PROVIDERS = [
	"ws://localhost:8181",
	"wss://rpc-polkadot.luckyfriday.io",
	"wss://polkadot.api.onfinality.io/public-ws",
	"wss://rpc.ibp.network/polkadot",
];

const ext = {
	limitReached: 0,
	invalidSubscription: 0,
};

export const simpleFollow = (): Script => {
	const ops = new Set<string>();
	return {
		onOpen: (send) => {
			send("follow", "chainHead_v1_follow", [true]);
		},

		onResponse: (res) => {
			if (res != null) {
				if (res.result === "started") {
					ops.add(res.operationId);
				}
				if (res.result === "limitReached") {
					ext.limitReached++;
				}
			} else {
				ext.invalidSubscription++;
			}
		},

		onEvent: (subId, ev, send) => {
			if (ev.event === "newBlock") {
				send("body", "chainHead_v1_body", [subId, ev.blockHash]);
				send("header", "chainHead_v1_header", [subId, ev.blockHash]);
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
						console.error(
							`Operation ${ev.operationId} not seen before, likely exfiltrated`,
						);
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

	const stats = runChainHeadBench(provider, opts, simpleFollow);

	setTimeout(() => {
		printSummary(stats);
		console.log(`Limit reached: ${ext.limitReached}`);
		console.log(`Invalid subscription: ${ext.invalidSubscription}`);
		process.exit(0);
	}, durationMs);
})();

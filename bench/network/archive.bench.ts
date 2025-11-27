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

export const simpleArchive = (): Script => {
	let finalizedHeight: null | number = null;
	let hashByHeight: null | string = null;
	return {
		onOpen: (send) => {
			send("archive_v1_finalizedHeight");
		},

		onResponse: ({ result }, send) => {
			if (result != null) {
				if (finalizedHeight === null) {
					finalizedHeight = Number(result);
					send("archive_v1_hashByHeight", [finalizedHeight]);
				} else if (hashByHeight === null) {
					hashByHeight = result[0];
					send("archive_v1_header", [hashByHeight]);
					send("archive_v1_body", [hashByHeight]);
				}
			} else {
				console.error(`Error: null response`);
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
		iterations: 100,
		warmup: 0,
	};
	const durationMs = 5_000;
	console.log(
		`Benchmarking ${provider} (iters=${opts.iterations}, duration=${durationMs / 1_000}s)`,
	);

	const stats = runBench(provider, opts, simpleArchive);

	setTimeout(() => {
		printSummary(stats);
		process.exit(0);
	}, durationMs);
})();

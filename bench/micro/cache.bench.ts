import { bench, run, summary } from "mitata";
import { type Cache, createLRUCache } from "@/util/cache";
import { createFIFOCache } from "./baselines/fifo";

const MAX_SIZES = [100, 1_000, 10_000];
const READS = 50_000;
const REMOVALS = 25_000;

summary(() => {
	for (const maxSize of MAX_SIZES) {
		bench(`write FIFO ${maxSize}`, function* () {
			yield {
				0() {
					return createFIFOCache<number>(maxSize);
				},
				bench(cache: Cache<number>) {
					for (let i = 0; i < maxSize * 10; i++) cache.set(String(i), i);
					return cache.size;
				},
			};
		});

		bench(`write LRU ${maxSize}`, function* () {
			yield {
				0() {
					return createLRUCache<number>(maxSize);
				},
				bench(cache: Cache<number>) {
					for (let i = 0; i < maxSize * 10; i++) cache.set(String(i), i);
					return cache.size;
				},
			};
		});
	}
});

summary(() => {
	for (const maxSize of MAX_SIZES) {
		bench(`read FIFO ${maxSize}`, function* () {
			const cache = createFIFOCache(maxSize);
			for (let i = 0; i < maxSize * 10; i++) cache.set(`key${i}`, i);
			const keys = Array.from(
				{ length: READS },
				() => `key${Math.floor(Math.random() * READS)}`,
			);

			yield {
				0() {
					return { cache, keys };
				},
				bench({ cache, keys }: { cache: Cache<number>; keys: string[] }) {
					let sum = 0;
					for (const key of keys) {
						const val = cache.get(key);
						if (val !== undefined) sum += val;
					}
					return sum;
				},
			};
		});

		bench(`read LRU ${maxSize}`, function* () {
			const cache = createLRUCache(maxSize);
			for (let i = 0; i < maxSize * 10; i++) cache.set(`key${i}`, i);
			const keys = Array.from(
				{ length: READS },
				() => `key${Math.floor(Math.random() * READS)}`,
			);

			yield {
				0() {
					return { cache, keys };
				},
				bench({ cache, keys }: { cache: Cache<number>; keys: string[] }) {
					let sum = 0;
					for (const key of keys) {
						const val = cache.get(key);
						if (val !== undefined) sum += val;
					}
					return sum;
				},
			};
		});
	}
});

summary(() => {
	for (const maxSize of MAX_SIZES) {
		bench(`remove FIFO ${maxSize}`, function* () {
			const cache = createFIFOCache(maxSize);
			for (let i = 0; i < maxSize * 10; i++) cache.set(`key${i}`, i);
			const keys = Array.from(
				{ length: REMOVALS },
				() => `key${Math.floor(Math.random() * REMOVALS)}`,
			);

			yield {
				0() {
					return { cache, keys };
				},
				bench({ cache, keys }: { cache: Cache<number>; keys: string[] }) {
					for (const key of keys) {
						cache.remove(key);
					}
					return cache.size;
				},
			};
		});

		bench(`remove LRU ${maxSize}`, function* () {
			const cache = createLRUCache(maxSize);
			for (let i = 0; i < maxSize * 10; i++) cache.set(`key${i}`, i);
			const keys = Array.from(
				{ length: REMOVALS },
				() => `key${Math.floor(Math.random() * REMOVALS)}`,
			);

			yield {
				0() {
					return { cache, keys };
				},
				bench({ cache, keys }: { cache: Cache<number>; keys: string[] }) {
					for (const key of keys) {
						cache.remove(key);
					}
					return cache.size;
				},
			};
		});
	}
});

await run();

import type { Cache } from "@/util/cache/types";

export function createFIFOCache<T>(maxSize = 1_000): Cache<T> {
	const cache = new Map<string, T>();
	const queue: string[] = [];

	return {
		get(key: string): T | undefined {
			return cache.get(key);
		},

		get size(): number {
			return cache.size;
		},

		set(key: string, value: T): void {
			if (!cache.has(key)) {
				queue.push(key);
				if (cache.size >= maxSize) {
					const oldestKey = queue.shift();
					if (oldestKey !== undefined) cache.delete(oldestKey);
				}
			}
			cache.set(key, value);
		},

		remove(key: string): void {
			if (cache.has(key)) {
				cache.delete(key);
				const index = queue.indexOf(key);
				if (index !== -1) queue.splice(index, 1);
			}
		},

		clear(): void {
			cache.clear();
			queue.length = 0;
		},
	};
}

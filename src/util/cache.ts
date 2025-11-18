export type Cache<T> = {
	get(key: string): T | undefined;
	set(key: string, value: T): void;
	remove(key: string): void;
	clear(): void;
};

export function createCache<T>(maxSize = 1_000): Cache<T> {
	const cache = new Map<string, T>();
	let size = 0;

	return {
		get(key: string): T | undefined {
			return cache.get(key);
		},

		set(key: string, value: T): void {
			if (cache.has(key)) {
				cache.set(key, value);
			} else {
				cache.set(key, value);
				size++;
				if (size > maxSize) {
					const oldestKey = cache.keys().next().value;
					if (oldestKey !== undefined) cache.delete(oldestKey);
					size--;
				}
			}
		},

		remove(key: string): void {
			if (cache.has(key)) {
				cache.delete(key);
				size--;
			}
		},

		clear(): void {
			cache.clear();
			size = 0;
		},
	};
}

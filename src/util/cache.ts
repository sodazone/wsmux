export type Cache<T> = {
	get(key: string): T | undefined;
	set(key: string, value: T): void;
	remove(key: string): void;
	size: number;
	clear(): void;
};

export function createCache<T>(maxSize = 1_000): Cache<T> {
	const cache = new Map<string, T>();
	let _size = 0;

	return {
		get(key: string): T | undefined {
			return cache.get(key);
		},

		get size(): number {
			return _size;
		},

		set(key: string, value: T): void {
			if (cache.has(key)) {
				cache.set(key, value);
			} else {
				cache.set(key, value);
				_size++;
				if (_size > maxSize) {
					const oldestKey = cache.keys().next().value;
					if (oldestKey !== undefined) cache.delete(oldestKey);
					_size--;
				}
			}
		},

		remove(key: string): void {
			if (cache.has(key)) {
				cache.delete(key);
				_size--;
			}
		},

		clear(): void {
			cache.clear();
			_size = 0;
		},
	};
}

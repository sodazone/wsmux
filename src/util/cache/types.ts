export type Cache<T> = {
	get(key: string): T | undefined;
	set(key: string, value: T): void;
	remove(key: string): void;
	size: number;
	clear(): void;
};

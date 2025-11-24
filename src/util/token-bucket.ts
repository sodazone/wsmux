export type TokenBucket = ReturnType<typeof createTokenBucket>;

export function createTokenBucket(capacity: number, windowMs: number) {
	let tokens = capacity;
	let last = performance.now();

	return {
		allow(): boolean {
			const now = performance.now();

			// refill
			const delta = now - last;
			last = now;

			const refillRate = capacity / windowMs; // tokens per ms
			tokens = Math.min(capacity, tokens + delta * refillRate);

			if (tokens >= 1) {
				tokens -= 1;
				return true;
			}
			return false;
		},
	};
}

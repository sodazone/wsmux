import { describe, expect, it } from "bun:test";
import { createTokenBucket } from "./token-bucket";

describe("Token Bucket", () => {
	it("allows bursts up to capacity and blocks after", () => {
		const capacity = 5;
		const windowMs = 100;
		const bucket = createTokenBucket(capacity, windowMs);

		for (let i = 0; i < capacity; i++) {
			expect(bucket.allow()).toBe(true);
		}

		expect(bucket.allow()).toBe(false);
	});

	it("refills tokens over time", async () => {
		const capacity = 5;
		const windowMs = 10;
		const bucket = createTokenBucket(capacity, windowMs);

		for (let i = 0; i < capacity; i++) bucket.allow();

		await Bun.sleep(5);

		let allowedCount = 0;
		for (let i = 0; i < 3; i++) {
			if (bucket.allow()) allowedCount++;
		}

		expect(allowedCount).toBe(2);
		expect(bucket.allow()).toBe(false);
	});

	it("resets to full capacity after full window", async () => {
		const capacity = 5;
		const windowMs = 5;
		const bucket = createTokenBucket(capacity, windowMs);

		for (let i = 0; i < capacity; i++) bucket.allow();

		await Bun.sleep(windowMs);

		let allowedCount = 0;
		for (let i = 0; i < capacity; i++) {
			if (bucket.allow()) allowedCount++;
		}

		expect(allowedCount).toBe(capacity);
		expect(bucket.allow()).toBe(false);
	});
});

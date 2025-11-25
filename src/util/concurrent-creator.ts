import { getLogger } from "@logtape/logtape";

const logger = getLogger("concurrent");

export interface ConcurrentCreatorOptions {
	maxWaiting?: number;
	label?: string;
}

export class TooManyWaitersError extends Error {
	constructor(message = "Too many concurrent waiters") {
		super(message);
		this.name = "TooManyWaitersError";
	}
}

export function createConcurrentCreator(
	options: ConcurrentCreatorOptions = {},
) {
	const creating = new Map<
		string,
		{ promise: Promise<unknown>; waiting: number }
	>();
	const maxWaiting = options.maxWaiting ?? 5;
	const label = options.label ?? "ConcurrentCreator";

	return async function getOrCreate<T>(
		key: string,
		createFn: () => Promise<T>,
	): Promise<T> {
		const existing = creating.get(key);

		if (!existing) {
			const promise = (async () => {
				try {
					return await createFn();
				} finally {
					creating.delete(key);
				}
			})();
			creating.set(key, { promise, waiting: 0 });
			const result = await promise;
			return result;
		}

		if (existing.waiting + 1 >= maxWaiting) {
			logger.warn(
				(l) =>
					l`[${label}] Too many concurrent waits (${existing.waiting + 1}) for ${key}`,
			);
			throw new TooManyWaitersError();
		}

		existing.waiting++;
		try {
			const result = await existing.promise;
			return result as T;
		} finally {
			existing.waiting--;
		}
	};
}

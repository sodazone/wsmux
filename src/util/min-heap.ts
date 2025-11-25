/**
 * Generic Min-Heap.
 */
export function createMinHeap<T>(getKey: (item: T) => number) {
	type Item = T | undefined;

	const heap: Item[] = [];
	const indexMap = new Map<T, number>();

	const getIndex = (item: T | undefined) =>
		item ? (indexMap.get(item) ?? null) : null;

	const setIndex = (item: T | undefined, idx: number | null) => {
		if (!item) return;
		if (idx === null) indexMap.delete(item);
		else indexMap.set(item, idx);
	};

	function swap(i: number, j: number) {
		const a = heap[i];
		const b = heap[j];

		heap[i] = b;
		heap[j] = a;

		setIndex(a, j);
		setIndex(b, i);
	}

	function siftUp(i: number) {
		let idx = i;
		while (idx > 0) {
			const p = (idx - 1) >>> 1;
			const a = heap[idx]!;
			const b = heap[p]!;

			if (getKey(a) >= getKey(b)) break;

			swap(idx, p);
			idx = p;
		}
	}

	function siftDown(i: number) {
		let idx = i;
		const n = heap.length;

		while (true) {
			const l = (idx << 1) + 1;
			const r = l + 1;
			let smallest = idx;

			if (l < n && getKey(heap[l]!) < getKey(heap[smallest]!)) {
				smallest = l;
			}
			if (r < n && getKey(heap[r]!) < getKey(heap[smallest]!)) {
				smallest = r;
			}
			if (smallest === idx) break;

			swap(idx, smallest);
			idx = smallest;
		}
	}

	function push(item: T | undefined) {
		if (!item) return;

		const idx = heap.length;
		heap.push(item);
		setIndex(item, idx);

		siftUp(idx);
	}

	function remove(item: T | undefined) {
		if (!item) return;

		const idx = getIndex(item);
		if (idx == null) return;

		const last = heap.pop()!;
		setIndex(item, null);

		// nothing to repair
		if (idx === heap.length) return;

		heap[idx] = last;
		setIndex(last, idx);

		siftDown(idx);
		siftUp(idx);
	}

	function update(item: T | undefined) {
		if (!item) return;

		const idx = getIndex(item);
		if (idx == null) return;

		siftDown(idx);
		siftUp(idx);
	}

	function peek(): T | undefined {
		return heap[0];
	}

	function size() {
		return heap.length;
	}

	function clear() {
		heap.length = 0;
		indexMap.clear();
	}

	function items() {
		return heap as T[];
	}

	function at(index: number): T | undefined {
		return heap[index];
	}

	return { push, remove, update, peek, size, clear, items, at };
}

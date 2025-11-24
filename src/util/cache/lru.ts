import type { Cache } from "./types";

type Node<T> = {
	key: string;
	value: T;
	prev: Node<T> | null;
	next: Node<T> | null;
};

export function createLRUCache<T>(maxSize = 1_000): Cache<T> {
	const map = new Map<string, Node<T>>();
	let head: Node<T> | null = null;
	let tail: Node<T> | null = null;
	let _size = 0;

	function moveToHead(node: Node<T>) {
		if (node === head) return;

		if (node.prev) node.prev.next = node.next;
		if (node.next) node.next.prev = node.prev;

		if (node === tail) tail = node.prev;

		node.prev = null;
		node.next = head;
		if (head) head.prev = node;
		head = node;

		if (!tail) tail = head;
	}

	function removeTail() {
		if (!tail) return;
		map.delete(tail.key);
		if (tail.prev) {
			tail = tail.prev;
			tail.next = null;
		} else {
			head = tail = null;
		}
		_size--;
	}

	return {
		get(key: string): T | undefined {
			const node = map.get(key);
			if (!node) return undefined;
			moveToHead(node);
			return node.value;
		},

		set(key: string, value: T): void {
			let node = map.get(key);
			if (node) {
				node.value = value;
				moveToHead(node);
			} else {
				node = { key, value, prev: null, next: head };
				if (head) head.prev = node;
				head = node;
				if (!tail) tail = node;
				map.set(key, node);
				_size++;
				if (_size > maxSize) removeTail();
			}
		},

		remove(key: string): void {
			const node = map.get(key);
			if (!node) return;

			if (node.prev) node.prev.next = node.next;
			if (node.next) node.next.prev = node.prev;
			if (node === head) head = node.next;
			if (node === tail) tail = node.prev;

			map.delete(key);
			_size--;
		},

		clear(): void {
			map.clear();
			head = tail = null;
			_size = 0;
		},

		get size(): number {
			return _size;
		},
	};
}

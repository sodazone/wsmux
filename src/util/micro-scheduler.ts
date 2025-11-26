const MAX_PENDING_TASKS = 4096; // must be power of 2

const tasks = new Array(MAX_PENDING_TASKS);
let rqHead = 0;
let rqTail = 0;
let rqScheduled = false;

export function enqueueTask(fn: () => void) {
	tasks[rqTail] = fn;
	rqTail = (rqTail + 1) & (MAX_PENDING_TASKS - 1);

	if (!rqScheduled) {
		rqScheduled = true;
		queueMicrotask(flushTaskQueue);
	}
}

function flushTaskQueue() {
	rqScheduled = false;

	while (rqHead !== rqTail) {
		const fn = tasks[rqHead];
		tasks[rqHead] = null;
		rqHead = (rqHead + 1) & (MAX_PENDING_TASKS - 1);
		fn();
	}
}

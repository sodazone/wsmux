import { heapStats, memoryUsage } from "bun:jsc";
import { Gauge } from "prom-client";

const jscMetrics = {
	memCurrent: new Gauge({
		name: "bun_mem_current_bytes",
		help: "Current bytes allocated by the Bun/JSC memory allocator",
	}),
	memPeak: new Gauge({
		name: "bun_mem_peak_bytes",
		help: "Peak memory allocated by JSC",
	}),
	memCurrentCommit: new Gauge({
		name: "bun_mem_current_commit_bytes",
		help: "Committed memory reserved by JSC",
	}),
	memPeakCommit: new Gauge({
		name: "bun_mem_peak_commit_bytes",
		help: "Peak committed memory",
	}),
	memPageFaults: new Gauge({
		name: "bun_mem_page_faults_total",
		help: "Total page faults",
	}),

	heapSize: new Gauge({
		name: "bun_heap_size_bytes",
		help: "JS heap size currently in use",
	}),
	heapCapacity: new Gauge({
		name: "bun_heap_capacity_bytes",
		help: "Total JS heap capacity",
	}),
	heapExtra: new Gauge({
		name: "bun_heap_extra_memory_bytes",
		help: "Extra memory used by JS runtime",
	}),

	heapObjects: new Gauge({
		name: "bun_heap_object_count",
		help: "Total number of JS objects",
	}),
	heapProtectedObjects: new Gauge({
		name: "bun_heap_protected_object_count",
		help: "Protected objects (GC roots)",
	}),

	globals: new Gauge({
		name: "bun_heap_global_object_count",
		help: "Global objects",
	}),
	globalsProtected: new Gauge({
		name: "bun_heap_protected_global_object_count",
		help: "Protected global objects",
	}),
};

export function startJscMetrics(intervalMs = 5_000) {
	setInterval(() => {
		const mem = memoryUsage();
		const heap = heapStats();

		jscMetrics.memCurrent.set(mem.current);
		jscMetrics.memPeak.set(mem.peak);
		jscMetrics.memCurrentCommit.set(mem.currentCommit);
		jscMetrics.memPeakCommit.set(mem.peakCommit);
		jscMetrics.memPageFaults.set(mem.pageFaults);

		jscMetrics.heapSize.set(heap.heapSize);
		jscMetrics.heapCapacity.set(heap.heapCapacity);
		jscMetrics.heapExtra.set(heap.extraMemorySize);

		jscMetrics.heapObjects.set(heap.objectCount);
		jscMetrics.heapProtectedObjects.set(heap.protectedObjectCount);

		jscMetrics.globals.set(heap.globalObjectCount);
		jscMetrics.globalsProtected.set(heap.protectedGlobalObjectCount);
	}, intervalMs).unref();
}

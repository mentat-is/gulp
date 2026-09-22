import type { Operation } from "@/entities/Operation";
import type { EventStoreNamespace } from "@/event-store";
import {
	createEmptyRendererBenchmarkDiagnostics,
	type RendererBenchmarkStartOptions,
	type RendererBenchmarkWriteProgress,
} from "@/page/RendererTest.benchmark";
import {
	RendererBenchmarkWorkerStore,
	type RendererBenchmarkWorkerLike,
} from "@/page/RendererTest.worker-store";
import type {
	RendererBenchmarkWorkerCommand,
	RendererBenchmarkWorkerEvent,
} from "@/page/RendererTest.worker-protocol";

const NAMESPACE: EventStoreNamespace = {
	cacheNamespaceId: "renderer-test",
	operationId: "renderer-test" as Operation.Id,
};
const OPTIONS: RendererBenchmarkStartOptions = {
	targetEvents: 10,
	sourceIds: [],
	generation: 1,
	documentMinTimestamp: 0,
	frameSpanMs: 100,
};

/** In-memory worker boundary that delivers only explicitly emitted messages. */
class BenchmarkWorkerSpy implements RendererBenchmarkWorkerLike {
	public onmessage: ((event: MessageEvent<RendererBenchmarkWorkerEvent>) => void) | null = null;
	public onerror: ((event: ErrorEvent) => void) | null = null;
	public readonly commands: RendererBenchmarkWorkerCommand[] = [];
	public terminated = false;

	/** Records one page command without running work on the test main realm. */
	public postMessage(command: RendererBenchmarkWorkerCommand): void {
		this.commands.push(command);
	}

	/** Records termination of the isolated worker. */
	public terminate(): void {
		this.terminated = true;
	}

	/** Delivers one correlated worker event to the page adapter. */
	public emit(event: RendererBenchmarkWorkerEvent): void {
		this.onmessage?.({ data: event } as MessageEvent<RendererBenchmarkWorkerEvent>);
	}
}

/** Produces one detached progress snapshot with a distinct committed total. */
function progress(persistedEvents: number, done = false): RendererBenchmarkWriteProgress {
	const diagnostics = createEmptyRendererBenchmarkDiagnostics(10);
	diagnostics.status = done ? "complete" : "writing";
	diagnostics.persistedEvents = persistedEvents;
	return {
		status: diagnostics.status,
		persistedEvents,
		targetEvents: 10,
		done,
		diagnostics,
	};
}

describe("RendererTest dedicated benchmark worker boundary", () => {
	it("coalesces progress and preserves the peak main cache measurement", async () => {
		const worker = new BenchmarkWorkerSpy();
		let cacheDocuments = 0;
		const store = new RendererBenchmarkWorkerStore(
			worker,
			NAMESPACE,
			() => undefined,
			() => cacheDocuments,
		);
		const started = store.start(OPTIONS);
		expect(worker.commands[0].type).toBe("START");
		worker.emit({ type: "STARTED", progress: progress(0) });
		await started;

		worker.emit({ type: "PROGRESS", progress: progress(1) });
		worker.emit({ type: "PROGRESS", progress: progress(2) });
		worker.emit({ type: "PROGRESS", progress: progress(3) });
		expect((await store.appendNextPage()).persistedEvents).toBe(3);
		cacheDocuments = 7;
		expect(store.getDiagnostics().maximumMainCacheDocuments).toBe(7);
		cacheDocuments = 1;
		worker.emit({ type: "FINISHED", progress: progress(10, true) });
		expect((await store.appendNextPage()).persistedEvents).toBe(10);
		expect((await store.finalize()).maximumMainCacheDocuments).toBe(7);
		store.close();
		expect(worker.terminated).toBe(true);
	});

	it("recovers from a projection error without poisoning later requests", async () => {
		const worker = new BenchmarkWorkerSpy();
		const store = new RendererBenchmarkWorkerStore(
			worker,
			NAMESPACE,
			() => undefined,
			() => 0,
		);
		const descriptor = {
			operationEpoch: 1,
			viewportRevision: 1,
			projectionRevision: 1,
			frame: { min: 0, max: 100 },
			width: 800,
			scale: 2.5,
			scrollX: 0,
			horizontalOverscanPixels: 800,
			sources: [],
		};
		const failed = store.publishRenderWindow(descriptor);
		const firstCommand = worker.commands[0];
		expect(firstCommand.type).toBe("PROJECT");
		if (firstCommand.type !== "PROJECT") throw new Error("Expected projection command.");
		worker.emit({
			type: "PROJECTION",
			requestId: firstCommand.requestId,
			projection: null,
			diagnostics: progress(1).diagnostics,
			error: "transient read",
		});
		await expect(failed).rejects.toThrow("transient read");

		const retried = store.publishRenderWindow(descriptor);
		const secondCommand = worker.commands[1];
		if (secondCommand.type !== "PROJECT") throw new Error("Expected projection retry.");
		expect(secondCommand.requestId > firstCommand.requestId).toBe(true);
		worker.emit({
			type: "PROJECTION",
			requestId: secondCommand.requestId,
			projection: null,
			diagnostics: progress(2).diagnostics,
		});
		expect(await retried).toBe(false);
		store.close();
	});

	it("settles cancellation when the worker fails and rejects stale progress", async () => {
		const worker = new BenchmarkWorkerSpy();
		const store = new RendererBenchmarkWorkerStore(
			worker,
			NAMESPACE,
			() => undefined,
			() => 0,
		);
		const pendingProgress = store.appendNextPage();
		const cancellation = store.cancel();
		worker.emit({ type: "ERROR", message: "IndexedDB failed" });
		await expect(pendingProgress).rejects.toThrow("IndexedDB failed");
		const diagnostics = await cancellation;
		expect(diagnostics.status).toBe("failed");
		expect(diagnostics.failureReason).toBe("IndexedDB failed");
		await expect(store.appendNextPage()).rejects.toThrow("IndexedDB failed");
		store.close();
	});

	it("isolates a restarted run from late messages sent by the prior worker", async () => {
		const oldWorker = new BenchmarkWorkerSpy();
		const oldStore = new RendererBenchmarkWorkerStore(
			oldWorker, NAMESPACE, () => undefined, () => 0,
		);
		const oldStart = oldStore.start(OPTIONS);
		oldStore.close();
		await expect(oldStart).rejects.toThrow("closed");

		const nextWorker = new BenchmarkWorkerSpy();
		const nextStore = new RendererBenchmarkWorkerStore(
			nextWorker, NAMESPACE, () => undefined, () => 0,
		);
		const nextStart = nextStore.start(OPTIONS);
		oldWorker.emit({ type: "FINISHED", progress: progress(10, true) });
		expect(nextStore.getDiagnostics().persistedEvents).toBe(0);
		nextWorker.emit({ type: "STARTED", progress: progress(0) });
		expect((await nextStart).persistedEvents).toBe(0);
		nextStore.close();
	});
});

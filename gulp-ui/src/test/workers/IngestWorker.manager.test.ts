import type { Request } from "@/entities/Request";
import {
	INGEST_WORKER_IDLE_TIMEOUT_MS,
	IngestWorkerManager,
	type IngestTask,
} from "@/workers/IngestWorker.manager";

jest.mock("@/workers/IngestWorker.factory", () => ({
	createIngestWorker: () => {
		throw new Error("Tests must inject an ingestion worker factory.");
	},
}));

class FakeIngestWorker {
	public onmessage: ((event: MessageEvent) => void) | null = null;
	public readonly messages: Array<{ payload: { req_id: Request.Id } }> = [];
	public terminateCalls = 0;

	/** Records one manager command. */
	public postMessage(message: { payload: { req_id: Request.Id } }): void {
		this.messages.push(message);
	}

	/** Records idle disposal. */
	public terminate(): void {
		this.terminateCalls += 1;
	}

	/** Delivers one terminal upload event to the manager. */
	public finish(type: "DONE" | "ERROR", message = "failed"): void {
		const reqId = this.messages.at(-1)?.payload.req_id;
		if (!reqId) throw new Error("Fake worker has no active request.");
		this.onmessage?.(
			new MessageEvent("message", {
				data: { type, payload: { req_id: reqId, message } },
			}),
		);
	}
}

/** Creates one minimal upload task. */
function createTask(id: number, callbacks: Partial<IngestTask> = {}): IngestTask {
	return {
		req_id: `ingestion-${id}` as Request.Id,
		file: new File([`file-${id}`], `file-${id}.txt`),
		operation_id: "operation",
		context_name: "context",
		ws_id: "socket",
		settings: {},
		server: "http://gulp.test",
		token: "token",
		...callbacks,
	};
}

describe("IngestWorkerManager", () => {
	beforeEach(() => jest.useFakeTimers());
	afterEach(() => {
		jest.useRealTimers();
	});

	it("reuses at most five workers and terminates the idle pool after its grace period", () => {
		const workers: FakeIngestWorker[] = [];
		const manager = new IngestWorkerManager(() => {
			const worker = new FakeIngestWorker();
			workers.push(worker);
			return worker as unknown as Worker;
		});

		for (let id = 1; id <= 6; id++) manager.enqueue(createTask(id));
		expect(workers).toHaveLength(5);

		workers[0].finish("DONE");
		expect(workers).toHaveLength(5);
		expect(workers[0].messages).toHaveLength(2);

		for (const worker of workers.slice(1)) worker.finish("DONE");
		workers[0].finish("DONE");
		jest.advanceTimersByTime(INGEST_WORKER_IDLE_TIMEOUT_MS - 1);
		expect(workers.every((worker) => worker.terminateCalls === 0)).toBe(true);

		jest.advanceTimersByTime(1);
		expect(workers.every((worker) => worker.terminateCalls === 1)).toBe(true);
	});

	it("cancels idle disposal when a new upload arrives", () => {
		const workers: FakeIngestWorker[] = [];
		const manager = new IngestWorkerManager(() => {
			const worker = new FakeIngestWorker();
			workers.push(worker);
			return worker as unknown as Worker;
		});

		manager.enqueue(createTask(1));
		workers[0].finish("DONE");
		jest.advanceTimersByTime(INGEST_WORKER_IDLE_TIMEOUT_MS - 1_000);
		manager.enqueue(createTask(2));
		jest.advanceTimersByTime(1_000);

		expect(workers).toHaveLength(1);
		expect(workers[0].terminateCalls).toBe(0);
		workers[0].finish("DONE");
		jest.advanceTimersByTime(INGEST_WORKER_IDLE_TIMEOUT_MS);
		expect(workers[0].terminateCalls).toBe(1);
	});

	it("holds the five upload slots until their ingestion barriers settle", async () => {
		const workers: FakeIngestWorker[] = [];
		const releases: Array<() => void> = [];
		const barriers = Array.from(
			{ length: 6 },
			() =>
				new Promise<void>((resolve) => {
					releases.push(resolve);
				}),
		);
		const manager = new IngestWorkerManager(() => {
			const worker = new FakeIngestWorker();
			workers.push(worker);
			return worker as unknown as Worker;
		});

		for (let id = 1; id <= 6; id++) {
			manager.enqueue(createTask(id, { releaseWhen: barriers[id - 1] }));
		}
		for (const worker of workers) worker.finish("DONE");
		expect(workers.reduce((count, worker) => count + worker.messages.length, 0)).toBe(
			5,
		);

		for (const release of releases.slice(0, 5)) release();
		await Promise.resolve();
		expect(workers.reduce((count, worker) => count + worker.messages.length, 0)).toBe(
			6,
		);
		const reusedWorker = workers.find((worker) => worker.messages.length === 2);
		if (!reusedWorker) throw new Error("Expected one worker to be reused.");
		reusedWorker.finish("DONE");
		releases[5]();
		await Promise.resolve();
		jest.advanceTimersByTime(INGEST_WORKER_IDLE_TIMEOUT_MS);
		expect(workers.every((worker) => worker.terminateCalls === 1)).toBe(true);
	});

	it("runs error callbacks before releasing and disposing the worker", () => {
		const workers: FakeIngestWorker[] = [];
		const onError = jest.fn();
		const manager = new IngestWorkerManager(() => {
			const worker = new FakeIngestWorker();
			workers.push(worker);
			return worker as unknown as Worker;
		});

		manager.enqueue(createTask(1, { onError }));
		workers[0].finish("ERROR", "upload failed");
		expect(onError).toHaveBeenCalledWith("upload failed");

		manager.enqueue(createTask(2));
		expect(workers).toHaveLength(1);
		expect(workers[0].messages).toHaveLength(2);
		workers[0].finish("DONE");
		jest.advanceTimersByTime(INGEST_WORKER_IDLE_TIMEOUT_MS);
		expect(workers[0].terminateCalls).toBe(1);
	});
});

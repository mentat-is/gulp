import type { Doc } from "@/entities/Doc";
import type { EventSourceSummary, EventStoreNamespace, EventStoreViewportDescriptor } from "@/event-store";
import { renderWindowProjectionBus } from "@/render-worker/RenderWindowProjection.bus";
import {
	createEmptyRendererBenchmarkDiagnostics,
	evaluateRendererBenchmarkResourceGate,
	type RendererBenchmarkDiagnostics,
	type RendererBenchmarkStartOptions,
	type RendererBenchmarkWriteProgress,
} from "./RendererTest.benchmark";
import type {
	RendererBenchmarkWorkerCommand,
	RendererBenchmarkWorkerEvent,
} from "./RendererTest.worker-protocol";

/** Worker surface injectable into benchmark protocol tests. */
export interface RendererBenchmarkWorkerLike {
	onmessage: ((event: MessageEvent<RendererBenchmarkWorkerEvent>) => void) | null;
	onerror: ((event: ErrorEvent) => void) | null;
	postMessage(command: RendererBenchmarkWorkerCommand): void;
	terminate(): void;
}

interface PendingProjection {
	descriptor: EventStoreViewportDescriptor;
	resolve: (published: boolean) => void;
	reject: (error: Error) => void;
}

interface PendingUpsert {
	resolve: (summaries: EventSourceSummary[]) => void;
	reject: (error: Error) => void;
}

/**
 * Adapts a dedicated synthetic EventStore worker to the benchmark page lifecycle.
 * Progress is coalesced to one pending snapshot instead of retaining a page queue.
 */
export class RendererBenchmarkWorkerStore {
	private diagnostics = createEmptyRendererBenchmarkDiagnostics();
	private bufferedProgress: RendererBenchmarkWriteProgress | null = null;
	private terminalProgress: RendererBenchmarkWriteProgress | null = null;
	private pendingStart: {
		resolve: (progress: RendererBenchmarkWriteProgress) => void;
		reject: (error: Error) => void;
	} | null = null;
	private pendingProgress: {
		resolve: (progress: RendererBenchmarkWriteProgress) => void;
		reject: (error: Error) => void;
	} | null = null;
	private pendingFinish: {
		resolve: (diagnostics: RendererBenchmarkDiagnostics) => void;
		reject: (error: Error) => void;
	} | null = null;
	private pendingCancel: ((diagnostics: RendererBenchmarkDiagnostics) => void) | null = null;
	private readonly projections = new Map<number, PendingProjection>();
	private readonly upserts = new Map<number, PendingUpsert>();
	private requestId = 0;
	private failure: Error | null = null;
	private closed = false;
	private maximumMainCacheDocuments = 0;
	private mainHeapStartBytes = 0;
	private mainHeapPeakBytes = 0;
	private mainHeapEndBytes = 0;

	/**
	 * Attaches the bounded progress and projection protocol to a worker.
	 * @param worker Dedicated benchmark worker.
	 * @param namespace IndexedDB identity shared with the main reader.
	 * @param publishSourceSummary Main-realm summary publication callback.
	 * @param readMainCacheDocuments Main-realm exact-document cache size.
	 */
	public constructor(
		private readonly worker: RendererBenchmarkWorkerLike,
		private readonly namespace: EventStoreNamespace,
		private readonly publishSourceSummary: (summary: EventSourceSummary) => void,
		private readonly readMainCacheDocuments: () => number,
	) {
		worker.onmessage = (message) => this.handleEvent(message.data);
		worker.onerror = (event) => this.fail(new Error(event.message || "Renderer benchmark worker failed."));
	}

	/** Starts one deterministic worker-owned generation. */
	public start(options: RendererBenchmarkStartOptions): Promise<RendererBenchmarkWriteProgress> {
		if (this.closed || this.pendingStart) {
			return Promise.reject(new Error("Renderer benchmark worker cannot start."));
		}
		this.diagnostics = createEmptyRendererBenchmarkDiagnostics(
			options.targetEvents,
			options.cacheMode ?? "cold",
		);
		this.maximumMainCacheDocuments = 0;
		this.mainHeapStartBytes = this.readMainHeapUsageBytes() ?? 0;
		this.mainHeapPeakBytes = this.mainHeapStartBytes;
		this.mainHeapEndBytes = this.mainHeapStartBytes;
		return new Promise((resolve, reject) => {
			this.pendingStart = { resolve, reject };
			this.post({ type: "START", namespace: this.namespace, options });
		});
	}

	/** Waits for the newest bounded progress snapshot or terminal result. */
	public appendNextPage(): Promise<RendererBenchmarkWriteProgress> {
		if (this.failure) return Promise.reject(this.failure);
		if (this.closed) return Promise.reject(new Error("Renderer benchmark worker closed."));
		if (this.bufferedProgress) {
			const progress = this.bufferedProgress;
			this.bufferedProgress = null;
			return Promise.resolve(progress);
		}
		if (this.terminalProgress) return Promise.resolve(this.terminalProgress);
		return new Promise((resolve, reject) => {
			this.pendingProgress = { resolve, reject };
		});
	}

	/** Waits for every source's terminal manifest and diagnostics. */
	public finalize(): Promise<RendererBenchmarkDiagnostics> {
		if (this.failure) return Promise.reject(this.failure);
		if (this.terminalProgress) return Promise.resolve(this.getDiagnostics());
		return new Promise((resolve, reject) => {
			this.pendingFinish = { resolve, reject };
		});
	}

	/** Applies a bounded release-gate document delta inside the storage worker. */
	public upsertDocuments(documents: readonly Doc.Type[]): Promise<EventSourceSummary[]> {
		if (this.failure) return Promise.reject(this.failure);
		const requestId = ++this.requestId;
		return new Promise((resolve, reject) => {
			this.upserts.set(requestId, { resolve, reject });
			this.post({ type: "UPSERT", requestId, documents: [...documents] });
		});
	}

	/** Transfers a bounded worker projection to the mounted Canvas listener. */
	public publishRenderWindow(descriptor: EventStoreViewportDescriptor): Promise<boolean> {
		if (this.failure) return Promise.reject(this.failure);
		const requestId = ++this.requestId;
		return new Promise((resolve, reject) => {
			this.projections.set(requestId, { descriptor, resolve, reject });
			this.post({ type: "PROJECT", requestId, descriptor });
		});
	}

	/** Returns worker diagnostics with the current bounded main-reader cache size. */
	public getDiagnostics(): RendererBenchmarkDiagnostics {
		const mainCacheDocuments = this.readMainCacheDocuments();
		this.maximumMainCacheDocuments = Math.max(
			this.maximumMainCacheDocuments,
			mainCacheDocuments,
		);
		const mainHeapBytes = this.readMainHeapUsageBytes();
		if (mainHeapBytes !== null) {
			this.mainHeapPeakBytes = Math.max(this.mainHeapPeakBytes, mainHeapBytes);
			this.mainHeapEndBytes = mainHeapBytes;
		}
		const diagnostics = {
			...this.diagnostics,
			mainCacheDocuments,
			maximumMainCacheDocuments: this.maximumMainCacheDocuments,
			mainHeapSupported: this.mainHeapStartBytes > 0,
			mainHeapStartBytes: this.mainHeapStartBytes,
			mainHeapPeakBytes: this.mainHeapPeakBytes,
			mainHeapEndBytes: this.mainHeapEndBytes,
			mainHeapDeltaBytes: Math.max(0, this.mainHeapEndBytes - this.mainHeapStartBytes),
		};
		return {
			...diagnostics,
			resourceGatePassed: evaluateRendererBenchmarkResourceGate(diagnostics).passed,
		};
	}

	/** Requests cancellation and waits for the worker's generation cleanup. */
	public cancel(): Promise<RendererBenchmarkDiagnostics> {
		if (this.closed || this.failure) return Promise.resolve(this.getDiagnostics());
		if (this.pendingCancel) {
			return Promise.reject(new Error("Renderer benchmark cancellation is already pending."));
		}
		return new Promise((resolve) => {
			this.pendingCancel = resolve;
			this.post({ type: "CANCEL" });
		});
	}

	/** Terminates the isolated worker and rejects outstanding requests. */
	public close(): void {
		if (this.closed) return;
		this.closed = true;
		this.worker.terminate();
		this.worker.onmessage = null;
		this.worker.onerror = null;
		this.fail(new Error("Renderer benchmark worker closed."));
	}

	/** Sends one typed command and propagates structured-clone failures. */
	private post(command: RendererBenchmarkWorkerCommand): void {
		try {
			this.worker.postMessage(command);
		} catch (error) {
			this.fail(error instanceof Error ? error : new Error(String(error)));
		}
	}

	/** Publishes the newest coalesced snapshot to the active page waiter. */
	private publishProgress(progress: RendererBenchmarkWriteProgress): void {
		this.diagnostics = progress.diagnostics;
		if (this.pendingProgress) {
			this.pendingProgress.resolve(progress);
			this.pendingProgress = null;
		} else {
			this.bufferedProgress = progress;
		}
	}

	/** Routes one correlated worker event to its waiting page operation. */
	private handleEvent(event: RendererBenchmarkWorkerEvent): void {
		switch (event.type) {
			case "STARTED":
				this.diagnostics = event.progress.diagnostics;
				this.pendingStart?.resolve(event.progress);
				this.pendingStart = null;
				break;
			case "PROGRESS":
				this.publishProgress(event.progress);
				break;
			case "FINISHED":
				this.terminalProgress = event.progress;
				this.publishProgress(event.progress);
				this.pendingFinish?.resolve(this.getDiagnostics());
				this.pendingFinish = null;
				break;
			case "CANCELLED":
				this.diagnostics = event.diagnostics;
				this.pendingCancel?.(this.getDiagnostics());
				this.pendingCancel = null;
				this.terminalProgress = {
					status: event.diagnostics.status,
					persistedEvents: event.diagnostics.persistedEvents,
					targetEvents: event.diagnostics.targetEvents,
					done: true,
					diagnostics: event.diagnostics,
				};
				this.publishProgress(this.terminalProgress);
				this.pendingFinish?.resolve(this.getDiagnostics());
				this.pendingFinish = null;
				break;
			case "SOURCE_SUMMARY":
				this.publishSourceSummary(event.summary);
				break;
			case "PROJECTION": {
				this.diagnostics = event.diagnostics;
				const pending = this.projections.get(event.requestId);
				if (!pending) break;
				this.projections.delete(event.requestId);
				if (event.error) {
					pending.reject(new Error(event.error));
				} else {
					try {
						pending.resolve(event.projection
							? renderWindowProjectionBus.publish(event.projection, pending.descriptor)
							: false);
					} catch (error) {
						pending.reject(error instanceof Error ? error : new Error(String(error)));
					}
				}
				break;
			}
			case "UPSERTED": {
				this.diagnostics = event.diagnostics;
				const pending = this.upserts.get(event.requestId);
				if (!pending) break;
				this.upserts.delete(event.requestId);
				if (event.error) pending.reject(new Error(event.error));
				else pending.resolve(event.summaries);
				break;
			}
			case "ERROR":
				this.fail(new Error(event.message));
				break;
		}
	}

	/** Rejects all correlated operations after one fatal worker failure. */
	private fail(error: Error): void {
		this.failure = error;
		this.diagnostics = {
			...this.diagnostics,
			status: "failed",
			failureReason: error.message,
		};
		this.pendingStart?.reject(error);
		this.pendingStart = null;
		this.pendingProgress?.reject(error);
		this.pendingProgress = null;
		this.pendingFinish?.reject(error);
		this.pendingFinish = null;
		this.pendingCancel?.(this.getDiagnostics());
		this.pendingCancel = null;
		for (const pending of this.projections.values()) pending.reject(error);
		this.projections.clear();
		for (const pending of this.upserts.values()) pending.reject(error);
		this.upserts.clear();
	}

	/** Reads Chromium's optional heap counter for the page realm. */
	private readMainHeapUsageBytes(): number | null {
		const memory = (performance as Performance & {
			memory?: { usedJSHeapSize?: number };
		}).memory;
		return typeof memory?.usedJSHeapSize === "number" &&
			Number.isFinite(memory.usedJSHeapSize)
			? memory.usedJSHeapSize
			: null;
	}
}

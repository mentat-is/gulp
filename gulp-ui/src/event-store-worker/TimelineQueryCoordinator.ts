import type {
	DocumentStreamDescriptor,
	DocumentStreamStateChange,
	QueryWorkerInitialization,
	QuerySocketMessage,
	TimelineQueryRequest,
	TimelineQueryUpdate,
} from "@/event-stream";
import type { EventSourceQueryCacheReconciliation } from "@/event-store/EventStore.types";
import type { Source } from "@/entities/Source";

interface TimelineQueryHooks {
	lookup: (request: TimelineQueryRequest, mode: "initial" | "reuse") => Promise<EventSourceQueryCacheReconciliation | null>;
	register: (descriptor: DocumentStreamDescriptor) => Promise<void>;
	cancel: (requestId: string) => Promise<void>;
	publish: (update: TimelineQueryUpdate) => void;
	isReady: () => boolean;
}

interface QueryJob {
	request: TimelineQueryRequest;
	abort: AbortController;
	stage: "checking" | "ready" | "running";
	sequence: number;
	dispatched: boolean;
	initialized?: boolean;
	activation?: EventSourceQueryCacheReconciliation;
	settle: () => void;
	terminal: Promise<void>;
}

/** Owns source-local cache decisions and bounded HTTP scheduling inside QueryWorker. */
export class TimelineQueryCoordinator {
	private readonly jobs = new Map<string, QueryJob>();
	private readonly initialized = new Map<string, number>();
	private priority = new Map<string, number>();
	private checking = 0;
	private running = 0;
	private sequence = 0;
	private startChain: Promise<void> = Promise.resolve();
	private readonly operationFences = new Map<string, object>();
	private readonly checkingJobs = new Set<QueryJob>();
	private destroyed = false;

	/** Creates the worker scheduler with injectable HTTP and persistence dependencies. */
	public constructor(
		private readonly initialization: QueryWorkerInitialization,
		private readonly hooks: TimelineQueryHooks,
		private readonly fetcher: typeof fetch = (...args) => fetch(...args),
	) {}

	/** Enqueues replacements after canceling older jobs for the same sources. */
	public start(requests: TimelineQueryRequest[]): Promise<void> {
		const fences = new Map<string, object>();
		for (const { descriptor } of requests) {
			const key = `${descriptor.operationId}\0${descriptor.operationEpoch}`;
			const token = this.operationFences.get(key) ?? {};
			this.operationFences.set(key, token);
			fences.set(key, token);
		}
		const next = this.startChain.then(() => this.enqueue(requests, fences));
		this.startChain = next.catch(() => undefined);
		return next;
	}

	/** Serializes overlapping starts so each source has exactly one newest owner. */
	private async enqueue(requests: TimelineQueryRequest[], fences: ReadonlyMap<string, object>): Promise<void> {
		if (this.destroyed) throw new Error("Timeline query coordinator is destroyed.");
		requests = requests.filter(({ descriptor }) => {
			const key = `${descriptor.operationId}\0${descriptor.operationEpoch}`;
			return this.operationFences.get(key) === fences.get(key);
		});
		for (const request of requests) {
			const { kind, sourceIds } = request.descriptor;
			if (kind !== "replace" || sourceIds?.length !== 1 || sourceIds[0] !== request.sourceId) {
				throw new Error("Timeline queries require one replacement source.");
			}
		}
		const sources = new Set(requests.map((request) => this.sourceKey(request)));
		await Promise.all(
			[...this.jobs.values()]
				.filter((job) => sources.has(this.sourceKey(job.request)))
				.map((job) => this.cancel(job.request.descriptor.requestId)),
		);
		if (this.destroyed) return;
		for (const request of requests) {
			const key = `${request.descriptor.operationId}\0${request.descriptor.operationEpoch}`;
			if (this.operationFences.get(key) !== fences.get(key)) continue;
			let settle = (): void => undefined;
			const terminal = new Promise<void>((resolve) => { settle = resolve; });
			this.jobs.set(request.descriptor.requestId, {
				request,
				abort: new AbortController(),
				stage: "checking",
				sequence: this.sequence++,
				dispatched: false,
				settle,
				terminal,
			});
		}
		this.pump();
	}

	/** Updates source priority without delaying dispatch until interaction stops. */
	public setPriority(sourceIds: readonly Source.Id[]): void {
		this.priority = new Map(sourceIds.map((id, index) => [id, index]));
		this.pump();
	}

	/** Releases HTTP slots only after the durable stream reaches a terminal state. */
	public onStreamState(change: DocumentStreamStateChange): void {
		if (!["completed", "canceled", "failed"].includes(change.state)) return;
		const job = this.jobs.get(change.descriptor.requestId);
		if (!job) return;
		job.settle();
		if (change.state === "failed" && job.dispatched) {
			this.cancelBackend(change.descriptor.requestId);
		}
	}

	/** Releases requests rejected or canceled by the backend before a final document chunk. */
	public onServerMessage(message: QuerySocketMessage): void {
		if (message.type !== "stats_update" && message.type !== "collab_update") return;
		const object = message.payload?.obj;
		if (!object || typeof object !== "object") return;
		const stats = object as { type?: string; id?: string; status?: string; errors?: unknown };
		if (stats.type !== "request_stats") return;
		const job = this.jobs.get(message.req_id ?? stats.id ?? "");
		if (!job) return;
		if (stats.status === "canceled") {
			void this.cancel(job.request.descriptor.requestId).catch(() => undefined);
		} else if (stats.status === "failed") {
			void this.fail(job, new Error(`Backend query failed: ${JSON.stringify(stats.errors ?? [])}`));
		}
	}

	/** Cancels queued or active work, fences late responses, and drains its source lease. */
	public async cancel(requestId: string): Promise<boolean> {
		const job = this.jobs.get(requestId);
		if (!job) return false;
		this.jobs.delete(requestId);
		job.abort.abort();
		job.settle();
		try {
			await this.hooks.cancel(requestId);
		} finally {
			if (job.dispatched) {
				this.cancelBackend(requestId);
			}
			this.publish(job, "canceled");
			this.pump();
		}
		return true;
	}

	/** Cancels one operation and forgets startup history when its session is released. */
	public async cancelOperation(operationId: string, operationEpoch?: number, release = false): Promise<void> {
		for (const key of this.operationFences.keys()) {
			const matches = operationEpoch === undefined
				? key.startsWith(`${operationId}\0`)
				: key === `${operationId}\0${operationEpoch}`;
			if (matches) this.operationFences.delete(key);
		}
		if (release) {
			for (const key of this.initialized.keys()) {
				if (key.startsWith(`${operationId}\0`) &&
					(operationEpoch === undefined || this.initialized.get(key) === operationEpoch)) {
					this.initialized.delete(key);
				}
			}
		}
		await Promise.all(
			[...this.jobs.values()]
				.filter(({ request: { descriptor } }) =>
					descriptor.operationId === operationId &&
					(operationEpoch === undefined || descriptor.operationEpoch === operationEpoch))
				.map((job) => this.cancel(job.request.descriptor.requestId)),
		);
	}

	/** Stops scheduling and cancels all pending and active requests. */
	public destroy(): void {
		this.destroyed = true;
		for (const id of this.jobs.keys()) void this.cancel(id).catch(() => undefined);
		this.initialized.clear();
	}

	/** Starts at most four lookups independently of the four HTTP/stream slots. */
	public pump(): void {
		if (this.destroyed) return;
		const ordered = [...this.jobs.values()].sort((a, b) =>
			(this.priority.get(a.request.sourceId) ?? Infinity) -
			(this.priority.get(b.request.sourceId) ?? Infinity) || a.sequence - b.sequence);
		for (const job of ordered) {
			if (job.stage === "checking" && !this.checkingJobs.has(job) && this.checking < 4) {
				this.checking++;
				this.checkingJobs.add(job);
				void this.prepare(job).finally(() => {
					this.checking--;
					this.checkingJobs.delete(job);
					this.pump();
				});
			} else if (job.stage === "ready" && this.running < 4 && this.hooks.isReady()) {
				job.stage = "running";
				this.running++;
				void this.execute(job).finally(() => {
					this.running--;
					this.pump();
				});
			}
		}
	}

	/** Reconciles startup once, then uses non-destructive runtime cache lookup. */
	private async prepare(job: QueryJob): Promise<void> {
		try {
			const request = job.request;
			const key = this.sourceKey(request);
			const initial = this.initialized.get(key) !== request.descriptor.operationEpoch;
			const activation = initial || request.cachePolicy === "reuse"
				? await this.hooks.lookup(request, initial ? "initial" : "reuse")
				: null;
			job.initialized = initial;
			job.activation = activation ?? undefined;
			if (!this.current(job)) return;
			this.initialized.set(key, request.descriptor.operationEpoch);
			if (activation && request.cachePolicy === "reuse") {
				this.publish(job, "cache-hit", { activation });
				this.jobs.delete(request.descriptor.requestId);
			} else job.stage = "ready";
		} catch (error) {
			await this.fail(job, error);
		}
	}

	/** Registers persistence before HTTP and retains the slot until durable completion. */
	private async execute(job: QueryJob): Promise<void> {
		try {
			this.publish(job, "started", { initialized: job.initialized, activation: job.activation });
			await this.hooks.register(job.request.descriptor);
			if (!this.current(job)) {
				await this.hooks.cancel(job.request.descriptor.requestId);
				return;
			}
			job.dispatched = true;
			const descriptor = job.request.descriptor;
			const response = await this.fetcher(this.url("query_raw", {
				operation_id: descriptor.operationId,
				req_id: descriptor.requestId,
				ws_id: this.initialization.wsId,
			}), {
				method: "POST",
				headers: { token: this.initialization.token, "Content-Type": "application/json" },
				body: JSON.stringify(job.request.body, (_key: string, value: unknown) =>
					typeof value === "bigint" ? value.toString() : value),
				signal: job.abort.signal,
			});
			const body = await response.json() as { status?: string; message?: string };
			if (!response.ok || body.status !== "pending") {
				await this.fail(job, new Error(body.message ?? `Query HTTP ${response.status}: ${body.status ?? response.statusText}`), response.status);
				return;
			}
			await job.terminal;
			if (this.current(job)) this.jobs.delete(descriptor.requestId);
		} catch (error) {
			await this.fail(job, error);
		}
	}

	/** Restores failed work through the same persistence cancellation path. */
	private async fail(job: QueryJob, error: unknown, httpStatus?: number): Promise<void> {
		if (!this.current(job)) return;
		this.jobs.delete(job.request.descriptor.requestId);
		job.abort.abort();
		job.settle();
		await this.hooks.cancel(job.request.descriptor.requestId).catch(() => undefined);
		if (job.dispatched) this.cancelBackend(job.request.descriptor.requestId);
		this.publish(job, "failed", { error: error instanceof Error ? error.message : String(error), httpStatus });
	}

	/** Returns whether a job still owns its request. */
	private current(job: QueryJob): boolean {
		return !this.destroyed && this.jobs.get(job.request.descriptor.requestId) === job;
	}

	/** Best-effort backend cancellation; local persistence cleanup owns rollback. */
	private cancelBackend(requestId: string): void {
		void this.fetcher(this.url("request_cancel", { req_id_to_cancel: requestId }), {
			method: "PATCH",
			headers: { token: this.initialization.token },
		}).catch(() => undefined);
	}

	/** Emits a compact update carrying the same operation fence as its request. */
	private publish(job: QueryJob, state: TimelineQueryUpdate["state"], details: Partial<TimelineQueryUpdate> = {}): void {
		const { requestId, operationId, operationEpoch } = job.request.descriptor;
		this.hooks.publish({ requestId, operationId, operationEpoch, sourceId: job.request.sourceId, state, ...details });
	}

	/** Keys initialization by operation and source without ambiguous delimiters. */
	private sourceKey(request: TimelineQueryRequest): string {
		return `${request.descriptor.operationId}\0${request.sourceId}`;
	}

	/** Builds an authenticated API URL without importing browser UI dependencies. */
	private url(path: string, query: Record<string, string>): string {
		const base = this.initialization.apiUrl ?? this.initialization.socketUrl.replace(/\/ws\/?$/, "").replace(/^ws/, "http");
		return `${base.replace(/\/$/, "")}/${path}?${new URLSearchParams(query)}`;
	}
}

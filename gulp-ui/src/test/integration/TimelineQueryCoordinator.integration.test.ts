import { TimelineQueryCoordinator } from "@/event-store-worker/TimelineQueryCoordinator";
import { QueryPersistenceCoordinator } from "@/event-store-worker/QueryPersistenceCoordinator";
import { MemoryEventStoreBackend } from "@/event-store/MemoryEventStore.backend";
import { EventStoreRepository } from "@/event-store/EventStore.repository";
import type { TimelineQueryRequest, TimelineQueryUpdate, DocumentStreamDescriptor } from "@/event-stream";
import type { Source } from "@/entities/Source";
import type { QueryCacheProvenance } from "@/entities/QueryCache";
import { TextEncoder, TextDecoder } from "util";

Object.defineProperties(globalThis, { TextEncoder: { configurable: true, value: TextEncoder }, TextDecoder: { configurable: true, value: TextDecoder } });

/** Creates distinct, source-scoped query identities without UI dependencies. */
function request(index: number, version = 1, epoch = 1): TimelineQueryRequest {
	const sourceId = `timeline-${index}` as Source.Id;
	const provenance: QueryCacheProvenance = {
		query: { string: "Replay query", filters: [], source_config: { operation_id: "timeline", source_ids: [sourceId], range: { min: "0", max: "100000000000" } } },
		fingerprint: `${sourceId}-${version}`, shapeFingerprint: `${sourceId}-${version}`,
		normalization: { field: "gulp.event_code", hashFunction: "fnv1a", requestedFields: [] },
		serverSnapshot: { total: 331, minNanoseconds: "0", maxNanoseconds: "100000000000", timeUpdated: 1, millisecondPrecision: false },
	};
	return {
		sourceId, cachePolicy: "reuse", body: { q: [{ q: { match_all: {} } }], q_options: {} },
		desired: { fingerprint: provenance.fingerprint, shapeFingerprint: provenance.shapeFingerprint!, range: { min: "0", max: "100000000000" }, serverSnapshot: provenance.serverSnapshot },
		descriptor: { requestId: `${sourceId}-v${version}-e${epoch}`, operationId: "timeline", operationEpoch: epoch, owner: "persist", kind: "replace", sourceIds: [sourceId], queryCache: provenance, durableProjectionOnly: true },
	};
}

/** Waits for an observable asynchronous condition with a bounded failure deadline. */
async function until(predicate: () => boolean): Promise<void> {
	const deadline = Date.now() + 10000;
	while (!predicate()) {
		if (Date.now() > deadline) throw new Error("Timeline integration timed out");
		await new Promise((resolve) => setTimeout(resolve, 0));
	}
}

/** Connects the actual scheduler and persistence coordinator to a controllable HTTP peer. */
function harness(options: { beforeLookup?: () => Promise<void>; responseStatus?: number } = {}) {
	const backend = new MemoryEventStoreBackend();
	const repository = new EventStoreRepository(backend);
	const descriptors = new Map<string, DocumentStreamDescriptor>();
	const updates: TimelineQueryUpdate[] = [];
	const completed = new Set<string>();
	const fetched: string[] = [];
	const fetchedBodies: string[] = [];
	const httpRequests: Array<{
		path: string;
		method: string;
		query: Record<string, string>;
	}> = [];
	const errors: string[] = [];
	const persisted: Array<{ id: string; count: number; status: string }> = [];
	const lookups: Array<{ sourceId: Source.Id; mode: "initial" | "reuse" }> = [];
	let scheduler: TimelineQueryCoordinator;
	const persistence = new QueryPersistenceCoordinator("timeline-integration", {
		onSourceSummary: ({ summary }) => persisted.push({ id: summary.sourceId, count: summary.eventCount, status: summary.status }),
		onIngestionProgress: () => undefined, onStreamFailure: ({ message }) => errors.push(message),
		onStreamFlushing: () => undefined, onDocumentChunkAcknowledged: () => undefined,
		onStreamCompleted: (requestId) => {
			completed.add(requestId);
			const descriptor = descriptors.get(requestId);
			if (descriptor) scheduler.onStreamState({ descriptor, state: "completed" });
		},
	}, { createOperationStore: () => ({ backend, repository }), createRevisionBus: () => ({ publish: () => undefined, close: () => undefined }) });
	const fetcher = jest.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
		const url = new URL(String(input));
		httpRequests.push({
			path: url.pathname,
			method: init?.method ?? "GET",
			query: Object.fromEntries(url.searchParams.entries()),
		});
		if (url.pathname === "/query_raw") {
			fetched.push(url.searchParams.get("req_id")!);
			fetchedBodies.push(String(init?.body));
		}
		const status = options.responseStatus ?? 200;
		return { ok: status === 200, status, json: async () => ({ status: status === 200 ? "pending" : "failed" }) } as Response;
	});
	scheduler = new TimelineQueryCoordinator({ apiUrl: "http://test", socketUrl: "ws://test/ws", wsId: "test", token: "test", cacheNamespaceId: "test" }, {
		lookup: async (job, mode) => {
			lookups.push({ sourceId: job.sourceId, mode });
			await options.beforeLookup?.();
			return persistence.reconcileSourceQueryCache({ operationId: job.descriptor.operationId, sourceId: job.sourceId, desired: job.desired, mode });
		},
		register: async (descriptor) => { descriptors.set(descriptor.requestId, descriptor); await persistence.register(descriptor); },
		cancel: async (id) => { await persistence.cancel(id); }, publish: (update) => updates.push(update), isReady: () => true,
	}, fetcher);
	/** Delivers a source-exact server chunk through the production persistence path. */
	const chunk = (id: string, count: number, last = true, sequence = 0): void => {
		const descriptor = descriptors.get(id)!;
		persistence.handleDocumentChunk({ descriptor, rawByteLength: 0, sequence, message: { type: "docs_chunk", req_id: id, operation_id: "timeline", payload: { docs: Array.from({ length: count }, (_, i) => ({ _id: `${id}-${sequence}-${i}`, "gulp.source_id": descriptor.sourceIds![0], "gulp.timestamp": i, "gulp.event_code": i % 8 })), last, request_last: last, chunk_num: sequence } } });
	};
	return { scheduler, persistence, repository, backend, fetched, fetchedBodies, httpRequests, updates, completed, errors, persisted, lookups, chunk };
}

describe("worker-owned timeline query integration", () => {
	it("preserves exact nanosecond integers in worker HTTP serialization", async () => {
		const h = harness();
		try {
			const job = request(0);
			job.body = { q: [{ q: { range: { "gulp.timestamp": { gte: 1700000000000000001n } } } }] };
			await h.scheduler.start([job]); await until(() => h.fetched.length === 1);
			expect(JSON.parse(h.fetchedBodies[0]).q[0].q.range["gulp.timestamp"].gte).toBe("1700000000000000001");
			h.chunk(job.descriptor.requestId, 0);
			await until(() => h.completed.size === 1);
		} finally { h.scheduler.destroy(); await h.persistence.destroy(); }
	});
	it("progressively completes 140 sources / 331 events with four HTTP streams", async () => {
		const h = harness();
		try {
			const jobs = Array.from({ length: 140 }, (_, i) => request(i));
			h.scheduler.setPriority([jobs[139].sourceId]);
			await h.scheduler.start(jobs);
			await until(() => h.fetched.length === 4);
			expect(h.fetched[0]).toBe(jobs[139].descriptor.requestId);
			const delivered = new Set<string>();
			let total = 0;
			while (h.completed.size < jobs.length) {
				for (const id of h.fetched) {
					if (delivered.has(id)) continue;
					const index = jobs.findIndex((job) => job.descriptor.requestId === id);
					const count = index < 51 ? 3 : 2;
					delivered.add(id); total += count; h.chunk(id, count);
				}
				await until(() => h.completed.size === delivered.size);
				expect(h.fetched.length - h.completed.size <= 4).toBe(true);
				if (h.completed.size < jobs.length) await until(() => h.fetched.length > delivered.size);
			}
			expect(total).toBe(331);
			expect(h.errors).toEqual([]);
			expect(h.persisted.filter((event) => event.status === "complete").reduce((sum, event) => sum + event.count, 0)).toBe(331);
			const before = h.fetched.length;
			await h.scheduler.start(jobs.map((job) => ({ ...job, descriptor: { ...job.descriptor, requestId: `${job.descriptor.requestId}-cached` } })));
			await until(() => h.updates.filter((update) => update.state === "cache-hit").length === 140);
			expect(h.fetched).toHaveLength(before);
		} finally { h.scheduler.destroy(); await h.persistence.destroy(); }
	});

	it("reuses an ingestion-built cache for the first selected timerange without HTTP", async () => {
		const h = harness();
		try {
			const baseline = request(7);
			const baselineProvenance = baseline.descriptor.queryCache!;
			const ingestionProvenance: QueryCacheProvenance = {
				...baselineProvenance,
				query: {
					...baselineProvenance.query,
					source_config: {
						...baselineProvenance.query.source_config!,
						range: {
							min: "500000000000",
							max: "600000000000",
						},
					},
				},
			};
			const ingestionDescriptor: DocumentStreamDescriptor = {
				...baseline.descriptor,
				requestId: "timeline-ingestion-cache",
				kind: "ingestion-build",
				completionMode: "explicit",
				defaultSourceSettings: {
					field: "gulp.event_code",
					hash_function: "fnv1a",
				},
			};
			await h.persistence.register(ingestionDescriptor);
			h.persistence.handleDocumentChunk({
				descriptor: ingestionDescriptor,
				rawByteLength: 0,
				sequence: 0,
				message: {
					type: "docs_chunk",
					req_id: ingestionDescriptor.requestId,
					operation_id: "timeline",
					payload: {
						docs: [10, 20].map((timestamp) => ({
							_id: `ingested-${timestamp}`,
							"gulp.source_id": baseline.sourceId,
							"gulp.timestamp": timestamp,
							"gulp.event_code": timestamp,
						})),
						last: true,
						request_last: true,
						chunk_num: 0,
					},
				},
			});
			await h.persistence.complete(ingestionDescriptor.requestId, {
				queryCachesBySourceId: {
					[baseline.sourceId]: ingestionProvenance,
				},
			});

			const [ingestionCache] = await h.repository.listQueryCaches(
				baseline.sourceId,
			);
			expect(ingestionCache.provenance.materializedRange).toEqual({
				min: "10000000000",
				max: "20000000000",
			});
			const selected = request(7, 2);
			selected.desired = {
				...selected.desired,
				shapeFingerprint: baseline.desired.shapeFingerprint,
				range: { min: "10000000000", max: "20000000000" },
				serverSnapshot: ingestionCache.provenance.serverSnapshot,
			};
			selected.descriptor = {
				...selected.descriptor,
				queryCache: {
					...selected.descriptor.queryCache!,
					shapeFingerprint: selected.desired.shapeFingerprint,
					serverSnapshot: selected.desired.serverSnapshot,
				},
			};

			await h.scheduler.start([selected]);
			await until(() =>
				h.updates.some(
					(update) =>
						update.requestId === selected.descriptor.requestId &&
						update.state === "cache-hit",
				),
			);
			expect(h.fetched).toEqual([]);
			expect(
				h.updates.find(
					(update) => update.requestId === selected.descriptor.requestId,
				)?.activation?.match,
			).toBe("covered");
		} finally {
			h.scheduler.destroy();
			await h.persistence.destroy();
		}
	});

	it("keeps a complete warning baseline on first selection without HTTP", async () => {
		const h = harness();
		try {
			const cached = request(8);
			await h.scheduler.start([cached]);
			await until(() => h.fetched.includes(cached.descriptor.requestId));
			h.chunk(cached.descriptor.requestId, 1);
			await until(() => h.completed.has(cached.descriptor.requestId));

			const warning = request(8, 2, 2);
			const fetchCount = h.fetched.length;
			await h.scheduler.start([warning]);
			await until(() =>
				h.updates.some(
					(update) =>
						update.requestId === warning.descriptor.requestId &&
						update.state === "cache-hit",
				),
			);
			expect(h.fetched).toHaveLength(fetchCount);
			expect(
				h.updates.find(
					(update) => update.requestId === warning.descriptor.requestId,
				)?.activation?.match,
			).toBe("latest");
		} finally {
			h.scheduler.destroy();
			await h.persistence.destroy();
		}
	});

	it("resets history once per operation epoch and preserves runtime Undo/Redo", async () => {
		const h = harness();
		try {
			for (const version of [1, 2, 3]) {
				const job = request(0, version);
				await h.scheduler.start([job]);
				await until(() => h.fetched.includes(job.descriptor.requestId));
				h.chunk(job.descriptor.requestId, version);
				await until(() => h.completed.has(job.descriptor.requestId));
			}
			expect(await h.repository.listQueryCaches(request(0).sourceId)).toHaveLength(3);
			const reopen = request(0, 2, 2);
			await h.scheduler.start([reopen]);
			await until(() => h.updates.some((event) => event.requestId === reopen.descriptor.requestId && event.state === "cache-hit"));
			expect(await h.repository.listQueryCaches(reopen.sourceId)).toHaveLength(1);
			expect(await h.repository.undoQueryCache(reopen.sourceId)).toBeNull();
			const next = request(0, 4, 2);
			await h.scheduler.start([next]); await until(() => h.fetched.includes(next.descriptor.requestId));
			h.chunk(next.descriptor.requestId, 4); await until(() => h.completed.has(next.descriptor.requestId));
			expect(await h.repository.listQueryCaches(next.sourceId)).toHaveLength(2);
			expect((await h.repository.undoQueryCache(next.sourceId))?.cache.provenance.fingerprint).toBe(reopen.desired.fingerprint);
			expect((await h.repository.redoQueryCache(next.sourceId))?.cache.provenance.fingerprint).toBe(next.desired.fingerprint);
		} finally { h.scheduler.destroy(); await h.persistence.destroy(); }
	});

	it("prunes a later-selected source's old generations once and retains its new session history", async () => {
		const h = harness();
		try {
			for (const version of [1, 2, 3]) {
				const job = request(99, version);
				await h.scheduler.start([job]); await until(() => h.fetched.includes(job.descriptor.requestId));
				h.chunk(job.descriptor.requestId, version); await until(() => h.completed.has(job.descriptor.requestId));
			}
			await h.scheduler.cancelOperation("timeline", 1, true);
			const firstSelection = request(0, 1, 2);
			await h.scheduler.start([firstSelection]); await until(() => h.fetched.includes(firstSelection.descriptor.requestId));
			h.chunk(firstSelection.descriptor.requestId, 1); await until(() => h.completed.has(firstSelection.descriptor.requestId));
			expect(await h.repository.listQueryCaches(request(99).sourceId)).toHaveLength(3);

			const lateSelection = request(99, 2, 2);
			await h.scheduler.start([lateSelection]);
			await until(() => h.updates.some((event) => event.requestId === lateSelection.descriptor.requestId && event.state === "cache-hit"));
			expect((await h.repository.listQueryCaches(lateSelection.sourceId)).map((cache) => cache.generation)).toEqual([2]);
			expect(await h.repository.undoQueryCache(lateSelection.sourceId)).toBeNull();
			expect(await h.repository.redoQueryCache(lateSelection.sourceId)).toBeNull();
			for (const generation of [1, 3]) {
				expect(await h.backend.getAllByPrefix("pages", [lateSelection.sourceId, generation])).toHaveLength(0);
			}

			const next = request(99, 4, 2);
			await h.scheduler.start([next]); await until(() => h.fetched.includes(next.descriptor.requestId));
			h.chunk(next.descriptor.requestId, 4); await until(() => h.completed.has(next.descriptor.requestId));
			const reselection = { ...next, descriptor: { ...next.descriptor, requestId: "late-reselection" } };
			await h.scheduler.start([reselection]);
			await until(() => h.updates.some((event) => event.requestId === "late-reselection" && event.state === "cache-hit"));
			expect(await h.repository.listQueryCaches(next.sourceId)).toHaveLength(2);
			expect((await h.repository.undoQueryCache(next.sourceId))?.cache.generation).toBe(2);
			expect((await h.repository.redoQueryCache(next.sourceId))?.cache.provenance.fingerprint).toBe(next.desired.fingerprint);
			expect(h.lookups.filter((lookup) => lookup.sourceId === next.sourceId).map((lookup) => lookup.mode)).toEqual(["initial", "reuse", "reuse", "initial", "reuse", "reuse"]);
		} finally { h.scheduler.destroy(); await h.persistence.destroy(); }
	});

	it("publishes durable pages before completion and cancels a queued source without HTTP", async () => {
		const h = harness();
		try {
			const jobs = Array.from({ length: 5 }, (_, i) => request(i));
			await h.scheduler.start(jobs); await until(() => h.fetched.length === 4);
			await h.scheduler.cancel(jobs[4].descriptor.requestId);
			expect(
				h.httpRequests.filter(
					(request) => request.path === "/request_cancel",
				),
			).toEqual([]);
			h.chunk(jobs[0].descriptor.requestId, 16384, false);
			await until(() => h.persisted.some((event) => event.id === jobs[0].sourceId && event.count === 16384));
			expect(h.completed.has(jobs[0].descriptor.requestId)).toBe(false);
			h.chunk(jobs[0].descriptor.requestId, 3, true, 1);
			await until(() => h.completed.has(jobs[0].descriptor.requestId));
			expect(h.persisted.some((event) => event.id === jobs[0].sourceId && event.count === 16387 && event.status === "complete")).toBe(true);
			expect(h.fetched).not.toContain(jobs[4].descriptor.requestId);
			expect(h.errors).toEqual([]);
		} finally { h.scheduler.destroy(); await h.persistence.destroy(); }
	});

	it("uses the backend cancellation contract only for dispatched overlapping sources", async () => {
		const h = harness();
		try {
			const obsolete = request(0);
			const unaffected = request(1);
			await h.scheduler.start([obsolete, unaffected]);
			await until(() => h.fetched.length === 2);

			const replacement = request(0, 2);
			await h.scheduler.start([replacement]);
			await until(() => h.fetched.includes(replacement.descriptor.requestId));

			const cancellations = h.httpRequests.filter(
				(httpRequest) => httpRequest.path === "/request_cancel",
			);
			expect(cancellations).toEqual([
				{
					path: "/request_cancel",
					method: "PATCH",
					query: {
						req_id_to_cancel: obsolete.descriptor.requestId,
					},
				},
			]);
			expect(
				cancellations.some(
					(httpRequest) =>
						httpRequest.query.req_id_to_cancel ===
						unaffected.descriptor.requestId,
				),
			).toBe(false);
			const cancellationIndex = h.httpRequests.indexOf(cancellations[0]);
			const replacementIndex = h.httpRequests.findIndex(
				(httpRequest) =>
					httpRequest.path === "/query_raw" &&
					httpRequest.query.req_id === replacement.descriptor.requestId,
			);
			expect(cancellationIndex < replacementIndex).toBe(true);

			// Persistence cancellation fences late chunks from the obsolete request.
			h.chunk(obsolete.descriptor.requestId, 5);
			h.chunk(unaffected.descriptor.requestId, 1);
			h.chunk(replacement.descriptor.requestId, 2);
			await until(
				() =>
					h.completed.has(unaffected.descriptor.requestId) &&
					h.completed.has(replacement.descriptor.requestId),
			);
			expect(
				h.persisted.some(
					(event) =>
						event.id === obsolete.sourceId &&
						event.status === "complete" &&
						event.count === 5,
				),
			).toBe(false);
		} finally {
			h.scheduler.destroy();
			await h.persistence.destroy();
		}
	});

	it("publishes cached sources while four other sources are still streaming", async () => {
		const h = harness();
		try {
			const warm = request(9);
			await h.scheduler.start([warm]);
			await until(() => h.fetched.length === 1);
			h.chunk(warm.descriptor.requestId, 0);
			await until(() => h.completed.size === 1);
			const cached = { ...warm, descriptor: { ...warm.descriptor, requestId: "cached-empty" } };
			await h.scheduler.start([...Array.from({ length: 4 }, (_, index) => request(index)), cached]);
			await until(() => h.fetched.length === 5 && h.updates.some((update) => update.requestId === "cached-empty"));
			expect(h.completed.size).toBe(1);
			expect(h.updates.find((update) => update.requestId === "cached-empty")?.activation?.summary.eventCount).toBe(0);
			expect(h.fetched).not.toContain("cached-empty");
		} finally { h.scheduler.destroy(); await h.persistence.destroy(); }
	});

	it("restores the previous generation after active cancellation and backend failure", async () => {
		const h = harness();
		try {
			const baseline = request(0);
			await h.scheduler.start([baseline]); await until(() => h.fetched.length === 1);
			h.chunk(baseline.descriptor.requestId, 3); await until(() => h.completed.size === 1);
			for (const version of [2, 3]) {
				const replacement = request(0, version);
				await h.scheduler.start([replacement]);
				await until(() => h.fetched.includes(replacement.descriptor.requestId));
				h.chunk(replacement.descriptor.requestId, 16384, false);
				await until(() => h.persisted.filter((event) => event.count === 16384).length === version - 1);
				if (version === 2) await h.scheduler.cancel(replacement.descriptor.requestId);
				else h.scheduler.onServerMessage({ type: "stats_update", req_id: replacement.descriptor.requestId, payload: { obj: { type: "request_stats", status: "failed", errors: ["test failure"] } } });
				await until(() => h.updates.some((update) => update.requestId === replacement.descriptor.requestId && ["canceled", "failed"].includes(update.state)));
				const caches = await h.repository.listQueryCaches(baseline.sourceId);
				expect(caches).toHaveLength(1);
				expect(caches[0].provenance.fingerprint).toBe(baseline.desired.fingerprint);
			}
		} finally { h.scheduler.destroy(); await h.persistence.destroy(); }
	});

	it("fences pending starts and delayed cache lookups when the operation is released", async () => {
		let resume = (): void => undefined;
		const gate = new Promise<void>((resolve) => { resume = resolve; });
		const h = harness({ beforeLookup: () => gate });
		try {
			await h.scheduler.start([request(0)]);
			await until(() => h.lookups.length === 1);
			const pending = h.scheduler.start([request(1)]);
			await h.scheduler.cancelOperation("timeline", 1, true);
			resume(); await pending;
			const next = request(2, 1, 2);
			await h.scheduler.start([next]); await until(() => h.fetched.length === 1);
			expect(h.fetched).toEqual([next.descriptor.requestId]);
			expect(h.updates.some((update) => update.operationEpoch === 1 && ["started", "cache-hit"].includes(update.state))).toBe(false);
		} finally { resume(); h.scheduler.destroy(); await h.persistence.destroy(); }
	});

	it("initializes sources selected later once and rejects stale results without losing runtime history", async () => {
		const h = harness();
		try {
			for (const sourceIndex of [0, 99]) {
				const original = request(sourceIndex);
				await h.scheduler.start([original]); await until(() => h.fetched.includes(original.descriptor.requestId));
				h.chunk(original.descriptor.requestId, 1); await until(() => h.completed.has(original.descriptor.requestId));
				const stale = request(sourceIndex, 1);
				stale.descriptor = { ...stale.descriptor, requestId: `${stale.descriptor.requestId}-fresh`, queryCache: { ...stale.descriptor.queryCache!, serverSnapshot: { ...stale.desired.serverSnapshot!, timeUpdated: 2 } } };
				stale.desired = { ...stale.desired, serverSnapshot: stale.descriptor.queryCache!.serverSnapshot };
				await h.scheduler.start([stale]); await until(() => h.fetched.includes(stale.descriptor.requestId));
				h.chunk(stale.descriptor.requestId, 2); await until(() => h.completed.has(stale.descriptor.requestId));
				expect(await h.repository.listQueryCaches(stale.sourceId)).toHaveLength(2);
				expect(h.lookups.filter((entry) => entry.sourceId === stale.sourceId).map((entry) => entry.mode)).toEqual(["initial", "reuse"]);
			}
		} finally { h.scheduler.destroy(); await h.persistence.destroy(); }
	});

	it("reports HTTP failures and releases all queued slots", async () => {
		const h = harness({ responseStatus: 503 });
		try {
			await h.scheduler.start(Array.from({ length: 9 }, (_, index) => request(index)));
			await until(() => h.updates.filter((update) => update.state === "failed").length === 9);
			expect(h.fetched).toHaveLength(9);
			expect(h.updates.filter((update) => update.state === "failed").every((update) => update.httpStatus === 503)).toBe(true);
		} finally { h.scheduler.destroy(); await h.persistence.destroy(); }
	});
});

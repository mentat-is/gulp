let mockRequestSequence = 0;
import type { TimelineQueryRequest } from "@/event-stream";
import { Info } from "@/class/Info";
import type { App } from "@/entities/App";
import type { Doc } from "@/entities/Doc";
import type { Link } from "@/entities/Link";
import type { Note } from "@/entities/Note";
import type { Operation } from "@/entities/Operation";
import type { Query } from "@/entities/Query";
import { Request } from "@/entities/Request";
import type { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import { SmartSocket } from "@/class/SmartSocket";
import { DataStore } from "@/store/DataStore";
import type {
	EventSourceQueryCacheActivation,
	EventSourceQueryCacheReconciliation,
} from "@/event-store";
import { requestStore } from "@/store/request.store";
import type { QueryRawDocument } from "@/workers/DataWorker.types";

jest.mock("@/class/IndexedDB", () => ({
	GulpIndexedDB: class MockGulpIndexedDB {},
}));

jest.mock("@/locales/core", () => ({
	translate: (key: string): string => key,
}));

jest.mock("@/components/QueryBuilder", () => ({
	OpenSearchQueryBuilder: {},
}));

jest.mock("@/dto/Logger.class", () => ({
	Logger: {
		log: jest.fn(),
		warn: jest.fn(),
		error: jest.fn(),
		fatal: jest.fn(),
		assert: (value: unknown) => value,
	},
}));

jest.mock("@/ui/utils", () => ({
	generateUUID: () => `frame-refetch-${++mockRequestSequence}`,
	NodeFile: class MockNodeFile {},
	Refractor: {
		array: (...values: unknown[]): unknown[] => values,
		object: <T extends object>(value: T): T => ({ ...value }),
	},
	stringToHexColor: (): string => "#000000",
}));

jest.mock("@/overlays/Highlights", () => ({ Highlights: {} }));
jest.mock("@/components/Pointers", () => ({ Pointers: {} }));
jest.mock("@/components/CustomParameters", () => ({ CustomParameters: {} }));

jest.mock("@/class/RenderEngine", () => ({
	RenderEngine: {
		clearAllCaches: jest.fn(),
		reset: jest.fn(),
		resetSource: jest.fn(),
	},
}));

jest.mock("@/class/SmartSocket", () => ({
	SmartSocket: {
		Class: {
			instance: null,
		},
		Message: {
			Type: {
				STATS_UPDATE: "stats_update",
				REBASE_DONE: "rebase_done",
			},
		},
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {
			/** Resolves source fixtures from the application state. */
			id: (app: App.Type, source: Source.Type | Source.Id): Source.Type =>
				typeof source === "object"
					? source
					: app.target.files.find((file) => file.id === source)!,
			/** Returns selected source fixtures. */
			selected: (app: App.Type): Source.Type[] =>
				app.target.files.filter((file) => file.selected),
			/** Returns a simple union frame for the supplied fixtures. */
			renderedFrame: (files: Source.Type[]) => ({
				min: Math.min(...files.map((file) => file.timestamp.min)),
				max: Math.max(...files.map((file) => file.timestamp.max)),
			}),
		},
	},
}));

jest.mock("@/entities/App", () => ({ App: {} }));
jest.mock("@/entities/Doc", () => ({
	Doc: {
		Entity: {
			delete: jest.fn(),
			clearHashRange: jest.fn(),
			operationId: (_app: App.Type, document: Doc.Type) =>
				(
					document as unknown as Record<
						"gulp.operation_id",
						Operation.Id
					>
				)["gulp.operation_id"],
			normalize: (documents: Array<Record<string, unknown>>) =>
				documents.map((document) => ({
					_id: document._id,
					gulp_timestamp:
						typeof document["@timestamp"] === "number"
							? document["@timestamp"]
							: Date.parse(String(document["@timestamp"])),
					"gulp.source_id": document["gulp.source_id"],
					"gulp.event_code": document["gulp.event_code"],
					number_hash: 1,
				})),
		},
	},
}));
jest.mock("@/entities/Note", () => ({ Note: { Entity: {} } }));
jest.mock("@/entities/Link", () => ({ Link: { Entity: {} } }));
jest.mock("react-markdown", () => (): null => null);
jest.mock("remark-gfm", () => (): null => null);
jest.mock("rehype-prism-plus", () => (): null => null);
jest.mock("@/workers/IngestWorker.manager", () => ({
	ingestWorkerManager: {},
}));
jest.mock("@/workers/DataWorker.class", () => ({ DataWorker: {} }));

const sourceId = "frame-refetch-source" as Source.Id;
const sourceTimestamp = {
	min: 1_700_000_000_000,
	max: 1_800_000_000_000,
};

/**
 * Creates one source-local timeline document for edge-navigation assertions.
 * @param id Stable document identifier.
 * @param timestamp Timeline timestamp used to select the source edge.
 * @param ownerSourceId Source that owns the document.
 * @returns Complete minimal timeline document.
 */
function createTimelineDocument(
	id: string,
	timestamp: number,
	ownerSourceId: Source.Id = sourceId,
): Doc.Type {
	return {
		_id: id as Doc.Id,
		gulp_timestamp: timestamp,
		"gulp.source_id": ownerSourceId,
		"gulp.event_code": 1,
		number_hash: timestamp,
	};
}

/**
 * Creates a selected source with a two-second positive render offset.
 * @param id Optional source identity for multi-source startup coverage.
 * @returns Source fixture for the refetch test.
 */
function createSource(id: Source.Id = sourceId): Source.Type {
	return {
		id,
		name: `${id}.csv`,
		selected: true,
		operation_id: "frame-refetch-operation",
		total: 120,
		time_updated: 1_800_000_000_123,
		settings: {
			offset: 2_000,
			field: "event.code",
			hash_function: "fnv1a",
		},
		timestamp: sourceTimestamp,
		nanotimestamp: {
			min: BigInt(sourceTimestamp.min) * 1_000_000n,
			max: BigInt(sourceTimestamp.max) * 1_000_000n,
		},
		appliedQuery: null,
		queryCache: createEmptyQueryCacheState(),
	} as unknown as Source.Type;
}

/**
 * Creates a source-scoped query suitable for persisted cache provenance.
 * @param source Source that owns the query.
 * @param label Human-readable query label restored in the menu.
 * @param textFilter Backend text filter represented by the cache.
 * @returns Serializable query fixture.
 */
function createCacheQuery(
	source: Source.Type,
	label: string,
	textFilter = "*cached*",
): Query.Type {
	return {
		string: label,
		text_filter: textFilter,
		filters: [],
		source_config: {
			operation_id: source.operation_id,
			source_ids: [source.id],
			range: {
				min: source.nanotimestamp.min.toString(),
				max: source.nanotimestamp.max.toString(),
			},
		},
	};
}

interface CacheActivationOptions {
	query?: Query.Type;
	generation?: number;
	authorityRevision?: number;
	history?: number[];
	historyCursor?: number;
	eventCount?: number;
	serverSnapshot?: Partial<
		EventSourceQueryCacheActivation["cache"]["provenance"]["serverSnapshot"]
	>;
}

/**
 * Creates a complete worker response without materializing any cached document.
 * @param source Source whose EventStore cache is activated.
 * @param options Optional cache/query/history overrides.
 * @returns Cache activation fixture with matching compact source summary.
 */
function createCacheActivation(
	source: Source.Type,
	options: CacheActivationOptions = {},
): EventSourceQueryCacheActivation {
	const generation = options.generation ?? 4;
	const history = options.history ?? [generation];
	const historyCursor = options.historyCursor ?? history.indexOf(generation);
	const eventCount = options.eventCount ?? 17;
	const query =
		options.query ?? createCacheQuery(source, "cached authentication query");
	return {
		cache: {
			sourceId: source.id,
			generation,
			provenance: {
				query,
				fingerprint: `cache-fingerprint-${generation}`,
				normalization: {
					field: String(source.settings.field),
					hashFunction: source.settings.hash_function,
					requestedFields: [String(source.settings.field)],
				},
				serverSnapshot: {
					total: source.total,
					minNanoseconds: source.nanotimestamp.min.toString(),
					maxNanoseconds: source.nanotimestamp.max.toString(),
					timeUpdated: source.time_updated,
					millisecondPrecision: false,
					...options.serverSnapshot,
				},
			},
			state: "exact",
			createdAt: 100,
			lastActivatedAt: 200,
			eventCount,
			minTimestamp: source.timestamp.min,
			maxTimestamp: source.timestamp.max,
		},
		summary: {
			sourceId: source.id,
			generation,
			revision: 1,
			authorityRevision: options.authorityRevision ?? generation,
			status: "complete",
			eventCount,
			minTimestamp: source.timestamp.min,
			maxTimestamp: source.timestamp.max,
			hashMin: 1,
			hashMax: 2,
			activeCacheGeneration: generation,
			cacheHistory: history,
			cacheHistoryCursor: historyCursor,
			nextGeneration: Math.max(generation + 1, ...history.map((value) => value + 1)),
		},
	};
}

/**
 * Installs a partial SmartSocket singleton consumed by Info cache operations.
 * @param socket Cache methods exposed by the test worker facade.
 * @returns Nothing.
 */
function setSmartSocket(
	socket:
		| {
				startTimelineQueries?: (requests: TimelineQueryRequest[]) => Promise<void>;
				setTimelineQueryPriority?: (sourceIds: Source.Id[]) => void;
				reconcileSourceQueryCache?: (
					input: unknown,
				) => Promise<EventSourceQueryCacheReconciliation | null>;
				navigateSourceQueryCache?: (
					input: unknown,
				) => Promise<EventSourceQueryCacheActivation | null>;
				clearSourceQueryCache?: (
					operationId: string,
					sourceId: Source.Id,
				) => Promise<void>;
				registerDocumentStream?: (descriptor: unknown) => Promise<void>;
				cancelDocumentStream?: (requestId: string) => Promise<void>;
				completeDocumentStream?: (requestId: string) => Promise<void>;
				con?: (...arguments_: unknown[]) => string;
				conce?: (...arguments_: unknown[]) => string;
				coff?: (...arguments_: unknown[]) => void;
			  }
		| undefined,
): void {
	(
		SmartSocket.Class as unknown as {
			instance: typeof socket;
		}
	).instance = socket;
}

/**
 * Creates an Info instance whose non-query reload work is isolated from the test.
 * @param source Source fixture stored in application state.
 * @returns Info instance, current state accessor, and backend query spy.
 */
function createInfoHarness(source: Source.Type): {
	info: Info;
	getApp: () => App.Type;
	queryFile: JestMock<Promise<undefined>, [Query.Type, object]>;
} {
	let currentApp = createApp(source);
	const info = new Info({
		app: currentApp,
		setInfo: (action) => {
			currentApp = typeof action === "function" ? action(currentApp) : action;
		},
		timeline: { current: document.createElement("div") },
	});
	const queryFile = jest.fn<Promise<undefined>, [Query.Type, object]>(
		async () => undefined,
	);
	info.query_file = queryFile as typeof info.query_file;
	info.notes_reload = jest.fn(async () => undefined);
	info.links_reload = jest.fn(async () => undefined);
	info.events_reset_in_file = jest.fn();
	(
		info as unknown as { highlights_reload: () => void }
	).highlights_reload = jest.fn();
	return { info, getApp: () => currentApp, queryFile };
}

/**
 * Creates application state with a persisted structured filter over the full source.
 * @param source Source stored in target.files.
 * @returns Application fixture owned by Info.
 */
function createApp(source: Source.Type): App.Type {
	const query: Query.Type = {
		string: "existing filter",
		text_filter: "*authentication*",
		filters: [
			{
				id: "outcome-filter",
				type: "eq",
				operator: "must",
				field: "event.outcome",
				value: "failure",
				enabled: true,
			} as Query.Type["filters"][number],
		],
		source_config: {
			operation_id: source.operation_id,
			source_ids: [source.id],
			range: {
				min: source.nanotimestamp.min.toString(),
				max: source.nanotimestamp.max.toString(),
			},
		},
	};
	source.appliedQuery = query;

	return {
		target: {
			files: [source],
			events: new Map(),
			operations: [],
			contexts: [],
			notes: [],
			links: [],
			highlights: [],
			glyphs: [],
			mappings: [],
			plugins: [],
		},
		timeline: {
			scale: 1,
			frame: sourceTimestamp,
			filter: "",
			target: null,
			isScrollReversed: false,
			dialogSize: 0,
			pointers: [],
			renderVersion: 0,
		},
		general: {
			server: "http://localhost:8080",
			ws_id: "frame-refetch-workspace",
			glyphs_syncronized: false,
			requests: [],
			user: null,
			loadings: requestStore.getLoadings(),
			tableViewSource: null,
		},
		settings: {},
		hidden: {
			notes: false,
			links: false,
			sourcesWithNoEvents: false,
			toasts: false,
		},
	} as App.Type;
}

/** Installs the worker command boundary; lifecycle events are driven explicitly by each test. */
function createWorkerHarness(source = createSource()) {
	const harness = createInfoHarness(source);
	const startTimelineQueries = jest.fn(async (_requests: TimelineQueryRequest[]) => undefined);
	setSmartSocket({ startTimelineQueries, setTimelineQueryPriority: jest.fn(), cancelDocumentStream: async () => undefined });
	return { ...harness, source, startTimelineQueries };
}

/** Waits for the bounded source metadata publication. */
async function flushSourceMetadata(): Promise<void> { await new Promise((resolve) => setTimeout(resolve, 20)); }

describe("Info timeline-frame refetch", () => {
	beforeEach(() => {
		mockRequestSequence = 0;
		requestStore.reset();
		localStorage.removeItem("gulp:event-store-lod-enabled");
		DataStore.resetRenderOperation(
			"frame-refetch-operation" as Operation.Id,
		);
		setSmartSocket(undefined);
	});
	afterEach(() => {
		setSmartSocket(undefined);
		DataStore.resetRenderOperation();
		requestStore.reset();
	});

	it("sends scoped frame queries to the worker and preserves frame scope on subsequent filters", async () => {
		const { info, source, getApp, queryFile, startTimelineQueries } = createWorkerHarness();
		const frame = { min: 1_742_169_600_000, max: 1_743_033_600_000 };
		await info.refetch({ ids: source.id, frame });
		const first = startTimelineQueries.mock.calls[0][0][0];
		expect(queryFile).not.toHaveBeenCalled();
		expect(first.descriptor.kind).toBe("replace");
		expect(first.descriptor.durableProjectionOnly).toBe(true);
		expect(first.desired.range).toEqual({ min: String(BigInt(frame.min - source.settings.offset) * 1_000_000n), max: String(BigInt(frame.max - source.settings.offset) * 1_000_000n) });
		expect(first.descriptor.queryCache?.query.text_filter).toBe("*authentication*");
		expect(first.descriptor.queryCache?.query.filters).toHaveLength(1);
		expect(getApp().timeline.frame).toEqual(frame);
		info.handleTimelineQueryUpdate({ ...first.descriptor, sourceId: source.id, state: "started" });
		await flushSourceMetadata();
		expect(getApp().target.files[0].appliedQuery?.source_config?.range).toEqual(first.desired.range);
		await info.refetch({ ids: source.id, query: { ...info.getQuery(source), text_filter: "*subsequent filter*" } });
		const second = startTimelineQueries.mock.calls[1][0][0];
		expect(second.desired.range).toEqual(first.desired.range);
		expect(second.descriptor.queryCache?.query.text_filter).toBe("*subsequent filter*");
	});

	it("uses the same cache policy at startup and on subsequent source selection", async () => {
		const { info, source, startTimelineQueries } = createWorkerHarness();
		await info.refetch({ ids: source.id, cachePolicy: "initial" });
		await info.refetch({ ids: source.id });
		expect(startTimelineQueries.mock.calls.map(([requests]) => requests[0].cachePolicy)).toEqual(["reuse", "reuse"]);
	});

	it("leaves LOD source clearing to the durable worker generation", async () => {
		const { info, source, startTimelineQueries } = createWorkerHarness();
		await info.refetch({ ids: source.id });
		const request = startTimelineQueries.mock.calls[0][0][0];

		info.handleTimelineQueryUpdate({
			...request.descriptor,
			sourceId: source.id,
			state: "started",
		});
		await flushSourceMetadata();

		expect(info.events_reset_in_file).not.toHaveBeenCalled();
	});

	it("publishes query source metadata as bounded idle work", async () => {
		const originalRequestIdleCallback = window.requestIdleCallback;
		const originalCancelIdleCallback = window.cancelIdleCallback;
		const requestIdleCallback = jest.fn(
			(
				_callback: IdleRequestCallback,
				_options?: IdleRequestOptions,
			): number => 17,
		);
		const cancelIdleCallback = jest.fn();
		window.requestIdleCallback = requestIdleCallback;
		window.cancelIdleCallback = cancelIdleCallback;

		try {
			const { info, source, startTimelineQueries } = createWorkerHarness();
			const publish = jest.fn(info.setInfo);
			info.setInfo = publish;
			await info.refetch({ ids: source.id });
			const request = startTimelineQueries.mock.calls[0][0][0];

			info.handleTimelineQueryUpdate({
				...request.descriptor,
				sourceId: source.id,
				state: "started",
			});

			expect(publish).not.toHaveBeenCalled();
			expect(requestIdleCallback).toHaveBeenCalledTimes(1);
			expect(requestIdleCallback.mock.calls[0][1]).toEqual({ timeout: 250 });
			requestIdleCallback.mock.calls[0][0]({
				didTimeout: false,
				timeRemaining: () => 10,
			});
			expect(publish).toHaveBeenCalledTimes(1);
		} finally {
			window.requestIdleCallback = originalRequestIdleCallback;
			window.cancelIdleCallback = originalCancelIdleCallback;
		}
	});

	it("retains the explicit main-thread clear for the legacy renderer", async () => {
		localStorage.setItem("gulp:event-store-lod-enabled", "false");
		const { info, source } = createWorkerHarness();

		await info.refetch({ ids: source.id });

		expect(info.events_reset_in_file).toHaveBeenCalledTimes(1);
	});

	it("removes legacy pending replacements before active cleanup can pump them", async () => {
		localStorage.setItem("gulp:event-store-lod-enabled", "false");
		const source = createSource();
		const { info, queryFile } = createInfoHarness(source);
		const obsoleteQuery = createCacheQuery(
			source,
			"obsolete pending query",
			"*obsolete*",
		);
		const replacementQuery = createCacheQuery(
			source,
			"replacement query",
			"*replacement*",
		);
		const requestCancel = jest.fn(async () => undefined);
		info.request_cancel = requestCancel as typeof info.request_cancel;

		const internals = info as unknown as {
			registerQueryStream: (
				requestId: Request.Id,
				sourceIds: Source.Id[],
				operationId: Operation.Id,
				operationEpoch: number,
				abortController: AbortController | undefined,
				queuePriority: "visible" | "background",
			) => unknown;
			refetchQueue: Array<{
				sourceId: Source.Id;
				operationId: Operation.Id;
				query: Query.Type;
				options: {
					id: Source.Id;
					operationId: Operation.Id;
					preview: false;
				};
				sequence: number;
			}>;
			activeQueuedQueryCount: number;
		};
		const obsoleteRequestId = "obsolete-active-query" as Request.Id;
		internals.registerQueryStream(
			obsoleteRequestId,
			[source.id],
			source.operation_id as Operation.Id,
			DataStore.getRenderOperationEpoch(),
			undefined,
			"visible",
		);
		internals.activeQueuedQueryCount = 1;
		internals.refetchQueue = [
			{
				sourceId: source.id,
				operationId: source.operation_id as Operation.Id,
				query: obsoleteQuery,
				options: {
					id: source.id,
					operationId: source.operation_id as Operation.Id,
					preview: false,
				},
				sequence: 0,
			},
		];

		await info.refetch({ ids: source.id, query: replacementQuery });

		expect(requestCancel).toHaveBeenCalledWith(obsoleteRequestId);
		expect(queryFile).toHaveBeenCalledTimes(1);
		expect(queryFile.mock.calls[0][0].text_filter).toBe("*replacement*");
	});

	it("updates loading state without invalidating canonical document data", () => {
		const source = createSource();
		const { info } = createInfoHarness(source);
		const originalMarkDirty = DataStore.markDirty;
		const markDirty = jest.fn();
		DataStore.markDirty = markDirty;
		const requestId = "query-loading-state" as Request.Id;

		try {
			info.setLoading(requestId, source.id);
			info.delLoadingByFile(source.id);

			expect(markDirty).not.toHaveBeenCalled();
		} finally {
			DataStore.markDirty = originalMarkDirty;
		}
	});

	it("forces a backend query when resetting one source's filters", async () => {
		const { info, source, startTimelineQueries } = createWorkerHarness();

		await info.filters_remove(source);

		const request = startTimelineQueries.mock.calls[0][0][0];
		expect(request.sourceId).toBe(source.id);
		expect(request.cachePolicy).toBe("force");
		expect(request.descriptor.queryCache?.query.source_config?.source_ids).toEqual([
			source.id,
		]);
	});

	it("reloads note normalization when settings change without reloading overlays for each filter", async () => {
		const { info, source } = createWorkerHarness();
		await info.refetch({ ids: source.id });
		await info.refetch({ ids: source.id, query: createCacheQuery(source, "another filter") });
		expect(info.notes_reload).toHaveBeenCalledTimes(1);
		info.app.target.files[0].settings.field = "gulp.event_code";
		await info.refetch({ ids: source.id });
		expect(info.notes_reload).toHaveBeenCalledTimes(2);
	});

	it("restores the committed query when consecutive replacements are canceled", async () => {
		const { info, source, getApp, startTimelineQueries } = createWorkerHarness();
		const baseline = source.appliedQuery;
		for (const label of ["first unfinished query", "second unfinished query"]) {
			await info.refetch({ ids: source.id, query: createCacheQuery(source, label, label) });
			const job = startTimelineQueries.mock.calls[startTimelineQueries.mock.calls.length - 1][0][0];
			info.handleTimelineQueryUpdate({ ...job.descriptor, sourceId: source.id, state: "started" });
			await flushSourceMetadata();
			expect(getApp().target.files[0].appliedQuery?.string).toBe(label);
		}
		const latest = startTimelineQueries.mock.calls[1][0][0];
		info.handleTimelineQueryUpdate({ ...latest.descriptor, sourceId: source.id, state: "canceled" });
		await flushSourceMetadata();
		expect(getApp().target.files[0].appliedQuery).toEqual(baseline);
		expect(getApp().target.files[0].queryCache.isActivating).toBe(false);
	});

	it("applies a cache hit without resetting source data or issuing main-thread HTTP", async () => {
		const { info, source, getApp, queryFile, startTimelineQueries } = createWorkerHarness();
		await info.refetch({ ids: source.id });
		const request = startTimelineQueries.mock.calls[0][0][0];
		const activation = createCacheActivation(source, { query: request.descriptor.queryCache!.query });
		info.handleTimelineQueryUpdate({ ...request.descriptor, sourceId: source.id, state: "cache-hit", activation: { ...activation, match: "exact" } });
		await flushSourceMetadata();
		expect(info.events_reset_in_file).not.toHaveBeenCalled();
		expect(queryFile).not.toHaveBeenCalled();
		expect(getApp().target.files[0].queryCache).toMatchObject({ activeGeneration: activation.summary.generation, canUndo: false, canRedo: false, isActivating: false });
	});

	it("keeps a latest cache hit in warning state without issuing HTTP", async () => {
		const { info, source, getApp, queryFile, startTimelineQueries } =
			createWorkerHarness();
		await info.refetch({ ids: source.id });
		const request = startTimelineQueries.mock.calls[0][0][0];
		const activation = createCacheActivation(source, {
			query: request.descriptor.queryCache!.query,
		});
		info.handleTimelineQueryUpdate({
			...request.descriptor,
			sourceId: source.id,
			state: "cache-hit",
			activation: { ...activation, match: "latest" },
		});
		await flushSourceMetadata();
		expect(queryFile).not.toHaveBeenCalled();
		expect(getApp().target.files[0].queryCache).toMatchObject({
			activeGeneration: activation.summary.generation,
			freshness: "query-mismatch",
			warningReason: "query-mismatch",
			isActivating: false,
		});
	});

	it("batches 140 source cache activations and leaves unselected sources untouched", async () => {
		const { info, source, startTimelineQueries } = createWorkerHarness();
		const sources = Array.from({ length: 140 }, (_, index) => createSource(`${source.id}-${index}` as Source.Id));
		info.app.target.files = [...sources, source];
		const publish = jest.fn(info.setInfo); info.setInfo = publish;
		await info.refetch({ ids: sources.map((entry) => entry.id) });
		const requests = startTimelineQueries.mock.calls[0][0];
		expect(requests).toHaveLength(140);
		expect(new Set(requests.map((request) => request.descriptor.requestId)).size).toBe(140);
		publish.mock.calls.length = 0;
		for (const [index, request] of requests.entries()) info.handleTimelineQueryUpdate({ ...request.descriptor, sourceId: request.sourceId, state: "cache-hit", activation: { ...createCacheActivation(sources[index]), match: "exact" } });
		await flushSourceMetadata();
		expect(publish).toHaveBeenCalledTimes(1);
		expect(info.app.target.files.slice(0, 140).every((file) => file.queryCache.activeGeneration !== null && !file.queryCache.isActivating)).toBe(true);
		expect(info.app.target.files[140]).toBe(source);
	});

	it("sends distinct Navigator queries without mutating their source snapshots", async () => {
		const { info, source, startTimelineQueries } = createWorkerHarness();
		const second = createSource("second-source" as Source.Id);
		info.app.target.files.push(second);
		const previous = source.appliedQuery;
		await info.refetch({ ids: [source.id, second.id], queriesBySourceId: { [source.id]: { ...info.getQuery(source), text_filter: "one" }, [second.id]: { ...info.getQuery(second), text_filter: "two" } } });
		expect(startTimelineQueries.mock.calls[0][0].map((request) => request.descriptor.queryCache?.query.text_filter)).toEqual(["one", "two"]);
		expect(source.appliedQuery).toBe(previous);
	});

	it("forces HTTP for note creation while preserving source-specific note options", async () => {
		const { info, source, startTimelineQueries } = createWorkerHarness();
		setSmartSocket({ startTimelineQueries, setTimelineQueryPriority: jest.fn(), con: () => "notes-listener", coff: () => undefined });
		await info.refetch({ ids: source.id, create_notes: true, notes_color: "#123456", notes_tags: ["investigation"], addToHistory: true });
		const request = startTimelineQueries.mock.calls[0][0][0];
		expect(request.cachePolicy).toBe("force");
		expect(request.body.q_options).toMatchObject({ create_notes: true, notes_color: "#123456", notes_tags: ["investigation"], add_to_history: true });
	});

	it("selects supplemental events beyond both active-query source edges", async () => {
		const source = createSource();
		const { info } = createInfoHarness(source);
		DataStore.replaceSourceEvents(source.id, [
			createTimelineDocument("query-newest", 300),
			createTimelineDocument("query-oldest", 100),
		]);
		const supplementalNewest = createTimelineDocument(
			"supplemental-newest",
			400,
		);
		const supplementalOldest = createTimelineDocument(
			"supplemental-oldest",
			50,
		);
		DataStore.upsertSupplementalEvents([
			supplementalNewest,
			createTimelineDocument("supplemental-interior", 200),
			supplementalOldest,
		]);

		expect(await info.getTimelineSourceEdge(source.id, false)).toBe(
			supplementalOldest,
		);
		expect(await info.getTimelineSourceEdge(source.id, true)).toBe(
			supplementalNewest,
		);
	});

	it("keeps active-query edges when supplemental events are inside them", async () => {
		const source = createSource();
		const { info } = createInfoHarness(source);
		const queryNewest = createTimelineDocument("query-newest", 300);
		const queryOldest = createTimelineDocument("query-oldest", 100);
		DataStore.replaceSourceEvents(source.id, [queryNewest, queryOldest]);
		DataStore.upsertSupplementalEvents([
			createTimelineDocument("supplemental-interior", 200),
		]);

		expect(await info.getTimelineSourceEdge(source.id, false)).toBe(
			queryOldest,
		);
		expect(await info.getTimelineSourceEdge(source.id, true)).toBe(
			queryNewest,
		);
	});

	it("selects sequence zero as the first event for equal timestamps", async () => {
		const source = createSource();
		const { info } = createInfoHarness(source);
		const second = {
			...createTimelineDocument("sequence-one", 100),
			"event.sequence": 1,
		};
		const first = {
			...createTimelineDocument("sequence-zero", 100),
			"event.sequence": 0,
		};
		DataStore.replaceSourceEvents(source.id, [second, first]);

		expect(await info.getTimelineSourceEdge(source.id, false)).toBe(first);
		expect(await info.getTimelineSourceEdge(source.id, true)).toBe(second);
	});

	it("selects a supplemental edge when the active query is empty", async () => {
		const source = createSource();
		const { info } = createInfoHarness(source);
		const supplemental = createTimelineDocument("supplemental-only", 200);
		DataStore.replaceSourceEvents(source.id, []);
		DataStore.upsertSupplementalEvents([supplemental]);

		expect(await info.getTimelineSourceEdge(source.id, false)).toBe(
			supplemental,
		);
		expect(await info.getTimelineSourceEdge(source.id, true)).toBe(
			supplemental,
		);
	});

	it("allows long frames to reach a ten-millisecond visible duration", () => {
		const source = createSource();
		let currentApp = createApp(source);
		const info = new Info({
			app: currentApp,
			setInfo: (action) => {
				currentApp =
					typeof action === "function" ? action(currentApp) : action;
			},
			timeline: { current: document.createElement("div") },
		});
		const frameRange = sourceTimestamp.max - sourceTimestamp.min;
		const maximumScale = info.clampTimelineScale(Number.POSITIVE_INFINITY);

		expect(maximumScale).toBe(10_000_000_000);
		expect(maximumScale > 9_999_999).toBe(true);
		expect(frameRange / maximumScale).toBe(10);

		info.setTimelineScale(Number.POSITIVE_INFINITY);
		expect(currentApp.timeline.scale).toBe(maximumScale);
	});

	it("neutralizes only generated source scope in the shape backend filter", () => {
		const source = createSource();
		const { info } = createInfoHarness(source);
		const createProvenance = (
			info as unknown as {
				createQueryCacheProvenance: (
					source: Source.Type,
					query: Query.Type,
				) => { shapeFingerprint?: string };
			}
		).createQueryCacheProvenance;
		const query: Query.Type = {
			...createCacheQuery(source, "numeric range"),
			filters: [
				{
					id: "event-code-range",
					type: "range",
					operator: "must",
					field: "event.code",
					value: "10,20",
					enabled: true,
				} as Query.Type["filters"][number],
			],
			fieldTypeMap: { "event.code": "long" },
		};
		const baseShape = createProvenance(source, query).shapeFingerprint;
		const differentGeneratedRange = createProvenance(source, {
			...query,
			string: "different display label",
			source_config: {
				...query.source_config!,
				range: { min: "1", max: "2" },
			},
		}).shapeFingerprint;
		const differentlyCoercedFilter = createProvenance(source, {
			...query,
			fieldTypeMap: { "event.code": "keyword" },
		}).shapeFingerprint;

		expect(differentGeneratedRange).toBe(baseShape);
		expect(differentlyCoercedFilter).not.toBe(baseShape);
	});

	it("marks a newly completed backend replace fresh by definition", async () => {
		const source = createSource();
		source.time_updated = null;
		const { info, getApp } = createInfoHarness(source);
		const activation = createCacheActivation(source);
		setSmartSocket({
			registerDocumentStream: jest.fn(async () => undefined),
			cancelDocumentStream: jest.fn(async () => undefined),
			completeDocumentStream: jest.fn(async () => undefined),
		});
		await info.beginEventDocumentStream({
			requestId: "frame-refetch-backend-request" as Request.Id,
			sourceIds: [source.id],
			operationId: source.operation_id as Operation.Id,
			kind: "query",
			mode: "replace",
			queryCache: activation.cache.provenance,
		});

		info.handleEventSourceSummary({
			requestId: "frame-refetch-backend-request",
			operationId: source.operation_id,
			operationEpoch: DataStore.getRenderOperationEpoch(),
			summary: activation.summary,
		});

		await new Promise((resolve) => setTimeout(resolve, 20));
		expect(getApp().target.files[0].queryCache).toMatchObject({
			activeGeneration: activation.summary.generation,
			freshness: "fresh",
			warningReason: null,
		});
	});

	it("does not refetch or apply a rebase when cache-pool invalidation fails", async () => {
		const source = createSource();
		const { info, getApp } = createInfoHarness(source);
		getApp().target.operations = [
			{
				id: source.operation_id,
				name: "Frame refetch operation",
				selected: true,
			} as Operation.Type,
		];
		let rebaseDoneListener:
			| ((message: {
					req_id: string;
					payload: { obj: Partial<Request.Type> };
			  }) => void)
			| undefined;
		const con = jest.fn<string, unknown[]>(() => "stats-listener");
		const conce = jest.fn<string, unknown[]>((...arguments_: unknown[]) => {
			rebaseDoneListener = arguments_[2] as typeof rebaseDoneListener;
			return "rebase-listener";
		});
		const clearSourceQueryCache = jest.fn<
			Promise<void>,
			[string, Source.Id]
		>(async () => {
			throw new Error("cache reset failed");
		});
		setSmartSocket({
			con,
			conce,
			coff: jest.fn(),
			clearSourceQueryCache,
		});
		const refetch = jest.fn(async () => undefined);
		info.refetch = refetch as typeof info.refetch;
		const previousApi = globalThis.api;
		globalThis.api = (async () => ({
			status: "pending",
			data: undefined,
		})) as unknown as typeof globalThis.api;

		try {
			expect(await info.rebase_by_offset(source)).toBe(true);
			if (!rebaseDoneListener) throw new Error("Missing rebase completion listener.");
			rebaseDoneListener({
				req_id: "frame-refetch-request-id",
				payload: {
					obj: {
						id: "frame-refetch-request-id" as Request.Id,
						status: Request.Status.DONE,
					},
				},
			});
			await new Promise((resolve) => setTimeout(resolve, 0));

			expect(clearSourceQueryCache).toHaveBeenCalledWith(
				source.operation_id,
				source.id,
			);
			expect(info.events_reset_in_file).not.toHaveBeenCalled();
			expect(refetch).not.toHaveBeenCalled();
			expect(getApp().target.files[0].settings.offset).toBe(2_000);
			expect(getApp().target.files[0].queryCache).toEqual(
				createEmptyQueryCacheState(),
			);
		} finally {
			globalThis.api = previousApi;
		}
	});

	it("includes note targets incrementally and retries only unresolved documents", async () => {
		const source = createSource();
		source.selected = false;
		const { info, getApp } = createInfoHarness(source);
		getApp().target.operations = [
			{
				id: source.operation_id,
				name: "Frame refetch operation",
				selected: true,
			} as Operation.Type,
		];
		const createNote = (id: string): Note.Type =>
			({
				id: `note-${id}` as Note.Id,
				type: "note",
				operation_id: source.operation_id,
				source_id: source.id,
				name: id,
				doc: {
					_id: id as Doc.Id,
					gulp_timestamp: 1_750_000_000_000,
					"gulp.source_id": source.id,
				} as Doc.Type,
			}) as Note.Type;
		const firstNote = createNote("first-target");
		const secondNote = createNote("second-target");
		let secondAttempt = false;
		const createDocument = (
			documentId: Doc.Id,
		): Doc.Type & QueryRawDocument =>
			({
				_id: documentId,
				"@timestamp": 1_750_000_000_000,
				"gulp.source_id": source.id,
				"gulp.event_code": 7,
				"event.code": 7,
			}) as unknown as Doc.Type & QueryRawDocument;
		const querySingleId = jest.fn(
			async (): Promise<Doc.Type> => createDocument(firstNote.doc._id),
		);
		const queryPaginate = jest.fn(async (query: Query.Type, _options: unknown) => {
			const requestedIds = (
				query.raw as { terms: { _id: Doc.Id[] } }
			).terms._id;
			const docs = requestedIds
				.filter(
					(documentId) =>
						documentId !== secondNote.doc._id || secondAttempt,
				)
				.map(createDocument);
			return { docs, total_hits: docs.length };
		});
		info.query_single_id = querySingleId as typeof info.query_single_id;
		info.query_paginate = queryPaginate as typeof info.query_paginate;
		let sourceSelectedWhenOverlayPublished = false;
		const unsubscribe = DataStore.subscribeRenderChanges((change) => {
			if (change.type === "supplemental-events") {
				sourceSelectedWhenOverlayPublished =
					getApp().target.files[0].selected;
			}
		});

		const partial = await info.includeNoteEvents([firstNote, secondNote, firstNote]);
		expect(partial.stale).toBe(false);
		expect(partial.events.map((event) => event._id)).toEqual([
			firstNote.doc._id,
		]);
		expect(partial.missingDocumentIds).toEqual([secondNote.doc._id]);
		expect(DataStore.getSupplementalEvents(source.id)).toHaveLength(1);
		expect(getApp().target.files[0].selected).toBe(true);
		expect(getApp().target.files[0].total).toBe(120);

		secondAttempt = true;
		const complete = await info.includeNoteEvents([firstNote, secondNote]);
		unsubscribe();
		expect(complete.events.map((event) => event._id)).toEqual([
			firstNote.doc._id,
			secondNote.doc._id,
		]);
		expect(complete.missingDocumentIds).toEqual([]);
		expect(DataStore.getSupplementalEvents(source.id)).toHaveLength(2);
		expect(querySingleId).not.toHaveBeenCalled();
		expect(queryPaginate.mock.calls.map(([query]) => query.raw)).toEqual([
			{ terms: { _id: [firstNote.doc._id, secondNote.doc._id] } },
			{ terms: { _id: [secondNote.doc._id] } },
		]);
		expect(queryPaginate.mock.calls[0][1]).toMatchObject({
			limit: 2,
			offset: 0,
			fields: "*",
		});
		expect(sourceSelectedWhenOverlayPublished).toBe(true);
	});

	it("discards a note fetch response after the operation epoch changes", async () => {
		const source = createSource();
		const { info, getApp } = createInfoHarness(source);
		getApp().target.operations = [
			{
				id: source.operation_id,
				name: "Frame refetch operation",
				selected: true,
			} as Operation.Type,
		];
		const note = {
			id: "note-stale-target" as Note.Id,
			type: "note",
			operation_id: source.operation_id,
			source_id: source.id,
			name: "stale target",
			doc: {
				_id: "stale-target" as Doc.Id,
				gulp_timestamp: 1_750_000_000_000,
				"gulp.source_id": source.id,
			} as Doc.Type,
		} as Note.Type;
		let resolveFetch!: (document: Doc.Type) => void;
		const deferredFetch = new Promise<Doc.Type>((resolve) => {
			resolveFetch = resolve;
		});
		const querySingleId = jest.fn(async () => deferredFetch);
		info.query_single_id = querySingleId as typeof info.query_single_id;

		const inclusion = info.includeNoteEvents([note]);
		await Promise.resolve();
		await Promise.resolve();
		expect(querySingleId).toHaveBeenCalledTimes(1);
		DataStore.resetRenderOperation("another-operation" as Operation.Id);
		resolveFetch({
			_id: note.doc._id,
			"@timestamp": 1_750_000_000_000,
			"gulp.source_id": source.id,
		} as unknown as Doc.Type);

		expect(await inclusion).toEqual({
			events: [],
			missingDocumentIds: [note.doc._id],
			stale: true,
		});
		expect(DataStore.getSupplementalEvents()).toEqual([]);
	});

	it("navigates source-local Undo and Redo without dispatching backend queries", async () => {
		const source = createSource();
		source.queryCache = {
			activeGeneration: 2,
			canUndo: true,
			canRedo: true,
			freshness: "fresh",
			warningReason: null,
			isActivating: false,
		};
		const { info, getApp, queryFile } = createInfoHarness(source);
		const undoQuery = createCacheQuery(source, "undo query", "*undo*");
		const redoQuery = createCacheQuery(source, "redo query", "*redo*");
		const undoActivation = createCacheActivation(source, {
			query: undoQuery,
			generation: 1,
			authorityRevision: 11,
			history: [1, 2, 3],
			historyCursor: 0,
		});
		const redoActivation = createCacheActivation(source, {
			query: redoQuery,
			generation: 2,
			authorityRevision: 12,
			history: [1, 2, 3],
			historyCursor: 1,
		});
		const pendingActivations = [undoActivation, redoActivation];
		const navigateSourceQueryCache = jest.fn(
			async (_input: unknown) => pendingActivations.shift() ?? null,
		);
		setSmartSocket({ navigateSourceQueryCache });
		DataStore.upsertSupplementalEvents([
			{
				_id: "included-before-undo" as Doc.Id,
				gulp_timestamp: 1_750_000_000_000,
				"gulp.source_id": source.id,
				number_hash: 1,
			} as Doc.Type,
		]);

		expect(await info.navigateSourceQueryCache(source, "undo")).toBe(true);
		expect(DataStore.getSupplementalEvents(source.id)).toEqual([]);
		expect(getApp().target.files[0].appliedQuery).toEqual(undoQuery);
			expect(getApp().target.files[0].queryCache).toMatchObject({
			activeGeneration: 1,
			canUndo: false,
			canRedo: true,
		});
		DataStore.upsertSupplementalEvents([
			{
				_id: "included-before-redo" as Doc.Id,
				gulp_timestamp: 1_750_000_000_000,
				"gulp.source_id": source.id,
				number_hash: 1,
			} as Doc.Type,
		]);

		expect(await info.navigateSourceQueryCache(source, "redo")).toBe(true);
		expect(DataStore.getSupplementalEvents(source.id)).toEqual([]);
		expect(navigateSourceQueryCache.mock.calls).toEqual([
			[
				{
					operationId: source.operation_id,
					sourceId: source.id,
					direction: "undo",
				},
			],
			[
				{
					operationId: source.operation_id,
					sourceId: source.id,
					direction: "redo",
				},
			],
		]);
		expect(queryFile).not.toHaveBeenCalled();
		expect(getApp().target.files[0].appliedQuery).toEqual(redoQuery);
		expect(getApp().target.files[0].queryCache).toMatchObject({
			activeGeneration: 2,
			canUndo: true,
			canRedo: true,
		});
	});

	it("retains supplemental events when Undo activation does not succeed", async () => {
		const source = createSource();
		source.queryCache = {
			activeGeneration: 2,
			canUndo: true,
			canRedo: false,
			freshness: "fresh",
			warningReason: null,
			isActivating: false,
		};
		const { info } = createInfoHarness(source);
		setSmartSocket({ navigateSourceQueryCache: async () => null });
		const supplemental = {
			_id: "included-retained" as Doc.Id,
			gulp_timestamp: 1_750_000_000_000,
			"gulp.source_id": source.id,
			number_hash: 1,
		} as Doc.Type;
		DataStore.upsertSupplementalEvents([supplemental]);

		expect(await info.navigateSourceQueryCache(source, "undo")).toBe(false);
		expect(DataStore.getSupplementalEvents(source.id)).toEqual([supplemental]);
	});

	it("classifies cache freshness using the server snapshot rather than filtered counts", () => {
		const source = createSource();
		const { info } = createInfoHarness(source);
		const classify = (activation: EventSourceQueryCacheActivation) => (info as unknown as { getQueryCacheFreshness: (source: Source.Type, cache: EventSourceQueryCacheActivation["cache"], match: string) => string }).getQueryCacheFreshness(source, activation.cache, "exact");
		expect(classify(createCacheActivation(source, { eventCount: 1 }))).toBe("fresh");
		for (const serverSnapshot of [{ total: source.total + 1 }, { minNanoseconds: "1" }, { timeUpdated: 123 }]) {
			expect(classify(createCacheActivation(source, { serverSnapshot }))).toBe("source-changed");
		}
	});

	it("serializes PIT pagination fields and closes the snapshot", async () => {
		const source = createSource();
		const { info, getApp } = createInfoHarness(source);
		getApp().target.operations = [
			{
				id: source.operation_id,
				name: "Pagination operation",
				selected: true,
			} as Operation.Type,
		];
		const previousApi = globalThis.api;
		const apiCalls: Array<[
			string,
			Record<string, unknown>,
		]> = [];
		globalThis.api = (async (path: string, options: Record<string, unknown>) => {
			apiCalls.push([path, options]);
			return {
				status: "success",
				data:
					options.method === "DELETE"
						? { closed: true }
						: {
								docs: [],
								total_hits: 120,
								pit_id: "next-pit",
								search_after: [1_700_000_000_000, "shard-doc"],
							},
			};
		}) as unknown as typeof globalThis.api;

		try {
			await info.query_paginate(source.appliedQuery!, {
				limit: 50,
				offset: 100_000,
				sort: { "@timestamp": "desc" },
				fields: "*",
				paginationMode: "pit",
				pitId: "current-pit",
				searchAfter: [1_700_000_000_000, "shard-doc"],
			});
			await info.query_raw_paginate_close(
				"next-pit",
				source.operation_id,
			);

			expect(apiCalls[0][0]).toBe("/query_raw_paginate");
			expect(apiCalls[0][1].method).toBe("POST");
			expect(apiCalls[0][1].showErrorToast).toBe(false);
			const requestBody = apiCalls[0][1].body as {
				q_options: Record<string, unknown>;
			};
			expect(requestBody.q_options).toEqual({
				limit: 50,
				offset: 100_000,
				sort: { "@timestamp": "desc" },
				fields: "*",
				pagination_mode: "pit",
				pit_id: "current-pit",
				search_after: [1_700_000_000_000, "shard-doc"],
			});
			expect(apiCalls[1]).toEqual([
				"/query_raw_paginate",
				{
					method: "DELETE",
					body: { pit_id: "next-pit" },
					query: { operation_id: source.operation_id },
					raw: true,
					showErrorToast: false,
					signal: undefined,
				},
			]);
		} finally {
			globalThis.api = previousApi;
		}
	});

	it("raises a named error for paginated-query JSend failures", async () => {
		const source = createSource();
		const { info, getApp } = createInfoHarness(source);
		getApp().target.operations = [
			{
				id: source.operation_id,
				name: "Pagination operation",
				selected: true,
			} as Operation.Type,
		];
		const previousApi = globalThis.api;
		globalThis.api = (async () => ({
			status: "error",
			data: {
				__error: {
					name: "PaginationSnapshotExpired",
					msg: "snapshot expired",
					trace: "",
				},
			},
		})) as unknown as typeof globalThis.api;

		try {
			let paginationError: unknown;
			try {
				await info.query_paginate(source.appliedQuery!, {
					paginationMode: "pit",
					pitId: "expired-pit",
				});
			} catch (error) {
				paginationError = error;
			}

			expect(paginationError).toBeInstanceOf(Error);
			const namedError = paginationError as Error & { backendName: string };
			expect(namedError.name).toBe("PaginationSnapshotExpired");
			expect(namedError.backendName).toBe("PaginationSnapshotExpired");
			expect(namedError.message).toBe("snapshot expired");
		} finally {
			globalThis.api = previousApi;
		}
	});

	it("applies link create and delete mutations without reloading the full collection", async () => {
		const source = createSource();
		const { info, getApp } = createInfoHarness(source);
		const operation = {
			id: source.operation_id,
			name: "Link mutation operation",
			selected: true,
		} as Operation.Type;
		getApp().target.operations = [operation];
		DataStore.replaceLinks([], "immediate");

		const link = {
			id: "link-without-reload" as Link.Id,
			type: "link",
			owner_user_id: "link-owner",
			name: "No reload link",
			description: "Applied from the mutation response",
			operation_id: operation.id,
			tags: [],
			color: "#ff0000",
			doc_id_from: "origin-document" as Doc.Id,
			doc_ids: ["target-document" as Doc.Id],
			glyph_id: "link-glyph" as Link.Type["glyph_id"],
			time_updated: 123,
		} as Link.Type;
		const apiMock = jest.fn(
			async (path: string): Promise<Link.Type | undefined> =>
				path === "/link_create" ? link : undefined,
		);
		const previousApi = globalThis.api;
		globalThis.api = apiMock as unknown as typeof globalThis.api;

		try {
			await info.link_create({
				name: link.name,
				event: {
					_id: link.doc_id_from,
					"gulp.operation_id": operation.id,
					"gulp.source_id": source.id,
					gulp_timestamp: sourceTimestamp.min,
					"gulp.event_code": 1,
					number_hash: 1,
				} as Doc.Type & { "gulp.operation_id": Operation.Id },
				doc_ids: link.doc_ids,
				glyph_id: link.glyph_id,
				color: link.color,
				description: link.description,
			});
			expect(DataStore.links).toEqual([link]);

			await info.link_delete(link);
			expect(DataStore.links).toEqual([]);
			expect(info.links_reload).not.toHaveBeenCalled();
			expect(apiMock.mock.calls.map(([path]) => path)).toEqual([
				"/link_create",
				"/link_delete",
			]);
		} finally {
			globalThis.api = previousApi;
			DataStore.replaceLinks([], "immediate");
		}
	});
});

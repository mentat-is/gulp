import { Info } from "@/class/Info";
import type { App } from "@/entities/App";
import type { Query } from "@/entities/Query";
import type { Source } from "@/entities/Source";
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
		assert: jest.fn(),
	},
}));

jest.mock("@/ui/utils", () => ({
	generateUUID: jest.fn(),
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
jest.mock("@/entities/Doc", () => ({ Doc: { Entity: { delete: jest.fn() } } }));
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
 * Creates a selected source with a two-second positive render offset.
 * @returns Source fixture for the refetch test.
 */
function createSource(): Source.Type {
	return {
		id: sourceId,
		name: "frame-refetch.csv",
		selected: true,
		operation_id: "frame-refetch-operation",
		settings: { offset: 2_000 },
		timestamp: sourceTimestamp,
		nanotimestamp: {
			min: BigInt(sourceTimestamp.min) * 1_000_000n,
			max: BigInt(sourceTimestamp.max) * 1_000_000n,
		},
	} as Source.Type;
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

	return {
		target: {
			files: [source],
			filters: { [source.id]: query },
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
			cache: { data: new Map(), filters: {} },
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

describe("Info timeline-frame refetch", () => {
	beforeEach(() => requestStore.reset());
	afterEach(() => requestStore.reset());

	it("persists the frame range and reapplies it to subsequent filtered refetches", async () => {
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

		const frame = {
			min: 1_742_169_600_000,
			max: 1_743_033_600_000,
		};
		await info.refetch({ ids: source.id, frame });

		expect(queryFile).toHaveBeenCalled();
		const dispatched = queryFile.mock.calls[0][0];
		expect(dispatched.source_config).toEqual({
			operation_id: source.operation_id,
			source_ids: [source.id],
			range: {
				min: (
					BigInt(frame.min - source.settings.offset) * 1_000_000n
				).toString(),
				max: (
					BigInt(frame.max - source.settings.offset) * 1_000_000n
				).toString(),
			},
		});
		expect(dispatched.text_filter).toBe("*authentication*");
		expect(dispatched.filters).toHaveLength(1);
		expect(currentApp.timeline.frame).toEqual(frame);
		expect(currentApp.target.filters[source.id].source_config).toEqual(
			dispatched.source_config,
		);
		expect(currentApp.target.filters[source.id].filters).toEqual(
			dispatched.filters,
		);

		info.setQuery(source, {
			...currentApp.target.filters[source.id],
			text_filter: "*subsequent filter*",
			source_config: {
				operation_id: source.operation_id,
				source_ids: [source.id],
				range: {
					min: source.nanotimestamp.min.toString(),
					max: source.nanotimestamp.max.toString(),
				},
			},
		});
		await info.refetch({ ids: source.id });

		const filteredRefetch = queryFile.mock.calls[1][0];
		expect(filteredRefetch.source_config?.range).toEqual(
			dispatched.source_config?.range,
		);
		expect(filteredRefetch.text_filter).toBe("*subsequent filter*");
	});

	it("activates an exact startup cache without dispatching a backend query", async () => {
		const source = createSource();
		const { info, getApp, queryFile } = createInfoHarness(source);
		const cachedQuery = createCacheQuery(
			source,
			"cached label restored from IndexedDB",
			"*authentication*",
		);
		const activation = createCacheActivation(source, { query: cachedQuery });
		const reconciliation: EventSourceQueryCacheReconciliation = {
			...activation,
			match: "exact",
		};
		const reconcileSourceQueryCache = jest.fn(
			async (_input: unknown) => reconciliation,
		);
		setSmartSocket({ reconcileSourceQueryCache });

		await info.refetch({ ids: source.id, cachePolicy: "initial" });

		expect(reconcileSourceQueryCache).toHaveBeenCalledTimes(1);
		const reconcileInput = reconcileSourceQueryCache.mock.calls[0][0] as {
			operationId: string;
			sourceId: Source.Id;
			desired: { fingerprint: string; shapeFingerprint: string };
		};
		expect(reconcileInput.operationId).toBe(source.operation_id);
		expect(reconcileInput.sourceId).toBe(source.id);
		expect(reconcileInput.desired.fingerprint.startsWith("source-query-v1:")).toBe(
			true,
		);
		expect(reconcileInput.desired.shapeFingerprint.startsWith("source-query-shape-v1:")).toBe(true);
		expect(queryFile).not.toHaveBeenCalled();
		expect(info.events_reset_in_file).not.toHaveBeenCalled();
		expect(getApp().target.files[0].appliedQuery).toEqual(cachedQuery);
		expect(getApp().target.files[0].queryCache).toMatchObject({
			activeGeneration: 4,
			canUndo: false,
			canRedo: false,
			freshness: "fresh",
			warningReason: null,
		});
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

	it("keeps an exact startup cache fresh when query_operations omits time_updated", async () => {
		const source = createSource();
		source.time_updated = null;
		const { info, getApp } = createInfoHarness(source);
		const activation = createCacheActivation(source);
		setSmartSocket({
			reconcileSourceQueryCache: jest.fn(async () => ({
				...activation,
				match: "exact" as const,
			})),
		});

		await info.refetch({ ids: source.id, cachePolicy: "initial" });

		expect(getApp().target.files[0].queryCache).toMatchObject({
			freshness: "fresh",
			warningReason: null,
		});
	});

	it("still detects changed source totals when time_updated is unavailable", async () => {
		const source = createSource();
		source.time_updated = null;
		const { info, getApp } = createInfoHarness(source);
		const activation = createCacheActivation(source, {
			serverSnapshot: { total: source.total + 1 },
		});
		setSmartSocket({
			reconcileSourceQueryCache: jest.fn(async () => ({
				...activation,
				match: "exact" as const,
			})),
		});

		await info.refetch({ ids: source.id, cachePolicy: "initial" });

		expect(getApp().target.files[0].queryCache).toMatchObject({
			freshness: "source-changed",
			warningReason: "source-changed",
		});
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

		expect(getApp().target.files[0].queryCache).toMatchObject({
			activeGeneration: activation.summary.generation,
			freshness: "fresh",
			warningReason: null,
		});
	});

	it("keeps the chosen frame while restoring the latest mismatched cache query", async () => {
		const source = createSource();
		const { info, getApp, queryFile } = createInfoHarness(source);
		const cachedQuery = createCacheQuery(
			source,
			"older failed-login query",
			"*failed login*",
		);
		const activation = createCacheActivation(source, { query: cachedQuery });
		const reconciliation: EventSourceQueryCacheReconciliation = {
			...activation,
			match: "latest",
		};
		setSmartSocket({
			reconcileSourceQueryCache: jest.fn(async () => reconciliation),
		});
		const selectedFrame = {
			min: 1_725_000_000_000,
			max: 1_726_000_000_000,
		};

		await info.refetch({
			ids: source.id,
			frame: selectedFrame,
			cachePolicy: "initial",
		});

		expect(queryFile).not.toHaveBeenCalled();
		expect(getApp().timeline.frame).toEqual(selectedFrame);
		expect(getApp().target.files[0].appliedQuery).toEqual(cachedQuery);
		expect(getApp().target.files[0].queryCache).toMatchObject({
			freshness: "query-mismatch",
			warningReason: "query-mismatch",
		});
	});

	it("treats a covered startup cache with unchanged source metadata as fresh", async () => {
		const source = createSource();
		const { info, getApp, queryFile } = createInfoHarness(source);
		const activation = createCacheActivation(source);
		setSmartSocket({
			reconcileSourceQueryCache: jest.fn(async () => ({
				...activation,
				match: "covered" as const,
			})),
		});

		await info.refetch({ ids: source.id, cachePolicy: "initial" });

		expect(queryFile).not.toHaveBeenCalled();
		expect(getApp().target.files[0].queryCache).toMatchObject({
			freshness: "fresh",
			warningReason: null,
		});
	});

	it("reports a changed source before a latest-query mismatch", async () => {
		const source = createSource();
		const { info, getApp } = createInfoHarness(source);
		const activation = createCacheActivation(source, {
			serverSnapshot: { total: source.total + 1 },
		});
		setSmartSocket({
			reconcileSourceQueryCache: jest.fn(async () => ({
				...activation,
				match: "latest" as const,
			})),
		});

		await info.refetch({ ids: source.id, cachePolicy: "initial" });

		expect(getApp().target.files[0].queryCache).toMatchObject({
			freshness: "source-changed",
			warningReason: "source-changed",
		});
	});

	it("falls back to the backend when startup reconciliation has no usable cache", async () => {
		const source = createSource();
		const { info, getApp, queryFile } = createInfoHarness(source);
		const reconcileSourceQueryCache = jest.fn(async (_input: unknown) => null);
		setSmartSocket({ reconcileSourceQueryCache });

		await info.refetch({ ids: source.id, cachePolicy: "initial" });

		expect(reconcileSourceQueryCache).toHaveBeenCalledTimes(1);
		expect(queryFile).toHaveBeenCalledTimes(1);
		expect(info.events_reset_in_file).toHaveBeenCalledWith(source);
		expect(getApp().target.files[0].appliedQuery?.text_filter).toBe(
			"*authentication*",
		);
	});

	it("reuses only cached sources during a mixed multi-source startup", async () => {
		const cachedSource = createSource();
		const missingSource = createSource(
			"frame-refetch-missing-source" as Source.Id,
		);
		missingSource.appliedQuery = createCacheQuery(
			missingSource,
			"missing source desired query",
			"*missing source*",
		);
		const { info, getApp, queryFile } = createInfoHarness(cachedSource);
		getApp().target.files.push(missingSource);
		const cachedQuery = createCacheQuery(
			cachedSource,
			"cached source restored query",
			"*cached source*",
		);
		const cachedActivation = createCacheActivation(cachedSource, {
			query: cachedQuery,
		});
		const reconciliation: EventSourceQueryCacheReconciliation = {
			...cachedActivation,
			match: "exact",
		};
		const reconcileSourceQueryCache = jest.fn(async (input: unknown) =>
			(input as { sourceId: Source.Id }).sourceId === cachedSource.id
				? reconciliation
				: null,
		);
		setSmartSocket({ reconcileSourceQueryCache });

		await info.refetch({
			ids: [cachedSource.id, missingSource.id],
			cachePolicy: "initial",
		});

		expect(reconcileSourceQueryCache).toHaveBeenCalledTimes(2);
		expect(
			reconcileSourceQueryCache.mock.calls.map(
				([input]) => (input as { sourceId: Source.Id }).sourceId,
			),
		).toEqual([cachedSource.id, missingSource.id]);
		expect(queryFile).toHaveBeenCalledTimes(1);
		expect(queryFile.mock.calls[0][0].source_config?.source_ids).toEqual([
			missingSource.id,
		]);
		expect(queryFile.mock.calls[0][1]).toMatchObject({ id: missingSource.id });
		expect(info.events_reset_in_file).toHaveBeenCalledTimes(1);
		expect(info.events_reset_in_file).toHaveBeenCalledWith(missingSource);
		expect(
			getApp().target.files.find((source) => source.id === cachedSource.id)
				?.appliedQuery,
		).toEqual(cachedQuery);
		expect(
			getApp().target.files.find((source) => source.id === missingSource.id)
				?.appliedQuery?.text_filter,
		).toBe("*missing source*");
	});

	it("keeps same-name cache freshness source-local when only one server total changes", async () => {
		const changedSource = createSource(
			"frame-refetch-rebased-source" as Source.Id,
		);
		const freshSource = createSource(
			"frame-refetch-unchanged-source" as Source.Id,
		);
		changedSource.name = "duplicated-evidence.csv";
		freshSource.name = "duplicated-evidence.csv";
		const { info, getApp, queryFile } = createInfoHarness(changedSource);
		const changedQuery = changedSource.appliedQuery!;
		const freshQuery: Query.Type = {
			...changedQuery,
			source_config: {
				...changedQuery.source_config!,
				source_ids: [freshSource.id],
				range: {
					min: freshSource.nanotimestamp.min.toString(),
					max: freshSource.nanotimestamp.max.toString(),
				},
			},
		};
		freshSource.appliedQuery = freshQuery;
		getApp().target.files.push(freshSource);

		const changedActivation = createCacheActivation(changedSource, {
			query: changedQuery,
			serverSnapshot: { total: changedSource.total - 1 },
		});
		const freshActivation = createCacheActivation(freshSource, {
			query: freshQuery,
			generation: 7,
		});
		setSmartSocket({
			reconcileSourceQueryCache: jest.fn(async (input: unknown) => {
				const { sourceId: requestedSourceId } = input as {
					sourceId: Source.Id;
				};
				const activation =
					requestedSourceId === changedSource.id
						? changedActivation
						: freshActivation;
				return { ...activation, match: "exact" as const };
			}),
		});

		await info.refetch({
			ids: [changedSource.id, freshSource.id],
			cachePolicy: "initial",
		});

		const restartedChangedSource = getApp().target.files.find(
			(source) => source.id === changedSource.id,
		)!;
		const restartedFreshSource = getApp().target.files.find(
			(source) => source.id === freshSource.id,
		)!;
		expect(queryFile).not.toHaveBeenCalled();
		expect(restartedChangedSource.name).toBe(restartedFreshSource.name);
		expect(restartedChangedSource.appliedQuery?.source_config?.source_ids).toEqual([
			changedSource.id,
		]);
		expect(restartedChangedSource.queryCache).toMatchObject({
			freshness: "source-changed",
			warningReason: "source-changed",
		});
		expect(restartedFreshSource.appliedQuery?.source_config?.source_ids).toEqual([
			freshSource.id,
		]);
		expect(restartedFreshSource.queryCache).toMatchObject({
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

	it("forces the same source query through the backend on every session action", async () => {
		const source = createSource();
		const { info, queryFile } = createInfoHarness(source);
		const reconcileSourceQueryCache = jest.fn(async (_input: unknown) => null);
		setSmartSocket({ reconcileSourceQueryCache });

		await info.refetch({ ids: source.id, cachePolicy: "force" });
		await info.refetch({ ids: source.id, cachePolicy: "force" });

		expect(reconcileSourceQueryCache).not.toHaveBeenCalled();
		expect(queryFile).toHaveBeenCalledTimes(2);
		expect(queryFile.mock.calls[0][0]).toMatchObject({
			text_filter: queryFile.mock.calls[1][0].text_filter,
			filters: queryFile.mock.calls[1][0].filters,
			source_config: queryFile.mock.calls[1][0].source_config,
		});
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

	it("does not compare a filtered cache count with the complete source total", async () => {
		const source = createSource();
		const { info, getApp } = createInfoHarness(source);
		const activation = createCacheActivation(source, { eventCount: 1 });
		setSmartSocket({
			reconcileSourceQueryCache: jest.fn(async () => ({
				...activation,
				match: "exact" as const,
			})),
		});

		await info.refetch({ ids: source.id, cachePolicy: "initial" });

		expect(source.total).toBe(120);
		expect(activation.summary.eventCount).toBe(1);
		expect(getApp().target.files[0].queryCache.freshness).toBe("fresh");
	});

	const staleSnapshots: Array<{
		field: string;
		serverSnapshot: CacheActivationOptions["serverSnapshot"];
	}> = [
		{ field: "total", serverSnapshot: { total: 121 } },
		{
			field: "range",
			serverSnapshot: {
				minNanoseconds: (
					BigInt(sourceTimestamp.min) * 1_000_000n - 1n
				).toString(),
			},
		},
		{
			field: "time_updated",
			serverSnapshot: { timeUpdated: 1_800_000_000_124 },
		},
	];
	staleSnapshots.forEach(({ field, serverSnapshot }) => {
		it(`marks an exact cache source-changed when server ${field} differs`, async () => {
			const source = createSource();
			const { info, getApp } = createInfoHarness(source);
			const activation = createCacheActivation(source, { serverSnapshot });
			setSmartSocket({
				reconcileSourceQueryCache: jest.fn(async () => ({
					...activation,
					match: "exact" as const,
				})),
			});

			await info.refetch({ ids: source.id, cachePolicy: "initial" });

				expect(getApp().target.files[0].queryCache).toMatchObject({
				freshness: "source-changed",
				warningReason: "source-changed",
			});
		});
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
});

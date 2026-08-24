import type { App } from "@/entities/App";
import { Doc } from "@/entities/Doc";
import { Source } from "@/entities/Source";
import type { Operation } from "@/entities/Operation";
import type { Context } from "@/entities/Context";
import { DataStore, type DataStoreRenderChange } from "@/store/DataStore";

jest.mock("@/locales", () => ({
	translate: (key: string): string => key,
}));
jest.mock("@/locales/core", () => ({
	translate: (key: string): string => key,
}));
jest.mock("@/entities/Source", () => ({
	Source: { Entity: { id: jest.fn() } },
}));
jest.mock("@/entities/Note", () => ({ Note: { Entity: {} } }));
jest.mock("@/entities/Operation", () => ({ Operation: { Entity: {} } }));
jest.mock("@/entities/Context", () => ({ Context: { Entity: {} } }));
jest.mock("@/entities/addon/Internal", () => ({
	Internal: {
		Transformator: {
			toTimestamp: (value: string | number | Date): number =>
				new Date(value).valueOf(),
		},
	},
}));
jest.mock("@/ui/Icon", () => ({ Icon: (): null => null }));
jest.mock("@/dto/Logger.class", () => ({ Logger: {} }));
jest.mock("@/ui/utils", () => ({
	Refractor: {
		any: { toNumber: (): number => 1 },
		get: (value: Record<string, unknown>, field: string): unknown => value[field],
	},
}));

const mockSourceEntityId = Source.Entity.id as unknown as {
	(app: App.Type, sourceId: Source.Id): Source.Type | undefined;
	mockReset(): void;
};
const SOURCE_ID = "doc-test-source" as Source.Id;
const SECOND_SOURCE_ID = "doc-test-source-second" as Source.Id;
const APP = {} as App.Type;

/**
 * Creates a minimal document fixture for asynchronous ingestion tests.
 * @param id Stable document identifier.
 * @param timestamp Timeline timestamp used for descending ordering.
 * @param sourceId Source that owns the document.
 * @returns Canonical document fixture owned by the test source.
 */
function createDocument(
	id: string,
	timestamp: number,
	sourceId = SOURCE_ID,
): Doc.Type {
	return {
		_id: id as Doc.Id,
		gulp_timestamp: timestamp,
		"gulp.source_id": sourceId,
		"gulp.event_code": 1,
		number_hash: timestamp,
	};
}

describe("Doc asynchronous ingestion", () => {
	beforeEach(() => {
		DataStore.resetRenderOperation();
		Doc.Entity.clearIndex();
		mockSourceEntityId.mockReset();
	});

	it("uses serialized operation and context IDs when a source is not hydrated", () => {
		const operationId = "operation-from-event" as Operation.Id;
		const contextId = "context-from-event" as Context.Id;
		const document = {
			...createDocument("unselected-source-event", 100),
			"gulp.operation_id": operationId,
			"gulp.context_id": contextId,
		};

		expect(Doc.Entity.operationId(APP, document)).toBe(operationId);
		expect(Doc.Entity.contextId(APP, document)).toBe(contextId);
	});

	it("normalizes the timeline timestamp from @timestamp first", () => {
		const documents = Doc.Entity.normalize(
			[
				{
					_id: "timestamp-authority",
					"@timestamp": "2025-03-26T09:24:39.186Z",
					"gulp.timestamp": "2025-03-25T17:57:19.339Z",
					"gulp.source_id": SOURCE_ID,
					"gulp.event_code": 1,
				} as unknown as Doc.Type,
			],
			"gulp.event_code",
		);

		expect(documents[0].gulp_timestamp).toBe(1_742_981_079_186);
	});

	it("publishes proven-new prepared batches as compact upserts", async () => {
		Doc.Entity.add(APP, [createDocument("existing", 300)]);
		const changes: DataStoreRenderChange[] = [];
		const unsubscribe = DataStore.subscribeRenderChanges((change) => {
			changes.push(change);
		});

		await Doc.Entity.addPreparedAsync(APP, [
			{
				sourceId: SOURCE_ID,
				docs: [
					createDocument("older", 100),
					createDocument("middle", 200),
				],
			},
		]);
		unsubscribe();

		expect(DataStore.getSourceEvents(SOURCE_ID).map((event) => event._id)).toEqual([
			"existing",
			"middle",
			"older",
		]);
		expect(changes).toHaveLength(1);
		const sourceChange = changes[0];
		expect(sourceChange.type).toBe("source-events");
		if (sourceChange.type === "source-events") {
			expect(sourceChange.mutation).toBe("upsert");
			expect(sourceChange.changedEvents).toHaveLength(2);
		}
	});

	it("rejects a queued batch captured before an operation reset", async () => {
		const staleEpoch = DataStore.getRenderOperationEpoch();
		DataStore.resetRenderOperation();

		await Doc.Entity.addPreparedAsync(
			APP,
			[
				{
					sourceId: SOURCE_ID,
					docs: [createDocument("stale", 100)],
				},
			],
			staleEpoch,
		);

		expect(DataStore.getSourceEvents(SOURCE_ID)).toEqual([]);
	});

	it("adopts a handle allocated through DataStore before the document is indexed", () => {
		const document = createDocument("render-indexed-first", 300);
		expect(DataStore.getRenderHandle(SOURCE_ID, document._id)).toBe(1);

		Doc.Entity.add(APP, [document]);

		expect(Doc.Entity.id(APP, document._id)).toBe(document);
		expect(DataStore.getRenderHandle(SOURCE_ID, document._id)).toBe(1);
	});

	it("clears document lookup references without invalidating render handles", () => {
		const document = createDocument("clear-index", 300);
		Doc.Entity.add(APP, [document]);
		expect(DataStore.getRenderHandle(SOURCE_ID, document._id)).toBe(1);

		Doc.Entity.clearIndex();

		expect(Doc.Entity.id(APP, document._id)).toBeUndefined();
		expect(DataStore.getRenderHandle(SOURCE_ID, document._id)).toBe(1);
	});

	it("yields between proven-new source commits", async () => {
		const firstDocument = createDocument("first-source", 200);
		const secondDocument = createDocument(
			"second-source",
			100,
			SECOND_SOURCE_ID,
		);

		const ingestion = Doc.Entity.addAsync(APP, [
			firstDocument,
			secondDocument,
		]);

		expect(DataStore.getSourceEvents(SOURCE_ID)).toEqual([firstDocument]);
		expect(DataStore.getSourceEvents(SECOND_SOURCE_ID)).toEqual([]);
		await ingestion;
		expect(DataStore.getSourceEvents(SECOND_SOURCE_ID)).toEqual([
			secondDocument,
		]);
	});

	it("stops later proven-new source commits after an operation reset", async () => {
		const operationEpoch = DataStore.getRenderOperationEpoch();
		const ingestion = Doc.Entity.addAsync(
			APP,
			[
				createDocument("first-before-reset", 200),
				createDocument("second-after-reset", 100, SECOND_SOURCE_ID),
			],
			operationEpoch,
		);

		expect(DataStore.getSourceEvents(SOURCE_ID)).toHaveLength(1);
		DataStore.resetRenderOperation();
		await ingestion;

		expect(DataStore.getSourceEvents(SOURCE_ID)).toEqual([]);
		expect(DataStore.getSourceEvents(SECOND_SOURCE_ID)).toEqual([]);
	});

	it("releases canonical documents, indexes, and hash ranges when a source is deleted", () => {
		const document = createDocument("deleted-source-document", 500);
		Doc.Entity.add(APP, [document]);
		const originalHandle = DataStore.getRenderHandle(SOURCE_ID, document._id);
		const changes: DataStoreRenderChange[] = [];
		const unsubscribe = DataStore.subscribeRenderChanges((change) => {
			changes.push(change);
		});

		Doc.Entity.delete(APP, { id: SOURCE_ID } as Source.Type);
		unsubscribe();

		expect(DataStore.getSourceEvents(SOURCE_ID)).toEqual([]);
		expect(Doc.Entity.id(APP, document._id)).toBeUndefined();
		expect(Doc.Entity.hashRange(SOURCE_ID)).toBeUndefined();
		expect(changes).toHaveLength(1);
		const sourceChange = changes[0];
		expect(sourceChange.type).toBe("source-events");
		if (sourceChange.type === "source-events") {
			expect(sourceChange.mutation).toBe("clear");
		}

		const replacement = createDocument("deleted-source-document", 600);
		Doc.Entity.add(APP, [replacement]);
		expect(Doc.Entity.id(APP, replacement._id)).toBe(replacement);
		expect(DataStore.getRenderHandle(SOURCE_ID, replacement._id)).toBe(
			originalHandle,
		);
	});

	it("retries a replacement merge when a concurrent source mutation commits", async () => {
		const initialDocuments = Array.from({ length: 25_001 }, (_, index) =>
			createDocument(`existing-${index}`, 100_000 - index),
		);
		Doc.Entity.add(APP, initialDocuments);
		const replacement = {
			...createDocument("existing-0", 100_001),
			number_hash: 7,
		};

		const replacementPromise = Doc.Entity.addPreparedAsync(APP, [
			{ sourceId: SOURCE_ID, docs: [replacement] },
		]);
		await new Promise<void>((resolve) => {
			setTimeout(() => {
				Doc.Entity.add(APP, [createDocument("concurrent", 50_000.5)]);
				resolve();
			}, 0);
		});
		await replacementPromise;

		const finalDocuments = DataStore.getSourceEvents(SOURCE_ID);
		expect(finalDocuments).toHaveLength(initialDocuments.length + 1);
		expect(finalDocuments.find((event) => event._id === replacement._id)).toEqual(
			replacement,
		);
		expect(finalDocuments.some((event) => event._id === "concurrent")).toBe(true);
	});
});

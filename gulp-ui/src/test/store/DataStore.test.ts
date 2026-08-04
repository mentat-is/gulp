import type { Doc } from "@/entities/Doc";
import type { Note } from "@/entities/Note";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import {
	DataStore,
	type DataStoreRenderChange,
	type NotesSyncChange,
} from "@/store/DataStore";

const OPERATION_ID = "operation-a" as Operation.Id;
const SOURCE_ID = "source-a" as Source.Id;
const SECOND_SOURCE_ID = "source-b" as Source.Id;

/**
 * Creates one minimal canonical document fixture.
 * @param id Stable document identifier suffix.
 * @param timestamp Timeline timestamp used for ordering assertions.
 * @param sourceId Source that owns the document.
 * @returns Strict document fixture owned by the test source.
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

/**
 * Creates one minimal note fixture for semantic synchronization tests.
 * @param id Stable note identifier.
 * @returns Note fixture owned by the active operation.
 */
function createNote(id: string): Note.Type {
	return {
		id: id as Note.Id,
		type: "note",
		operation_id: OPERATION_ID,
		source_id: SOURCE_ID,
		doc: createDocument(`${id}-doc`, 100),
	} as Note.Type;
}

describe("DataStore source insertion", () => {
	beforeEach(() => {
		DataStore.resetRenderOperation();
	});

	it("uses loaded render events before fallback totals for source visibility", () => {
		expect(DataStore.hasRenderableSourceEvents(SOURCE_ID, 5)).toBe(true);

		DataStore.clearSourceEvents(SOURCE_ID);

		expect(DataStore.hasRenderableSourceEvents(SOURCE_ID, 5)).toBe(false);

		DataStore.replaceSourceEvents(SOURCE_ID, [createDocument("visible", 300)]);

		expect(DataStore.hasRenderableSourceEvents(SOURCE_ID, 0)).toBe(true);
	});

	it("peeks source events without marking an unqueried source loaded empty", () => {
		expect(DataStore.peekSourceEvents(SOURCE_ID)).toBeUndefined();
		expect(DataStore.hasRenderableSourceEvents(SOURCE_ID, 5)).toBe(true);

		expect(DataStore.peekSourceEvents(SOURCE_ID) ?? []).toEqual([]);

		expect(DataStore.peekSourceEvents(SOURCE_ID)).toBeUndefined();
		expect(DataStore.hasRenderableSourceEvents(SOURCE_ID, 5)).toBe(true);
	});

	it("uses compact event-store summaries without materializing source arrays", () => {
		const summaries: number[] = [];
		const unsubscribe = DataStore.subscribeEventSourceSummaries((summary) => {
			summaries.push(summary.eventCount);
		});
		const summary = {
			sourceId: SOURCE_ID,
			revision: 3,
			status: "complete",
			eventCount: 12,
			minTimestamp: 100,
			maxTimestamp: 300,
			hashMin: 1,
			hashMax: 9,
		} as const;
		DataStore.publishEventSourceSummary(summary);
		DataStore.publishEventSourceSummary(summary);

		expect(DataStore.peekSourceEvents(SOURCE_ID)).toBeUndefined();
		expect(DataStore.hasRenderableSourceEvents(SOURCE_ID, 0)).toBe(true);
		expect(DataStore.getSourceRevision(SOURCE_ID)).toBe(3);
		expect(DataStore.getEventSourceSummary(SOURCE_ID)?.eventCount).toBe(12);
		expect(summaries).toEqual([12]);
		unsubscribe();
	});

	it("publishes quota capacity independently from canonical documents", () => {
		const observedMaximums: number[] = [];
		const unsubscribe = DataStore.subscribeEventStoreCapacity((capacity) => {
			observedMaximums.push(capacity.effectiveMaximumEvents);
		});
		DataStore.publishEventStoreCapacity({
			quotaBytes: 10_000,
			usageBytes: 1_000,
			reserveBytes: 2_000,
			availableBytes: 7_000,
			conservativeBytesPerEvent: 100,
			currentEventCount: 20,
			maximumAdditionalEvents: 70,
			effectiveMaximumEvents: 90,
		});

		expect(DataStore.getEventStoreCapacity()?.effectiveMaximumEvents).toBe(90);
		expect(observedMaximums).toEqual([90]);
		expect(DataStore.events.size).toBe(0);
		unsubscribe();
	});

	it("appends proven-new older batches without replacing the canonical array", () => {
		const existing = [createDocument("newest", 300)];
		DataStore.replaceSourceEvents(SOURCE_ID, existing);

		const result = DataStore.insertNewSourceEvents(SOURCE_ID, [
			createDocument("oldest", 100),
			createDocument("middle", 200),
		]);

		expect(result).toBe(existing);
		expect(result.map((event) => event.gulp_timestamp)).toEqual([300, 200, 100]);
	});

	it("linearly merges interleaved new batches with stable timestamp ties", () => {
		const existingTie = createDocument("existing-tie", 200);
		DataStore.replaceSourceEvents(SOURCE_ID, [
			createDocument("newest", 300),
			existingTie,
			createDocument("oldest", 100),
		]);

		const result = DataStore.insertNewSourceEvents(SOURCE_ID, [
			createDocument("new-tie", 200),
			createDocument("interleaved", 250),
		]);

		expect(result.map((event) => event.gulp_timestamp)).toEqual([
			300, 250, 200, 200, 100,
		]);
		expect(result[2]).toBe(existingTie);
	});
});

describe("DataStore render mutation contracts", () => {
	beforeEach(() => {
		DataStore.resetRenderOperation();
	});

	it("keeps document handles stable until an operation reset", () => {
		const firstDocument = createDocument("first", 300);
		const secondDocument = createDocument("second", 200);

		const firstHandle = DataStore.getRenderHandle(
			SOURCE_ID,
			firstDocument._id,
		);
		const repeatedHandle = DataStore.getRenderHandle(
			SOURCE_ID,
			firstDocument._id,
		);
		const secondHandle = DataStore.getRenderHandle(
			SOURCE_ID,
			secondDocument._id,
		);

		expect([firstHandle, repeatedHandle, secondHandle]).toEqual([1, 1, 2]);

		DataStore.resetRenderOperation();

		expect(DataStore.getRenderHandle(SOURCE_ID, secondDocument._id)).toBe(1);
	});

	it("keeps lazy DataStore-only IDs independent across source collisions", () => {
		const sharedDocumentId = "shared-document" as Doc.Id;

		expect(DataStore.getRenderHandle(SOURCE_ID, sharedDocumentId)).toBe(1);
		expect(DataStore.getRenderHandle(SECOND_SOURCE_ID, sharedDocumentId)).toBe(1);
		expect(
			DataStore.getRenderHandle(
				SOURCE_ID,
				createDocument("source-a-next", 200)._id,
			),
		).toBe(2);
		expect(
			DataStore.getRenderHandle(
				SECOND_SOURCE_ID,
				createDocument("source-b-next", 100, SECOND_SOURCE_ID)._id,
			),
		).toBe(2);
	});

	it("writes large projection ranges without changing stable handle order", () => {
		const documents = Array.from({ length: 5_000 }, (_, index) =>
			createDocument(`document-${index}`, 10_000 - index),
		);
		const handles = new Uint32Array(documents.length);

		DataStore.writeRenderHandleBuffer(
			SOURCE_ID,
			documents,
			handles,
			0,
			2_048,
		);
		DataStore.writeRenderHandleBuffer(
			SOURCE_ID,
			documents,
			handles,
			2_048,
			documents.length,
			2_048,
		);

		expect(handles[0]).toBe(1);
		expect(handles[2_047]).toBe(2_048);
		expect(handles[4_999]).toBe(5_000);
		expect(
			Array.from(DataStore.createRenderHandleBuffer(SOURCE_ID, documents, 4_990)),
		).toEqual(Array.from(handles.subarray(4_990)));
	});

	it("advances source revisions and global sequences for observable mutations", () => {
		const startingSequence = DataStore.getRenderSequence();
		const changes: DataStoreRenderChange[] = [];
		const unsubscribe = DataStore.subscribeRenderChanges((change) => {
			changes.push(change);
		});

		DataStore.replaceSourceEvents(SOURCE_ID, [createDocument("first", 300)]);
		DataStore.upsertSourceEvents(SOURCE_ID, [createDocument("second", 200)]);
		DataStore.replaceNotes([], "immediate");
		unsubscribe();

		expect(DataStore.getSourceRevision(SOURCE_ID)).toBe(2);
		expect(DataStore.getCollectionRevision("notes")).toBe(1);
		expect(DataStore.getRenderSequence()).toBe(startingSequence + 3);
		expect(
			changes.map((change) => ({
				type: change.type,
				sequence: change.sequence,
			})),
		).toEqual([
			{ type: "source-events", sequence: startingSequence + 1 },
			{ type: "source-events", sequence: startingSequence + 2 },
			{ type: "notes", sequence: startingSequence + 3 },
		]);
	});

	it("publishes a same-length upsert and restores timestamp ordering", () => {
		const originalFirst = createDocument("first", 300);
		const originalSecond = createDocument("second", 200);
		DataStore.replaceSourceEvents(SOURCE_ID, [originalFirst, originalSecond]);
		const replacementFirst = {
			...createDocument("first", 100),
			number_hash: 42,
		};
		const changes: DataStoreRenderChange[] = [];
		const unsubscribe = DataStore.subscribeRenderChanges((change) => {
			changes.push(change);
		});

		const result = DataStore.upsertSourceEvents(SOURCE_ID, [replacementFirst]);
		unsubscribe();

		expect(result).toHaveLength(2);
		expect(result).toEqual([originalSecond, replacementFirst]);
		expect(DataStore.getSourceRevision(SOURCE_ID)).toBe(2);
		expect(changes).toHaveLength(1);
		const sourceChange = changes[0];
		expect(sourceChange.type).toBe("source-events");
		if (sourceChange.type === "source-events") {
			expect(sourceChange.mutation).toBe("upsert");
			expect(sourceChange.sourceRevision).toBe(2);
			expect(sourceChange.changedEvents).toEqual([replacementFirst]);
		}
	});

	it("clears revisions, canonical events, and handle registries at reset", () => {
		const document = createDocument("second", 200);
		DataStore.replaceSourceEvents(SOURCE_ID, [document]);
		DataStore.getRenderHandle(SOURCE_ID, createDocument("first", 300)._id);
		DataStore.getRenderHandle(SOURCE_ID, document._id);
		const previousEpoch = DataStore.getRenderOperationEpoch();
		const previousSequence = DataStore.getRenderSequence();
		const changes: DataStoreRenderChange[] = [];
		const unsubscribe = DataStore.subscribeRenderChanges((change) => {
			changes.push(change);
		});

		const nextOperationId = "operation-b" as Operation.Id;
		DataStore.resetRenderOperation(nextOperationId);
		unsubscribe();

		expect(DataStore.getRenderOperationEpoch()).toBe(previousEpoch + 1);
		expect(DataStore.getRenderSequence()).toBe(previousSequence + 1);
		expect(DataStore.getSourceRevision(SOURCE_ID)).toBe(0);
		expect(DataStore.getRenderOperationId()).toBe(nextOperationId);
		expect(DataStore.events.size).toBe(0);
		expect(DataStore.getRenderHandle(SOURCE_ID, document._id)).toBe(1);
		expect(changes.map((change) => change.type)).toEqual(["operation-reset"]);
	});

	it("stops notifying a render subscriber immediately after unsubscribe", () => {
		const changes: DataStoreRenderChange[] = [];
		const unsubscribe = DataStore.subscribeRenderChanges((change) => {
			changes.push(change);
		});

		DataStore.replaceSourceEvents(SOURCE_ID, [createDocument("first", 300)]);
		unsubscribe();
		DataStore.clearSourceEvents(SOURCE_ID);

		expect(changes).toHaveLength(1);
		expect(changes[0].type).toBe("source-events");
	});

	it("does not notify overlay-only subscribers for document ingestion", () => {
		let documentNotifications = 0;
		let overlayNotifications = 0;
		const unsubscribeDocuments = DataStore.subscribeDocuments(() => {
			documentNotifications++;
		});
		const unsubscribeOverlays = DataStore.subscribeOverlays(() => {
			overlayNotifications++;
		});

		DataStore.replaceSourceEvents(SOURCE_ID, [createDocument("first", 300)]);
		expect(documentNotifications).toBe(1);
		expect(overlayNotifications).toBe(0);

		DataStore.replaceNotes([], "immediate");
		expect(documentNotifications).toBe(1);
		expect(overlayNotifications).toBe(1);
		unsubscribeDocuments();
		unsubscribeOverlays();
	});

	it("keeps canonical note replacements outside the semantic sync channel", () => {
		const changes: NotesSyncChange[] = [];
		const unsubscribe = DataStore.subscribeNotesSyncChanges((change) => {
			changes.push(change);
		});

		DataStore.replaceNotes([createNote("hydrated")], "immediate");
		unsubscribe();

		expect(changes).toEqual([]);
	});

	it("publishes snapshot completion and sequenced realtime note deltas", () => {
		const note = createNote("created");
		const secondNote = createNote("created-second");
		const changes: NotesSyncChange[] = [];
		const unsubscribe = DataStore.subscribeNotesSyncChanges((change) => {
			changes.push(change);
		});

		DataStore.publishNotesSnapshotReady(OPERATION_ID);
		const created = DataStore.publishNotesUpsertDelta(
			OPERATION_ID,
			"create",
			[note, secondNote],
		);
		const updated = DataStore.publishNotesUpsertDelta(
			OPERATION_ID,
			"update",
			[note],
		);
		const deleted = DataStore.publishNotesDeleteDelta(
			OPERATION_ID,
			[note.id],
		);
		unsubscribe();
		DataStore.publishNotesDeleteDelta(OPERATION_ID, [note.id]);

		expect(changes.map((change) => change.mutation)).toEqual([
			"snapshot-ready",
			"create",
			"update",
			"delete",
		]);
		expect(updated.sequence).toBe(created.sequence + 1);
		expect(deleted.sequence).toBe(updated.sequence + 1);
		expect(created.operationId).toBe(OPERATION_ID);
		expect(created.notes).toEqual([note, secondNote]);
		expect(deleted.noteIds).toEqual([note.id]);
	});

	it("publishes a deletion delta even when the canonical note is already absent", () => {
		const missingNoteId = "already-removed" as Note.Id;
		DataStore.replaceNotes([], "immediate");
		expect(DataStore.deleteNote(missingNoteId, "immediate")).toBeUndefined();
		const changes: NotesSyncChange[] = [];
		const unsubscribe = DataStore.subscribeNotesSyncChanges((change) => {
			changes.push(change);
		});

		DataStore.publishNotesDeleteDelta(OPERATION_ID, [missingNoteId]);
		unsubscribe();

		expect(changes).toHaveLength(1);
		expect(changes[0].mutation).toBe("delete");
		if (changes[0].mutation !== "delete") {
			throw new Error("Expected a note deletion delta");
		}
		expect(changes[0].operationId).toBe(OPERATION_ID);
		expect(changes[0].noteIds).toEqual([missingNoteId]);
	});

	it("deletes a massive note batch with one collection notification", () => {
		const notes = Array.from({ length: 10_000 }, (_, index) =>
			createNote(`bulk-note-${index}`),
		);
		DataStore.replaceNotes(notes, "immediate");
		const changes: DataStoreRenderChange[] = [];
		const unsubscribe = DataStore.subscribeRenderChanges((change) => {
			changes.push(change);
		});

		const deleted = DataStore.deleteNotes(
			notes.map((note) => note.id),
			"immediate",
		);
		unsubscribe();

		expect(deleted).toHaveLength(notes.length);
		expect(DataStore.notes).toHaveLength(0);
		expect(changes).toHaveLength(1);
		expect(changes[0]).toMatchObject({
			type: "notes",
			mutation: "delete",
		});
	});

	it("preserves relayed sequences and advances later local deltas", () => {
		const note = createNote("relayed");
		const local = DataStore.publishNotesUpsertDelta(
			OPERATION_ID,
			"create",
			[note],
		);
		const relayedSequence = local.sequence + 10;
		const changes: NotesSyncChange[] = [];
		const unsubscribe = DataStore.subscribeNotesSyncChanges((change) => {
			changes.push(change);
		});

		DataStore.relayNotesRealtimeDelta({
			type: "notes-sync",
			mutation: "update",
			operationId: OPERATION_ID,
			sequence: relayedSequence,
			notes: [note],
		});
		const next = DataStore.publishNotesDeleteDelta(OPERATION_ID, [note.id]);
		unsubscribe();

		expect(changes[0]).toMatchObject({
			mutation: "update",
			sequence: relayedSequence,
		});
		expect(next.sequence).toBe(relayedSequence + 1);
	});
});

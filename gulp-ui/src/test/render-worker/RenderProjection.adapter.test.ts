import type { Doc } from "@/entities/Doc";
import type { Note } from "@/entities/Note";
import type { Source } from "@/entities/Source";
import {
	createRenderSourceMetadataSignature,
	RenderProjectionAdapter,
	type RenderSourceProjectionInput,
} from "@/render-worker/RenderProjection.adapter";
import { DataStore } from "@/store/DataStore";

const SOURCE_ID = "source-a" as Source.Id;

/**
 * Creates one canonical projection fixture.
 * @param index Stable identifier and numeric payload seed.
 * @returns Document owned by the test source.
 */
function createDocument(index: number): Doc.Type {
	return {
		_id: `document-${index}` as Doc.Id,
		gulp_timestamp: 10_000 - index,
		"gulp.source_id": SOURCE_ID,
		"gulp.event_code": 1,
		number_hash: index * 2,
	};
}

describe("RenderProjectionAdapter cooperative ranges", () => {
	beforeEach(() => {
		DataStore.resetRenderOperation();
	});

	it("projects a source range in bounded blocks without slicing canonical data", async () => {
		const documents = Array.from({ length: 5_000 }, (_, index) =>
			createDocument(index),
		);
		const yieldCallback = jest.fn(async () => undefined);
		const adapter = new RenderProjectionAdapter({
			resolveDocument: () => undefined,
		});

		const batch = await adapter.createEventBatchAsync(
			SOURCE_ID,
			1,
			0,
			"replace",
			documents,
			undefined,
			0,
			yieldCallback,
			1_000,
			4_100,
		);

		expect(batch.handles).toHaveLength(3_100);
		expect(batch.timestamps[0]).toBe(documents[1_000].gulp_timestamp);
		expect(batch.timestamps[3_099]).toBe(documents[4_099].gulp_timestamp);
		expect(batch.hashes[3_099]).toBe(documents[4_099].number_hash);
		expect(yieldCallback).toHaveBeenCalledTimes(1);
	});

	it("projects the configured timeline source label", () => {
		const adapter = new RenderProjectionAdapter({
			resolveDocument: () => undefined,
		});
		const source = {
			id: SOURCE_ID,
			name: "C:\\\\evidence\\\\events.json",
			settings: {
				offset: 0,
				render_engine: "default",
				timeline_label: "filename",
				frequency_sample: 1_000,
			} as Source.Type["settings"],
			timestamp: { min: 0, max: 1 },
			total: 0,
		} as Source.Type;
		const project = (timeline_label: "filename" | "full_path") =>
			adapter.createSource({
				source: {
					...source,
					settings: { ...source.settings, timeline_label },
				},
				revision: 1,
				rowOrder: 0,
				contextLabel: "Context",
				contextColor: "#000000",
				loadingKind: "none",
				cacheWarning: true,
				gradient: [],
			}).name;

		expect(project("filename")).toBe("events.json");
		expect(project("full_path")).toBe(source.name);
	});

	it("projects cache warnings and advances the metadata signature when they change", () => {
		const adapter = new RenderProjectionAdapter({
			resolveDocument: () => undefined,
		});
		const input = {
			source: {
				id: SOURCE_ID,
				name: "events.json",
				settings: {
					offset: 0,
					render_engine: "default",
					timeline_label: "filename",
					frequency_sample: 1_000,
				},
				timestamp: { min: 0, max: 1 },
				total: 0,
			} as Source.Type,
			revision: 1,
			rowOrder: 0,
			contextLabel: "Context",
			contextColor: "#000000",
			loadingKind: "none",
			cacheWarning: true,
			gradient: [],
		} satisfies RenderSourceProjectionInput;

		expect(adapter.createSource(input).cacheWarning).toBe(true);
		expect(createRenderSourceMetadataSignature(input)).not.toBe(
			createRenderSourceMetadataSignature({ ...input, cacheWarning: false }),
		);
	});

	it("projects supplemental events without query authority fields", () => {
		const adapter = new RenderProjectionAdapter({
			resolveDocument: () => undefined,
		});
		const document = createDocument(7);

		expect(adapter.createSupplementalEventRecords([document])).toEqual([
			{
				id: document._id,
				sourceId: SOURCE_ID,
				timestamp: document.gulp_timestamp,
				numberHash: document.number_hash,
			},
		]);
	});

	it("projects note identity, source, and timestamp from the embedded note document", () => {
		const noteDocument = createDocument(8);
		const residentDocument = {
			...noteDocument,
			gulp_timestamp: 99,
			"gulp.source_id": "different-source" as Source.Id,
		};
		const adapter = new RenderProjectionAdapter({
			resolveDocument: () => residentDocument,
		});
		const note = {
			id: "note-embedded-document" as Note.Id,
			name: "Embedded",
			color: null,
			glyph_id: "note",
			doc: noteDocument,
		} as Note.Type;

		expect(adapter.createNoteRecords([note])).toMatchObject([
			{
				id: note.id,
				sourceId: noteDocument["gulp.source_id"],
				timestamp: noteDocument.gulp_timestamp,
			},
		]);
	});

});

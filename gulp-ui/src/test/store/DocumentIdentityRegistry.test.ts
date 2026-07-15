import type { Doc } from "@/entities/Doc";
import type { Source } from "@/entities/Source";
import { DocumentIdentityRegistry } from "@/store/DocumentIdentityRegistry";

const FIRST_SOURCE_ID = "identity-source-a" as Source.Id;
const SECOND_SOURCE_ID = "identity-source-b" as Source.Id;

/**
 * Creates one minimal canonical document for identity-registry tests.
 * @param id Stable document identifier.
 * @param sourceId Source that owns the document.
 * @param timestamp Timestamp used to distinguish replacement objects.
 * @returns Strict canonical document fixture.
 */
function createDocument(
	id: string,
	sourceId = FIRST_SOURCE_ID,
	timestamp = 1,
): Doc.Type {
	return {
		_id: id as Doc.Id,
		gulp_timestamp: timestamp,
		"gulp.source_id": sourceId,
		"gulp.event_code": 1,
		number_hash: timestamp,
	};
}

describe("DocumentIdentityRegistry", () => {
	it("keeps one handle while the indexed document object is replaced", () => {
		const registry = new DocumentIdentityRegistry({ pageSize: 4 });
		const firstDocument = createDocument("replace-me", FIRST_SOURCE_ID, 10);
		const replacement = createDocument("replace-me", FIRST_SOURCE_ID, 20);

		registry.indexDocument(firstDocument);
		expect(registry.getDocument(firstDocument._id)).toBe(firstDocument);
		expect(
			registry.getRenderHandle(FIRST_SOURCE_ID, firstDocument._id),
		).toBe(1);
		registry.indexDocument(replacement);
		expect(registry.getDocument(firstDocument._id)).toBe(replacement);
		expect(
			registry.getRenderHandle(FIRST_SOURCE_ID, firstDocument._id),
		).toBe(1);
	});

	it("indexes a document against a handle allocated earlier by render projection", () => {
		const registry = new DocumentIdentityRegistry({ pageSize: 4 });
		const document = createDocument("render-first");

		expect(registry.getRenderHandle(FIRST_SOURCE_ID, document._id)).toBe(1);
		expect(registry.hasDocument(document._id)).toBe(false);
		registry.indexDocument(document);
		expect(registry.getDocument(document._id)).toBe(document);
	});

	it("assigns dense handles in first projection order rather than index order", () => {
		const registry = new DocumentIdentityRegistry({ pageSize: 4 });
		const indexedFirst = createDocument("indexed-first");
		const projectedFirst = createDocument("projected-first");
		registry.indexDocument(indexedFirst);
		registry.indexDocument(projectedFirst);

		expect(
			registry.getRenderHandle(FIRST_SOURCE_ID, projectedFirst._id),
		).toBe(1);
		expect(
			registry.getRenderHandle(FIRST_SOURCE_ID, indexedFirst._id),
		).toBe(2);
	});

	it("preserves source-local handles for duplicate IDs across sources", () => {
		const registry = new DocumentIdentityRegistry({ pageSize: 4 });
		const firstSourceDocument = createDocument("shared-id", FIRST_SOURCE_ID, 10);
		const secondSourceDocument = createDocument("shared-id", SECOND_SOURCE_ID, 20);

		registry.indexDocument(firstSourceDocument);
		registry.indexDocument(secondSourceDocument);
		expect(registry.getDocument(firstSourceDocument._id)).toBe(
			secondSourceDocument,
		);
		expect(
			registry.getRenderHandle(FIRST_SOURCE_ID, firstSourceDocument._id),
		).toBe(1);
		expect(
			registry.getRenderHandle(SECOND_SOURCE_ID, secondSourceDocument._id),
		).toBe(1);
		expect(
			registry.getRenderHandle(
				FIRST_SOURCE_ID,
				"first-source-next" as Doc.Id,
			),
		).toBe(2);
		expect(
			registry.getRenderHandle(
				SECOND_SOURCE_ID,
				"second-source-next" as Doc.Id,
			),
		).toBe(2);
	});

	it("falls back to the retained collision document when the active source is removed", () => {
		const registry = new DocumentIdentityRegistry({ pageSize: 4 });
		const firstSourceDocument = createDocument("fallback-id", FIRST_SOURCE_ID, 10);
		const secondSourceDocument = createDocument(
			"fallback-id",
			SECOND_SOURCE_ID,
			20,
		);

		registry.indexDocument(firstSourceDocument);
		registry.indexDocument(secondSourceDocument);
		expect(registry.getDocument(firstSourceDocument._id)).toBe(
			secondSourceDocument,
		);

		expect(
			registry.removeDocument(SECOND_SOURCE_ID, secondSourceDocument._id),
		).toBe(true);
		expect(registry.getDocument(firstSourceDocument._id)).toBe(
			firstSourceDocument,
		);
		expect(
			registry.removeDocument(FIRST_SOURCE_ID, firstSourceDocument._id),
		).toBe(true);
		expect(registry.getDocument(firstSourceDocument._id)).toBeUndefined();
	});

	it("keeps the active collision document when the inactive source is removed first", () => {
		const registry = new DocumentIdentityRegistry({ pageSize: 4 });
		const firstSourceDocument = createDocument("active-id", FIRST_SOURCE_ID, 10);
		const secondSourceDocument = createDocument(
			"active-id",
			SECOND_SOURCE_ID,
			20,
		);

		registry.indexDocument(firstSourceDocument);
		registry.indexDocument(secondSourceDocument);
		expect(
			registry.removeDocument(FIRST_SOURCE_ID, firstSourceDocument._id),
		).toBe(true);
		expect(registry.getDocument(secondSourceDocument._id)).toBe(
			secondSourceDocument,
		);
		expect(
			registry.removeDocument(SECOND_SOURCE_ID, secondSourceDocument._id),
		).toBe(true);
		expect(registry.getDocument(secondSourceDocument._id)).toBeUndefined();
	});

	it("releases active references while retaining tombstoned render identities", () => {
		const registry = new DocumentIdentityRegistry({ pageSize: 4 });
		const document = createDocument("tombstone");
		registry.indexDocument(document);

		expect(registry.removeDocument(FIRST_SOURCE_ID, document._id)).toBe(true);
		expect(registry.hasDocument(document._id)).toBe(false);
		expect(registry.getDocument(document._id)).toBeUndefined();
		expect(registry.getRenderHandle(FIRST_SOURCE_ID, document._id)).toBe(1);

		const replacement = createDocument("tombstone", FIRST_SOURCE_ID, 2);
		registry.indexDocument(replacement);
		expect(registry.getRenderHandle(FIRST_SOURCE_ID, replacement._id)).toBe(1);
		expect(registry.getDocument(document._id)).toBe(replacement);
	});

	it("drops document pages independently from handle pages and resets both", () => {
		const registry = new DocumentIdentityRegistry({ pageSize: 2 });
		const firstDocument = createDocument("first");
		const secondDocument = createDocument("second");
		registry.indexDocument(firstDocument);
		registry.indexDocument(secondDocument);

		registry.clearDocumentReferences();
		expect(registry.getDocuments([firstDocument._id, secondDocument._id])).toEqual(
			[],
		);
		expect(registry.getRenderHandle(FIRST_SOURCE_ID, firstDocument._id)).toBe(1);
		registry.indexDocument(secondDocument);
		expect(registry.getRenderHandle(FIRST_SOURCE_ID, secondDocument._id)).toBe(2);

		registry.reset();
		expect(registry.getDocument(secondDocument._id)).toBeUndefined();
		expect(registry.getRenderHandle(FIRST_SOURCE_ID, secondDocument._id)).toBe(1);
	});

	it("writes existing handles in caller order across identity page boundaries", () => {
		const registry = new DocumentIdentityRegistry({ pageSize: 2 });
		const documents = Array.from({ length: 5 }, (_, index) =>
			createDocument(`page-${index}`),
		);
		const forwardHandles = new Uint32Array(5);
		registry.writeRenderHandles(
			FIRST_SOURCE_ID,
			documents,
			forwardHandles,
			0,
			documents.length,
		);
		expect(Array.from(forwardHandles)).toEqual([1, 2, 3, 4, 5]);

		const reverseHandles = new Uint32Array(7);
		registry.writeRenderHandles(
			FIRST_SOURCE_ID,
			[...documents].reverse(),
			reverseHandles,
			0,
			documents.length,
			1,
		);
		expect(Array.from(reverseHandles)).toEqual([0, 5, 4, 3, 2, 1, 0]);
	});

	it("indexes source-bound batches without consuming render handles", () => {
		const registry = new DocumentIdentityRegistry({ pageSize: 2 });
		const documents = Array.from({ length: 5 }, (_, index) =>
			createDocument(`batch-${index}`, FIRST_SOURCE_ID, index),
		);

		registry.indexDocuments(FIRST_SOURCE_ID, documents);
		expect(registry.getDocuments(documents.map((document) => document._id))).toEqual(
			documents,
		);
		expect(
			registry.getRenderHandle(FIRST_SOURCE_ID, documents[4]._id),
		).toBe(1);
		expect(
			registry.getRenderHandle(FIRST_SOURCE_ID, documents[0]._id),
		).toBe(2);
	});

	it("enforces the configured non-zero uint32 handle capacity per source", () => {
		const registry = new DocumentIdentityRegistry({
			maximumRenderHandle: 2,
			pageSize: 2,
		});
		expect(
			registry.getRenderHandle(FIRST_SOURCE_ID, "capacity-1" as Doc.Id),
		).toBe(1);
		expect(
			registry.getRenderHandle(FIRST_SOURCE_ID, "capacity-2" as Doc.Id),
		).toBe(2);
		let capacityError: unknown;
		try {
			registry.getRenderHandle(FIRST_SOURCE_ID, "capacity-3" as Doc.Id);
		} catch (error) {
			capacityError = error;
		}
		expect(capacityError).toBeInstanceOf(RangeError);
		let repeatedCapacityError: unknown;
		try {
			registry.getRenderHandle(FIRST_SOURCE_ID, "capacity-4" as Doc.Id);
		} catch (error) {
			repeatedCapacityError = error;
		}
		expect(repeatedCapacityError).toBeInstanceOf(RangeError);
		expect(
			registry.removeDocument(FIRST_SOURCE_ID, "capacity-3" as Doc.Id),
		).toBe(false);
		expect(
			registry.removeDocument(FIRST_SOURCE_ID, "capacity-4" as Doc.Id),
		).toBe(false);
		expect(
			registry.getRenderHandle(SECOND_SOURCE_ID, "capacity-3" as Doc.Id),
		).toBe(1);
	});
});

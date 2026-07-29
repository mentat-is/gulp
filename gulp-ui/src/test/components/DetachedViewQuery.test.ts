import type { Context } from "@/entities/Context";
import type { Operation } from "@/entities/Operation";
import type { Query } from "@/entities/Query";
import type { Source } from "@/entities/Source";
import {
	getSemanticSyncedQuery,
	serializeSemanticValue,
	serializeSourceFieldMetadata,
	serializeSourceIdSet,
} from "@/components/DetachedViewQuery.utils";

const OPERATION_ID = "detached-query-operation" as Operation.Id;
const CONTEXT_ID = "detached-query-context" as Context.Id;
const SOURCE_A_ID = "detached-query-source-a" as Source.Id;
const SOURCE_B_ID = "detached-query-source-b" as Source.Id;

/**
 * Creates a minimal source used by detached-view query fingerprint tests.
 * @param overrides Source fields to replace.
 * @returns Source fixture with deterministic metadata.
 */
function createSource(overrides: Partial<Source.Type> = {}): Source.Type {
	return {
		id: SOURCE_A_ID,
		operation_id: OPERATION_ID,
		context_id: CONTEXT_ID,
		name: "Source A",
		plugin: "test-plugin",
		time_updated: 10,
		...overrides,
	} as Source.Type;
}

describe("detached view query fingerprints", () => {
	it("treats cloned query objects and reordered object keys as equivalent", () => {
		const first = {
			sourceIds: [SOURCE_A_ID, SOURCE_B_ID],
			filter: {
				field: "event.code",
				value: 10,
			},
			range: { min: BigInt(1), max: BigInt(2) },
		};
		const cloneWithReorderedKeys = {
			range: { max: BigInt(2), min: BigInt(1) },
			filter: {
				value: 10,
				field: "event.code",
			},
			sourceIds: [SOURCE_A_ID, SOURCE_B_ID],
		};

		expect(serializeSemanticValue(first)).toBe(
			serializeSemanticValue(cloneWithReorderedKeys),
		);
	});

	it("treats source ids as a semantic set", () => {
		expect(serializeSourceIdSet([SOURCE_A_ID, SOURCE_B_ID])).toBe(
			serializeSourceIdSet([SOURCE_B_ID, SOURCE_A_ID, SOURCE_A_ID]),
		);
	});

	it("keeps cloned source metadata stable while reacting to freshness changes", () => {
		const source = createSource();
		const clone = createSource();
		const renamedWithSameUpdateMarker = createSource({ name: "Renamed source" });
		const updated = createSource({ time_updated: 11 });

		expect(serializeSourceFieldMetadata([source])).toBe(
			serializeSourceFieldMetadata([clone]),
		);
		expect(serializeSourceFieldMetadata([source])).toBe(
			serializeSourceFieldMetadata([renamedWithSameUpdateMarker]),
		);
		expect(serializeSourceFieldMetadata([source])).not.toBe(
			serializeSourceFieldMetadata([updated]),
		);
	});

	it("uses fallback metadata when the backend update marker is absent", () => {
		const source = createSource({ time_updated: null });
		const changedPlugin = createSource({
			time_updated: null,
			plugin: "changed-plugin",
		});

		expect(serializeSourceFieldMetadata([source])).not.toBe(
			serializeSourceFieldMetadata([changedPlugin]),
		);
	});

	it("excludes the display label and inherited source range from synced query keys", () => {
		const query = {
			string: "display label",
			filters: [],
			text_filter: "needle",
			fieldTypeMap: { "event.code": "long" },
			source_config: {
				operation_id: OPERATION_ID,
				source_ids: [SOURCE_A_ID],
				range: { min: "1", max: "2" },
			},
		} as Query.Type;
		const equivalentQuery = {
			...query,
			string: "translated display label",
			source_config: {
				...query.source_config!,
				range: { min: "100", max: "200" },
			},
		};

		expect(serializeSemanticValue(getSemanticSyncedQuery(query))).toBe(
			serializeSemanticValue(getSemanticSyncedQuery(equivalentQuery)),
		);
	});
});

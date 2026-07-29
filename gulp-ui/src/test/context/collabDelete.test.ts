import {
	normalizeCollabDeleteIds,
	normalizeCollabDeleteOperationId,
} from "@/context/collabDelete";

describe("collaboration delete payload normalization", () => {
	it("preserves the existing singular delete contract", () => {
		expect(
			normalizeCollabDeleteIds({
				type: "note",
				id: "note-a",
			}),
		).toEqual(["note-a"]);
	});

	it("normalizes a massive bulk payload once and removes invalid duplicates", () => {
		const ids = Array.from({ length: 10_000 }, (_, index) => `note-${index}`);

		expect(
			normalizeCollabDeleteIds({
				type: "note",
				ids: [...ids, "note-1", "", null],
			}),
		).toEqual(ids);
	});

	it("reads the operation_id field emitted on the websocket wire", () => {
		expect(
			normalizeCollabDeleteOperationId({
				operation_id: "dfir_report",
			}),
		).toBe("dfir_report");
	});

	it("keeps accepting the aliased operation field", () => {
		expect(
			normalizeCollabDeleteOperationId({
				"gulp.operation_id": "legacy-operation",
			}),
		).toBe("legacy-operation");
	});
});

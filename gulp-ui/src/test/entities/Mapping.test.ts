import { Mapping } from "@/entities/Mapping";

describe("Mapping.Entity.parse", () => {
	let errorSpy: JestSpy & { mockRestore(): void };

	beforeEach(() => {
		errorSpy = jest.spyOn(console, "error") as JestSpy & {
			mockRestore(): void;
		};
		errorSpy.mockReturnValue(undefined);
	});

	afterEach(() => {
		errorSpy.mockRestore();
	});

	it("groups valid mapping files by every declared plugin", () => {
		const parsed = Mapping.Entity.parse([
			{
				metadata: { plugin: ["csv.py", "json.py"] },
				filename: "shared.json",
				mapping_ids: ["default"],
			},
			{
				metadata: { plugin: ["csv.py"] },
				filename: "csv-extra.json",
				mapping_ids: ["first", "second"],
			},
		]);

		expect(parsed).toEqual([
			{
				name: "csv.py",
				methods: [
					{ name: "shared.json", mappings: ["default"] },
					{ name: "csv-extra.json", mappings: ["first", "second"] },
				],
			},
			{
				name: "json.py",
				methods: [{ name: "shared.json", mappings: ["default"] }],
			},
		]);
		expect(errorSpy).not.toHaveBeenCalled();
	});

	it("logs a malformed mapping filename and preserves valid files", () => {
		const parsed = Mapping.Entity.parse([
			{
				metadata: {},
				filename: "broken.json",
				mapping_ids: ["default"],
			},
			{
				metadata: { plugin: ["win_evtx.py"] },
				filename: "windows.json",
				mapping_ids: ["windows"],
			},
		]);

		expect(parsed).toEqual([
			{
				name: "win_evtx.py",
				methods: [{ name: "windows.json", mappings: ["windows"] }],
			},
		]);
		expect(errorSpy).toHaveBeenCalledWith(
			'[Mapping.parse] Failed to parse mapping file "broken.json".',
			errorSpy.mock.calls[0][1],
		);
		expect(errorSpy.mock.calls[0][1]).toBeInstanceOf(TypeError);
	});

	it("returns an empty result and logs a non-array response", () => {
		expect(Mapping.Entity.parse(undefined)).toEqual([]);
		expect(errorSpy).toHaveBeenCalledWith(
			"[Mapping.parse] Failed to parse the mapping file list.",
			errorSpy.mock.calls[0][1],
		);
		expect(errorSpy.mock.calls[0][1]).toBeInstanceOf(TypeError);
	});
});

import {
	isUploadFileSettingsValid,
	NO_METHOD_SELECTED,
	NO_MAPPING_SELECTED,
	resolveOptionalMethod,
	resolveOptionalMapping,
} from "@/banners/Upload.validation";

describe("upload file settings validation", () => {
	it("allows a mapping file without a mapping ID", () => {
		expect(
			isUploadFileSettingsValid(
				{
					plugin: "csv.py",
					method: "default",
				},
			),
		).toBe(true);
	});

	it("allows a plugin without a mapping file", () => {
		expect(
			isUploadFileSettingsValid(
				{
					plugin: "csv.py",
				},
			),
		).toBe(true);
	});

	it("still requires a plugin", () => {
		expect(isUploadFileSettingsValid({})).toBe(false);
	});
});

describe("optional mapping selection", () => {
	it("clears a previously selected mapping file", () => {
		expect(resolveOptionalMethod(NO_METHOD_SELECTED)).toBeUndefined();
	});

	it("preserves a selected mapping file", () => {
		expect(resolveOptionalMethod("csv.json")).toBe("csv.json");
	});

	it("clears a previously selected mapping", () => {
		expect(resolveOptionalMapping(NO_MAPPING_SELECTED)).toBeUndefined();
	});

	it("preserves a selected mapping", () => {
		expect(resolveOptionalMapping("csv.json")).toBe("csv.json");
	});
});

import {
	getFilePathsWithoutPlugin,
	isUploadFileSettingsValid,
	NO_METHOD_SELECTED,
	NO_MAPPING_SELECTED,
	resolveOptionalMethod,
	resolveOptionalMapping,
} from "@/banners/Upload.validation";
import {
	detectFilePlugin,
	detectPluginByExtension,
	detectPluginBySignature,
} from "@/banners/Upload.detection";

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

describe("upload plugin auto-selection", () => {
	const plugins = [
		"csv.py",
		"json.py",
		"suricata_fast_log.py",
		"win_evtx.py",
		"zip.py",
	];

	it("uses an available plugin that matches the file signature", () => {
		const evtxHeader = new Uint8Array([
			0x45, 0x6c, 0x66, 0x46, 0x69, 0x6c, 0x65,
		]).buffer;

		expect(detectPluginBySignature(evtxHeader, plugins)).toBe("win_evtx.py");
		expect(detectPluginBySignature(evtxHeader, ["csv.py"])).toBeUndefined();
	});

	it("prioritizes a file signature over a conflicting extension", () => {
		const zipHeader = new Uint8Array([0x50, 0x4b, 0x03, 0x04]).buffer;

		expect(detectFilePlugin(zipHeader, "events.csv", plugins)).toBe("zip.py");
	});

	it("matches an explicitly configured file extension", () => {
		expect(detectPluginByExtension("events.CSV", plugins)).toBe("csv.py");
		expect(detectPluginByExtension("Security.evtx", plugins)).toBe(
			"win_evtx.py",
		);
	});

	it("does not infer a plugin from an unconfigured extension", () => {
		expect(detectPluginByExtension("suricata.log", plugins)).toBeUndefined();
		expect(detectPluginByExtension("events.unknown", plugins)).toBeUndefined();
	});

	it("keeps the plugin blank when the configured plugin is unavailable", () => {
		expect(
			detectPluginByExtension("events.csv", ["json.py"]),
		).toBeUndefined();
	});
});

describe("files without plugins", () => {
	it("lists only current files that need a plugin selection", () => {
		expect(
			getFilePathsWithoutPlugin(["logs/one.csv", "logs/two.log"], {
				"logs/one.csv": { plugin: "csv.py" },
				"logs/two.log": {},
				"removed.log": {},
			}),
		).toEqual(["logs/two.log"]);
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

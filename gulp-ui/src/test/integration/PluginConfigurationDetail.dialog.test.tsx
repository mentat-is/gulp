import { act, cloneElement, useEffect, useState, type ReactElement, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { GulpDataset } from "@/class/Info";
import { DisplayPluginConfigurationDetailDialog } from "@/dialogs/PluginConfigurationDetail.dialog";
import { CustomParameters } from "@/components/CustomParameters";
import { QueryExternal } from "@/banners/QueryExternal.banner";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

type JsonObject = GulpDataset.SharedObject.JsonObject;
type Configuration = GulpDataset.SharedObject.Type<JsonObject>;

let mockDetails: Configuration;
const mockSpawnBanner = jest.fn<void, [ReactNode]>();
const mockDestroyBanner = jest.fn();
const mockOnUpdated = jest.fn();
const mockOnClose = jest.fn();
const mockGetConfiguration = jest.fn<void, [string]>();
const mockUpdateConfiguration = jest.fn<
	void,
	[string, GulpDataset.SharedObject.UpdatePayload]
>();
const mockInfo = {
	/**
	 * Records the requested identifier and returns the current API fixture.
	 * @param objId Shared-object identifier requested by the dialog.
	 * @returns Current saved configuration response.
	 */
	shared_object_get_by_id: async (objId: string): Promise<Configuration> => {
		mockGetConfiguration(objId);
		return mockDetails;
	},
	/**
	 * Records the update and makes the saved JSON available to the next fetch.
	 * @param objId Shared-object identifier being updated.
	 * @param payload JSON and metadata fields submitted by the editor.
	 * @returns True to emulate a successful shared-object update.
	 */
	shared_object_update: async (
		objId: string,
		payload: GulpDataset.SharedObject.UpdatePayload,
	): Promise<boolean> => {
		mockUpdateConfiguration(objId, payload);
		mockDetails = { ...mockDetails, obj: payload.obj ?? mockDetails.obj };
		return true;
	},
};

jest.mock("@/context/Application.context", () => {
	const ReactModule = require("react") as typeof import("react");
	return {
		Application: {
			Context: ReactModule.createContext({
				currentDocument: globalThis.document,
				destroyBanner: () => mockDestroyBanner(),
			}),
			use: () => ({
				Info: mockInfo,
				spawnBanner: mockSpawnBanner,
				destroyBanner: mockDestroyBanner,
				currentDocument: globalThis.document,
			}),
		},
	};
});

jest.mock("@/entities/Glyph", () => ({
	Glyph: {
		List: new Map(),
		getIdByName: () => "AcronymJson",
		Chooser: () => null,
	},
}));

// Keep unrelated timeline entities outside the UI/API integration boundary.
jest.mock("@/entities/Doc", () => ({ Doc: {} }));
jest.mock("@/entities/Source", () => ({ Source: {} }));
jest.mock("@/banners/Preview.banner", () => ({ Preview: {} }));
jest.mock("@/banners/SelectFiles.banner", () => ({ SelectFiles: {} }));
jest.mock("@/components/AdvancedPluginParams", () => ({ AdvancedPluginParams: () => null }));

jest.mock("@/locales/core", () => {
	const { strings } = require("@/locales/en.json") as {
		strings: Record<string, string>;
	};
	return { translate: (key: string) => strings[key] ?? key };
});

jest.mock("@/locales", () => {
	const { translate } = require("@/locales/core") as {
		translate: (key: string) => string;
	};
	return { Locale: { use: () => ({ t: translate }) } };
});

jest.mock("sonner", () => ({
	toast: { success: jest.fn(), error: jest.fn() },
}));

jest.mock("@/ui/utils", () => ({
	cn: (...values: Array<string | false | null | undefined>) =>
		values.filter(Boolean).join(" "),
}));

/**
 * Creates a saved configuration response with the requested JSON payload.
 * @param obj Stored plugin configuration JSON.
 * @returns Shared-object response returned by the mocked API boundary.
 */
function createConfiguration(obj: JsonObject): Configuration {
	return {
		id: "configuration-one",
		name: "Test configuration",
		type: "shared_object",
		obj_type: "plugin_configuration",
		obj,
	};
}

/**
 * Changes a controlled text or numeric field through its native input event.
 * @param control Input or textarea rendered by the real UI component.
 * @param value Text entered by the user.
 * @returns Nothing.
 */
function changeControl(control: HTMLInputElement | HTMLTextAreaElement, value: string): void {
	act(() => {
		const prototype = control instanceof HTMLTextAreaElement
			? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
		Object.getOwnPropertyDescriptor(prototype, "value")?.set?.call(control, value);
		control.dispatchEvent(new Event("input", { bubbles: true }));
	});
}

describe("Plugin configuration dialog integration", () => {
	let container: HTMLDivElement;
	let bannerContainer: HTMLDivElement;
	let root: Root;
	let bannerRoot: Root;

	beforeEach(() => {
		mockSpawnBanner.mock.calls.splice(0);
		mockDestroyBanner.mock.calls.splice(0);
		mockOnUpdated.mock.calls.splice(0);
		mockGetConfiguration.mock.calls.splice(0);
		mockUpdateConfiguration.mock.calls.splice(0);
		mockDetails = createConfiguration({
			plugin: "query_example",
			default: true,
			configuration: [
				{ name: "url", type: "str", value: "https://before.test" },
				{ name: "options", type: "dict", value: { enabled: false, retries: 0 } },
			],
		});
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
		bannerContainer = document.createElement("div");
		document.body.appendChild(bannerContainer);
		bannerRoot = createRoot(bannerContainer);
	});

	afterEach(() => {
		act(() => bannerRoot.unmount());
		act(() => root.unmount());
		container.remove();
		bannerContainer.remove();
	});

	/**
	 * Renders the complete dialog and waits for its shared-object request.
	 * @returns A promise that settles after the loaded detail tree is rendered.
	 */
	async function renderDialog(): Promise<void> {
		await act(async () => {
			root.render(
				<DisplayPluginConfigurationDetailDialog
					objId="configuration-one"
					onClose={mockOnClose}
					onUpdated={mockOnUpdated}
				/>,
			);
		});
	}

	/**
	 * Opens the configuration action and mounts its banner into the test container.
	 * @returns Nothing.
	 */
	function openConfigurationEditor(): void {
		const editButton = container.querySelectorAll<HTMLButtonElement>(
			'button[title="Edit"]',
		)[1];
		act(() => editButton.click());
		act(() => bannerRoot.render(cloneElement(
			mockSpawnBanner.mock.calls[0][0] as ReactElement<{ container: HTMLElement }>,
			{ container: bannerContainer },
		)));
	}

	/**
	 * Finds the banner's real save action without depending on its layout classes.
	 * @returns The save button.
	 */
	function getSaveButton(): HTMLButtonElement {
		const button = bannerContainer.querySelector("button svg.lucide-check")
			?.closest<HTMLButtonElement>("button");
		if (!button) throw new Error("Missing configuration save action");
		return button;
	}

	it("renders Configuration and typed parameters without plugin/default or Parameters headings", async () => {
		await renderDialog();

		expect(mockGetConfiguration).toHaveBeenCalledWith("configuration-one");
		expect(container.textContent?.includes("query_example")).toBe(false);
		expect(container.textContent?.includes("Parameters")).toBe(false);
		const url = container.querySelector<HTMLInputElement>('input[name="url"]');
		expect(url?.value).toBe("https://before.test");
		expect(url?.readOnly).toBe(true);
		const options = container.querySelector<HTMLTextAreaElement>('textarea[name="options"]');
		expect(JSON.parse(options?.value ?? "null")).toEqual({ enabled: false, retries: 0 });
		expect(options?.readOnly).toBe(true);
		expect(container.textContent?.includes("default_value")).toBe(false);
		expect(container.textContent?.includes("location")).toBe(false);
		expect(container.querySelectorAll('button[title="Edit"]')).toHaveLength(2);
		expect(
			[...container.querySelectorAll("span")].filter(
				(element) => element.textContent === "Configuration",
			),
		).toHaveLength(1);
		const defaultSwitch = container.querySelector<HTMLButtonElement>(
			'[role="switch"][aria-label="Default"]',
		);
		expect(defaultSwitch).toBeNull();
	});

	it("renders an empty state for missing, null, or empty parameter lists", async () => {
		const configurations: JsonObject[] = [
			{},
			{ configuration: null },
			{ configuration: [] },
		];
		for (const obj of configurations) {
			mockDetails = createConfiguration(obj);
			await renderDialog();
			expect(container.textContent?.includes("No records")).toBe(true);
			expect(container.querySelector('[role="switch"]')).toBeNull();
			act(() => root.unmount());
			root = createRoot(container);
		}
	});

	it("keeps non-array JSON and parameters without names inspectable", async () => {
		mockDetails = createConfiguration({
			plugin: { name: "nested plugin" },
			default: "true",
			configuration: { timeout: 0, enabled: false },
			extra: "additional field",
		});
		await renderDialog();
		expect(container.textContent?.includes("nested plugin")).toBe(false);
		expect(container.textContent?.includes("timeout0")).toBe(true);
		expect(container.textContent?.includes("additional field")).toBe(true);
		expect(container.querySelector('[role="switch"]')).toBeNull();

		act(() => root.unmount());
		root = createRoot(container);
		mockDetails = createConfiguration({ configuration: [null, 0, false, {}] });
		await renderDialog();
		expect(container.textContent?.includes("[0]null")).toBe(true);
		expect(container.textContent?.includes("[1]0")).toBe(true);
		expect(container.textContent?.includes("[2]false")).toBe(true);
	});

	it("edits a parameter through the banner and saves the original JSON shape", async () => {
		await renderDialog();
		openConfigurationEditor();
		expect(bannerContainer.querySelector("h6")?.textContent).toBe("Configuration");
		expect(bannerContainer.textContent?.includes("Parameters")).toBe(false);
		expect([...bannerContainer.querySelectorAll("span")].some(
			(element) => element.textContent === "plugin" || element.textContent === "default",
		)).toBe(false);
		expect([...bannerContainer.querySelectorAll("input")].some(
			(element) => element.value === "query_example",
		)).toBe(false);
		const input = bannerContainer.querySelector<HTMLInputElement>('input[name="url"]');
		expect(input?.readOnly).toBe(false);
		expect(getSaveButton().getAttribute("aria-disabled")).toBe("true");
		await act(async () => getSaveButton().click());
		expect(mockUpdateConfiguration).not.toHaveBeenCalled();
		changeControl(input!, "https://after.test");
		expect(getSaveButton().getAttribute("aria-disabled")).toBe("false");
		await act(async () => getSaveButton().click());

		expect(mockUpdateConfiguration).toHaveBeenCalledWith("configuration-one", {
			obj: {
				plugin: "query_example",
				default: true,
				configuration: [
					{ name: "url", type: "str", value: "https://after.test" },
					{ name: "options", type: "dict", value: { enabled: false, retries: 0 } },
				],
			},
		});
		expect(mockOnUpdated).toHaveBeenCalledTimes(1);
		expect(mockDestroyBanner).toHaveBeenCalledTimes(1);
		expect(container.querySelector<HTMLInputElement>('input[name="url"]')?.value)
			.toBe("https://after.test");
	});

	it("masks secrets in read-only details and permits visibility toggling only", async () => {
		mockDetails = createConfiguration({
			plugin: "ai_assistant",
			default: true,
			configuration: [{
				name: "openrouter_key", type: "str", is_secret: true,
				required: true, default_value: "test-secret", desc: "API credential",
			}],
		});
		const original = JSON.stringify(mockDetails.obj);
		await renderDialog();
		const secret = container.querySelector<HTMLInputElement>('input[name="openrouter_key"]')!;
		expect(secret.type).toBe("password");
		expect(secret.readOnly).toBe(true);
		expect(secret.required).toBe(true);
		expect(container.textContent?.includes("API credential")).toBe(true);
		expect(container.textContent?.includes("is_secret")).toBe(false);
		act(() => container.querySelector<HTMLButtonElement>('button[title="Show password"]')!.click());
		expect(secret.type).toBe("text");
		expect(secret.value).toBe("test-secret");
		changeControl(secret, "changed-secret");
		expect(secret.value).toBe("test-secret");
		act(() => container.querySelector<HTMLButtonElement>('button[title="Hide password"]')!.click());
		expect(secret.type).toBe("password");
		expect(JSON.stringify(mockDetails.obj)).toBe(original);
		expect(mockUpdateConfiguration).not.toHaveBeenCalled();
	});

	it("shows typed falsy defaults without materializing missing values on save", async () => {
		const configuration: JsonObject = {
			plugin: "ai_assistant", default: false, extra: { retained: true },
			configuration: [
				{ name: "default_model", type: "str", default_value: "before", location: "query", extra: "retained" },
				{ name: "limit", type: "int", default_value: 0 },
				{ name: "ratio", type: "float", default_value: 0.5 },
				{ name: "enabled", type: "bool", default_value: false },
				{ name: "blank", type: "str", value: "", default_value: "fallback" },
				{ name: "nullable", type: "str", value: null, default_value: "fallback" },
				{ name: "unset", type: "str" },
				{ name: "tags", type: "list", default_value: ["a", "b"] },
				{ name: "mode", type: "str", values: ["one", "two"], default_value: "two" },
			],
		};
		mockDetails = createConfiguration(configuration);
		await renderDialog();
		expect(container.querySelector<HTMLInputElement>('input[name="limit"]')?.value).toBe("0");
		expect(container.querySelector<HTMLInputElement>('input[name="ratio"]')?.step).toBe("any");
		expect(container.querySelector<HTMLInputElement>('input[name="blank"]')?.value).toBe("");
		expect(container.querySelector<HTMLInputElement>('input[name="nullable"]')?.value).toBe("");
		expect(container.querySelector<HTMLInputElement>('input[name="tags"]')?.value).toBe("a, b");
		const enabled = container.querySelector<HTMLButtonElement>('[role="switch"][aria-label="enabled"]')!;
		expect(enabled.disabled).toBe(true);
		expect(enabled.getAttribute("aria-checked")).toBe("false");
		const mode = container.querySelector<HTMLButtonElement>('[role="combobox"][aria-label="mode"]')!;
		expect(mode.disabled).toBe(true);
		expect(mode.textContent).toBe("two");
		openConfigurationEditor();
		expect(getSaveButton().getAttribute("aria-disabled")).toBe("true");
		changeControl(bannerContainer.querySelector<HTMLInputElement>('input[name="default_model"]')!, "after");
		await act(async () => getSaveButton().click());
		expect(mockUpdateConfiguration).toHaveBeenCalledWith("configuration-one", {
			obj: { ...configuration, configuration: [
				{ ...(configuration.configuration as JsonObject[])[0], default_value: "after" },
				...(configuration.configuration as JsonObject[]).slice(1),
			] },
		});
	});

	it("retains the default when editing an explicit value and clears dirty state on revert", async () => {
		mockDetails = createConfiguration({ configuration: [
			{ name: "url", type: "str", value: "before", default_value: "fallback", required: true },
		] });
		await renderDialog();
		openConfigurationEditor();
		const input = bannerContainer.querySelector<HTMLInputElement>('input[name="url"]')!;
		expect(input.value).toBe("before");
		changeControl(input, "after");
		expect(getSaveButton().getAttribute("aria-disabled")).toBe("false");
		changeControl(input, "before");
		expect(getSaveButton().getAttribute("aria-disabled")).toBe("true");
		changeControl(input, "");
		await act(async () => getSaveButton().click());
		expect(mockUpdateConfiguration).toHaveBeenCalledWith("configuration-one", {
			obj: { configuration: [
				{ name: "url", type: "str", value: "", default_value: "fallback", required: true },
			] },
		});
	});

	it("blocks invalid dictionary drafts and saves corrected typed values", async () => {
		await renderDialog();
		openConfigurationEditor();
		changeControl(bannerContainer.querySelector<HTMLInputElement>('input[name="url"]')!, "https://after.test");
		const options = bannerContainer.querySelector<HTMLTextAreaElement>('textarea[name="options"]')!;
		for (const invalid of ['{"enabled":', "[]", "false"]) {
			changeControl(options, invalid);
			expect(options.getAttribute("aria-invalid")).toBe("true");
			expect(getSaveButton().getAttribute("aria-disabled")).toBe("true");
			await act(async () => getSaveButton().click());
			expect(mockUpdateConfiguration).not.toHaveBeenCalled();
		}
		changeControl(options, '{"enabled":true,"retries":0}');
		expect(options.getAttribute("aria-invalid")).toBe("false");
		expect(getSaveButton().getAttribute("aria-disabled")).toBe("false");
		await act(async () => getSaveButton().click());
		expect((mockUpdateConfiguration.mock.calls[0][1].obj?.configuration as JsonObject[])[1].value)
			.toEqual({ enabled: true, retries: 0 });
	});

	it("saves edited boolean, numeric, and list defaults with their JSON types", async () => {
		mockDetails = createConfiguration({ configuration: [
			{ name: "enabled", type: "bool", default_value: false },
			{ name: "limit", type: "int", default_value: 1 },
			{ name: "ratio", type: "float", default_value: 0.5 },
			{ name: "tags", type: "list", default_value: ["before"] },
		] });
		await renderDialog();
		openConfigurationEditor();
		act(() => bannerContainer.querySelector<HTMLButtonElement>('[role="switch"][aria-label="enabled"]')!.click());
		changeControl(bannerContainer.querySelector<HTMLInputElement>('input[name="limit"]')!, "0");
		changeControl(bannerContainer.querySelector<HTMLInputElement>('input[name="ratio"]')!, "1.25");
		changeControl(bannerContainer.querySelector<HTMLInputElement>('input[name="tags"]')!, "one, two");
		await act(async () => getSaveButton().click());
		expect(mockUpdateConfiguration).toHaveBeenCalledWith("configuration-one", {
			obj: { configuration: [
				{ name: "enabled", type: "bool", default_value: true },
				{ name: "limit", type: "int", default_value: 0 },
				{ name: "ratio", type: "float", default_value: 1.25 },
				{ name: "tags", type: "list", default_value: ["one", "two"] },
			] },
		});
	});

	it("shows MISP parameter rows and preserves hidden metadata when editing custom JSON", async () => {
		const obj: JsonObject = { plugin: "misp", default: true, limit: 2, api_key: "misp-test-key", timeout: 60, published: true, verify_ssl: false };
		mockDetails = createConfiguration(obj);
		await renderDialog();
		expect(container.textContent?.includes("Plugin")).toBe(false);
		expect(container.textContent?.includes("Parameters")).toBe(false);
		expect(container.querySelector('[role="switch"][aria-label="Default"]')).toBeNull();
		expect(container.textContent?.includes("No records")).toBe(true);
		expect(container.textContent?.includes("limit2")).toBe(true);
		expect(container.textContent?.includes("verify_sslfalse")).toBe(true);
		expect(container.querySelector("input")).toBeNull();
		openConfigurationEditor();
		expect(bannerContainer.querySelector("h6")?.textContent).toBe("Configuration");
		expect(bannerContainer.textContent?.includes("Parameters")).toBe(false);
		expect([...bannerContainer.querySelectorAll("span")].some(
			(element) => element.textContent === "Configuration" ||
				element.textContent === "plugin" || element.textContent === "default",
		)).toBe(false);
		const limit = bannerContainer.querySelector<HTMLInputElement>('input[type="number"]')!;
		changeControl(limit, "3");
		await act(async () => getSaveButton().click());
		expect(mockUpdateConfiguration).toHaveBeenCalledWith("configuration-one", {
			obj: { ...obj, limit: 3 },
		});
	});

	const unsupportedConfigurations: Array<[string, GulpDataset.SharedObject.JsonValue[]]> = [
		["mixed definitions", [{ name: "url", type: "str", value: "before" }, null]],
		["unknown types", [{ name: "url", type: "unknown", value: "before" }]],
		["duplicate names", [{ name: "url", type: "str" }, { name: "url", type: "str" }]],
		["unsupported enums", [{ name: "url", type: "str", values: [{}] }]],
	];
	for (const [caseName, configuration] of unsupportedConfigurations) {
		it(`keeps ${caseName} inspectable in the JSON fallback`, async () => {
			mockDetails = createConfiguration({ configuration });
			await renderDialog();
			expect(container.querySelector('input[name="url"]')).toBeNull();
			expect(container.textContent?.includes("url")).toBe(true);
			openConfigurationEditor();
			expect(bannerContainer.textContent?.includes("type")).toBe(true);
			expect(getSaveButton().getAttribute("aria-disabled")).toBe("true");
		});
	}
});

/**
 * Controls the real external-query request builder and reports its resulting values.
 * @param props.parameters Parameter definitions supplied by plugin metadata.
 * @param props.onValues Observer receiving the controlled request values.
 * @param props.onValidityChange Observer receiving dictionary draft validity.
 * @returns The external-query custom parameter controls.
 */
function ExternalRequestHarness({ parameters, onValues, onValidityChange }: {
	parameters: CustomParameters.Interface[];
	onValues: (values: CustomParameters.ValueMap) => void;
	onValidityChange: (isValid: boolean) => void;
}) {
	const [values, setValues] = useState<CustomParameters.ValueMap>({});
	useEffect(() => onValues(values), [onValues, values]);
	return <QueryExternal.DynamicRequestBuilder
		selectedOption={{ custom_parameters: parameters } as GulpDataset.PluginList.Interface}
		params={values}
		setParams={setValues}
		onValidityChange={onValidityChange}
	/>;
}

describe("External query shared parameter controls integration", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("initializes and edits query controls by default with typed values", () => {
		const onValues = jest.fn<void, [CustomParameters.ValueMap]>();
		const onValidityChange = jest.fn<void, [boolean]>();
		const parameters: CustomParameters.Interface[] = [
			{ name: "limit", type: "int", required: false, default_value: 0 },
			{ name: "ratio", type: "float", required: false, default_value: 0.5 },
			{ name: "enabled", type: "bool", required: false, default_value: false },
			{ name: "tags", type: "list", required: false, default_value: [] },
			{ name: "mode", type: "bool", required: false, default_value: false, values: [false, true] },
			{ name: "secret", type: "str", required: true, is_secret: true, default_value: "before" },
		];
		act(() => root.render(<ExternalRequestHarness parameters={parameters} onValues={onValues} onValidityChange={onValidityChange} />));
		expect(onValues.mock.calls.at(-1)?.[0]).toEqual({ limit: 0, ratio: 0.5, enabled: false, tags: [], mode: false, secret: "before" });
		const limit = container.querySelector<HTMLInputElement>('input[name="limit"]')!;
		expect(limit.readOnly).toBe(false);
		changeControl(limit, "12");
		changeControl(container.querySelector<HTMLInputElement>('input[name="ratio"]')!, "1.25");
		changeControl(container.querySelector<HTMLInputElement>('input[name="tags"]')!, "one, two");
		const enabled = container.querySelector<HTMLButtonElement>('[role="switch"][aria-label="enabled"]')!;
		expect(enabled.disabled).toBe(false);
		act(() => enabled.click());
		const mode = container.querySelector<HTMLButtonElement>('[role="combobox"][aria-label="mode"]')!;
		expect(mode.disabled).toBe(false);
		expect(mode.textContent).toBe("false");
		changeControl(container.querySelector<HTMLInputElement>('input[name="secret"]')!, "after");
		expect(onValues.mock.calls.at(-1)?.[0]).toEqual({ limit: 12, ratio: 1.25, enabled: true, tags: ["one", "two"], mode: false, secret: "after" });
		changeControl(limit, "");
		expect(onValues.mock.calls.at(-1)?.[0].limit).toBeNull();
		expect(onValidityChange.mock.calls.at(-1)?.[0]).toBe(true);
	});

	it("reports dictionary validity without publishing an invalid query value", () => {
		const onValues = jest.fn<void, [CustomParameters.ValueMap]>();
		const onValidityChange = jest.fn<void, [boolean]>();
		const parameters: CustomParameters.Interface[] = [
			{ name: "options", type: "dict", required: false, default_value: { enabled: false } },
		];
		act(() => root.render(<ExternalRequestHarness parameters={parameters} onValues={onValues} onValidityChange={onValidityChange} />));
		const options = container.querySelector<HTMLTextAreaElement>('textarea[name="options"]')!;
		changeControl(options, "{");
		expect(onValidityChange.mock.calls.at(-1)?.[0]).toBe(false);
		expect(onValues.mock.calls.at(-1)?.[0]).toEqual({ options: { enabled: false } });
		changeControl(options, '{"enabled":true}');
		expect(onValidityChange.mock.calls.at(-1)?.[0]).toBe(true);
		expect(onValues.mock.calls.at(-1)?.[0]).toEqual({ options: { enabled: true } });
	});
});


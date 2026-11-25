import { act, forwardRef, useImperativeHandle } from "react";
import { createRoot, type Root } from "react-dom/client";
import { Extension } from "@/context/Extension.context";
import type { UiPluginModule, UiPluginRuntimeMetadata } from "@/plugin-api/v1";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

let mockLocal: Extension.LocalUiPlugin[] = [];
let mockRemote: Extension.Payload[] = [];
let mockToken = "";
let mockServer = "http://backend.test";
let mockFetchFailure = false;
const mockRelease = jest.fn();
const mockActivate = jest.fn();
const mockRuntimeRelease = jest.fn();

jest.mock("@/context/Application.context", () => ({
	Application: { use: () => ({ app: { general: { user: mockToken ? { token: mockToken } : undefined } }, banner: null }) },
}));
jest.mock("@/entities/addon/Internal", () => ({ Internal: { Settings: { get server() { return mockServer; } } } }));
jest.mock("@/locales/core", () => ({
	translate: (key: string) => key,
}));

/** Creates a source plugin using the same metadata and component contract as custom users. */
function localPlugin(filename = "Custom.tsx", slot = "login"): Extension.LocalUiPlugin {
	return {
		metadata: { filename, display_name: "Custom", plugin: "custom", extension: true, version: "1.0.0", desc: "", type: [slot] },
		load: async () => ({ default: () => <span data-local />, activate: () => { mockActivate(); return mockRelease; } }),
	};
}

/** Describes a compatible runtime fixture in the same slot as the local fixture. */
function remotePlugin(): Extension.Payload & UiPluginRuntimeMetadata {
	return { ...localPlugin("Remote.tsx").metadata, id: "remote", build_hash: "a".repeat(64),
		entry_url: "/ui_plugin_asset/remote/plugin.js", requires_ui_api: { major: 1, minor: 0 } };
}

/** Builds an anonymous backend list response without adding any session credentials. */
async function fetchList(): Promise<Response> {
	if (mockFetchFailure) throw new Error("backend unavailable");
	return { ok: true, json: async () => ({ status: "success", data: mockRemote }) } as Response;
}

describe("UI plugin integration", () => {
	let root: Root;
	let container: HTMLDivElement;
	let originalFetch: typeof fetch;
	let acquireSpy: JestSpy;
	let localSpy: JestSpy;

	beforeEach(() => {
		mockLocal = [localPlugin()];
		mockRemote = [];
		mockToken = "";
		mockServer = "http://backend.test";
		mockFetchFailure = false;
		mockRelease.mock.calls.splice(0);
		mockActivate.mock.calls.splice(0);
		mockRuntimeRelease.mock.calls.splice(0);
		acquireSpy = jest.spyOn(Extension, "acquireUiPlugin").mockResolvedValue({
			module: { default: () => <span data-runtime /> }, release: mockRuntimeRelease,
		});
		localSpy = jest.spyOn(Extension, "getLocalUiPlugins").mockImplementation(() => mockLocal);
		originalFetch = globalThis.fetch;
		globalThis.fetch = fetchList;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
		globalThis.fetch = originalFetch;
		acquireSpy.mockRestore();
		localSpy.mockRestore();
	});

	/** Renders real provider and slot components and settles their source imports. */
	async function render(slots?: string[]): Promise<void> {
		await act(async () => {
			root.render(<Extension.Provider slots={slots}><Extension.Components type="login" /></Extension.Provider>);
		});
	}

	it("loads a local login plugin when the backend list is empty", async () => {
		await render();
		expect(container.querySelector("[data-local]")).not.toBeNull();
		expect(mockActivate).toHaveBeenCalledTimes(1);
	});

	it("keeps custom plugins available when backend discovery fails", async () => {
		mockFetchFailure = true;
		await render();
		expect(container.querySelector("[data-local]")).not.toBeNull();
	});

	it("uses local metadata without requiring the filename in the backend response", async () => {
		mockRemote = [{ ...localPlugin("Unrelated.tsx").metadata, type: ["operation-menu"] }];
		await render(["login"]);
		expect(container.querySelector("[data-local]")).not.toBeNull();
	});

	it("gives local source plugins precedence over a runtime plugin with the same filename", async () => {
		mockRemote = [localPlugin().metadata];
		await render();
		expect(container.querySelectorAll("[data-local]")).toHaveLength(1);
		expect(acquireSpy.mock.calls).toHaveLength(0);
	});

	it("renders local plugins before a slow runtime download finishes", async () => {
		mockRemote = [remotePlugin()];
		let finish!: (result: { module: UiPluginModule; release: () => void }) => void;
		acquireSpy.mockReturnValue(new Promise((resolve) => { finish = resolve; }));
		await render();
		expect(container.querySelector("[data-local]")).not.toBeNull();
		expect(container.querySelector("[data-runtime]")).toBeNull();
		await act(async () => finish({ module: { default: () => <span data-runtime /> }, release: mockRuntimeRelease }));
		expect(container.querySelector("[data-runtime]")).not.toBeNull();
	});

	it("releases a stale download after a backend switch without mounting its component", async () => {
		mockRemote = [remotePlugin()];
		let finish!: (result: { module: UiPluginModule; release: () => void }) => void;
		acquireSpy.mockReturnValue(new Promise((resolve) => { finish = resolve; }));
		await render();
		mockServer = "http://second-backend.test";
		acquireSpy.mockResolvedValue({ module: { default: () => <span data-runtime /> }, release: mockRuntimeRelease });
		await act(async () => window.dispatchEvent(new Event("gulp-server-changed")));
		const staleRelease = jest.fn();
		await act(async () => finish({ module: { default: () => <span data-stale /> }, release: staleRelease }));
		expect(container.querySelector("[data-stale]")).toBeNull();
		expect(container.querySelector("[data-runtime]")).not.toBeNull();
		expect(staleRelease).toHaveBeenCalledTimes(1);
	});

	it("disposes session registrations and restores the login plugin after logout", async () => {
		await render();
		mockToken = "session-one";
		await render();
		expect(mockRelease).toHaveBeenCalledTimes(1);
		mockToken = "";
		await render();
		expect(mockRelease).toHaveBeenCalledTimes(2);
		expect(mockActivate).toHaveBeenCalledTimes(3);
		expect(container.querySelector("[data-local]")).not.toBeNull();
	});

	it("does not activate operational registrations before login and cleans them on logout", async () => {
		mockLocal = [localPlugin("Transforms.tsx", "event-actions")];
		await render();
		expect(mockActivate).not.toHaveBeenCalled();
		mockToken = "session-one";
		await render();
		expect(mockActivate).toHaveBeenCalledTimes(1);
		mockToken = "";
		await render();
		expect(mockRelease).toHaveBeenCalledTimes(1);
	});

	it("preserves capability filters for detached clients", async () => {
		await render(["event-actions"]);
		expect(container.querySelector("[data-local]")).toBeNull();
		expect(mockActivate).not.toHaveBeenCalled();
	});

	it("isolates a plugin render failure from healthy plugins and the host", async () => {
		const broken = localPlugin("Broken.tsx");
		broken.load = async () => ({ default: () => { throw new Error("broken plugin fixture"); } });
		mockLocal.push(broken);
		await render();
		expect(container.querySelector("[data-local]")).not.toBeNull();
	});

	it("preserves send-data refs when the component is rendered through the registry", async () => {
		const done = jest.fn();
		const component = forwardRef<{ onDone: () => void }>((_props, ref) => {
			useImperativeHandle(ref, () => ({ onDone: done }));
			return <span data-send-data />;
		});
		mockLocal = [{ metadata: localPlugin("Send.tsx", "send-data").metadata, load: async () => ({ default: component }) }];
		const ref = { current: null as { onDone: () => void } | null };
		await act(async () => root.render(
			<Extension.Provider><Extension.Component name="Send.tsx" props={{ ref }} /></Extension.Provider>,
		));
		ref.current?.onDone();
		expect(done).toHaveBeenCalledTimes(1);
	});

	it("checks the versioned ABI before importing an incompatible bundle", () => {
		let rejected = false;
		try {
			Extension.assertUiPluginCompatible({ id: "new-api", build_hash: "a", entry_url: "/asset.js", requires_ui_api: { major: 2, minor: 0 } }, { major: 1, minor: 0 });
		} catch { rejected = true; }
		expect(rejected).toBe(true);
		Extension.assertUiPluginCompatible({ id: "v1", build_hash: "a", entry_url: "/asset.js", requires_ui_api: { major: 1, minor: 0 } }, { major: 1, minor: 0 });
		expect(Extension.uiPluginAssetUrl("https://backend.test/api", "/ui_plugin_asset/a.js")).toBe("https://backend.test/api/ui_plugin_asset/a.js");
	});
});

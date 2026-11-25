import {
	createContext, ReactNode, useContext, useEffect, useState,
} from "react";
import { Application } from "./Application.context";
import * as React from "react";
import type { Version } from "@/dto/Dataset";
import { Internal } from "@/entities/addon/Internal";
import type {
	UiPluginModule, UiPluginRuntimeMetadata, UiPluginApiVersion, UiPluginLocales,
} from "@/plugin-api/v1";
import { translate } from "@/locales/core";

type UiPluginPublicApi = typeof import("@/plugin-api/v1");

/** Browser bridge uses the actual UI exports and the same React/context instances. */
interface UiPluginHost extends UiPluginApiVersion {
	react: typeof React;
	jsxRuntime: typeof import("react/jsx-runtime");
	jsxDevRuntime: typeof import("react/jsx-dev-runtime");
	createApi: (locales: UiPluginLocales) => UiPluginPublicApi;
}

declare global {
	var __gulpUiPluginHost: UiPluginHost;
}

declare const require: {
	context(path: string, recursive: boolean, pattern: RegExp, mode?: "lazy"): {
		keys(): string[];
		<T>(key: string): T;
	};
};

const modules = new Map<string, Promise<UiPluginModule>>();
const stylesheets = new Map<string, { ready: Promise<void>; link: HTMLLinkElement; users: number }>();

const __component = Symbol("__extension_component");

/** Isolates render failures so one custom component cannot replace the host UI. */
class PluginBoundary extends React.Component<{ children: ReactNode }, { failed: boolean }> {
	state = { failed: false };

	/** Suppresses only the component tree that failed to render. */
	static getDerivedStateFromError(): { failed: boolean } {
		return { failed: true };
	}

	/** Reports the failed plugin without escalating it to the global app boundary. */
	componentDidCatch(error: Error): void {
		console.error("UI plugin render:", error);
	}

	/** Returns healthy plugin content, or an empty slot after a render error. */
	render(): ReactNode {
		return this.state.failed ? null : this.props.children;
	}
}

/** Discovers local and backend plugins independently and owns their activation lifecycle. */
function _({ children, filenames, slots }: Extension.Provider.Props) {
	const { banner, app } = Application.use();
	const [extensions, setExtensions] = useState<Record<string, Extension.Interface>>({});
	const [revision, setRevision] = useState(0);
	const filenameSignature = filenames ? [...filenames].sort().join("\u0000") : null;
	const slotSignature = slots ? [...slots].sort().join("\u0000") : null;
	const authenticationKey = app.general.user?.token ?? "";

	useEffect(() => {
		/** Invalidates pending work and re-discovers plugins for the newly selected backend. */
		const refresh = () => setRevision((value) => value + 1);
		window.addEventListener("gulp-server-changed", refresh);
		return () => window.removeEventListener("gulp-server-changed", refresh);
	}, []);

	useEffect(() => {
		let disposed = false;
		const cleanups = new Set<() => void>();
		const server = Internal.Settings.server.replace(/\/+$/, "");
		const requestedFilenames = filenameSignature === null ? null : new Set(filenameSignature.split("\u0000"));
		const requestedSlots = slotSignature === null ? null : new Set(slotSignature.split("\u0000"));
		setExtensions({});

		/** Applies host capability filters without consulting backend plugin membership. */
		const selected = (plugin: Extension.Payload): boolean => {
			if (requestedFilenames && !requestedFilenames.has(plugin.filename)) return false;
			return !requestedSlots || Extension.normalizeMountTargets(
				Extension.normalizeTypeList(plugin.type), plugin.targets,
			).some((target) => requestedSlots.has(target.slot));
		};

		/** Publishes a ready component and records its activation and stylesheet ownership. */
		const publish = (plugin: Extension.Payload, module: UiPluginModule, release: () => void) => {
			if (disposed) { release(); return; }
			if (!module.default) { release(); throw new Error(`Missing component: ${plugin.filename}`); }
			const type = Extension.normalizeTypeList(plugin.type);
			const targets = Extension.normalizeMountTargets(type, plugin.targets);
			let deactivate: void | (() => void);
			try {
				if (authenticationKey || targets.some((target) => target.slot === Extension.Slot.Login)) {
					deactivate = module.activate?.({
						server,
						t: (key, vars) => translate(key, vars, module.locales),
					});
				}
			} catch (error) { release(); throw error; }
			cleanups.add(() => { try { deactivate?.(); } finally { release(); } });
			setExtensions((current) => ({
				...current,
				[plugin.filename]: { ...plugin, type, targets, [__component]: module.default },
			}));
		};

		const local = Extension.getLocalUiPlugins();
		const localFilenames = new Set(local.map((plugin) => plugin.metadata.filename));
		for (const plugin of local.filter((item) => selected(item.metadata))) {
			void plugin.load().then((module) => publish(plugin.metadata, module, () => {}))
				.catch((error: unknown) => console.error(`Local UI plugin ${plugin.metadata.filename}:`, error));
		}

		/** Fetches public runtime metadata while local plugins continue loading independently. */
		const loadRuntime = async () => {
			const response = await fetch(`${server}/ui_plugin_list`, { credentials: "omit" });
			if (!response.ok) throw new Error(`UI plugin list: HTTP ${response.status}`);
			const result: { status?: string; data?: Array<Extension.Payload & UiPluginRuntimeMetadata> } = await response.json();
			if (result.status !== "success" || !Array.isArray(result.data)) throw new Error("Invalid UI plugin list");
			if (disposed) return;
			await Promise.all(result.data.filter((plugin) => !localFilenames.has(plugin.filename) && selected(plugin))
				.map(async (plugin) => {
					try {
						const loaded = await Extension.acquireUiPlugin(server, plugin);
						publish(plugin, loaded.module, loaded.release);
					} catch (error) { console.error(`Runtime UI plugin ${plugin.filename}:`, error); }
				}));
		};
		void loadRuntime().catch((error: unknown) => console.error("Runtime UI plugin discovery:", error));

		return () => {
			disposed = true;
			for (const cleanup of cleanups) {
				try { cleanup(); } catch (error) { console.error("UI plugin cleanup:", error); }
			}
		};
	}, [filenameSignature, slotSignature, authenticationKey, revision]);

	return (
		<Extension.Context.Provider value={{ extensions }}>
			{children}
			{banner?.target === "main" && banner.node}
		</Extension.Context.Provider>
	);
}

export namespace Extension {
	/** One custom plugin compiled into the UI, independently of backend discovery. */
	export interface LocalUiPlugin {
		metadata: Extension.Payload;
		load: () => Promise<UiPluginModule>;
	}

	/** Discovers source/companion JSON pairs and normalizes the logical filename. */
	export function getLocalUiPlugins(): LocalUiPlugin[] {
		const manifests = require.context("../plugins", true, /\.[jt]sx?\.json$/);
		const sources = require.context("../plugins", true, /\.[jt]sx?$/, "lazy");
		const sourceNames = new Set(sources.keys());
		return manifests.keys().sort().flatMap((key) => {
			const source = key.slice(0, -5);
			if (!sourceNames.has(source)) {
				console.warn(`Missing local UI plugin source: ${source}`);
				return [];
			}
			return [{
				metadata: { ...manifests<Extension.Payload>(key), filename: source.slice(2) },
				load: () => sources<Promise<UiPluginModule>>(source),
			}];
		});
	}

	/** Validates the declared host ABI before downloading or evaluating plugin code. */
	export function assertUiPluginCompatible(plugin: UiPluginRuntimeMetadata, available: UiPluginApiVersion): void {
		const required = plugin.requires_ui_api;
		if (!required || !Number.isInteger(required.major) || !Number.isInteger(required.minor)
			|| required.major !== available.major || required.minor < 0
			|| required.minor > available.minor) {
			throw new Error(`Unsupported UI plugin API for ${plugin.id}`);
		}
	}

	/** Resolves assets against the selected backend, preserving a reverse-proxy prefix. */
	export function uiPluginAssetUrl(server: string, asset: string): string {
		const base = server.replace(/\/+$/, "");
		const url = new URL(asset.startsWith("/") ? base + asset : asset, base + "/");
		if (!/^https?:$/.test(url.protocol) || url.origin !== new URL(base).origin) {
			throw new Error("UI plugin assets must belong to the selected backend");
		}
		return url.href;
	}

	/** Shares one CSS download per document and removes the link after its last user. */
	async function acquireStylesheet(url: string): Promise<() => void> {
		let stylesheet = stylesheets.get(url);
		if (!stylesheet) {
			const link = document.createElement("link");
			link.rel = "stylesheet";
			link.href = url;
			const ready = new Promise<void>((resolve, reject) => {
				link.onload = () => resolve();
				link.onerror = () => reject(new Error(`Failed to load plugin stylesheet ${url}`));
			});
			stylesheet = { ready, link, users: 0 };
			stylesheets.set(url, stylesheet);
			document.head.appendChild(link);
		}
		stylesheet.users += 1;
		const owned = stylesheet;
		let released = false;
		/** Releases exactly one stylesheet owner, including failed and stale downloads. */
		const release = () => {
			if (released) return;
			released = true;
			owned.users -= 1;
			if (owned.users === 0) {
				owned.link.remove();
				stylesheets.delete(url);
			}
		};
		try {
			await owned.ready;
			return release;
		} catch (error) {
			release();
			throw error;
		}
	}

	/** Loads a public ESM bundle using the host React/API and acquires its optional CSS. */
	export async function acquireUiPlugin(server: string, plugin: UiPluginRuntimeMetadata): Promise<{
		module: UiPluginModule;
		release: () => void;
	}> {
		const api = await import("@/plugin-api/v1");
		assertUiPluginCompatible(plugin, api.UI_PLUGIN_API_VERSION);
		if (!globalThis.__gulpUiPluginHost) {
			const [jsxRuntime, jsxDevRuntime] = await Promise.all([
				import("react/jsx-runtime"), import("react/jsx-dev-runtime"),
			]);
			globalThis.__gulpUiPluginHost = {
				...api.UI_PLUGIN_API_VERSION, react: React, jsxRuntime, jsxDevRuntime,
				/** Binds dictionaries while reusing the UI's components, services and locale engine. */
				createApi: (locales) => ({
					...api,
					Locale: { ...api.Locale, use: () => api.Locale.use(locales) },
					translate: (key, vars) => api.translate(key, vars, locales),
				}),
			};
		}
		const url = uiPluginAssetUrl(server, plugin.entry_url);
		let promise = modules.get(url);
		if (!promise) {
			promise = import(/* webpackIgnore: true */ url) as Promise<UiPluginModule>;
			modules.set(url, promise);
			void promise.catch(() => modules.delete(url));
		}
		const release = plugin.css_url
			? await acquireStylesheet(uiPluginAssetUrl(server, plugin.css_url)) : () => {};
		try {
			const module = await promise;
			if (!module.default) throw new Error(`Missing UI plugin component: ${plugin.id}`);
			return { module, release };
		} catch (error) {
			release();
			throw error;
		}
	}

	export type Type = string;
	export type Slot = string;
	export type ComponentProps = Record<string, unknown>;

	export const Slot = {
		Login: "login",
		OperationMenu: "operation-menu",
		SendData: "send-data",
		EventActions: "event-actions",
		EventContextMenu: "event-context-menu",
		SigmaUploadMode: "sigma-upload-mode",
		AIAssistantWindow: "ai-assistant-window",
		DashboardView: "dashboard-view",
	} as const;

	export interface MountTarget {
		slot: Slot;
		order?: number;
		group?: string;
		variant?: string;
		labelKey?: string;
		icon?: string;
	}

	export interface Payload {
		display_name: string;
		plugin: string;
		extension: boolean;
		version: Version;
		desc: string;
		path?: string;
		filename: string;
		type?: Type | Type[];
		targets?: MountTarget[];
	}

	export interface Interface extends Omit<Payload, "type"> {
		type: Type[];
		targets: MountTarget[];
		[__component]:
			| React.ComponentType<ComponentProps>
			| ((props: ComponentProps) => React.JSX.Element);
	}

	export const Context = createContext<Extension.Export | undefined>(undefined);

	export interface Export {
		extensions: Record<string, Extension.Interface>;
	}

	export const use = () => {
		const ctx = useContext(Extension.Context);
		if (!ctx) throw new Error("Extension.Context not found");
		return ctx;
	};

	export namespace Provider {
		export interface Props {
			children: ReactNode;
			/** Optional plugin filename allow-list for capability-scoped clients. */
			filenames?: readonly string[];
			/** Optional mount-slot allow-list for capability-scoped clients. */
			slots?: readonly Slot[];
		}
	}

	export const Provider = _;

	/**
	 * Converts server-provided plugin type data into a predictable array.
	 *
	 * @param type - Raw plugin type data from the backend.
	 * @returns A list of plugin type or slot identifiers.
	 */
	export function normalizeTypeList(type?: Type | Type[]): Type[] {
		if (Array.isArray(type)) {
			return type.filter((item) => typeof item === "string" && item.trim().length > 0);
		}

		if (typeof type === "string" && type.trim().length > 0) {
			return [type];
		}

		return [];
	}

	/**
	 * Resolves legacy plugin type names to their current slot identifiers.
	 *
	 * @param type - A plugin type or slot identifier.
	 * @returns The slot identifier represented by the type.
	 */
	export function resolveSlot(type: Type): Slot {
		if (type === "menu") return Slot.OperationMenu;
		if (type === "send_data") return Slot.SendData;
		return type;
	}

	/**
	 * Builds the final mount target list from explicit targets and slot-like types.
	 *
	 * @param types - Normalized plugin type or slot identifiers.
	 * @param targets - Explicit mount target metadata from plugin configuration.
	 * @returns A de-duplicated list of mount targets.
	 */
	export function normalizeMountTargets(
		types: Type[],
		targets?: MountTarget[],
	): MountTarget[] {
		const normalizedTargets = Array.isArray(targets)
			? targets
				.filter(
					(target) =>
						typeof target.slot === "string" &&
						target.slot.trim().length > 0,
				)
				.map((target) => ({ ...target, slot: target.slot.trim() }))
			: [];
		const knownSlots = new Set(normalizedTargets.map((target) => target.slot));

		for (const type of types) {
			const slot = resolveSlot(type);
			if (!knownSlots.has(slot)) {
				normalizedTargets.push({ slot });
				knownSlots.add(slot);
			}
		}

		return normalizedTargets.sort(
			(a, b) => (a.order ?? Number.MAX_SAFE_INTEGER) - (b.order ?? Number.MAX_SAFE_INTEGER),
		);
	}

	/**
	 * Checks whether an extension declares support for a UI slot.
	 *
	 * @param extension - The loaded extension entry to inspect.
	 * @param slot - The slot identifier to match.
	 * @returns True when the extension targets the given slot.
	 */
	export function targetsSlot(
		extension: Extension.Interface,
		slot: Slot,
	): boolean {
		return extension.targets.some((target) => target.slot === slot);
	}

	/**
	 * Returns all extensions that target a specific UI slot.
	 *
	 * @param extensions - The loaded extension registry.
	 * @param slot - The slot identifier to query.
	 * @returns Slot-matching extensions sorted by mount order and display name.
	 */
	export function getBySlot(
		extensions: Record<string, Extension.Interface>,
		slot: Slot,
	): Extension.Interface[] {
		return Object.values(extensions)
			.filter((extension) => targetsSlot(extension, slot))
			.sort((first, second) => {
				const firstTarget = first.targets.find((target) => target.slot === slot);
				const secondTarget = second.targets.find((target) => target.slot === slot);
				const firstOrder = firstTarget?.order ?? Number.MAX_SAFE_INTEGER;
				const secondOrder = secondTarget?.order ?? Number.MAX_SAFE_INTEGER;

				if (firstOrder !== secondOrder) {
					return firstOrder - secondOrder;
				}

				return (first.display_name || first.filename).localeCompare(
					second.display_name || second.filename,
				);
			});
	}

	/**
	 * Checks whether at least one extension targets a specific UI slot.
	 *
	 * @param extensions - The loaded extension registry.
	 * @param slot - The slot identifier to query.
	 * @returns True when at least one extension targets the slot.
	 */
	export function hasSlot(
		extensions: Record<string, Extension.Interface>,
		slot: Slot,
	): boolean {
		return Object.values(extensions).some((extension) =>
			targetsSlot(extension, slot),
		);
	}

	export const safe =
		(func: () => Promise<{ default: React.ComponentType<ComponentProps> }>) =>
		async () => {
			try {
				return await func();
			} catch (error) {
				console.log("Component not found or failed to load:", String(error));
				return null;
			}
		};

	export namespace Component {
		export interface Props {
			className?: string;
			name: string;
			props?: ComponentProps;
		}
	}

	/**
	 * Renders a loaded extension component by filename.
	 *
	 * @param props - Component filename, optional host wrapper class, and optional props for the mounted plugin.
	 * @returns The plugin component, or null when unavailable.
	 */
	export function Component({ className, name, props }: Extension.Component.Props) {
		const { extensions } = Extension.use();
		const extension = extensions[name];
		if (!extension) {
			console.warn(`Extenstion ${name} not found in plugin list. Skipping...`);
			return null;
		}

		const Component = extension[__component];
		if (!Component) {
			console.error(
				`Extenstion ${name} was found in plugin list, but there is no component. Skipping...`,
			);
			return null;
		}

		const element = <PluginBoundary><Component {...props} /></PluginBoundary>;

		if (!className) {
			return element;
		}

		return <div className={className}>{element}</div>;
	}

	export namespace Optional {
		export interface Props {
			name: string;
			children: React.ReactNode;
		}
	}

	/**
	 * Renders children only when a filename-based extension is available.
	 *
	 * @param props - Extension filename and children to render conditionally.
	 * @returns The children when the extension exists, otherwise null.
	 */
	export function Optional({ name, children }: Extension.Optional.Props) {
		const { extensions } = Extension.use();
		const extension = extensions[name];
		if (!extension) {
			return null;
		}

		return children;
	}

	export namespace Components {
		export interface Props {
			type: Slot;
			props?: ComponentProps;
		}
	}

	/**
	 * Renders all extensions assigned to a UI slot.
	 *
	 * @param props - Slot identifier and optional props passed to every plugin.
	 * @returns All plugin components registered for the slot.
	 */
	export function Components({ type, props }: Components.Props) {
		const { extensions } = Extension.use();

		return getBySlot(extensions, type)
			.map((extension) => (
				<Extension.Component
					key={extension.filename}
					name={extension.filename}
					props={props}
				/>
			));
	}
}

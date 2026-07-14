import {
	createContext,
	ReactNode,
	useContext,
	useEffect,
	useState,
} from "react";
import { Application } from "./Application.context";
import React from "react";
import { Version } from "@/dto/Dataset";
import { Icon } from "@/ui/Icon";

const __component = Symbol("__extension_component");

let extensionsPromise: Promise<Record<string, Extension.Interface>> | null =
	null;

function _({ children }: Extension.Provider.Props) {
	const { banner } = Application.use();
	const [extensions, setExtensions] = useState<
		Record<string, Extension.Interface>
	>({});

	useEffect(() => {
		let isMounted = true;

		const load = async () => {
			extensionsPromise = (async () => {
				const new_extensions: Record<string, Extension.Interface> = {};

				const plugins = await api<Extension.Payload[]>("/ui_plugin_list");
				if (!Array.isArray(plugins)) {
					console.error(
						`Backend returned unexpected type of ${plugins}. Expected array of plugins, but got ${typeof plugins}`,
						"Extension.Provider",
						{
							richColors: true,
							icon: <Icon name="Warning" />,
						},
					);
					return {};
				}

				await Promise.all(
					plugins.map(async (plugin) => {
						try {
							const Component = Extension.safe(
								() => import(`@/plugins/${plugin.filename}`),
							);
							const component = await Component();

							if (!component) {
								console.error(
									`Failed to load component ${plugin.filename}: component is null`,
									_,
								);
								return;
							}

							if (component.default) {
								console.log(
									`Component ${plugin.filename} has been successfully loaded and memorized`,
									_,
								);
							} else {
								console.warn(
									`Component ${plugin.filename} loaded but has no default export`,
									_,
								);
							}

							const types = Extension.normalizeTypeList(plugin.type);

							new_extensions[plugin.filename] = {
								...plugin,
								type: types,
								targets: Extension.normalizeMountTargets(types, plugin.targets),
								[__component]: component.default,
							};
						} catch (err) {
							console.error(
								`Failed to load component ${plugin.filename}: ${err}`,
								_,
							);
						}
					}),
				);

				return new_extensions;
			})();

			try {
				const data = await extensionsPromise;
				if (isMounted) {
					setExtensions(data);
				}
			} catch (err) {
				console.error("Failed to resolve extensions promise:", err);
			}
		};

		if (!extensionsPromise) {
			load();
		} else {
			extensionsPromise.then((data) => {
				if (isMounted) {
					setExtensions(data);
				}
			});
		}

		const handleServerChanged = () => {
			extensionsPromise = null;
			load();
		};

		window.addEventListener("gulp-server-changed", handleServerChanged);

		return () => {
			isMounted = false;
			window.removeEventListener("gulp-server-changed", handleServerChanged);
		};
	}, []);

	const extensionProps: Extension.Export = {
		extensions,
	};

	return (
		<Extension.Context.Provider value={extensionProps}>
			{children}
			{banner?.target === "main" && banner.node}
		</Extension.Context.Provider>
	);
}

export namespace Extension {
	export type Type = string;
	export type Slot = string;
	export type ComponentProps = Record<string, unknown>;

	export const Slot = {
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
		path: string;
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
			return type.filter((item) => item.trim().length > 0);
		}

		if (type && type.trim().length > 0) {
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

		const element = <Component {...props} />;

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

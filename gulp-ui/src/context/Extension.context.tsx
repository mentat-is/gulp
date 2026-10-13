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
import { EventStoreFallbackBanner } from "@/banners/EventStoreFallback.banner";

const __component = Symbol("__extension_component");

const extensionPromises = new Map<
	string,
	Promise<Record<string, Extension.Interface>>
>();

function _({ children, filenames, slots }: Extension.Provider.Props) {
	const { banner, eventStoreFallbackRequest } = Application.use();
	const [extensions, setExtensions] = useState<
		Record<string, Extension.Interface>
	>({});
	const [ready, setReady] = useState<boolean>(false);

	const filenameSignature = filenames
		? [...filenames].sort().join("\u0000")
		: null;
	const slotSignature = slots
		? [...slots].sort().join("\u0000")
		: null;

	useEffect(() => {
		let isMounted = true;
		const requestedFilenames = filenameSignature === null
			? null
			: new Set(filenameSignature ? filenameSignature.split("\u0000") : []);
		const requestedSlots = slotSignature === null
			? null
			: new Set(slotSignature ? slotSignature.split("\u0000") : []);
		const cacheKey =
			`files:${filenameSignature ?? "*"}:slots:${slotSignature ?? "*"}`;

		/** Creates and caches the extension request for the active capability scope. */
		const createExtensionPromise = () => {
			const promise = (async () => {
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

				const selectedPlugins = plugins.filter((plugin) => {
					if (
						requestedFilenames &&
						!requestedFilenames.has(plugin.filename)
					) {
						return false;
					}
					if (!requestedSlots) return true;
					const types = Extension.normalizeTypeList(plugin.type);
					return Extension.normalizeMountTargets(
						types,
						plugin.targets,
					).some((target) => requestedSlots.has(target.slot));
				});
				await Promise.all(
					selectedPlugins.map(async (plugin) => {
						try {
							// A template import() makes webpack bundle every file under
							// src/plugins: keep the test suites colocated with paid plugins
							// (__tests__/, *.test.*, *.spec.*) and their mocks out of the
							// production build.
							const Component = Extension.safe(
								() =>
									import(
										/* webpackExclude: /(\/__tests__\/|\/__mocks__\/|\.(test|spec)\.[cm]?[jt]sx?$)/ */
										`@/plugins/${plugin.filename}`
									),
							);
							const component = await Component();

							if (!component) {
								console.error(
									`Failed to load component ${plugin.filename}: component is null`,
									_,
								);
								return;
							}

							if (!component.default) {
								console.warn(
									`Component ${plugin.filename} loaded but has no default export`,
									_,
								);
								return;
							}

							console.log(
								`Component ${plugin.filename} has been successfully loaded and memorized`,
								_,
							);

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
			extensionPromises.set(cacheKey, promise);
			return promise;
		};

		/** Applies one cached or newly created extension request to this provider. */
		const load = async () => {
			const promise = extensionPromises.get(cacheKey) ?? createExtensionPromise();
			try {
				const data = await promise;
				if (isMounted) {
					setExtensions(data);
				}
			} catch (err) {
				console.error("Failed to resolve extensions promise:", err);
				if (isMounted) {
					setExtensions({});
				}
			} finally {
				if (isMounted) {
					setReady(true);
				}
			}
		};

		setReady(false);
		void load();

		const handleServerChanged = () => {
			extensionPromises.delete(cacheKey);
			setReady(false);
			void load();
		};

		window.addEventListener("gulp-server-changed", handleServerChanged);

		return () => {
			isMounted = false;
			window.removeEventListener("gulp-server-changed", handleServerChanged);
		};
	}, [filenameSignature, slotSignature]);

	const extensionProps: Extension.Export = {
		extensions,
		ready,
	};

	return (
		<Extension.Context.Provider value={extensionProps}>
			{children}
			{banner?.target === "main" ? banner.node : null}
			{!banner && eventStoreFallbackRequest ? (
				<EventStoreFallbackBanner request={eventStoreFallbackRequest} />
			) : null}
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
		QueryExternal: "query-external",
		EventActions: "event-actions",
		EventContextMenu: "event-context-menu",
		SigmaUploadMode: "sigma-upload-mode",
		AIAssistantWindow: "ai-assistant-window",
		DashboardView: "dashboard-view",
		HomeSection: "home-section",
	} as const;

	export interface MountTarget {
		slot: Slot;
		order?: number;
		group?: string;
		variant?: string;
		labelKey?: string;
		titleKey?: string;
		icon?: string;
		route?: string;
		requiredPermissions?: string[];
	}

	export interface HomeSectionTarget extends MountTarget {
		slot: typeof Slot.HomeSection;
		route: string;
		labelKey: string;
		titleKey: string;
		icon: string;
	}

	export interface HomeSectionMount {
		extension: Interface;
		target: HomeSectionTarget;
	}

	/** Props supplied by the Home shell to every home-section component. */
	export interface HomeSectionComponentProps {
		/** Slot selected by the host for this plugin mount. */
		slot: typeof Slot.HomeSection;
		/** Manifest target selected by the Home route resolver. */
		target: HomeSectionTarget;
		/** Incrementing value requesting that the component reload its data. */
		reloadToken: number;
		/** Reports list loading state to the shared Home header control. */
		onLoadingChange: (loading: boolean) => void;
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
		/** Whether the current extension discovery request has settled. */
		ready: boolean;
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
			return type
				.filter(
					(item): item is string =>
						typeof item === "string" && item.trim().length > 0,
				)
				.map((item) => item.trim());
		}

		if (typeof type === "string" && type.trim().length > 0) {
			return [type.trim()];
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
					(target): target is MountTarget =>
						typeof target === "object" &&
						target !== null &&
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

	const ReservedHomeRoutes = new Set([
		"/",
		"/groups",
		"/login",
		"/renderer-test",
		"/users",
	]);
	const ReservedHomeRoutePrefixes = ["/detached", "/operations"];

	/**
	 * Converts a manifest value to a trimmed non-empty string.
	 *
	 * @param value - Raw metadata value received from the backend.
	 * @returns The normalized string, or null when the value is invalid.
	 */
	function normalizeTargetText(value: unknown): string | null {
		if (typeof value !== "string") return null;
		const normalizedValue = value.trim();
		return normalizedValue.length > 0 ? normalizedValue : null;
	}

	/**
	 * Normalizes a manifest-provided Home route for exact pathname matching.
	 *
	 * @param route - Raw route read from one Home mount target.
	 * @returns A normalized absolute path, or null when the route is unsafe or reserved.
	 */
	export function normalizeHomeRoute(route: unknown): string | null {
		if (typeof route !== "string") return null;

		const trimmedRoute = route.trim();
		if (
			!/^\/[A-Za-z0-9][A-Za-z0-9/_-]*$/.test(trimmedRoute) ||
			trimmedRoute.includes("//")
		) {
			return null;
		}

		const normalizedRoute = trimmedRoute.replace(/\/+$/, "");
		if (ReservedHomeRoutes.has(normalizedRoute)) return null;
		if (
			ReservedHomeRoutePrefixes.some(
				(prefix) =>
					normalizedRoute === prefix ||
					normalizedRoute.startsWith(`${prefix}/`),
			)
		) {
			return null;
		}

		return normalizedRoute;
	}

	/**
	 * Checks whether the current user satisfies every permission declared by a target.
	 *
	 * @param target - Home target whose permission policy should be evaluated.
	 * @param permissions - Permission names assigned to the authenticated user.
	 * @returns True when the target has no restriction or every requirement is present.
	 */
	export function canAccessHomeSection(
		target: HomeSectionTarget,
		permissions: readonly string[],
	): boolean {
		return (target.requiredPermissions ?? []).every((permission) =>
			permissions.includes(permission),
		);
	}

	/**
	 * Builds validated, deterministically ordered Home mounts from loaded extensions.
	 *
	 * Invalid targets and duplicate routes are omitted. For a duplicate route, the
	 * first target by order, display name, and filename remains authoritative.
	 *
	 * @param extensions - Loaded extension registry.
	 * @returns Valid Home section mounts with unique normalized routes.
	 */
	export function getHomeSectionMounts(
		extensions: Record<string, Extension.Interface>,
	): HomeSectionMount[] {
		const mounts: HomeSectionMount[] = [];

		for (const extension of Object.values(extensions)) {
			for (const rawTarget of extension.targets) {
				if (rawTarget.slot !== Slot.HomeSection) continue;

				const route = normalizeHomeRoute(rawTarget.route);
				const labelKey = normalizeTargetText(rawTarget.labelKey);
				const titleKey = normalizeTargetText(rawTarget.titleKey);
				const icon = normalizeTargetText(rawTarget.icon);
				const group = rawTarget.group === undefined
					? undefined
					: normalizeTargetText(rawTarget.group);
				const requiredPermissions = rawTarget.requiredPermissions;
				const hasValidPermissions =
					requiredPermissions === undefined ||
					(Array.isArray(requiredPermissions) &&
						requiredPermissions.every(
							(permission) =>
								typeof permission === "string" &&
								permission.trim().length > 0,
						));

				const hasValidOrder =
					rawTarget.order === undefined ||
					(typeof rawTarget.order === "number" &&
						Number.isFinite(rawTarget.order));
				const hasValidGroup = rawTarget.group === undefined || group !== null;

				if (
					!route ||
					!labelKey ||
					!titleKey ||
					!icon ||
					!hasValidPermissions ||
					!hasValidOrder ||
					!hasValidGroup
				) {
					console.warn(
						`Extension ${extension.filename} has an invalid home-section target. Skipping...`,
					);
					continue;
				}

				mounts.push({
					extension,
					target: {
						...rawTarget,
						slot: Slot.HomeSection,
						route,
						labelKey,
						titleKey,
						icon,
						group: group ?? undefined,
						requiredPermissions: requiredPermissions?.map((permission) =>
							permission.trim(),
						),
					},
				});
			}
		}

		mounts.sort((first, second) => {
			const orderDifference =
				(first.target.order ?? Number.MAX_SAFE_INTEGER) -
				(second.target.order ?? Number.MAX_SAFE_INTEGER);
			if (orderDifference !== 0) return orderDifference;

			const nameDifference = (
				first.extension.display_name || first.extension.filename
			).localeCompare(
				second.extension.display_name || second.extension.filename,
			);
			if (nameDifference !== 0) return nameDifference;
			return first.extension.filename.localeCompare(second.extension.filename);
		});

		const uniqueMounts: HomeSectionMount[] = [];
		const knownRoutes = new Set<string>();
		for (const mount of mounts) {
			if (knownRoutes.has(mount.target.route)) {
				console.warn(
					`Home extension route ${mount.target.route} is already registered. Skipping ${mount.extension.filename}...`,
				);
				continue;
			}

			knownRoutes.add(mount.target.route);
			uniqueMounts.push(mount);
		}

		return uniqueMounts;
	}

	/**
	 * Returns validated Home mounts visible to a user in manifest order.
	 *
	 * @param extensions - Loaded extension registry.
	 * @param permissions - Permission names assigned to the authenticated user.
	 * @returns Ordered Home mounts whose complete permission policy is satisfied.
	 */
	export function getAccessibleHomeSectionMounts(
		extensions: Record<string, Extension.Interface>,
		permissions: readonly string[],
	): HomeSectionMount[] {
		return getHomeSectionMounts(extensions).filter((mount) =>
			canAccessHomeSection(mount.target, permissions),
		);
	}

	/**
	 * Resolves an exact normalized pathname against accessible Home mounts.
	 *
	 * @param mounts - Validated and permission-filtered Home mounts.
	 * @param pathname - Browser pathname to resolve.
	 * @returns The matching mount, or undefined for an unknown or invalid route.
	 */
	export function resolveHomeSectionRoute(
		mounts: readonly HomeSectionMount[],
		pathname: string,
	): HomeSectionMount | undefined {
		const route = normalizeHomeRoute(pathname);
		return route
			? mounts.find((mount) => mount.target.route === route)
			: undefined;
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

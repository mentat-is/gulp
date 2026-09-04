import { Extension } from "@/context/Extension.context";

jest.mock("@/context/Application.context", () => ({
	Application: { use: () => ({ banner: null, eventStoreFallbackRequest: null }) },
}));

jest.mock("@/ui/Icon", () => ({ Icon: (): null => null }));

jest.mock("@/banners/EventStoreFallback.banner", () => ({
	EventStoreFallbackBanner: (): null => null,
}));

/**
 * Builds a minimal extension fixture for Home resolver tests.
 *
 * @param filename - Unique UI component filename.
 * @param displayName - Name used as the deterministic secondary sort key.
 * @param targets - Manifest targets exposed by the fixture.
 * @returns A runtime-compatible extension record.
 */
function createExtension(
	filename: string,
	displayName: string,
	targets: Extension.MountTarget[],
): Extension.Interface {
	return {
		display_name: displayName,
		plugin: filename,
		extension: true,
		version: "1.0.0",
		desc: `${displayName} fixture`,
		path: ".",
		filename,
		type: [Extension.Slot.HomeSection],
		targets,
	} as unknown as Extension.Interface;
}

/**
 * Builds one complete Home mount target.
 *
 * @param route - Route owned by the target.
 * @param order - Optional menu ordering value.
 * @returns A manifest-compatible Home target.
 */
function createTarget(
	route: string,
	order?: number,
): Extension.MountTarget {
	return {
		slot: Extension.Slot.HomeSection,
		route,
		labelKey: `label.${route}`,
		titleKey: `title.${route}`,
		icon: "Puzzle",
		group: "common.plugins",
		order,
	};
}

describe("Home section extension resolver", () => {
	it("orders mounts by order, display name, and filename", () => {
		const extensions = {
			"later.tsx": createExtension(
				"later.tsx",
				"Later",
				[createTarget("/later", 200)],
			),
			"zeta.tsx": createExtension(
				"zeta.tsx",
				"Alpha",
				[createTarget("/zeta", 100)],
			),
			"alpha.tsx": createExtension(
				"alpha.tsx",
				"Alpha",
				[createTarget("/alpha", 100)],
			),
		};

		expect(
			Extension.getHomeSectionMounts(extensions).map(
				(mount) => mount.extension.filename,
			),
		).toEqual(["alpha.tsx", "zeta.tsx", "later.tsx"]);
	});

	it("normalizes exact routes and rejects invalid or reserved paths", () => {
		expect(Extension.normalizeHomeRoute(" /plugin-configurations/ ")).toBe(
			"/plugin-configurations",
		);
		expect(Extension.normalizeHomeRoute("plugin-configurations")).toBeNull();
		expect(Extension.normalizeHomeRoute("/plugin//configurations")).toBeNull();
		expect(Extension.normalizeHomeRoute("/users")).toBeNull();
		expect(Extension.normalizeHomeRoute("/operations/owned-by-core")).toBeNull();
		expect(Extension.normalizeHomeRoute("/plugin-configurations?edit=1")).toBeNull();
	});

	it("skips invalid metadata and keeps the first deterministic route owner", () => {
		const warning = jest.spyOn(console, "warn");
		warning.mockReturnValue(undefined);
		const extensions = {
			"winner.tsx": createExtension(
				"winner.tsx",
				"Winner",
				[createTarget("/duplicate/", 10)],
			),
			"loser.tsx": createExtension(
				"loser.tsx",
				"Loser",
				[createTarget("/duplicate", 20)],
			),
			"invalid.tsx": createExtension("invalid.tsx", "Invalid", [
				{
					...createTarget("/invalid", 5),
					titleKey: "",
				},
			]),
		};

		const mounts = Extension.getHomeSectionMounts(extensions);

		expect(mounts).toHaveLength(1);
		expect(mounts[0].extension.filename).toBe("winner.tsx");
		expect(mounts[0].target.route).toBe("/duplicate");
		expect(warning).toHaveBeenCalledTimes(2);
		(
			warning as JestSpy & { mockRestore: () => void }
		).mockRestore();
	});

	it("requires every declared permission before exposing or resolving a mount", () => {
		const target = {
			...createTarget("/restricted", 1),
			requiredPermissions: ["read", "admin"],
		};
		const extensions = {
			"restricted.tsx": createExtension(
				"restricted.tsx",
				"Restricted",
				[target],
			),
		};

		expect(
			Extension.getAccessibleHomeSectionMounts(extensions, ["admin"]),
		).toEqual([]);

		const accessible = Extension.getAccessibleHomeSectionMounts(extensions, [
			"read",
			"admin",
		]);
		expect(accessible).toHaveLength(1);
		expect(
			Extension.resolveHomeSectionRoute(accessible, "/restricted/"),
		).toBe(accessible[0]);
		expect(
			Extension.resolveHomeSectionRoute(accessible, "/restricted/child"),
		).toBeUndefined();
	});
});

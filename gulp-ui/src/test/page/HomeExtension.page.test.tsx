import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import {
	MemoryRouter,
	Route,
	Routes,
	useLocation,
} from "react-router-dom";
import { Home } from "@/page/Home.page";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

interface MockHomeTarget {
	slot: "home-section";
	route: string;
	labelKey: string;
	titleKey: string;
	icon: string;
	group?: string;
	requiredPermissions?: string[];
	order?: number;
}

interface MockHomeMount {
	extension: { filename: string };
	target: MockHomeTarget;
}

interface MockHomeComponentProps {
	slot: string;
	target: MockHomeTarget;
	reloadToken: number;
	onLoadingChange: (loading: boolean) => void;
}

interface MockUser {
	permission: string[];
}

const PluginConfigurationMount: MockHomeMount = {
	extension: { filename: "PluginConfiguration.plugin.tsx" },
	target: {
		slot: "home-section",
		route: "/plugin-configurations",
		labelKey: "plugins.pluginConfiguration.menu",
		titleKey: "plugins.pluginConfiguration.title",
		icon: "AcronymJson",
		group: "common.administration",
		requiredPermissions: ["admin"],
		order: 100,
	},
};

let mockExtensionReady = true;
let mockHomeMounts: MockHomeMount[] = [PluginConfigurationMount];
let mockHomeComponentProps: MockHomeComponentProps | null = null;
let mockApplication: {
	target: { operations: unknown[] };
	general: { user: MockUser | null };
	timeline: { dialogSize: number };
};
let mockInfo: {
	plugin_list: JestMock<Promise<void>, []>;
	glyphs_reload: JestMock<Promise<void>, []>;
	sync: JestMock<Promise<void>, []>;
	request_list: JestMock<void, []>;
	setDialogSize: JestMock<void, []>;
	user_get_by_id: JestMock<Promise<MockUser | null>, [string]>;
	setInfoByKey: JestMock<void, [MockUser, string, string]>;
};

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: () => ({
			app: mockApplication,
			Info: mockInfo,
			spawnBanner: jest.fn(),
			spawnDialog: jest.fn(),
			dialog: null,
			banner: null,
		}),
	},
}));

jest.mock("@/context/Extension.context", () => ({
	Extension: {
		Slot: { HomeSection: "home-section" },
		use: () => ({ extensions: {}, ready: mockExtensionReady }),
		getAccessibleHomeSectionMounts: (
			_extensions: unknown,
			permissions: readonly string[],
		) =>
			mockHomeMounts.filter((mount) =>
				(mount.target.requiredPermissions ?? []).every((permission) =>
					permissions.includes(permission),
				),
			),
		resolveHomeSectionRoute: (
			mounts: MockHomeMount[],
			pathname: string,
		) => {
			const normalizedPath = pathname.replace(/\/+$/, "") || "/";
			return mounts.find((mount) => mount.target.route === normalizedPath);
		},
		Component: ({
			name,
			props,
		}: {
			name: string;
			props: MockHomeComponentProps;
		}) => {
			mockHomeComponentProps = props;
			return (
				<div
					data-extension={name}
					data-slot={props.slot}
					data-route={props.target.route}
					data-reload-token={props.reloadToken}
				/>
			);
		},
	},
}));

jest.mock("@/components/menu", () => ({
	Menu: ({ topItems }: { topItems: Array<{ label: string; action: () => void }> }) => (
		<nav data-home-menu>
			{topItems.map((item) => (
				<button
					key={item.label}
					type="button"
					onClick={item.action}
				>
					{item.label}
				</button>
			))}
		</nav>
	),
}));

jest.mock("@/components/HomeContent", () => ({
	BackButton: () => <div data-back-button />,
	GroupsList: () => <div data-groups-list />,
	HeaderAction: () => <div data-header-action />,
	HomeContent: {},
	OperationsList: () => <div data-operations-list />,
	UsersList: () => <div data-users-list />,
}));

jest.mock("@/banners/Session.banner", () => ({
	Session: { Save: { Banner: (): null => null } },
}));

jest.mock("@/banners/Settings.banner", () => ({
	Settings: { Banner: (): null => null },
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: () => ({ t: (key: string) => `localized:${key}` }),
	},
}));

jest.mock("@/ui/Resizer", () => ({ Resizer: (): null => null }));

jest.mock("@/ui/Button", () => ({
	Button: ({
		children,
		loading: _loading,
		icon: _icon,
		variant: _variant,
		size: _size,
		...props
	}: import("react").ButtonHTMLAttributes<HTMLButtonElement> & {
		loading?: boolean;
		icon?: string;
		variant?: string;
		size?: string;
	}) => <button {...props}>{children}</button>,
}));

jest.mock("@/ui/Stack", () => ({
	Stack: (require("react") as typeof import("react")).forwardRef(
		(
			{ children }: { children?: import("react").ReactNode },
			ref: import("react").ForwardedRef<HTMLDivElement>,
		) => <div ref={ref}>{children}</div>,
	),
}));

/** Renders the active router pathname for navigation assertions. */
function LocationProbe() {
	const location = useLocation();
	return <output data-location>{location.pathname}</output>;
}

/** Renders built-in and extension Home routes using the production route split. */
function TestRoutes() {
	return (
		<Routes>
			<Route
				path="/"
				element={
					<>
						<Home.Page section="operations" />
						<LocationProbe />
					</>
				}
			/>
			<Route
				path="*"
				element={
					<>
						<Home.Page extensionRoute />
						<LocationProbe />
					</>
				}
			/>
		</Routes>
	);
}

describe("Home extension shell", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		localStorage.clear();
		mockExtensionReady = true;
		mockHomeMounts = [PluginConfigurationMount];
		mockHomeComponentProps = null;
		mockApplication = {
			target: { operations: [{}] },
			general: { user: { permission: ["admin"] } },
			timeline: { dialogSize: 320 },
		};
		mockInfo = {
			plugin_list: jest.fn<Promise<void>, []>(async () => undefined),
			glyphs_reload: jest.fn<Promise<void>, []>(async () => undefined),
			sync: jest.fn<Promise<void>, []>(async () => undefined),
			request_list: jest.fn<void, []>(),
			setDialogSize: jest.fn<void, []>(),
			user_get_by_id: jest.fn<Promise<MockUser | null>, [string]>(
				async () => mockApplication.general.user,
			),
			setInfoByKey: jest.fn<void, [MockUser, string, string]>((user) => {
				mockApplication.general.user = user;
			}),
		};
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	/**
	 * Renders the Home router and flushes initialization microtasks.
	 *
	 * @param path - Initial browser path.
	 */
	async function renderHome(path: string): Promise<void> {
		await act(async () => {
			root.render(
				<MemoryRouter
					initialEntries={[path]}
					future={{
						v7_relativeSplatPath: true,
						v7_startTransition: true,
					}}
				>
					<TestRoutes />
				</MemoryRouter>,
			);
			await Promise.resolve();
		});
	}

	it("uses localized manifest labels, navigates, and mounts the selected plugin", async () => {
		await renderHome("/");

		const pluginMenuButton = Array.from(
			container.querySelectorAll("button"),
		).find(
			(button) =>
				button.textContent ===
				"localized:plugins.pluginConfiguration.menu",
		);
		expect(pluginMenuButton).not.toBeUndefined();

		await act(async () => {
			pluginMenuButton?.dispatchEvent(
				new MouseEvent("click", { bubbles: true }),
			);
			await Promise.resolve();
		});

		expect(container.querySelector("[data-location]")?.textContent).toBe(
			"/plugin-configurations",
		);
		expect(
			container.querySelector("[data-extension]")?.getAttribute("data-extension"),
		).toBe("PluginConfiguration.plugin.tsx");
		expect(
			container.querySelector("[data-extension]")?.getAttribute("data-slot"),
		).toBe("home-section");
		expect(
			container.textContent?.includes(
				"localized:plugins.pluginConfiguration.title",
			),
		).toBe(true);
	});

	it("waits for extension discovery before resolving a deep link", async () => {
		mockExtensionReady = false;
		await renderHome("/plugin-configurations");

		expect(container.querySelector("[data-extension]")).toBeNull();
		expect(container.querySelector("[data-location]")?.textContent).toBe(
			"/plugin-configurations",
		);

		mockExtensionReady = true;
		await act(async () => {
			root.render(
				<MemoryRouter
					initialEntries={["/plugin-configurations"]}
					future={{
						v7_relativeSplatPath: true,
						v7_startTransition: true,
					}}
				>
					<TestRoutes />
				</MemoryRouter>,
			);
			await Promise.resolve();
		});

		expect(container.querySelector("[data-extension]")).not.toBeNull();
	});

	it("requests one custom-list reload and waits for its loading callback", async () => {
		await renderHome("/plugin-configurations");

		const reloadButton = container.querySelector<HTMLButtonElement>(
			'button[aria-label="localized:common.refresh"]',
		);
		expect(reloadButton?.disabled).toBe(false);
		expect(
			container.querySelector("[data-extension]")?.getAttribute(
				"data-reload-token",
			),
		).toBe("0");

		await act(async () => {
			reloadButton?.click();
			await Promise.resolve();
		});

		expect(
			container.querySelector("[data-extension]")?.getAttribute(
				"data-reload-token",
			),
		).toBe("1");
		expect(reloadButton?.disabled).toBe(true);

		await act(async () => {
			mockHomeComponentProps?.onLoadingChange(false);
			await Promise.resolve();
		});

		expect(reloadButton?.disabled).toBe(false);
	});

	it("waits for user restoration before applying permissions", async () => {
		let resolveUser: (user: MockUser) => void = () => undefined;
		mockApplication.target.operations = [];
		mockApplication.general.user = null;
		localStorage.setItem("__user_id", "saved-admin");
		mockInfo.user_get_by_id = jest.fn<
			Promise<MockUser | null>,
			[string]
		>(
			() => new Promise<MockUser>((resolve) => {
				resolveUser = resolve;
			}),
		);

		await renderHome("/plugin-configurations");
		expect(container.querySelector("[data-extension]")).toBeNull();
		expect(container.querySelector("[data-location]")?.textContent).toBe(
			"/plugin-configurations",
		);

		await act(async () => {
			resolveUser({ permission: ["admin"] });
			await Promise.resolve();
			await Promise.resolve();
		});

		expect(mockInfo.setInfoByKey).toHaveBeenCalledWith(
			{ permission: ["admin"] },
			"general",
			"user",
		);
		expect(container.querySelector("[data-extension]")).not.toBeNull();
	});

	/**
	 * Verifies that a protected deep link returns to the built-in Home route.
	 *
	 * @param mounts - Home targets visible to the resolver before permission filtering.
	 * @param permissions - Permissions assigned to the current user.
	 */
	async function expectProtectedRedirect(
		mounts: MockHomeMount[],
		permissions: string[],
	): Promise<void> {
		mockHomeMounts = mounts;
		mockApplication.general.user = { permission: permissions };

		await renderHome("/plugin-configurations");

		expect(container.querySelector("[data-location]")?.textContent).toBe("/");
		expect(container.querySelector("[data-operations-list]")).not.toBeNull();
		expect(
			container.textContent?.includes(
				"localized:plugins.pluginConfiguration.menu",
			),
		).toBe(false);
	}

	it("redirects when the route-owning plugin is absent", async () => {
		await expectProtectedRedirect([], ["admin"]);
	});

	it("blocks menu and direct-route access for a non-admin user", async () => {
		await expectProtectedRedirect([PluginConfigurationMount], ["read"]);
	});
});

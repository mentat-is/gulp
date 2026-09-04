import type {
	GulpDataset,
	QueryExternalLifecycleOptions,
} from "@/class/Info";
import { QueryExternal } from "@/banners/QueryExternal.banner";
import {
	act,
	isValidElement,
	type ButtonHTMLAttributes,
	type ReactElement,
	type ReactNode,
} from "react";
import { createRoot, type Root } from "react-dom/client";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

interface MockChildrenProps {
	children?: ReactNode;
}

interface MockBannerProps extends MockChildrenProps {
	done?: ReactNode;
	option?: ReactNode;
}

interface MockButtonProps
	extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, "children">,
		MockChildrenProps {
	icon?: string;
	loading?: boolean;
	variant?: string;
}

interface MockSelectRootProps extends MockChildrenProps {
	onValueChange?: (value: string) => void;
}

interface MockExtensionComponentProps {
	name: string;
	props?: QueryExternal.PluginEditorProps;
}

interface MockPluginEditor {
	display_name: string;
	filename: string;
	plugin: string;
}

type MockQueryResult = { total_hits: number; docs: never[] } | null;
type MockQueryExternalArguments = [
	plugin: string,
	pluginParams?: Record<string, unknown>,
	previewMode?: boolean,
	query?: unknown,
	queryOptions?: Record<string, unknown>,
	lifecycle?: QueryExternalLifecycleOptions,
];

let mockExtensionsReady = true;
let mockPluginEditors: MockPluginEditor[] = [];
let mockCapturedEditorName: string | null = null;
let mockCapturedEditorProps: QueryExternal.PluginEditorProps | null = null;
let mockSelectOnValueChange: ((value: string) => void) | null = null;
let mockQueryExternalResult: Promise<MockQueryResult> = Promise.resolve({
	total_hits: 1,
	docs: [],
});

const mockSpawnBanner = jest.fn<void, [ReactNode]>();
const mockQueryExternal = jest.fn<void, MockQueryExternalArguments>();

const mockExternalPlugin: GulpDataset.PluginList.Interface = {
	display_name: "External query",
	type: ["external"],
	desc: "External query plugin",
	path: ".",
	data: {},
	filename: "query_external",
	sigma_support: [],
	custom_parameters: [],
	depends_on: [],
	tags: [],
	version: "1.0.0",
};

const mockSecondExternalPlugin: GulpDataset.PluginList.Interface = {
	...mockExternalPlugin,
	display_name: "Second external query",
	filename: "query_second",
};

const mockApp = {
	target: { plugins: [mockExternalPlugin, mockSecondExternalPlugin] },
};

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: () => ({
			Info: {
				query_external: (...arguments_: MockQueryExternalArguments) => {
					mockQueryExternal(...arguments_);
					return mockQueryExternalResult;
				},
			},
			spawnBanner: mockSpawnBanner,
			app: mockApp,
		}),
	},
}));

jest.mock("@/context/Extension.context", () => ({
	Extension: {
		Slot: { QueryExternal: "query-external" },
		use: () => ({ extensions: {}, ready: mockExtensionsReady }),
		getBySlot: () => mockPluginEditors,
		Component: ({ name, props }: MockExtensionComponentProps): ReactNode => {
			mockCapturedEditorName = name;
			mockCapturedEditorProps = props ?? null;
			return <div data-query-external-editor={name} />;
		},
	},
}));

jest.mock("@/banners/Preview.banner", () => ({
	Preview: {
		Banner: (): ReactNode => <div data-preview-banner />,
	},
}));

jest.mock("@/banners/SelectFiles.banner", () => ({
	SelectFiles: {
		Banner: (): ReactNode => <div data-select-files-banner />,
	},
}));

jest.mock("@/components/AdvancedPluginParams", () => ({
	AdvancedPluginParams: (): ReactNode => <div data-default-advanced-editor />,
}));

jest.mock("@/components/CustomParameters", () => ({
	CustomParameters: {
		Editor: (): ReactNode => <div data-default-custom-editor />,
	},
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: () => ({ t: (key: string): string => key }),
	},
}));

jest.mock("sonner", () => ({
	toast: { success: jest.fn(), error: jest.fn() },
}));

jest.mock("@/ui/Banner", () => ({
	Banner: ({ children, done, option }: MockBannerProps): ReactNode => (
		<div>
			{children}
			{option}
			{done}
		</div>
	),
}));

jest.mock("@/ui/Button", () => ({
	Button: ({
		children,
		icon,
		loading: _loading,
		variant: _variant,
		...props
	}: MockButtonProps): ReactNode => (
		<button type="button" data-icon={icon} {...props}>
			{children}
		</button>
	),
}));

jest.mock("@/ui/Icon", () => ({ Icon: (): null => null }));
jest.mock("@/ui/Label", () => ({ Label: (): null => null }));
jest.mock("@/ui/Skeleton", () => ({
	Skeleton: (): ReactNode => <div data-extension-skeleton />,
}));
jest.mock("@/ui/Stack", () => ({
	Stack: ({ children }: MockChildrenProps): ReactNode => <div>{children}</div>,
}));
jest.mock("@/ui/Textarea", () => ({ Textarea: (): null => null }));
jest.mock("@/ui/Select", () => ({
	Select: {
		Root: ({ children, onValueChange }: MockSelectRootProps): ReactNode => {
			mockSelectOnValueChange = onValueChange ?? null;
			return <div>{children}</div>;
		},
		Trigger: ({ children }: MockChildrenProps): ReactNode => (
			<div>{children}</div>
		),
		Content: ({ children }: MockChildrenProps): ReactNode => (
			<div>{children}</div>
		),
		Item: ({ children }: MockChildrenProps): ReactNode => <div>{children}</div>,
	},
}));

/** Returns the most recently spawned valid React element. */
function lastSpawnedElement(): ReactElement {
	const candidate =
		mockSpawnBanner.mock.calls[mockSpawnBanner.mock.calls.length - 1]?.[0];
	if (!isValidElement(candidate)) {
		throw new Error("Expected spawnBanner to receive a React element.");
	}
	return candidate;
}

describe("QueryExternal dedicated plugin editor", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockExtensionsReady = true;
		mockPluginEditors = [];
		mockCapturedEditorName = null;
		mockCapturedEditorProps = null;
		mockSelectOnValueChange = null;
		mockSpawnBanner.mock.calls.length = 0;
		mockQueryExternal.mock.calls.length = 0;
		mockQueryExternalResult = Promise.resolve({ total_hits: 1, docs: [] });
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("mounts the exact editor match and submits its controlled payload", async () => {
		mockPluginEditors = [
			{
				display_name: "External editor",
				filename: "QueryExternalEditor.tsx",
				plugin: mockExternalPlugin.filename,
			},
		];
		const initialRequest: QueryExternal.PreparedRequest = {
			q: { match_all: {} },
			plugin_params: { custom_parameters: { url: "initial" } },
			q_options: { limit: 10 },
		};
		const preparedRequest: QueryExternal.PreparedRequest = {
			q: { term: { source: "dedicated" } },
			plugin_params: { custom_parameters: { url: "updated" } },
			q_options: { limit: 50 },
		};

		act(() => {
			root.render(
				<QueryExternal.Banner
					preselectedOption={mockExternalPlugin}
					initialRequest={initialRequest}
				/>,
			);
		});

		expect(mockCapturedEditorName).toBe("QueryExternalEditor.tsx");
		expect(mockCapturedEditorProps?.backendPlugin).toBe(mockExternalPlugin);
		expect(mockCapturedEditorProps?.value).toEqual(initialRequest);
		expect(container.querySelector("[data-default-custom-editor]")).toBeNull();
		expect(
			container.querySelector<HTMLButtonElement>('button[data-icon="PreviewEye"]')
				?.disabled,
		).toBe(true);

		act(() => {
			mockCapturedEditorProps?.onChange(preparedRequest);
			mockCapturedEditorProps?.onValidityChange(true);
		});

		await act(async () => {
			container
				.querySelector<HTMLButtonElement>('button[data-icon="PreviewEye"]')
				?.click();
			await mockQueryExternalResult;
		});

		expect(mockQueryExternal).toHaveBeenCalledWith(
			mockExternalPlugin.filename,
			preparedRequest.plugin_params,
			true,
			preparedRequest.q,
			preparedRequest.q_options,
			undefined,
		);
		const preview = lastSpawnedElement() as ReactElement<{ back?: () => void }>;
		act(() => preview.props.back?.());
		const restoredBanner = lastSpawnedElement() as ReactElement<
			QueryExternal.Banner.Props
		>;
		expect(restoredBanner.props.initialRequest).toEqual(preparedRequest);
	});

	it("keeps the fallback hidden while loading and uses it without an exact match", () => {
		mockExtensionsReady = false;
		mockPluginEditors = [
			{
				display_name: "Different editor",
				filename: "DifferentEditor.tsx",
				plugin: "different_backend",
			},
		];

		act(() => {
			root.render(
				<QueryExternal.Banner preselectedOption={mockExternalPlugin} />,
			);
		});

		expect(container.querySelector("[data-extension-skeleton]")).not.toBeNull();
		expect(container.querySelector("[data-default-custom-editor]")).toBeNull();
		expect(
			container.querySelector<HTMLButtonElement>('button[data-icon="Check"]')
				?.disabled,
		).toBe(true);

		mockExtensionsReady = true;
		act(() => {
			root.render(
				<QueryExternal.Banner preselectedOption={mockExternalPlugin} />,
			);
		});

		expect(container.querySelector("[data-extension-skeleton]")).toBeNull();
		expect(container.querySelector("[data-default-custom-editor]")).not.toBeNull();
		expect(container.querySelector("[data-default-advanced-editor]")).not.toBeNull();
	});

	it("uses the first ordered duplicate and warns about the manifest conflict", () => {
		const warn = jest.spyOn(console, "warn");
		mockPluginEditors = [
			{
				display_name: "First editor",
				filename: "FirstEditor.tsx",
				plugin: mockExternalPlugin.filename,
			},
			{
				display_name: "Second editor",
				filename: "SecondEditor.tsx",
				plugin: mockExternalPlugin.filename,
			},
		];

		act(() => {
			root.render(
				<QueryExternal.Banner preselectedOption={mockExternalPlugin} />,
			);
		});

		expect(mockCapturedEditorName).toBe("FirstEditor.tsx");
		expect(warn).toHaveBeenCalledWith(
			"Multiple Query External UI plugins target query_external. Using FirstEditor.tsx.",
		);
		(warn as JestSpy & { mockRestore: () => void }).mockRestore();
	});

	it("resets request state and ignores callbacks from the previous plugin", async () => {
		mockPluginEditors = [
			{
				display_name: "First editor",
				filename: "FirstEditor.tsx",
				plugin: mockExternalPlugin.filename,
			},
			{
				display_name: "Second editor",
				filename: "SecondEditor.tsx",
				plugin: mockSecondExternalPlugin.filename,
			},
		];

		act(() => {
			root.render(
				<QueryExternal.Banner preselectedOption={mockExternalPlugin} />,
			);
		});
		const staleEditorProps = mockCapturedEditorProps;
		const staleRequest: QueryExternal.PreparedRequest = {
			q: "stale",
			plugin_params: { custom_parameters: { stale: true } },
			q_options: { limit: 999 },
		};

		act(() => mockSelectOnValueChange?.(mockSecondExternalPlugin.filename));
		expect(mockCapturedEditorName).toBe("SecondEditor.tsx");
		expect(mockCapturedEditorProps?.value).toEqual(
			QueryExternal.createPreparedRequest(),
		);

		act(() => {
			staleEditorProps?.onChange(staleRequest);
			staleEditorProps?.onValidityChange(true);
		});
		expect(mockCapturedEditorProps?.value).toEqual(
			QueryExternal.createPreparedRequest(),
		);
		expect(
			container.querySelector<HTMLButtonElement>('button[data-icon="PreviewEye"]')
				?.disabled,
		).toBe(true);

		act(() => mockCapturedEditorProps?.onValidityChange(true));
		await act(async () => {
			container
				.querySelector<HTMLButtonElement>('button[data-icon="PreviewEye"]')
				?.click();
			await mockQueryExternalResult;
		});

		const emptyRequest = QueryExternal.createPreparedRequest();
		expect(mockQueryExternal).toHaveBeenCalledWith(
			mockSecondExternalPlugin.filename,
			emptyRequest.plugin_params,
			true,
			emptyRequest.q,
			emptyRequest.q_options,
			undefined,
		);
	});
});

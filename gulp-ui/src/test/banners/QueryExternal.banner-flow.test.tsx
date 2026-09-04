import type {
	GulpDataset,
	QueryExternalLifecycleOptions,
} from "@/class/Info";
import type { CustomParameters } from "@/components/CustomParameters";
import { QueryExternal } from "@/banners/QueryExternal.banner";
import { Preview } from "@/banners/Preview.banner";
import { SelectFiles } from "@/banners/SelectFiles.banner";
import type { Request } from "@/entities/Request";
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

type MockQueryResult = { total_hits: number; docs: never[] } | null;
type MockQueryExternalArguments = [
	plugin: string,
	pluginParams?: Record<string, unknown>,
	previewMode?: boolean,
	query?: string,
	queryOptions?: Record<string, unknown>,
	lifecycle?: QueryExternalLifecycleOptions,
];

const mockSpawnBanner = jest.fn<void, [ReactNode]>();
let mockQueryExternalResult: Promise<MockQueryResult> = Promise.resolve(null);
const mockQueryExternal = jest.fn<void, MockQueryExternalArguments>();
const mockToastSuccess = jest.fn<void, unknown[]>();
const mockToastError = jest.fn<void, unknown[]>();

const mockExternalPlugin: GulpDataset.PluginList.Interface = {
	display_name: "External query",
	type: ["external"],
	desc: "External query plugin",
	path: ".",
	data: {},
	filename: "query_external.py",
	sigma_support: [],
	custom_parameters: [],
	depends_on: [],
	tags: [],
	version: "1.0.0",
};
const mockApp = { target: { plugins: [mockExternalPlugin] } };

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
	AdvancedPluginParams: (): null => null,
}));

jest.mock("@/components/CustomParameters", () => ({
	CustomParameters: {
		Editor: (): ReactNode => <div data-custom-parameters-editor />,
	},
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: () => ({ t: (key: string): string => key }),
	},
}));

jest.mock("sonner", () => ({
	toast: {
		success: (...arguments_: unknown[]) => mockToastSuccess(...arguments_),
		error: (...arguments_: unknown[]) => mockToastError(...arguments_),
	},
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
jest.mock("@/ui/Skeleton", () => ({ Skeleton: (): null => null }));
jest.mock("@/ui/Stack", () => ({
	Stack: ({ children }: MockChildrenProps): ReactNode => <div>{children}</div>,
}));
jest.mock("@/ui/Textarea", () => ({ Textarea: (): null => null }));
jest.mock("@/ui/Select", () => ({
	Select: {
		Root: ({ children }: MockChildrenProps): ReactNode => <div>{children}</div>,
		Trigger: ({ children }: MockChildrenProps): ReactNode => (
			<div>{children}</div>
		),
		Content: ({ children }: MockChildrenProps): ReactNode => (
			<div>{children}</div>
		),
		Item: ({ children }: MockChildrenProps): ReactNode => <div>{children}</div>,
	},
}));

/**
 * Creates a controllable promise for asserting behavior before and after settlement.
 * @returns The promise and its resolve/reject controls.
 */
function deferred<T>(): {
	promise: Promise<T>;
	resolve: (value: T) => void;
	reject: (reason?: unknown) => void;
} {
	let resolve!: (value: T) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<T>((promiseResolve, promiseReject) => {
		resolve = promiseResolve;
		reject = promiseReject;
	});
	return { promise, resolve, reject };
}

/**
 * Returns the last valid React element passed to the banner coordinator.
 * @returns The most recently spawned element.
 */
function lastSpawnedElement(): ReactElement {
	const candidate =
		mockSpawnBanner.mock.calls[mockSpawnBanner.mock.calls.length - 1]?.[0];
	if (!isValidElement(candidate)) {
		throw new Error("Expected spawnBanner to receive a React element.");
	}
	return candidate;
}

describe("QueryExternal banner transitions", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockSpawnBanner.mock.calls.length = 0;
		mockQueryExternal.mock.calls.length = 0;
		mockToastSuccess.mock.calls.length = 0;
		mockToastError.mock.calls.length = 0;
		mockQueryExternalResult = Promise.resolve(null);
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("opens source selection on start and preserves state when returning", async () => {
		const ingestion = deferred<null>();
		mockQueryExternalResult = ingestion.promise;
		const customParameters: CustomParameters.ValueMap = { limit: 25 };
		const query = { query: { match_all: {} } };
		const queryOptions = { limit: 500 };
		const originalOnClose = jest.fn();

		act(() => {
			root.render(
				<QueryExternal.Banner
					preselectedOption={mockExternalPlugin}
					preparams={customParameters}
					preQ={query}
					preQOptions={queryOptions}
					onClose={originalOnClose}
				/>,
			);
		});

		act(() => {
			container
				.querySelector<HTMLButtonElement>('button[data-icon="Check"]')
				?.click();
		});

		expect(mockQueryExternal).toHaveBeenCalledTimes(1);
		const lifecycle = mockQueryExternal.mock.calls[0][5];
		expect(lifecycle).not.toBeUndefined();
		expect(mockSpawnBanner).not.toHaveBeenCalled();

		act(() =>
			lifecycle?.onIngestionStarted?.(
				"ingestion-request" as Request.Id,
			),
		);
		expect(mockSpawnBanner).toHaveBeenCalledTimes(1);
		const sourceSelection = lastSpawnedElement() as ReactElement<{
			onClose?: () => void;
		}>;
		expect(sourceSelection.type).toBe(SelectFiles.Banner);

		act(() => sourceSelection.props.onClose?.());
		expect(mockSpawnBanner).toHaveBeenCalledTimes(2);
		const restoredQuery = lastSpawnedElement() as ReactElement<
			QueryExternal.Banner.Props
		>;
		expect(restoredQuery.type).toBe(QueryExternal.Banner);
		expect(restoredQuery.props.preselectedOption).toBe(mockExternalPlugin);
		expect(restoredQuery.props.preparams).toBe(customParameters);
		expect(restoredQuery.props.preQ).toBe(
			QueryExternal.restoredQueryText(query),
		);
		expect(restoredQuery.props.preQOptions).toBe(queryOptions);
		expect(restoredQuery.props.onClose).toBe(originalOnClose);

		await act(async () => {
			ingestion.resolve(null);
			await ingestion.promise;
		});

		expect(mockToastSuccess).toHaveBeenCalledTimes(1);
		expect(mockSpawnBanner).toHaveBeenCalledTimes(2);
	});

	it("keeps preview navigation terminal and does not install ingestion lifecycle", async () => {
		mockQueryExternalResult = Promise.resolve({ total_hits: 0, docs: [] });

		act(() => {
			root.render(
				<QueryExternal.Banner preselectedOption={mockExternalPlugin} />,
			);
		});

		await act(async () => {
			container
				.querySelector<HTMLButtonElement>('button[data-icon="PreviewEye"]')
				?.click();
			await Promise.resolve();
		});

		expect(mockQueryExternal.mock.calls[0][2]).toBe(true);
		expect(mockQueryExternal.mock.calls[0][5]).toBeUndefined();
		expect(mockSpawnBanner).toHaveBeenCalledTimes(1);
		expect(lastSpawnedElement().type).toBe(Preview.Banner);
		expect(mockToastSuccess).not.toHaveBeenCalled();
	});

	it("keeps restored query strings stable across banner transitions", () => {
		const serializedQuery = '{"query":{"term":{"event.code":"1"}}}';
		expect(QueryExternal.restoredQueryText(serializedQuery)).toBe(
			serializedQuery,
		);
	});
});

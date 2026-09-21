import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { MemoryRouter, useLocation } from "react-router-dom";
import {
	GroupsList,
	OperationsList,
	UsersList,
} from "@/components/HomeContent";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

interface TestEntity {
	id: string;
	name: string;
	glyph_id?: string;
	permission?: string[];
}

interface CapturedTableProps {
	values: TestEntity[];
	onRowClick?: (row: TestEntity, index: number) => void;
	onRowDoubleClick?: (row: TestEntity, index: number) => void;
	actions?: Array<{
		icon?: string;
		label: string;
		onClick: (row: TestEntity, index: number, event: MouseEvent) => void;
	}>;
}

const operation: TestEntity = {
	id: "operation-1",
	name: "Operation one",
	glyph_id: "ActivitySquare",
};
const user: TestEntity = {
	id: "user-1",
	name: "User one",
	permission: ["read"],
};
const group: TestEntity = {
	id: "group-1",
	name: "Group one",
};

let mockTableProps: CapturedTableProps | null = null;
let mockApp: {
	target: { operations: TestEntity[] };
	general: { user: TestEntity | null };
};
let mockInfo: {
	sync: JestMock<Promise<void>, []>;
	operations_select: JestMock<void, [string]>;
	deleteOperation: JestMock<Promise<string[]>, [TestEntity[], () => void]>;
	user_list: JestMock<Promise<TestEntity[]>, [Record<string, boolean>]>;
	user_delete: JestMock<Promise<{ success: boolean }>, [string]>;
};
let mockSpawnBanner: JestMock<void, unknown[]>;
let mockSpawnDialog: JestMock<void, unknown[]>;
let mockGroupList: JestMock<Promise<TestEntity[]>, [Record<string, boolean>]>;
let mockGroupDelete: JestMock<Promise<boolean>, [string]>;
const mockTranslate = (key: string): string => key;

jest.mock("sonner", () => ({
	toast: {
		error: jest.fn(),
		success: jest.fn(),
	},
}));

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: () => ({
			Info: mockInfo,
			app: mockApp,
			spawnBanner: mockSpawnBanner,
			spawnDialog: mockSpawnDialog,
		}),
	},
}));

jest.mock("@/banners/Permissions.banner", () => ({
	Permissions: {
		UserListChangedEvent: "test-user-list-changed",
		GroupListChangedEvent: "test-group-list-changed",
		Users: {
			Create: { Banner: (): null => null },
			Groups: {
				FormBanner: (): null => null,
				list: (options: Record<string, boolean>) => mockGroupList(options),
				deleteById: (groupId: string) => mockGroupDelete(groupId),
			},
		},
	},
}));

jest.mock("@/entities/Operation", () => ({
	Operation: {
		Entity: { icon: (): string => "ActivitySquare" },
		CreateOrUpdate: { Banner: (): null => null },
	},
}));

jest.mock("@/entities/User", () => ({ User: {} }));
jest.mock("@/entities/Group", () => ({ Group: {} }));
jest.mock("@/entities/Glyph", () => ({ Glyph: { List: new Map() } }));

jest.mock("@/dialogs/OperationDetail.dialog", () => ({
	DisplayOperationDetailDialog: (): null => null,
}));
jest.mock("@/dialogs/UserDetail.dialog", () => ({
	DisplayUserDetailDialog: (): null => null,
}));
jest.mock("@/dialogs/GroupsDetail.dialog", () => ({
	DisplayGroupDetailDialog: (): null => null,
}));

jest.mock("@/components/Table", () => ({
	Table: (props: CapturedTableProps) => {
		mockTableProps = props;
		return <div data-table />;
	},
}));

jest.mock("@/ui/Button", () => ({
	Button: ({ children }: { children?: import("react").ReactNode }) => (
		<button>{children}</button>
	),
}));
jest.mock("@/ui/DeleteConfirmationBanner", () => ({
	DeleteConfirmationBanner: ({
		children,
	}: {
		children?: import("react").ReactNode;
	}) => <>{children}</>,
}));
jest.mock("@/ui/Icon", () => ({ Icon: (): null => null }));
jest.mock("@/ui/Stack", () => ({
	Stack: ({ children }: { children?: import("react").ReactNode }) => (
		<div>{children}</div>
	),
}));
jest.mock("@/locales", () => ({
	Locale: { use: () => ({ t: mockTranslate }) },
}));

/** Exposes the current in-memory route for navigation assertions. */
function LocationProbe() {
	const location = useLocation();
	return <output data-location>{location.pathname}</output>;
}

describe("HomeContent lists", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockTableProps = null;
		mockApp = {
			target: { operations: [operation] },
			general: { user },
		};
		mockInfo = {
			sync: jest.fn<Promise<void>, []>(async () => undefined),
			operations_select: jest.fn<void, [string]>(),
			deleteOperation: jest.fn<
				Promise<string[]>,
				[TestEntity[], () => void]
			>(async () => []),
			user_list: jest.fn<
				Promise<TestEntity[]>,
				[Record<string, boolean>]
			>(async () => [user]),
			user_delete: jest.fn<Promise<{ success: boolean }>, [string]>(
				async () => ({ success: true }),
			),
		};
		mockSpawnBanner = jest.fn<void, unknown[]>();
		mockSpawnDialog = jest.fn<void, unknown[]>();
		mockGroupList = jest.fn<
			Promise<TestEntity[]>,
			[Record<string, boolean>]
		>(async () => [group]);
		mockGroupDelete = jest.fn<Promise<boolean>, [string]>(async () => true);
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	/**
	 * Renders one Home list and flushes its async loading effect.
	 *
	 * @param component - List component under test.
	 */
	async function renderList(component: React.ReactNode): Promise<void> {
		await act(async () => {
			root.render(
				<MemoryRouter
					initialEntries={["/"]}
					future={{
						v7_relativeSplatPath: true,
						v7_startTransition: true,
					}}
				>
					{component}
					<LocationProbe />
				</MemoryRouter>,
			);
			await Promise.resolve();
			await Promise.resolve();
		});
	}

	it("reloads operations and delegates click behaviors to the owning view", async () => {
		const onLoadingChange = jest.fn<void, [boolean]>();
		await renderList(
			<OperationsList
				loading={false}
				reloadToken={0}
				onLoadingChange={onLoadingChange}
			/>,
		);

		expect(mockInfo.sync).not.toHaveBeenCalled();
		expect(mockTableProps?.actions?.[0]).toMatchObject({
			label: "home.operation.goTo",
		});
		expect(mockTableProps?.actions?.[0]?.icon).toBeUndefined();

		act(() => mockTableProps?.onRowClick?.(operation, 0));
		expect(mockSpawnDialog).toHaveBeenCalledTimes(1);
		expect(container.querySelector("[data-location]")?.textContent).toBe("/");

		await act(async () => {
			mockTableProps?.onRowDoubleClick?.(operation, 0);
			await Promise.resolve();
		});
		expect(mockInfo.operations_select).toHaveBeenCalledWith(operation.id);
		expect(container.querySelector("[data-location]")?.textContent).toBe(
			`/operations/${operation.id}`,
		);

		await renderList(
			<OperationsList
				loading={false}
				reloadToken={1}
				onLoadingChange={onLoadingChange}
			/>,
		);
		expect(mockInfo.sync).toHaveBeenCalledTimes(1);
		expect(onLoadingChange).toHaveBeenLastCalledWith(false);
	});

	it("reloads users whenever the Home reload token changes", async () => {
		const onLoadingChange = jest.fn<void, [boolean]>();
		await renderList(
			<UsersList
				reloadToken={0}
				onLoadingChange={onLoadingChange}
			/>,
		);
		expect(mockInfo.user_list).toHaveBeenCalledTimes(1);

		await renderList(
			<UsersList
				reloadToken={1}
				onLoadingChange={onLoadingChange}
			/>,
		);
		expect(mockInfo.user_list).toHaveBeenCalledTimes(2);
		expect(onLoadingChange).toHaveBeenLastCalledWith(false);
	});

	it("reloads groups whenever the Home reload token changes", async () => {
		const onLoadingChange = jest.fn<void, [boolean]>();
		await renderList(
			<GroupsList
				reloadToken={0}
				onLoadingChange={onLoadingChange}
			/>,
		);
		expect(mockGroupList).toHaveBeenCalledTimes(1);

		await renderList(
			<GroupsList
				reloadToken={1}
				onLoadingChange={onLoadingChange}
			/>,
		);
		expect(mockGroupList).toHaveBeenCalledTimes(2);
		expect(onLoadingChange).toHaveBeenLastCalledWith(false);
	});
});

import { act } from "react";
import { createRoot } from "react-dom/client";
import type { Root } from "react-dom/client";
import {
	moveTableColumn,
	useTableConfig,
} from "@/hooks/useTableConfig";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const mockGetConfiguration = jest.fn<Promise<unknown>, [string]>();
const mockUpdateConfiguration = jest.fn<
	Promise<unknown>,
	[unknown, string]
>();
const mockDeleteConfiguration = jest.fn<Promise<unknown>, [string]>();

jest.mock("@/class/IndexedDB", () => ({
	GulpIndexedDB: class MockGulpIndexedDB {
		GetConfiguration(configurationKey: string): Promise<unknown> {
			return mockGetConfiguration(configurationKey);
		}

		UpdateConfiguration(
			configuration: unknown,
			configurationKey: string,
		): Promise<unknown> {
			return mockUpdateConfiguration(configuration, configurationKey);
		}

		DeleteConfiguration(configurationKey: string): Promise<unknown> {
			return mockDeleteConfiguration(configurationKey);
		}
	},
}));

type TableConfigState = ReturnType<typeof useTableConfig>;
let mockLatestTableConfig: TableConfigState | null = null;

interface HookHarnessProps {
	columns: string[];
	persistId?: string;
}

/**
 * Exposes the hook state to persistence-focused integration tests.
 * @param props - Current schema and optional persistence key.
 * @returns No rendered UI.
 */
function HookHarness({
	columns,
	persistId = "table-test",
}: HookHarnessProps): null {
	mockLatestTableConfig = useTableConfig(persistId, columns, true);
	return null;
}

describe("useTableConfig column ordering", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockGetConfiguration.mockReset();
		mockUpdateConfiguration.mockReset();
		mockDeleteConfiguration.mockReset();
		mockUpdateConfiguration.mockResolvedValue(undefined);
		mockDeleteConfiguration.mockResolvedValue(undefined);
		mockLatestTableConfig = null;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	/**
	 * Renders the hook and flushes its asynchronous IndexedDB read.
	 * @param columns - Current table schema for the harness.
	 * @returns The loaded table configuration state.
	 */
	async function renderHook(columns: string[]): Promise<TableConfigState> {
		await act(async () => {
			root.render(<HookHarness columns={columns} />);
			await Promise.resolve();
			await Promise.resolve();
		});
		if (!mockLatestTableConfig?.isLoaded) {
			throw new Error("Table configuration did not finish loading");
		}
		return mockLatestTableConfig;
	}

	it("loads legacy visibility entries without a stored order", async () => {
		mockGetConfiguration.mockResolvedValue({
			hiddenColumns: ["email"],
			columnSnapshot: ["name", "email"],
		});

		const state = await renderHook(["name", "email"]);

		expect([...state.hiddenColumns]).toEqual(["email"]);
		expect(state.orderedColumns).toEqual(["name", "email"]);
		expect(mockDeleteConfiguration).not.toHaveBeenCalled();
	});

	it("falls back to the supplied order when the persisted order is invalid", async () => {
		mockGetConfiguration.mockResolvedValue({
			hiddenColumns: [],
			columnSnapshot: ["name", "email", "team"],
			columnOrder: ["team", "team", "name"],
		});

		const state = await renderHook(["name", "email", "team"]);

		expect(state.orderedColumns).toEqual(["name", "email", "team"]);
	});

	it("keeps hidden slots while reordering and preserves the order on select all", async () => {
		mockGetConfiguration.mockResolvedValue({
			hiddenColumns: ["email"],
			columnSnapshot: ["name", "email", "team"],
			columnOrder: ["name", "email", "team"],
		});
		await renderHook(["name", "email", "team"]);

		act(() => {
			mockLatestTableConfig?.moveColumn("team", "name");
		});
		expect(mockLatestTableConfig?.orderedColumns).toEqual([
			"team",
			"email",
			"name",
		]);
		expect(mockUpdateConfiguration).toHaveBeenLastCalledWith(
			{
				hiddenColumns: ["email"],
				columnSnapshot: ["name", "email", "team"],
				columnOrder: ["team", "email", "name"],
			},
			"table-test",
		);

		act(() => {
			mockLatestTableConfig?.selectAllColumns();
		});
		expect([...mockLatestTableConfig!.hiddenColumns]).toEqual([]);
		expect(mockLatestTableConfig?.orderedColumns).toEqual([
			"team",
			"email",
			"name",
		]);
		expect(mockUpdateConfiguration).toHaveBeenLastCalledWith(
			{
				hiddenColumns: [],
				columnSnapshot: ["name", "email", "team"],
				columnOrder: ["team", "email", "name"],
			},
			"table-test",
		);
		expect(mockDeleteConfiguration).not.toHaveBeenCalled();
	});

	it("discards visibility and ordering when the table schema changes", async () => {
		mockGetConfiguration.mockResolvedValue({
			hiddenColumns: ["old"],
			columnSnapshot: ["name", "old"],
			columnOrder: ["old", "name"],
		});

		const state = await renderHook(["name", "email"]);

		expect([...state.hiddenColumns]).toEqual([]);
		expect(state.orderedColumns).toEqual(["name", "email"]);
		expect(mockDeleteConfiguration).toHaveBeenCalledWith("table-test");
	});

	it("returns the original order when a move is not valid", () => {
		const order = ["name", "email"];
		expect(moveTableColumn(order, "missing", "name")).toBe(order);
		expect(moveTableColumn(order, "name", "email", new Set(["email"]))).toBe(
			order,
		);
	});
});

import {
	act,
	type InputHTMLAttributes,
	type ReactNode,
} from "react";
import { createRoot, type Root } from "react-dom/client";
import { Navigator } from "@/app/body/Navigator";
import type { Filter } from "@/entities/Filter";
import type { Query } from "@/entities/Query";
import type { Source } from "@/entities/Source";
import { scrollStore } from "@/store/scroll.store";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

type SourceFixture = { id: Source.Id; appliedQuery: Query.Type };
interface NavigatorSelectorValue {
	key: string;
	label: string;
	icon: string;
}

type NavigatorInputProps = InputHTMLAttributes<HTMLInputElement> & {
	icon?: string;
	iconTitle?: string;
	onIconClick?: () => void;
	selector?: {
		values: NavigatorSelectorValue[];
		selected: string;
		onSelectedChange: (selected: string) => void;
	};
};

let mockInput: NavigatorInputProps;
let mockSources: SourceFixture[] = [];
const mockRefetch = jest.fn(
	(_options: {
		ids: Source.Id[];
		queriesBySourceId: Record<Source.Id, Query.Type>;
	}): void => undefined,
);
const mockSetQuery = jest.fn();
const mockSetTimelineFilter = jest.fn();
const mockApp = {
	timeline: { filter: "", scale: 1, frame: { min: 0, max: 100 } },
	hidden: {},
	target: { files: [] },
};

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: () => ({
			app: mockApp,
			Info: {
				app: mockApp,
				getQuery: (source: SourceFixture) => source.appliedQuery,
				refetch: mockRefetch,
				setQuery: mockSetQuery,
				setTimelineFilter: mockSetTimelineFilter,
			},
		}),
	},
}));
jest.mock("@/context/DetachedWindow.context", () => ({
	DetachedWindow: { use: () => ({}) },
}));
jest.mock("@/context/Extension.context", () => ({
	Extension: {
		use: () => ({ extensions: [] }),
		getBySlot: () => [],
		Slot: { AIAssistantWindow: "assistant" },
	},
}));
jest.mock("@/locales", () => ({
	Locale: { use: () => ({ t: (key: string) => key }) },
}));
jest.mock("@/entities/Source", () => ({
	Source: { Entity: { selected: () => mockSources } },
}));
jest.mock("@/entities/Context", () => ({
	Context: { Entity: { frame: () => ({ min: 0, max: 100 }) } },
}));
jest.mock("@/entities/Filter", () => ({
	Filter: {
		Entity: {
			createCondition: (id: string, condition: object) => ({
				id,
				enabled: true,
				operator: "must",
				...condition,
			}),
		},
	},
}));
jest.mock("@/entities/App", () => ({ App: {} }));
jest.mock("@/overlays/Highlights", () => ({ Highlights: {} }));
jest.mock("@/dto/Logger.class", () => ({
	Logger: { error: () => undefined },
}));
jest.mock("@/ui/utils", () => ({ cn: () => "", getTimestamp: () => 0 }));
jest.mock("@/ui/Stack", () => ({
	Stack: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
}));
jest.mock("@/ui/Button", () => ({
	Button: ({
		title,
		onClick,
	}: {
		title?: string;
		onClick?: () => void;
	}) => <button onClick={onClick}>{title}</button>,
}));
jest.mock("@/ui/Input", () => ({
	Input: Object.assign(
		(props: NavigatorInputProps) => {
			mockInput = props;
			return (
				<div>
					<input
						readOnly
						value={props.value}
						placeholder={props.placeholder}
					/>
					{props.selector?.values.map((value) => (
						<button
							key={value.key}
							data-input-selector-value={value.key}
							data-input-selector-icon={value.icon}
							aria-pressed={props.selector?.selected === value.key}
							onClick={() => props.selector?.onSelectedChange(value.key)}
						>
							{value.label}
						</button>
					))}
				</div>
			);
		},
		{ Timestamp: () => null },
	),
}));
jest.mock("@/ui/Popover", () => ({
	Popover: {
		Root: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
		Trigger: ({ children }: { children?: ReactNode }) => children,
		Content: () => null,
	},
}));
jest.mock("@/ui/Label", () => ({ Label: () => null }));
jest.mock("@/ui/Switch", () => ({ Switch: () => null }));

/** Creates a structured filter fixture for Navigator replacement tests. */
function createFilter(
	id: string,
	overrides: Partial<Filter.Type> = {},
): Filter.Type {
	return {
		id: id as Filter.Id,
		type: "wildcard",
		operator: "must",
		field: "event.original",
		value: "*existing*",
		enabled: true,
		case_insensitive: true,
		...overrides,
	};
}

describe("Navigator query submission", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockSources = [];
		mockApp.timeline.filter = "";
		scrollStore.setScroll(0, -26);
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
		scrollStore.setScroll(0, -26);
	});

	/** Renders the Navigator and lets its initial effects settle. */
	async function renderNavigator(): Promise<void> {
		await act(async () => {
			root.render(<Navigator timeline={{ current: container }} />);
		});
	}

	/** Clicks the Navigator search button. */
	function clickSearch(): void {
		const searchButton = Array.from(container.querySelectorAll("button")).find(
			(button) => button.textContent === "common.search",
		);
		if (!searchButton) throw new Error("Missing Navigator search button");
		searchButton.click();
	}

	it("defaults to text event search and submits every source in one immutable batch", async () => {
		mockApp.timeline.filter = "persisted source filter";
		mockSources = Array.from({ length: 140 }, (_, index) => ({
			id: `navigator-source-${index}` as Source.Id,
			appliedQuery: {
				string: `source ${index}`,
				filters: [],
				text_filter: `existing-${index}`,
			},
		}));
		const original = mockSources.map((source) => source.appliedQuery);

		await renderNavigator();

		expect(mockInput.placeholder).toBe("navigator.filterEventsPlaceholder");
		expect(mockInput.icon).toBe("Activity");
		expect(mockInput.value).toBe("");
		expect(mockInput.selector?.selected).toBe("text");
		expect(mockInput.selector?.values).toEqual([
			{ key: "text", label: "common.text", icon: "ALargeSmall" },
			{
				key: "regexp",
				label: "queryBuilder.condition.regexp",
				icon: "Regex",
			},
		]);
		expect(mockSetTimelineFilter).toHaveBeenCalledWith("");

		await act(async () => {
			mockInput.onChange?.({
				target: { value: "  failed   login  " },
			} as React.ChangeEvent<HTMLInputElement>);
		});
		await act(async () => clickSearch());

		expect(mockRefetch).toHaveBeenCalledTimes(1);
		expect(mockSetQuery).not.toHaveBeenCalled();
		const submitted = mockRefetch.mock.calls[0][0] as {
			ids: Source.Id[];
			queriesBySourceId: Record<Source.Id, Query.Type>;
		};
		expect(submitted.ids).toHaveLength(140);
		for (const [index, source] of mockSources.entries()) {
			expect(source.appliedQuery).toBe(original[index]);
			expect(source.appliedQuery.filters).toEqual([]);
			expect(submitted.queriesBySourceId[source.id].text_filter).toBe(
				`existing-${index}`,
			);
			expect(submitted.queriesBySourceId[source.id].filters[0]).toMatchObject({
				id: "navigator-event-filter",
				type: "wildcard",
				field: "event.original",
				value: "*failed* *login*",
				case_insensitive: true,
			});
			expect(submitted.queriesBySourceId[source.id].fieldTypeMap).toEqual({
				"event.original": "wildcard",
			});
		}
	});

	it("submits a trimmed regexp unchanged when selected", async () => {
		mockSources = [
			{
				id: "regexp-source" as Source.Id,
				appliedQuery: { string: "", filters: [] },
			},
		];
		await renderNavigator();

		await act(async () => {
			container
				.querySelector<HTMLButtonElement>(
					'button[data-input-selector-value="regexp"]',
				)
				?.click();
		});
		expect(mockInput.selector?.selected).toBe("regexp");

		const regexp = String.raw`fail(ed|ure)\s+login`;
		await act(async () => {
			mockInput.onChange?.({
				target: { value: `  ${regexp}  ` },
			} as React.ChangeEvent<HTMLInputElement>);
		});
		await act(async () => {
			mockInput.onKeyDown?.({
				key: "Enter",
			} as React.KeyboardEvent<HTMLInputElement>);
		});

		const submitted = mockRefetch.mock.calls[0][0] as {
			queriesBySourceId: Record<Source.Id, Query.Type>;
		};
		expect(
			submitted.queriesBySourceId[mockSources[0].id].filters[0],
		).toMatchObject({
			type: "regexp",
			field: "event.original",
			value: regexp,
		});
	});

	it("keeps both timeline scroll axes when applying an event query", async () => {
		mockSources = [{
			id: "visible-source" as Source.Id,
			appliedQuery: { string: "", filters: [] },
		}];
		scrollStore.setScroll(480, 118);
		await renderNavigator();

		await act(async () => {
			mockInput.onChange?.({
				target: { value: "login failed" },
			} as React.ChangeEvent<HTMLInputElement>);
		});
		await act(async () => clickSearch());

		expect(mockRefetch).toHaveBeenCalledTimes(1);
		expect(scrollStore.getSnapshot()).toEqual({ x: 480, y: 118 });
	});

	it("removes only the Navigator filter when an empty search is submitted", async () => {
		const otherFilter = createFilter("other-filter", {
			field: "event.category",
			value: "authentication",
		});
		mockSources = [
			{
				id: "clear-source" as Source.Id,
				appliedQuery: {
					string: "existing",
					filters: [
						otherFilter,
						createFilter("navigator-event-filter"),
					],
				},
			},
		];
		await renderNavigator();

		await act(async () => clickSearch());

		const submitted = mockRefetch.mock.calls[0][0] as {
			queriesBySourceId: Record<Source.Id, Query.Type>;
		};
		expect(submitted.queriesBySourceId[mockSources[0].id].filters).toEqual([
			otherFilter,
		]);
		expect(mockSources[0].appliedQuery.filters).toHaveLength(2);
	});

	it("cleans the Navigator filter when switching to source filtering", async () => {
		const otherFilter = createFilter("other-filter");
		mockSources = [
			{
				id: "mode-source" as Source.Id,
				appliedQuery: {
					string: "existing",
					filters: [
						otherFilter,
						createFilter("navigator-event-filter"),
					],
				},
			},
		];
		await renderNavigator();

		await act(async () => mockInput.onIconClick?.());

		expect(mockInput.placeholder).toBe("navigator.filterFilesPlaceholder");
		expect(mockInput.icon).toBe("Filter");
		expect(
			container.querySelector('button[data-input-selector-value="text"]'),
		).toBeNull();
		const submitted = mockRefetch.mock.calls[0][0] as {
			queriesBySourceId: Record<Source.Id, Query.Type>;
		};
		expect(submitted.queriesBySourceId[mockSources[0].id].filters).toEqual([
			otherFilter,
		]);
	});
});

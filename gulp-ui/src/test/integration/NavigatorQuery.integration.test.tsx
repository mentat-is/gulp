import { act, type ReactNode, type InputHTMLAttributes } from "react";
import { createRoot } from "react-dom/client";
import { Navigator } from "@/app/body/Navigator";
import type { Query } from "@/entities/Query";
import type { Source } from "@/entities/Source";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

type SourceFixture = { id: Source.Id; appliedQuery: Query.Type };
type InputProps = InputHTMLAttributes<HTMLInputElement> & { onIconClick?: () => void };
let mockInput: InputProps;
let mockSources: SourceFixture[] = [];
const mockRefetch = jest.fn((_options: { ids: Source.Id[]; queriesBySourceId: Record<Source.Id, Query.Type> }): void => undefined);
const mockSetQuery = jest.fn();
const mockApp = { timeline: { filter: "", scale: 1, frame: { min: 0, max: 100 } }, hidden: {}, target: { files: [] } };

jest.mock("@/context/Application.context", () => ({ Application: { use: () => ({
	app: mockApp,
	Info: { app: mockApp, getQuery: (source: SourceFixture) => source.appliedQuery, refetch: mockRefetch, setQuery: mockSetQuery, setTimelineFilter: () => undefined },
}) } }));
jest.mock("@/context/DetachedWindow.context", () => ({ DetachedWindow: { use: () => ({}) } }));
jest.mock("@/context/Extension.context", () => ({ Extension: { use: () => ({ extensions: [] }), getBySlot: () => [], Slot: { AIAssistantWindow: "assistant" } } }));
jest.mock("@/locales", () => ({ Locale: { use: () => ({ t: (key: string) => key }) } }));
jest.mock("@/entities/Source", () => ({ Source: { Entity: { selected: () => mockSources } } }));
jest.mock("@/entities/Context", () => ({ Context: { Entity: { frame: () => ({ min: 0, max: 100 }) } } }));
jest.mock("@/entities/Filter", () => ({ Filter: { Entity: { createCondition: (id: string, condition: object) => ({ id, enabled: true, operator: "must", ...condition }) } } }));
jest.mock("@/entities/App", () => ({ App: {} }));
jest.mock("@/overlays/Highlights", () => ({ Highlights: {} }));
jest.mock("@/dto/Logger.class", () => ({ Logger: { error: () => undefined } }));
jest.mock("@/ui/utils", () => ({ cn: () => "", getTimestamp: () => 0 }));
jest.mock("@/ui/Stack", () => ({ Stack: ({ children }: { children?: ReactNode }) => <div>{children}</div> }));
jest.mock("@/ui/Button", () => ({ Button: ({ title, onClick }: { title?: string; onClick?: () => void }) => <button onClick={onClick}>{title}</button> }));
jest.mock("@/ui/Input", () => ({ Input: Object.assign((props: InputProps) => { mockInput = props; return <input readOnly value={props.value} />; }, { Timestamp: () => null }) }));
jest.mock("@/ui/Popover", () => ({ Popover: { Root: ({ children }: { children?: ReactNode }) => <div>{children}</div>, Trigger: ({ children }: { children?: ReactNode }) => children, Content: () => null } }));
jest.mock("@/ui/Label", () => ({ Label: () => null }));
jest.mock("@/ui/Switch", () => ({ Switch: () => null }));

describe("Navigator query submission", () => {
	it("submits and clears 140 source queries in one batch without mutating applied queries", async () => {
		mockSources = Array.from({ length: 140 }, (_, index) => ({
			id: `navigator-source-${index}` as Source.Id,
			appliedQuery: { string: `source ${index}`, filters: [], text_filter: `existing-${index}` },
		}));
		const original = mockSources.map((source) => source.appliedQuery);
		const container = document.createElement("div");
		document.body.appendChild(container);
		const root = createRoot(container);
		try {
			await act(async () => { root.render(<Navigator timeline={{ current: container }} />); });
			await act(async () => { mockInput.onIconClick?.(); });
			await act(async () => { mockInput.onChange?.({ target: { value: "failed login" } } as React.ChangeEvent<HTMLInputElement>); });
			await act(async () => { Array.from(container.querySelectorAll("button")).find((button) => button.textContent === "common.search")!.click(); });
			expect(mockRefetch).toHaveBeenCalledTimes(1);
			expect(mockSetQuery).not.toHaveBeenCalled();
			const submitted = mockRefetch.mock.calls[0][0] as { ids: Source.Id[]; queriesBySourceId: Record<Source.Id, Query.Type> };
			expect(submitted.ids).toHaveLength(140);
			for (const [index, source] of mockSources.entries()) {
				expect(source.appliedQuery).toBe(original[index]);
				expect(source.appliedQuery.filters).toEqual([]);
				expect(submitted.queriesBySourceId[source.id].text_filter).toBe(`existing-${index}`);
				expect(submitted.queriesBySourceId[source.id].filters[0]).toMatchObject({ id: "navigator-event-filter", value: "failed login" });
				source.appliedQuery = submitted.queriesBySourceId[source.id];
			}
			await act(async () => { mockInput.onIconClick?.(); });
			expect(mockRefetch).toHaveBeenCalledTimes(2);
			const cleared = mockRefetch.mock.calls[1][0] as typeof submitted;
			expect(Object.values(cleared.queriesBySourceId).every((query) => query.filters.length === 0)).toBe(true);
			expect(mockSources.every((source) => source.appliedQuery.filters.length === 1)).toBe(true);
		} finally {
			await act(async () => { root.unmount(); });
			container.remove();
		}
	});
});

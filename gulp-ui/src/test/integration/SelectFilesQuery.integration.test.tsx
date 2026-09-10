import { act, type ReactNode, type ReactElement, type InputHTMLAttributes } from "react";
import { createRoot } from "react-dom/client";
import { SelectFiles } from "@/banners/SelectFiles.banner";
import { Frame } from "@/banners/Frame.banner";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

const mockApp = {
	general: { user: null },
	target: {
		operations: [{ id: "selection-operation" }],
		contexts: Array.from({ length: 3 }, (_, index) => ({ id: `context-${index}`, name: `Context ${index}`, operation_id: "selection-operation", selected: false })),
		files: Array.from({ length: 140 }, (_, index) => ({ id: `source-${index}`, name: `Source ${index}`, context_id: `context-${index % 3}`, operation_id: "selection-operation", selected: false, total: index < 51 ? 3 : 2 })),
	},
};
const mockRefetch = jest.fn();
const mockSpawnBanner = jest.fn((_banner: ReactNode): void => undefined);
const mockBatchUpdates = jest.fn();
const mockInfo = {
	refetch: mockRefetch,
	session_list: async () => [], session_subscribe: () => () => undefined,
	resync_ingestion_state: () => undefined, stop_ingestion_resync_poll: () => undefined,
	activeUploads: new Map(), ingestionProgress: new Map(), notes_reload: () => undefined,
	/** Publishes selection state while separately recording the number of commits. */
	batchUpdate: (update: (draft: typeof mockApp) => void): void => { mockBatchUpdates(); update(mockApp); },
};
const mockFrame = { min: 1700000000000, max: 1700000060000 };
type ChildrenProps = { children?: ReactNode };

jest.mock("@/context/Application.context", () => ({ Application: { use: () => ({ app: mockApp, Info: mockInfo, spawnBanner: mockSpawnBanner, destroyBanner: () => undefined, timeline: { current: null } }) } }));
jest.mock("@/locales", () => ({ Locale: { use: () => ({ t: (key: string) => key }) } }));
jest.mock("react-router-dom", () => ({ useParams: () => ({ operation_id: "selection-operation" }) }));
jest.mock("@tanstack/react-virtual", () => ({ useVirtualizer: () => ({ getTotalSize: () => 0, getVirtualItems: () => [] }) }));
jest.mock("@/entities/Operation", () => ({ Operation: { Entity: { selected: () => mockApp.target.operations[0], contexts: () => mockApp.target.contexts } } }));
jest.mock("@/entities/Context", () => ({ Context: { Entity: {
	selected: () => mockApp.target.contexts.filter((context) => context.selected),
	sources: (_app: unknown, context: { id: string } | string) => mockApp.target.files.filter((file) => file.context_id === (typeof context === "string" ? context : context.id)),
	frame: () => mockFrame,
} } }));
jest.mock("@/entities/Source", () => ({ Source: { Entity: { selected: () => mockApp.target.files.filter((file) => file.selected) } } }));
jest.mock("@/entities/Request", () => ({ Request: {} }));
jest.mock("@/class/IngestionResync", () => ({ isIngestionRequest: () => false }));
jest.mock("@/class/API", () => ({}));
jest.mock("@/ui/utils", () => ({ cn: () => "", Refractor: { array: (...values: unknown[]) => values } }));
jest.mock("@/dto/Logger.class", () => ({ Logger: { error: jest.fn() } }));
jest.mock("@/banners/Upload.banner", () => ({ UploadBanner: () => null }));
jest.mock("@/banners/Preview.banner", () => ({ Preview: {} }));
jest.mock("@/banners/FilterFile.banner", () => ({ FilterFileBanner: () => null }));
jest.mock("@/banners/Session.banner", () => ({ Session: {} }));
jest.mock("@/dialogs/Event.dialog", () => ({ DisplayEventDialog: () => null }));
jest.mock("@/ui/Banner", () => ({ Banner: ({ children, done }: ChildrenProps & { done?: ReactNode }) => <div>{done}{children}</div> }));
jest.mock("@/ui/Button", () => ({ Button: ({ children, icon, onClick, disabled }: ChildrenProps & { icon?: string; onClick?: () => void; disabled?: boolean }) => <button data-icon={icon} onClick={onClick} disabled={disabled}>{children}</button> }));
jest.mock("@/ui/Stack", () => ({ Stack: ({ children }: ChildrenProps) => <div>{children}</div> }));
jest.mock("@/ui/Skeleton", () => ({ Skeleton: ({ children }: ChildrenProps) => <div>{children}</div> }));
jest.mock("@/ui/Input", () => ({ Input: ({ value, onChange, type }: InputHTMLAttributes<HTMLInputElement>) => <input value={value} onChange={onChange} type={type} /> }));
jest.mock("@/ui/Checkbox", () => ({ Checkbox: () => null }));
jest.mock("@/ui/Label", () => ({ Label: () => null }));
jest.mock("@/ui/Badge", () => ({ Badge: () => null }));
jest.mock("@/ui/Separator", () => ({ Separator: () => null }));
jest.mock("@/ui/Spinner", () => ({ Spinner: () => null }));
jest.mock("@/ui/Progress", () => ({ Progress: () => null }));
jest.mock("@/ui/Toggle", () => ({ Toggle: () => null }));
jest.mock("@/ui/Tooltip", () => ({}));
jest.mock("@/ui/Select", () => ({ Select: {} }));

describe("SelectFiles and Frame query handoff", () => {
	it("selects 3 contexts / 140 sources / 331 events and refetches them once through the real Frame banner", async () => {
		const container = document.createElement("div");
		document.body.appendChild(container);
		const root = createRoot(container);
		try {
			await act(async () => { root.render(<SelectFiles.Banner />); });
			await act(async () => { container.querySelector<HTMLButtonElement>('[data-icon="FilePlus"]')!.click(); });
			await act(async () => {
				container.querySelector<HTMLButtonElement>('[data-icon="Check"]')!.click();
				await new Promise((resolve) => setTimeout(resolve, 20));
			});
			expect(mockBatchUpdates).toHaveBeenCalledTimes(1);
			expect(mockApp.target.contexts.filter((context) => context.selected)).toHaveLength(3);
			expect(mockApp.target.files.filter((file) => file.selected)).toHaveLength(140);
			expect(mockApp.target.files.reduce((total, file) => total + file.total, 0)).toBe(331);
			expect(mockRefetch).not.toHaveBeenCalled();
			const frame = mockSpawnBanner.mock.calls[0][0] as ReactElement<Frame.Banner.Props>;
			expect(frame.type).toBe(Frame.Banner);
			expect(frame.props.initialCacheLookup).toBe(true);
			await act(async () => { root.render(frame); });
			await act(async () => { container.querySelector<HTMLButtonElement>('[data-icon="Check"]')!.click(); });
			expect(mockRefetch).toHaveBeenCalledTimes(1);
			expect(mockRefetch).toHaveBeenCalledWith({ ids: mockApp.target.files.map((file) => file.id), frame: mockFrame, cachePolicy: "initial" });
		} finally {
			await act(async () => { root.unmount(); });
			container.remove();
		}
	});
});

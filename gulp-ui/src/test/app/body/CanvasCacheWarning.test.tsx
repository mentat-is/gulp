import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { Source } from "@/entities/Source";
import type { Request } from "@/entities/Request";
import { requestStore } from "@/store/request.store";
import {
	createEmptyQueryCacheState,
	type QueryCacheState,
} from "@/entities/QueryCache";
import {
	CanvasCacheWarningOverlay,
	getCanvasSourceLoadingSuffix,
	getCanvasCacheWarningTop,
	shouldShowCanvasCacheWarning,
	shouldShowCanvasSourceCacheWarning,
} from "@/app/body/CanvasCacheWarning";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

let mockScrollY = 0;

jest.mock("@/store/scroll.store", () => ({
	useScrollY: (): number => mockScrollY,
}));

jest.mock("@/locales", () => ({
	Locale: {
		use: (): { t: (key: string) => string } => ({
			t: (key: string): string => key,
		}),
	},
}));

jest.mock("@/ui/Tooltip", () => ({
	Tooltip: ({ children }: { children?: ReactNode }) => children ?? null,
	TooltipContent: ({ children }: { children?: ReactNode }) => (
		<span data-tooltip-content>{children}</span>
	),
	TooltipProvider: ({ children }: { children?: ReactNode }) => children ?? null,
	TooltipTrigger: ({ children }: { children?: ReactNode }) => children ?? null,
}));

jest.mock("@/ui/Icon", () => ({
	Icon: ({ name }: { name: string }) => <svg data-icon={name} />,
}));

/** Creates one runtime cache state with optional semantic overrides. */
function createQueryCacheState(
	overrides: Partial<QueryCacheState> = {},
): QueryCacheState {
	return {
		...createEmptyQueryCacheState(),
		activeGeneration: 7,
		freshness: "query-mismatch",
		warningReason: "query-mismatch",
		...overrides,
	};
}

/** Creates the minimal source projection consumed by the DOM warning overlay. */
function createSource(
	id: string,
	queryCache: QueryCacheState,
	options: {
		name?: string;
		querySourceIds?: string[];
		timelineLabel?: "filename" | "full_path";
	} = {},
): Source.Type {
	return {
		id: id as Source.Id,
		name: options.name ?? `${id}.json`,
		appliedQuery: {
			string: "cached query",
			filters: [],
			source_config: {
				operation_id: "operation-id",
				source_ids: options.querySourceIds ?? [id],
				range: { min: 1, max: 2 },
			},
		},
		settings: {
			timeline_label: options.timelineLabel ?? "filename",
		},
		queryCache,
	} as unknown as Source.Type;
}

describe("CanvasCacheWarningOverlay", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockScrollY = 0;
		requestStore.reset();
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => {
			root.unmount();
			requestStore.reset();
		});
		container.remove();
	});

	it("shows AlertTriangle and the localized cache tooltip only for cached warnings", () => {
		const cachedWarning = createSource(
			"cached-warning",
			createQueryCacheState(),
			{ name: "/evidence/host/cached-warning.json" },
		);
		requestStore.setLoading(
			"ingestion-active" as Request.Id,
			cachedWarning.id,
		);
		const sources = [
			cachedWarning,
			createSource(
				"cached-fresh",
				createQueryCacheState({ freshness: "fresh", warningReason: null }),
			),
			createSource(
				"server-unknown",
				createQueryCacheState({ activeGeneration: null }),
			),
			createSource(
				"cache-building",
				createQueryCacheState({ isActivating: true }),
			),
		];

		act(() => root.render(<CanvasCacheWarningOverlay sources={sources} />));

		expect(
			container.querySelectorAll('[data-icon="AlertTriangle"]'),
		).toHaveLength(1);
		const indicator = container.querySelector<HTMLElement>(
			'[data-source-cache-warning="cached-warning"]',
		);
		expect(indicator).not.toBeNull();
		expect(indicator?.style.top).toBe("14px");
		expect(indicator?.textContent?.includes("cached-warning.json")).toBe(true);
		expect(
			indicator?.textContent?.includes("renderEngine.ingestingSuffix"),
		).toBe(true);
		expect(indicator?.textContent?.includes("/evidence/host/")).toBe(false);
		expect(
			indicator?.textContent?.includes("source.cacheWarning.loadedFromCache"),
		).toBe(true);
		expect(container.querySelector('[role="img"]')?.getAttribute("aria-label"))
			.toBe("source.cacheWarning.loadedFromCache");
		expect(container.querySelector('[role="img"]')?.getAttribute("tabindex"))
			.toBe("0");
	});

	it("resolves only recognized request prefixes to loading suffixes", () => {
		const translations = {
			ingestingSuffix: " | ingesting",
			loadingSuffix: " | loading",
		};
		expect(
			getCanvasSourceLoadingSuffix(
				"ingestion-active" as Request.Id,
				translations,
			),
		).toBe(" | ingesting");
		expect(
			getCanvasSourceLoadingSuffix("query-active" as Request.Id, translations),
		).toBe(" | loading");
		expect(
			getCanvasSourceLoadingSuffix("unknown-active" as Request.Id, translations),
		).toBe("");
		expect(
			getCanvasSourceLoadingSuffix("query" as Request.Id, translations),
		).toBe("");
	});

	it("tracks row geometry independently from the parent Canvas", () => {
		expect(getCanvasCacheWarningTop(0, 0)).toBe(14);
		expect(getCanvasCacheWarningTop(3, 40)).toBe(118);
	});

	it("requires active settled cache authority and a non-fresh warning", () => {
		expect(shouldShowCanvasCacheWarning(createQueryCacheState())).toBe(true);
		expect(
			shouldShowCanvasCacheWarning(
				createQueryCacheState({ activeGeneration: null }),
			),
		).toBe(false);
		expect(
			shouldShowCanvasCacheWarning(
				createQueryCacheState({ isActivating: true }),
			),
		).toBe(false);
		expect(
			shouldShowCanvasCacheWarning(
				createQueryCacheState({ freshness: "fresh", warningReason: null }),
			),
		).toBe(false);
	});

	it("associates warnings by source id even when source names are identical", () => {
		const warningState = createQueryCacheState();
		const cachedSource = createSource("cached-source-id", warningState, {
			name: "duplicate.json",
		});
		const differentSource = createSource("different-source-id", warningState, {
			name: "duplicate.json",
			querySourceIds: [cachedSource.id],
		});

		expect(shouldShowCanvasSourceCacheWarning(cachedSource)).toBe(true);
		expect(shouldShowCanvasSourceCacheWarning(differentSource)).toBe(false);

		act(() =>
			root.render(
				<CanvasCacheWarningOverlay
					sources={[cachedSource, differentSource]}
				/>,
			),
		);

		expect(
			container.querySelectorAll('[data-icon="AlertTriangle"]'),
		).toHaveLength(1);
		expect(
			container.querySelector(
				'[data-source-cache-warning="cached-source-id"]',
			),
		).not.toBeNull();
		expect(
			container.querySelector(
				'[data-source-cache-warning="different-source-id"]',
			),
		).toBeNull();
	});
});

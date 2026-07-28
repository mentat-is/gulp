import { Info } from "@/class/Info";
import type { App } from "@/entities/App";
import type { Operation } from "@/entities/Operation";
import { Request } from "@/entities/Request";
import type { Source } from "@/entities/Source";
import { requestStore } from "@/store/request.store";

jest.mock("@/class/IndexedDB", () => ({
	GulpIndexedDB: class MockGulpIndexedDB {},
}));

jest.mock("@/locales/core", () => ({
	translate: (key: string): string => key,
}));

jest.mock("@/components/QueryBuilder", () => ({
	OpenSearchQueryBuilder: {},
}));

jest.mock("@/dto/Logger.class", () => ({
	Logger: {
		log: jest.fn(),
		warn: jest.fn(),
		error: jest.fn(),
		fatal: jest.fn(),
		assert: jest.fn(),
	},
}));

jest.mock("@/ui/utils", () => ({
	generateUUID: jest.fn(),
	NodeFile: class MockNodeFile {},
	Refractor: {
		array: (...values: unknown[]): unknown[] => values,
	},
	stringToHexColor: (): string => "#000000",
}));

jest.mock("@/overlays/Highlights", () => ({
	Highlights: {},
}));

jest.mock("@/components/Pointers", () => ({
	Pointers: {},
}));

jest.mock("@/components/CustomParameters", () => ({
	CustomParameters: {},
}));

jest.mock("@/class/RenderEngine", () => ({
	RenderEngine: {
		clearAllCaches: jest.fn(),
		reset: jest.fn(),
	},
}));

jest.mock("@/class/SmartSocket", () => ({
	SmartSocket: {
		Class: {
			instance: null,
		},
	},
}));

jest.mock("@/entities/Source", () => ({
	Source: {
		Entity: {},
	},
}));

jest.mock("@/entities/App", () => ({
	App: {},
}));

jest.mock("@/entities/Doc", () => ({
	Doc: {
		Entity: {},
	},
}));

jest.mock("@/entities/Note", () => ({
	Note: {
		Entity: {},
	},
}));

jest.mock("@/entities/Link", () => ({
	Link: {
		Entity: {},
	},
}));

jest.mock("react-markdown", () => (): null => null);
jest.mock("remark-gfm", () => (): null => null);
jest.mock("rehype-prism-plus", () => (): null => null);

jest.mock("@/workers/IngestWorker.manager", () => ({
	ingestWorkerManager: {},
}));

jest.mock("@/workers/DataWorker.class", () => ({
	DataWorker: {},
}));

const operationId = "operation-resync-lifecycle" as Operation.Id;
const ingestionRequestId = "ingestion-resync-lifecycle" as Request.Id;
const queryRequestId = "query-resync-lifecycle" as Request.Id;
const sourceId = "source-resync-lifecycle" as Source.Id;

/**
 * Creates the minimum application state required by the Info constructor.
 * @returns Application state fixture.
 */
function createApp(): App.Type {
	return {
		timeline: {
			scale: 1,
		},
		general: {
			loadings: requestStore.getLoadings(),
		},
	} as App.Type;
}

/**
 * Creates a source-linked backend request fixture.
 * @param id Request identifier.
 * @param reqType Backend request type.
 * @returns Ongoing request fixture.
 */
function createRequest(id: Request.Id, reqType: string): Request.Type {
	return {
		id,
		req_type: reqType,
		status: Request.Status.ONGOING,
		data: {
			sources: [["context-id", "context-name", sourceId, "source-name"]],
		},
	} as Request.Type;
}

/**
 * Creates an Info instance with isolated state callbacks.
 * @returns Info instance under test.
 */
function createInfo(): Info {
	return new Info({
		app: createApp(),
		setInfo: jest.fn(),
		timeline: { current: document.createElement("div") },
	});
}

describe("Info ingestion resynchronization lifecycle", () => {
	beforeEach(() => {
		jest.useFakeTimers();
		requestStore.reset();
	});

	afterEach(() => {
		jest.useRealTimers();
		requestStore.reset();
	});

	it("does not start source polling for an ongoing query request", async () => {
		const info = createInfo();
		jest
			.spyOn(info, "request_list")
			.mockResolvedValue([createRequest(queryRequestId, "query")]);
		const sync = jest.spyOn(info, "sync").mockResolvedValue(undefined);

		await info.resync_ingestion_state(operationId);
		jest.advanceTimersByTime(10_000);
		await Promise.resolve();

		expect(sync).not.toHaveBeenCalled();
		expect(requestStore.getLoadings().byFileId.size).toBe(0);
	});

	it("polls only while a reconstructed ingestion marker owns its source", async () => {
		const info = createInfo();
		jest
			.spyOn(info, "request_list")
			.mockResolvedValue([createRequest(ingestionRequestId, "ingest")]);
		const sync = jest.spyOn(info, "sync").mockResolvedValue(undefined);

		await info.resync_ingestion_state(operationId);
		jest.advanceTimersByTime(5_000);
		await Promise.resolve();
		expect(sync).toHaveBeenCalledTimes(1);

		info.delLoadingByFile(sourceId);
		info.setLoading(queryRequestId, sourceId);
		jest.advanceTimersByTime(5_000);
		await Promise.resolve();

		expect(sync).toHaveBeenCalledTimes(1);
	});

	it("does not start a late poll after lifecycle cleanup", async () => {
		const info = createInfo();
		let resolveRequests: (requests: Request.Type[]) => void = () => undefined;
		const requests = new Promise<Request.Type[]>((resolve) => {
			resolveRequests = resolve;
		});
		jest.spyOn(info, "request_list").mockReturnValue(requests);
		const sync = jest.spyOn(info, "sync").mockResolvedValue(undefined);

		const resync = info.resync_ingestion_state(operationId);
		info.stop_ingestion_resync_poll();
		resolveRequests([createRequest(ingestionRequestId, "ingest")]);
		await resync;
		jest.advanceTimersByTime(10_000);
		await Promise.resolve();

		expect(sync).not.toHaveBeenCalled();
		expect(requestStore.getLoadings().byFileId.size).toBe(0);
	});
});

import { Operation } from "@/entities/Operation";
import { Request } from "@/entities/Request";
import { Source } from "@/entities/Source";
import { requestStore } from "@/store/request.store";

const requestId = "query-store-test" as Request.Id;
const secondRequestId = "query-store-test-2" as Request.Id;
const operationId = "operation-store-test" as Operation.Id;
const secondOperationId = "operation-store-test-2" as Operation.Id;
const sourceId = "source-store-test" as Source.Id;
const secondSourceId = "source-store-test-2" as Source.Id;

describe("requestStore managed queries", () => {
	beforeEach(() => {
		requestStore.reset();
	});

	it("registers and completes managed query requests", () => {
		const abortController = new AbortController();

		requestStore.registerManagedQuery({
			requestId,
			operationId,
			sourceIds: [sourceId],
			abortController,
		});

		const managedQuery = requestStore.getManagedQuery(requestId);
		expect(managedQuery?.status).toBe("active");
		expect(managedQuery?.operationId).toBe(operationId);
		expect(managedQuery?.sourceIds).toEqual([sourceId]);
		expect(requestStore.isManagedQueryRequest(requestId)).toBe(true);

		requestStore.completeManagedQuery(requestId);

		expect(requestStore.getManagedQuery(requestId)?.status).toBe("completed");
		expect(abortController.signal.aborted).toBe(false);
	});

	it("attaches abort controllers and cancels managed queries", () => {
		const abortController = new AbortController();

		requestStore.registerManagedQuery({
			requestId,
			operationId,
			sourceIds: [sourceId],
		});
		requestStore.attachManagedQueryAbortController(requestId, abortController);
		requestStore.cancelManagedQuery(requestId);

		expect(requestStore.getManagedQuery(requestId)?.status).toBe("canceled");
		expect(abortController.signal.aborted).toBe(true);
	});

	it("returns managed queries scoped to an operation", () => {
		requestStore.registerManagedQuery({
			requestId,
			operationId,
			sourceIds: [sourceId],
		});
		requestStore.registerManagedQuery({
			requestId: secondRequestId,
			operationId: secondOperationId,
			sourceIds: [secondSourceId],
		});

		expect(
			requestStore
				.getManagedQueriesForOperation(operationId)
				.map((request) => request.requestId),
		).toEqual([requestId]);
	});
});

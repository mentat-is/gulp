import {
	collectIngestionResyncLoadings,
	hasActiveIngestionResyncLoading,
} from "@/class/IngestionResync";
import { Request } from "@/entities/Request";
import type { Source } from "@/entities/Source";
import type { RequestLoadingState } from "@/store/request.store";

const operationSourceId = "source-ingestion-resync" as Source.Id;
const secondSourceId = "source-raw-ingestion-resync" as Source.Id;
const querySourceId = "source-query-resync" as Source.Id;
const ingestionRequestId = "ingestion-resync" as Request.Id;
const rawIngestionRequestId = "raw-ingestion-resync" as Request.Id;
const queryRequestId = "query-resync" as Request.Id;

/**
 * Creates a minimal request fixture for ingestion resynchronization tests.
 * @param id Backend request identifier.
 * @param reqType Backend request type.
 * @param status Backend request status.
 * @param sourceIds Source identifiers linked to the request.
 * @returns Typed request fixture.
 */
function createRequest(
	id: Request.Id,
	reqType: string,
	status: Request.Status,
	sourceIds: Source.Id[],
): Request.Type {
	return {
		id,
		req_type: reqType,
		status,
		data: {
			sources: sourceIds.map((sourceId) => [
				"context-id",
				"context-name",
				sourceId,
				"source-name",
			]),
		},
	} as Request.Type;
}

/**
 * Creates request loading state from source-to-request pairs.
 * @param pairs Source and request identifiers to store.
 * @returns Request loading state fixture.
 */
function createLoadings(
	pairs: Array<[Source.Id, Request.Id]>,
): RequestLoadingState {
	return {
		byFileId: new Map(pairs),
		byRequestId: new Map(
			pairs.map(([sourceId, requestId]) => [requestId, sourceId]),
		),
	};
}

describe("ingestion resynchronization", () => {
	it("collects only ongoing ingestion and raw-ingestion sources", () => {
		const recovered = collectIngestionResyncLoadings([
			createRequest(
				ingestionRequestId,
				"ingest",
				Request.Status.ONGOING,
				[operationSourceId],
			),
			createRequest(
				rawIngestionRequestId,
				"raw_ingest",
				Request.Status.ONGOING,
				[secondSourceId],
			),
			createRequest(
				queryRequestId,
				"query",
				Request.Status.ONGOING,
				[querySourceId],
			),
			createRequest(
				"completed-ingestion" as Request.Id,
				"ingest",
				Request.Status.DONE,
				["completed-source" as Source.Id],
			),
		]);

		expect(Array.from(recovered.entries())).toEqual([
			[operationSourceId, ingestionRequestId],
			[secondSourceId, rawIngestionRequestId],
		]);
	});

	it("ignores unrelated query loadings when deciding whether to keep polling", () => {
		const recovered = new Map([[operationSourceId, ingestionRequestId]]);

		expect(
			hasActiveIngestionResyncLoading(
				recovered,
				createLoadings([[querySourceId, queryRequestId]]),
			),
		).toBe(false);
		expect(
			hasActiveIngestionResyncLoading(
				recovered,
				createLoadings([[operationSourceId, queryRequestId]]),
			),
		).toBe(false);
		expect(
			hasActiveIngestionResyncLoading(
				recovered,
				createLoadings([[operationSourceId, ingestionRequestId]]),
			),
		).toBe(true);
	});
});

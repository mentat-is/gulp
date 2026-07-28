import { Request } from "@/entities/Request";
import type { Source } from "@/entities/Source";
import type { RequestLoadingState } from "@/store/request.store";

const INGESTION_REQUEST_TYPES = new Set(["ingest", "raw_ingest"]);

export type IngestionResyncLoadings = Map<Source.Id, Request.Id>;

/**
 * Collects source loading markers for ongoing backend ingestion requests.
 * @param requests Requests returned by the operation-scoped request list API.
 * @returns Source-to-request markers that are eligible for ingestion recovery.
 */
export function collectIngestionResyncLoadings(
	requests: Request.Type[],
): IngestionResyncLoadings {
	const loadings: IngestionResyncLoadings = new Map();

	requests.forEach((request) => {
		if (
			request.status !== Request.Status.ONGOING ||
			!request.req_type ||
			!INGESTION_REQUEST_TYPES.has(request.req_type)
		) {
			return;
		}

		Request.Entity.sourceLinks(request).forEach(({ sourceId }) => {
			loadings.set(sourceId as Source.Id, request.id);
		});
	});

	return loadings;
}

/**
 * Checks whether any recovered ingestion marker still owns its source loading.
 * Timeline queries and unrelated background work are deliberately ignored.
 * @param recoveredLoadings Loading markers created by ingestion recovery.
 * @param currentLoadings Current application request-loading state.
 * @returns True when at least one recovered ingestion remains authoritative.
 */
export function hasActiveIngestionResyncLoading(
	recoveredLoadings: IngestionResyncLoadings,
	currentLoadings: RequestLoadingState,
): boolean {
	return Array.from(recoveredLoadings).some(
		([sourceId, requestId]) =>
			currentLoadings.byFileId.get(sourceId) === requestId,
	);
}

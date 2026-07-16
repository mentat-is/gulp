import { useSyncExternalStore } from "react";
import { Request } from "@/entities/Request";
import { Source } from "@/entities/Source";
import { Operation } from "@/entities/Operation";

export interface RequestLoadingState {
	byRequestId: Map<Request.Id, Source.Id>;
	byFileId: Map<Source.Id, Request.Id>;
}

export type ManagedQueryStatus = "active" | "completed" | "canceled";

export interface ManagedQueryRequest {
	requestId: Request.Id;
	operationId: Operation.Id;
	sourceIds: Source.Id[];
	status: ManagedQueryStatus;
	abortController?: AbortController;
}

export interface RequestStoreSnapshot {
	requests: Request.Type[];
	loadings: RequestLoadingState;
	managedQueries: Map<Request.Id, ManagedQueryRequest>;
	version: number;
}

const EMPTY_LOADING_STATE: RequestLoadingState = {
	byRequestId: new Map<Request.Id, Source.Id>(),
	byFileId: new Map<Source.Id, Request.Id>(),
};

/**
 * Keeps request progress outside Application.Context so websocket and API
 * loading churn does not force unrelated React consumers to re-render.
 */
class RequestStore {
	private listeners = new Set<() => void>();
	private snapshot: RequestStoreSnapshot = {
		requests: [],
		loadings: EMPTY_LOADING_STATE,
		managedQueries: new Map<Request.Id, ManagedQueryRequest>(),
		version: 0,
	};

	/**
	 * Subscribes a React listener to request store changes.
	 * @param listener Callback invoked after a store update.
	 * @returns Function that removes the listener.
	 */
	subscribe = (listener: () => void): (() => void) => {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	};

	/**
	 * Returns the immutable snapshot consumed by useSyncExternalStore.
	 * @returns Current request and loading state.
	 */
	getSnapshot = (): RequestStoreSnapshot => this.snapshot;

	/**
	 * Returns the current request list.
	 * @returns Request array in the current store snapshot.
	 */
	getRequests = (): Request.Type[] => this.snapshot.requests;

	/**
	 * Returns the current loading maps.
	 * @returns Loading maps keyed by request and source id.
	 */
	getLoadings = (): RequestLoadingState => this.snapshot.loadings;

	/**
	 * Returns the current managed query map.
	 * @returns Managed query requests keyed by backend request id.
	 */
	getManagedQueries = (): Map<Request.Id, ManagedQueryRequest> =>
		this.snapshot.managedQueries;

	/**
	 * Replaces the request list with a sorted copy.
	 * @param requests Requests returned by the backend.
	 * @returns Nothing.
	 */
	setRequests = (requests: Request.Type[]): void => {
		this.commit({
			requests: this.sortRequests(requests),
		});
	};

	/**
	 * Inserts or updates one request while keeping existing detail fields.
	 * @param request Request stats object from websocket or API.
	 * @returns Nothing.
	 */
	upsertRequest = (request: Request.Type): void => {
		const previousRequests = this.snapshot.requests;
		const index = previousRequests.findIndex((item) => item.id === request.id);
		const nextRequests = [...previousRequests];

		if (index >= 0) {
			const previous = nextRequests[index];
			nextRequests[index] = {
				...previous,
				...request,
				data: request.data ?? previous.data,
				errors: request.errors ?? previous.errors,
			};
		} else {
			nextRequests.push(request);
		}

		this.commit({
			requests: this.sortRequests(nextRequests),
		});
	};

	/**
	 * Marks a source as loading for a backend request.
	 * @param requestId Backend request identifier.
	 * @param sourceId Source affected by the request.
	 * @returns Nothing.
	 */
	setLoading = (requestId: Request.Id, sourceId: Source.Id): void => {
		const loadings = this.cloneLoadings();
		loadings.byRequestId.set(requestId, sourceId);
		loadings.byFileId.set(sourceId, requestId);
		this.commit({ loadings });
	};

	/**
	 * Removes the loading marker associated with a source.
	 * @param sourceId Source identifier.
	 * @returns Removed request id, when one existed.
	 */
	deleteLoadingByFile = (sourceId: Source.Id): Request.Id | undefined => {
		const requestId = this.snapshot.loadings.byFileId.get(sourceId);
		const loadings = this.cloneLoadings();
		loadings.byFileId.delete(sourceId);
		if (requestId) {
			loadings.byRequestId.delete(requestId);
		}
		this.commit({ loadings });
		return requestId;
	};

	/**
	 * Removes the loading marker associated with a request.
	 * @param requestId Backend request identifier.
	 * @returns Source id that was loading, when one existed.
	 */
	deleteLoadingByRequest = (requestId: Request.Id): Source.Id | undefined => {
		const sourceId = this.snapshot.loadings.byRequestId.get(requestId);
		const loadings = this.cloneLoadings();
		loadings.byRequestId.delete(requestId);
		if (sourceId) {
			loadings.byFileId.delete(sourceId);
		}
		this.commit({ loadings });
		return sourceId;
	};

	/**
	 * Returns the request currently associated with a source.
	 * @param sourceId Source identifier.
	 * @returns Request id or undefined.
	 */
	getRequestIdByFile = (sourceId: Source.Id): Request.Id | undefined =>
		this.snapshot.loadings.byFileId.get(sourceId);

	/**
	 * Returns the source currently associated with a request.
	 * @param requestId Backend request identifier.
	 * @returns Source id or undefined.
	 */
	getFileIdByRequest = (requestId: Request.Id): Source.Id | undefined =>
		this.snapshot.loadings.byRequestId.get(requestId);

	/**
	 * Checks whether a source has an active loading marker.
	 * @param sourceId Source identifier.
	 * @returns True when the source is loading.
	 */
	hasLoadingForFile = (sourceId: Source.Id): boolean =>
		this.snapshot.loadings.byFileId.has(sourceId);

	/**
	 * Checks whether a request has an active loading marker.
	 * @param requestId Backend request identifier.
	 * @returns True when the request is loading.
	 */
	hasLoadingForRequest = (requestId: Request.Id): boolean =>
		this.snapshot.loadings.byRequestId.has(requestId);

	/**
	 * Registers a query request that the UI can cancel locally.
	 * @param request Managed query request metadata.
	 * @returns Nothing.
	 */
	registerManagedQuery = (
		request: Omit<ManagedQueryRequest, "status"> & {
			status?: ManagedQueryStatus;
		},
	): void => {
		const managedQueries = this.cloneManagedQueries();
		managedQueries.set(request.requestId, {
			...request,
			sourceIds: [...request.sourceIds],
			status: request.status ?? "active",
		});
		this.commit({ managedQueries });
	};

	/**
	 * Attaches or replaces the local abort controller for a managed query.
	 * @param requestId Backend request identifier.
	 * @param abortController Controller used for the HTTP request.
	 * @returns Nothing.
	 */
	attachManagedQueryAbortController = (
		requestId: Request.Id,
		abortController: AbortController,
	): void => {
		const existing = this.snapshot.managedQueries.get(requestId);
		if (!existing) return;

		const managedQueries = this.cloneManagedQueries();
		managedQueries.set(requestId, {
			...existing,
			abortController,
		});
		this.commit({ managedQueries });
	};

	/**
	 * Marks a managed query as completed.
	 * @param requestId Backend request identifier.
	 * @returns Nothing.
	 */
	completeManagedQuery = (requestId: Request.Id): void => {
		this.setManagedQueryStatus(requestId, "completed");
	};

	/**
	 * Marks a managed query as canceled and aborts its local HTTP request.
	 * @param requestId Backend request identifier.
	 * @returns Nothing.
	 */
	cancelManagedQuery = (requestId: Request.Id): void => {
		const existing = this.snapshot.managedQueries.get(requestId);
		if (existing?.abortController && !existing.abortController.signal.aborted) {
			existing.abortController.abort();
		}
		this.setManagedQueryStatus(requestId, "canceled");
	};

	/**
	 * Returns one managed query, when known.
	 * @param requestId Backend request identifier.
	 * @returns Managed query metadata or undefined.
	 */
	getManagedQuery = (
		requestId: Request.Id,
	): ManagedQueryRequest | undefined => this.snapshot.managedQueries.get(requestId);

	/**
	 * Returns managed query metadata for one operation.
	 * @param operationId Operation identifier.
	 * @returns Managed query records for the operation.
	 */
	getManagedQueriesForOperation = (
		operationId: Operation.Id,
	): ManagedQueryRequest[] =>
		Array.from(this.snapshot.managedQueries.values()).filter(
			(request) => request.operationId === operationId,
		);

	/**
	 * Checks whether a backend request is managed by the query lifecycle.
	 * @param requestId Backend request identifier.
	 * @returns True when the request is a managed query.
	 */
	isManagedQueryRequest = (requestId: Request.Id): boolean =>
		this.snapshot.managedQueries.has(requestId);

	/**
	 * Clears all request store state.
	 * @returns Nothing.
	 */
	reset = (): void => {
		this.commit({
			requests: [],
			loadings: {
				byRequestId: new Map<Request.Id, Source.Id>(),
				byFileId: new Map<Source.Id, Request.Id>(),
			},
			managedQueries: new Map<Request.Id, ManagedQueryRequest>(),
		});
	};

	/**
	 * Commits a partial snapshot and notifies subscribers.
	 * @param patch Snapshot fields to replace.
	 * @returns Nothing.
	 */
	private commit = (patch: Partial<Omit<RequestStoreSnapshot, "version">>): void => {
		this.snapshot = {
			...this.snapshot,
			...patch,
			version: this.snapshot.version + 1,
		};
		this.listeners.forEach((listener) => listener());
	};

	/**
	 * Clones loading maps before mutation to keep snapshots stable.
	 * @returns Mutable copies of loading maps.
	 */
	private cloneLoadings = (): RequestLoadingState => ({
		byRequestId: new Map(this.snapshot.loadings.byRequestId),
		byFileId: new Map(this.snapshot.loadings.byFileId),
	});

	/**
	 * Clones managed query metadata before mutation.
	 * @returns Mutable copy of managed query map.
	 */
	private cloneManagedQueries = (): Map<Request.Id, ManagedQueryRequest> =>
		new Map(
			Array.from(this.snapshot.managedQueries.entries()).map(
				([requestId, request]) => [
					requestId,
					{
						...request,
						sourceIds: [...request.sourceIds],
					},
				],
			),
		);

	/**
	 * Updates the status for one managed query.
	 * @param requestId Backend request identifier.
	 * @param status New lifecycle status.
	 * @returns Nothing.
	 */
	private setManagedQueryStatus = (
		requestId: Request.Id,
		status: ManagedQueryStatus,
	): void => {
		const existing = this.snapshot.managedQueries.get(requestId);
		if (!existing || existing.status === status) return;

		const managedQueries = this.cloneManagedQueries();
		managedQueries.set(requestId, {
			...existing,
			status,
		});
		this.commit({ managedQueries });
	};

	/**
	 * Sorts requests with newest first.
	 * @param requests Requests to sort.
	 * @returns Sorted request copy.
	 */
	private sortRequests = (requests: Request.Type[]): Request.Type[] =>
		[...requests].sort((a, b) => b.time_created - a.time_created);
}

export const requestStore = new RequestStore();

/**
 * Subscribes a component to the current request list.
 * @returns Request array from the external store.
 */
export function useRequests(): Request.Type[] {
	return useSyncExternalStore(
		requestStore.subscribe,
		() => requestStore.getSnapshot().requests,
		() => requestStore.getSnapshot().requests,
	);
}

/**
 * Subscribes a component to loading maps.
 * @returns Request loading maps from the external store.
 */
export function useRequestLoadings(): RequestLoadingState {
	return useSyncExternalStore(
		requestStore.subscribe,
		() => requestStore.getSnapshot().loadings,
		() => requestStore.getSnapshot().loadings,
	);
}

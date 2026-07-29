import {
	useCallback,
	useEffect,
	useRef,
	useState,
	type Dispatch,
	type SetStateAction,
} from "react";
import type { Doc } from "@/entities/Doc";
import type { Operation } from "@/entities/Operation";

const EVENT_DETAIL_CACHE_SIZE = 20;

export interface EventDetailState {
	json: Record<string, unknown> | null;
	loading: boolean;
	error: unknown;
	setJson: Dispatch<SetStateAction<Record<string, unknown>>>;
	retry: () => void;
}

interface EventDetailOptions {
	eventId: Doc.Id;
	operationId?: Operation.Id;
	query: (
		eventId: Doc.Id,
		operationId: Operation.Id,
		signal?: AbortSignal,
	) => Promise<Doc.Type>;
	prepare: (event: Doc.Type) => Record<string, unknown>;
}

/**
 * Loads complete event JSON by semantic operation/event identity.
 *
 * Equivalent parent renders do not restart the request. Event changes abort the
 * stale request, and a small component-local cache makes back navigation
 * immediate without retaining details after the Dialog closes.
 */
export function useEventDetail({
	eventId,
	operationId,
	query,
	prepare,
}: EventDetailOptions): EventDetailState {
	const detailKey = operationId ? `${operationId}:${eventId}` : null;
	const cacheRef = useRef<Map<string, Record<string, unknown>>>(new Map());
	const [loadedDetail, setLoadedDetail] = useState<{
		key: string;
		json: Record<string, unknown>;
	} | null>(null);
	const [requestState, setRequestState] = useState<{
		key: string | null;
		loading: boolean;
		error: unknown;
	}>({
		key: detailKey,
		loading: true,
		error: null,
	});
	const [revision, setRevision] = useState(0);

	const json =
		detailKey && loadedDetail?.key === detailKey
			? loadedDetail.json
			: null;
	const setJson = useCallback<Dispatch<SetStateAction<Record<string, unknown>>>>(
		(value) => {
			if (!detailKey) return;
			setLoadedDetail((previous) => {
				const current =
					previous?.key === detailKey ? previous.json : {};
				const next =
					typeof value === "function" ? value(current) : value;
				cacheRef.current.delete(detailKey);
				cacheRef.current.set(detailKey, next);
				return { key: detailKey, json: next };
			});
		},
		[detailKey],
	);

	useEffect(() => {
		if (!detailKey || !operationId) {
			setRequestState({
				key: null,
				loading: false,
				error: new Error("Event operation is unavailable"),
			});
			return;
		}

		const cached = cacheRef.current.get(detailKey);
		if (cached) {
			setLoadedDetail({ key: detailKey, json: cached });
			setRequestState({ key: detailKey, loading: false, error: null });
			return;
		}

		const controller = new AbortController();
		setRequestState({ key: detailKey, loading: true, error: null });

		void query(eventId, operationId, controller.signal)
			.then((event) => {
				if (controller.signal.aborted) return;
				const prepared = prepare(event);
				cacheRef.current.delete(detailKey);
				cacheRef.current.set(detailKey, prepared);
				if (cacheRef.current.size > EVENT_DETAIL_CACHE_SIZE) {
					const oldestKey = cacheRef.current.keys().next().value;
					if (oldestKey) cacheRef.current.delete(oldestKey);
				}
				setLoadedDetail({ key: detailKey, json: prepared });
			})
			.catch((requestError: unknown) => {
				if (controller.signal.aborted) return;
				setRequestState({
					key: detailKey,
					loading: false,
					error: requestError,
				});
			})
			.finally(() => {
				if (controller.signal.aborted) return;
				setRequestState((current) =>
					current.key === detailKey
						? { ...current, loading: false }
						: current,
				);
			});

		return () => controller.abort();
	}, [
		detailKey,
		eventId,
		operationId,
		prepare,
		query,
		revision,
	]);

	const retry = useCallback(() => {
		if (detailKey) cacheRef.current.delete(detailKey);
		setRevision((current) => current + 1);
	}, [detailKey]);
	const currentRequestState =
		requestState.key === detailKey
			? requestState
			: { key: detailKey, loading: true, error: null };

	return {
		json,
		loading: currentRequestState.loading,
		error: currentRequestState.error,
		setJson,
		retry,
	};
}

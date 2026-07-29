import React, { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { Doc } from "@/entities/Doc";
import type { Operation } from "@/entities/Operation";
import {
	useEventDetail,
	type EventDetailState,
} from "@/dialogs/useEventDetail";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const OPERATION_ID = "detail-operation" as Operation.Id;

interface PendingRequest {
	eventId: Doc.Id;
	signal?: AbortSignal;
	resolve: (event: Doc.Type) => void;
}

const pendingRequests: PendingRequest[] = [];
const mockQueryCall = jest.fn<void, [Doc.Id, Operation.Id, AbortSignal?]>();
const mockQuery = (
	eventId: Doc.Id,
	operationId: Operation.Id,
	signal?: AbortSignal,
): Promise<Doc.Type> => {
	mockQueryCall(eventId, operationId, signal);
	return new Promise((resolve) => {
		pendingRequests.push({ eventId, signal, resolve });
	});
};

let latestState: EventDetailState | null = null;

function createEvent(id: string): Doc.Type {
	return {
		_id: id as Doc.Id,
		"gulp.source_id": "detail-source" as never,
		"gulp.event_code": 200,
		gulp_timestamp: 1,
		number_hash: 1,
	} as Doc.Type;
}

function DetailHarness({
	eventId,
	unrelatedRevision,
}: {
	eventId: Doc.Id;
	unrelatedRevision: number;
}) {
	void unrelatedRevision;
	latestState = useEventDetail({
		eventId,
		operationId: OPERATION_ID,
		query: mockQuery,
		prepare: prepareDetail,
	});
	return <div>{latestState.loading ? "loading" : latestState.json?._id as string}</div>;
}

function prepareDetail(event: Doc.Type): Record<string, unknown> {
	return { ...event };
}

describe("useEventDetail", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		pendingRequests.length = 0;
		mockQueryCall.mock.calls.splice(0);
		latestState = null;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("keys requests by operation/event, aborts stale work, and caches back navigation", async () => {
		const firstEvent = createEvent("event-one");
		const secondEvent = createEvent("event-two");

		act(() => {
			root.render(
				<DetailHarness
					eventId={firstEvent._id}
					unrelatedRevision={0}
				/>,
			);
		});
		expect(pendingRequests).toHaveLength(1);

		act(() => {
			root.render(
				<DetailHarness
					eventId={firstEvent._id}
					unrelatedRevision={1}
				/>,
			);
		});
		expect(pendingRequests).toHaveLength(1);

		act(() => {
			root.render(
				<DetailHarness
					eventId={secondEvent._id}
					unrelatedRevision={2}
				/>,
			);
		});
		expect(pendingRequests).toHaveLength(2);
		expect(pendingRequests[0].signal?.aborted).toBe(true);

		await act(async () => {
			pendingRequests[0].resolve(firstEvent);
			pendingRequests[1].resolve(secondEvent);
			await Promise.resolve();
		});
		expect(latestState?.json?._id).toBe(secondEvent._id);

		act(() => {
			root.render(
				<DetailHarness
					eventId={firstEvent._id}
					unrelatedRevision={3}
				/>,
			);
		});
		expect(pendingRequests).toHaveLength(3);
		await act(async () => {
			pendingRequests[2].resolve(firstEvent);
			await Promise.resolve();
		});

		act(() => {
			root.render(
				<DetailHarness
					eventId={secondEvent._id}
					unrelatedRevision={4}
				/>,
			);
		});
		expect(pendingRequests).toHaveLength(3);
		expect(latestState?.json?._id).toBe(secondEvent._id);
	});
});

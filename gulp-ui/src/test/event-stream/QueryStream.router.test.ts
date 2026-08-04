import {
	QueryStreamRouter,
	getDocumentChunkSequence,
	type DocumentChunkReady,
	type DocumentStreamDescriptor,
	type DocumentStreamStateChange,
	type QuerySocketMessage,
} from "@/event-stream";

const persistDescriptor: DocumentStreamDescriptor = {
	requestId: "request-persist",
	operationId: "operation-one",
	operationEpoch: 4,
	owner: "persist",
};

/**
 * Creates a minimal document chunk for router tests.
 * @param requestId Request routing key.
 * @param sequence Backend chunk sequence.
 * @param last Whether the chunk closes its stream.
 * @returns Routable document chunk message.
 */
function createChunk(
	requestId: string,
	sequence: number,
	last = false,
): QuerySocketMessage {
	return {
		type: "docs_chunk",
		req_id: requestId,
		"gulp.operation_id": "operation-one",
		payload: {
			docs: [{ _id: `${requestId}-${sequence}` }],
			chunk_num: sequence,
			chunk_number: 0,
			last,
		},
	};
}

describe("QueryStreamRouter", () => {
	it("forwards non-document messages without stream registration", () => {
		const forwarded: QuerySocketMessage[] = [];
		const router = new QueryStreamRouter({
			onForwardMessage: (message) => forwarded.push(message),
			onDocumentChunkReady: () => undefined,
		});
		const message: QuerySocketMessage = {
			type: "stats_update",
			req_id: "request-stats",
		};

		expect(router.route(message, 10)).toBe("forwarded");
		expect(forwarded).toEqual([message]);
	});

	it("never forwards an uncorrelated document chunk to the main thread", () => {
		const forwarded: QuerySocketMessage[] = [];
		const router = new QueryStreamRouter({
			onForwardMessage: (message) => forwarded.push(message),
			onDocumentChunkReady: () => undefined,
		});
		const message: QuerySocketMessage = {
			type: "docs_chunk",
			payload: { docs: [{ _id: "missing-request" }] },
		};

		expect(router.route(message, 100)).toBe("ignored");
		expect(forwarded).toHaveLength(0);
	});

	it("routes persist-owned chunks internally and deduplicates chunk_num", () => {
		const persisted: DocumentChunkReady[] = [];
		const states: DocumentStreamStateChange[] = [];
		const router = new QueryStreamRouter({
			onForwardMessage: () => undefined,
			onDocumentChunkReady: (chunk) => persisted.push(chunk),
			onStreamStateChange: (change) => states.push(change),
		});
		const chunk = createChunk(persistDescriptor.requestId, 7, true);

		router.register(persistDescriptor);
		expect(router.route(chunk, 100)).toBe("document-ready");
		expect(router.route(chunk, 100)).toBe("duplicate");
		expect(persisted).toHaveLength(1);
		expect(persisted[0].sequence).toBe(7);
		expect(persisted[0].rawByteLength).toBe(100);
		expect(router.getStreamState(persistDescriptor.requestId)).toBe("streaming");
		expect(router.complete(persistDescriptor.requestId)).toBe(true);
		expect(router.route(chunk, 100)).toBe("terminal");
		expect(states.map(({ state }) => state)).toEqual([
			"registered",
			"streaming",
			"completed",
		]);
	});

	it("accepts repeated child-query sequences when request-wide deduplication is disabled", () => {
		const persisted: DocumentChunkReady[] = [];
		const router = new QueryStreamRouter({
			onForwardMessage: () => undefined,
			onDocumentChunkReady: (chunk) => persisted.push(chunk),
		});
		router.register({
			...persistDescriptor,
			requestId: "multi-query",
			completionMode: "explicit",
			deduplicateSequences: false,
		});

		expect(router.route(createChunk("multi-query", 0, true), 100)).toBe(
			"document-ready",
		);
		expect(router.route(createChunk("multi-query", 0, true), 100)).toBe(
			"document-ready",
		);
		expect(persisted).toHaveLength(2);
	});

	it("honors ignore and passthrough ownership", () => {
		const forwarded: QuerySocketMessage[] = [];
		const persisted: DocumentChunkReady[] = [];
		const router = new QueryStreamRouter({
			onForwardMessage: (message) => forwarded.push(message),
			onDocumentChunkReady: (chunk) => persisted.push(chunk),
		});
		router.register({ ...persistDescriptor, requestId: "ignored", owner: "ignore" });
		router.register({
			...persistDescriptor,
			requestId: "legacy",
			owner: "passthrough",
		});

		expect(router.route(createChunk("ignored", 0), 10)).toBe("ignored");
		expect(router.route(createChunk("legacy", 0), 10)).toBe("forwarded");
		expect(forwarded).toHaveLength(1);
		expect(persisted).toHaveLength(0);
	});

	it("requires explicit completion when completionMode ignores last", () => {
		const states: DocumentStreamStateChange[] = [];
		const router = new QueryStreamRouter({
			onForwardMessage: () => undefined,
			onDocumentChunkReady: () => undefined,
			onStreamStateChange: (change) => states.push(change),
		});
		router.register({
			...persistDescriptor,
			requestId: "explicit-ignore",
			owner: "ignore",
			completionMode: "explicit",
		});

		router.route(createChunk("explicit-ignore", 0, true), 10);
		expect(router.getStreamState("explicit-ignore")).toBe("streaming");
		expect(router.complete("explicit-ignore")).toBe(true);
		expect(states.map(({ state }) => state)).toEqual([
			"registered",
			"streaming",
			"completed",
		]);
	});

	it("auto-completes last-owned passthrough streams by default", () => {
		const router = new QueryStreamRouter({
			onForwardMessage: () => undefined,
			onDocumentChunkReady: () => undefined,
		});
		router.register({
			...persistDescriptor,
			requestId: "last-passthrough",
			owner: "passthrough",
		});

		router.route(createChunk("last-passthrough", 0, true), 10);
		expect(router.getStreamState("last-passthrough")).toBe("completed");
	});

	it("replays early chunks in arrival order after registration", () => {
		const persisted: DocumentChunkReady[] = [];
		const unknownRequests: string[] = [];
		const router = new QueryStreamRouter({
			onForwardMessage: () => undefined,
			onDocumentChunkReady: (chunk) => persisted.push(chunk),
			onUnknownDocumentStream: (requestId) =>
				unknownRequests.push(requestId),
		});

		router.route(createChunk("early", 0), 40);
		router.route(createChunk("early", 1), 40);
		expect(router.bufferedByteLength).toBe(80);
		expect(unknownRequests).toEqual(["early"]);

		router.register({ ...persistDescriptor, requestId: "early" });
		expect(persisted.map(({ sequence }) => sequence)).toEqual([0, 1]);
		expect(persisted.map(({ rawByteLength }) => rawByteLength)).toEqual([40, 40]);
		expect(router.bufferedByteLength).toBe(0);
		expect(router.bufferedChunkCount).toBe(0);
	});

	it("expires early chunks and evicts oldest chunks over the byte limit", () => {
		let now = 1_000;
		const unknownRequests: string[] = [];
		const router = new QueryStreamRouter(
			{
				onForwardMessage: () => undefined,
				onDocumentChunkReady: () => undefined,
				onUnknownDocumentStream: (requestId) =>
					unknownRequests.push(requestId),
			},
			{
				now: () => now,
				unknownMailboxMaxBytes: 10,
				unknownMailboxTtlMs: 30,
			},
		);

		router.route(createChunk("oldest", 0), 6);
		router.route(createChunk("newest", 0), 6);
		expect(router.bufferedByteLength).toBe(6);
		expect(router.bufferedChunkCount).toBe(1);
		expect(unknownRequests).toEqual(["oldest", "newest"]);

		now += 31;
		expect(router.sweepExpired()).toBe(1);
		expect(router.bufferedByteLength).toBe(0);
	});

	it("cancels only matching operation epochs and rejects late chunks", () => {
		const router = new QueryStreamRouter({
			onForwardMessage: () => undefined,
			onDocumentChunkReady: () => undefined,
		});
		router.register({ ...persistDescriptor, requestId: "epoch-four" });
		router.register({
			...persistDescriptor,
			requestId: "epoch-five",
			operationEpoch: 5,
		});

		expect(router.cancelOperation("operation-one", 4)).toBe(1);
		expect(router.route(createChunk("epoch-four", 0), 10)).toBe("terminal");
		expect(router.route(createChunk("epoch-five", 0), 10)).toBe(
			"document-ready",
		);
	});

	it("replaces terminal contexts with TTL and count bounded guards", () => {
		let now = 1_000;
		const router = new QueryStreamRouter(
			{
				onForwardMessage: () => undefined,
				onDocumentChunkReady: () => undefined,
			},
			{
				now: () => now,
				terminalGuardMaxEntries: 2,
				terminalGuardTtlMs: 30,
			},
		);

		for (const requestId of ["terminal-one", "terminal-two", "terminal-three"]) {
			router.register({ ...persistDescriptor, requestId });
			expect(router.complete(requestId)).toBe(true);
		}

		expect(router.registeredStreamCount).toBe(0);
		expect(router.terminalGuardCount).toBe(2);
		expect(router.getStreamState("terminal-one")).toBeUndefined();
		expect(router.route(createChunk("terminal-two", 0), 10)).toBe("terminal");

		now += 31;
		expect(router.terminalGuardCount).toBe(0);
		expect(router.route(createChunk("terminal-two", 1), 10)).toBe("buffered");
	});

	it("prefers chunk_num over the model's default chunk_number alias", () => {
		expect(
			getDocumentChunkSequence({
				type: "docs_chunk",
				payload: { chunk_num: 12, chunk_number: 0 },
			}),
		).toBe(12);
	});
});

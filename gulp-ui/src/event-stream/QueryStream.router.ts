import {
	DOCUMENT_CHUNK_MESSAGE_TYPE,
	UNKNOWN_STREAM_MAILBOX_MAX_BYTES,
	UNKNOWN_STREAM_MAILBOX_TTL_MS,
	getDocumentChunkSequence,
	getQuerySocketOperationId,
	getQuerySocketRequestId,
	type DocumentChunkReady,
	type DocumentStreamDescriptor,
	type DocumentStreamState,
	type DocumentStreamStateChange,
	type QuerySocketMessage,
} from "./QueryStream.protocol";

interface RegisteredStreamContext {
	descriptor: DocumentStreamDescriptor;
	state: DocumentStreamState;
	seenSequences: Set<number>;
}

interface BufferedUnknownChunk {
	requestId: string;
	message: QuerySocketMessage;
	byteLength: number;
	expiresAt: number;
}

interface TerminalStreamGuard {
	state: Extract<DocumentStreamState, "completed" | "canceled" | "failed">;
	expiresAt: number;
}

const DEFAULT_TERMINAL_GUARD_MAX_ENTRIES = 4_096;
const DEFAULT_TERMINAL_GUARD_TTL_MS = 30_000;

export interface QueryStreamRouterCallbacks {
	onForwardMessage: (message: QuerySocketMessage) => void;
	onDocumentChunkReady: (chunk: DocumentChunkReady) => void;
	onStreamStateChange?: (change: DocumentStreamStateChange) => void;
	onUnknownDocumentStream?: (
		requestId: string,
		message: QuerySocketMessage,
	) => void;
}

export interface QueryStreamRouterOptions {
	unknownMailboxMaxBytes?: number;
	unknownMailboxTtlMs?: number;
	terminalGuardMaxEntries?: number;
	terminalGuardTtlMs?: number;
	now?: () => number;
}

export type QueryStreamRouteResult =
	| "forwarded"
	| "document-ready"
	| "ignored"
	| "buffered"
	| "duplicate"
	| "terminal";

/**
 * Owns request-aware document routing independently from WebSocket and worker APIs.
 * Unknown chunks are retained only long enough for request registration to win a race.
 */
export class QueryStreamRouter {
	private readonly callbacks: QueryStreamRouterCallbacks;
	private readonly contexts = new Map<string, RegisteredStreamContext>();
	private readonly terminalGuards = new Map<string, TerminalStreamGuard>();
	private readonly unknownMailboxMaxBytes: number;
	private readonly unknownMailboxTtlMs: number;
	private readonly terminalGuardMaxEntries: number;
	private readonly terminalGuardTtlMs: number;
	private readonly now: () => number;
	private unknownChunks: BufferedUnknownChunk[] = [];
	private notifiedUnknownRequests = new Set<string>();
	private unknownMailboxBytes = 0;

	/**
	 * Creates a bounded document stream router.
	 * @param callbacks Delivery hooks for application, persistence, and stream state events.
	 * @param options Optional mailbox limits and deterministic clock.
	 */
	public constructor(
		callbacks: QueryStreamRouterCallbacks,
		options: QueryStreamRouterOptions = {},
	) {
		this.callbacks = callbacks;
		this.unknownMailboxMaxBytes =
			options.unknownMailboxMaxBytes ?? UNKNOWN_STREAM_MAILBOX_MAX_BYTES;
		this.unknownMailboxTtlMs =
			options.unknownMailboxTtlMs ?? UNKNOWN_STREAM_MAILBOX_TTL_MS;
		this.terminalGuardMaxEntries =
			options.terminalGuardMaxEntries ?? DEFAULT_TERMINAL_GUARD_MAX_ENTRIES;
		this.terminalGuardTtlMs =
			options.terminalGuardTtlMs ?? DEFAULT_TERMINAL_GUARD_TTL_MS;
		this.now = options.now ?? Date.now;

		if (
			!Number.isFinite(this.unknownMailboxMaxBytes) ||
			this.unknownMailboxMaxBytes < 0
		) {
			throw new RangeError("Unknown stream mailbox bytes must be non-negative.");
		}
		if (
			!Number.isFinite(this.unknownMailboxTtlMs) ||
			this.unknownMailboxTtlMs < 0
		) {
			throw new RangeError("Unknown stream mailbox TTL must be non-negative.");
		}
		if (
			!Number.isSafeInteger(this.terminalGuardMaxEntries) ||
			this.terminalGuardMaxEntries < 0
		) {
			throw new RangeError("Terminal stream guard limit must be non-negative.");
		}
		if (
			!Number.isFinite(this.terminalGuardTtlMs) ||
			this.terminalGuardTtlMs < 0
		) {
			throw new RangeError("Terminal stream guard TTL must be non-negative.");
		}
	}

	/** Returns the number of bytes currently retained for unknown requests. */
	public get bufferedByteLength(): number {
		return this.unknownMailboxBytes;
	}

	/** Returns the number of document chunks currently retained for unknown requests. */
	public get bufferedChunkCount(): number {
		return this.unknownChunks.length;
	}

	/** Returns the number of active, document-owning stream contexts. */
	public get registeredStreamCount(): number {
		return this.contexts.size;
	}

	/** Returns the bounded number of retained late-chunk terminal guards. */
	public get terminalGuardCount(): number {
		this.sweepTerminalGuards();
		return this.terminalGuards.size;
	}

	/** Returns the earliest mailbox expiry time, or undefined when no chunk is retained. */
	public get nextBufferedExpiryAt(): number | undefined {
		let earliest: number | undefined;
		for (const chunk of this.unknownChunks) {
			if (earliest === undefined || chunk.expiresAt < earliest) {
				earliest = chunk.expiresAt;
			}
		}
		return earliest;
	}

	/**
	 * Returns the lifecycle state of one registered request.
	 * @param requestId Request identifier to inspect.
	 * @returns Current stream state, or undefined when no registration exists.
	 */
	public getStreamState(requestId: string): DocumentStreamState | undefined {
		this.sweepTerminalGuards();
		return this.contexts.get(requestId)?.state ?? this.terminalGuards.get(requestId)?.state;
	}

	/**
	 * Registers stream ownership and replays any early chunks in arrival order.
	 * @param descriptor Request identity and ownership policy.
	 */
	public register(descriptor: DocumentStreamDescriptor): void {
		this.validateDescriptor(descriptor);
		this.terminalGuards.delete(descriptor.requestId);
		const context: RegisteredStreamContext = {
			descriptor: {
				...descriptor,
				sourceIds: descriptor.sourceIds
					? [...descriptor.sourceIds]
					: undefined,
				defaultSourceSettings: descriptor.defaultSourceSettings
					? { ...descriptor.defaultSourceSettings }
					: undefined,
				sourceSettingsById: descriptor.sourceSettingsById
					? Object.fromEntries(
							Object.entries(descriptor.sourceSettingsById).map(
								([sourceId, settings]) => [sourceId, { ...settings }],
							),
						)
					: undefined,
			},
			state: "registered",
			seenSequences: new Set<number>(),
		};
		this.contexts.set(descriptor.requestId, context);
		this.publishState(context);

		const earlyChunks = this.takeUnknownChunks(descriptor.requestId);
		for (const chunk of earlyChunks) {
			this.routeRegisteredChunk(context, chunk.message, chunk.byteLength);
		}
	}

	/**
	 * Marks one request as canceled so late chunks cannot become unknown again.
	 * @param requestId Request identifier to cancel.
	 * @returns True when a registered stream was canceled.
	 */
	public cancel(requestId: string): boolean {
		const context = this.contexts.get(requestId);
		this.takeUnknownChunks(requestId);
		if (!context) {
			this.addTerminalGuard(requestId, "canceled");
			return false;
		}
		if (this.isTerminal(context.state)) return context.state === "canceled";
		this.transition(context, "canceled");
		this.retireContext(context);
		return true;
	}

	/**
	 * Completes one stream after its owner has finished the final durable action.
	 * @param requestId Request identifier whose stream is complete.
	 * @returns True when a non-terminal registered stream was completed.
	 */
	public complete(requestId: string): boolean {
		const context = this.contexts.get(requestId);
		if (!context || this.isTerminal(context.state)) return false;
		this.transition(context, "completed");
		this.retireContext(context);
		return true;
	}

	/**
	 * Marks one stream as flushing while its final storage transaction is pending.
	 * @param requestId Request identifier entering its durable flush phase.
	 * @returns True when a non-terminal stream entered flushing.
	 */
	public flush(requestId: string): boolean {
		const context = this.contexts.get(requestId);
		if (!context || this.isTerminal(context.state)) return false;
		this.transition(context, "flushing");
		return true;
	}

	/**
	 * Cancels every non-terminal stream in one operation or operation epoch.
	 * @param operationId Operation identifier whose streams should stop.
	 * @param operationEpoch Optional epoch used to avoid canceling a newer operation view.
	 * @returns Number of streams moved to the canceled state.
	 */
	public cancelOperation(
		operationId: string,
		operationEpoch?: number,
	): number {
		let canceled = 0;
		const retainedUnknownChunks: BufferedUnknownChunk[] = [];
		for (const chunk of this.unknownChunks) {
			if (getQuerySocketOperationId(chunk.message) === operationId) {
				this.unknownMailboxBytes -= chunk.byteLength;
				this.addTerminalGuard(chunk.requestId, "canceled");
			} else {
				retainedUnknownChunks.push(chunk);
			}
		}
		this.unknownChunks = retainedUnknownChunks;
		this.pruneUnknownNotifications();
		for (const context of this.contexts.values()) {
			if (
				context.descriptor.operationId !== operationId ||
				(operationEpoch !== undefined &&
					context.descriptor.operationEpoch !== operationEpoch) ||
				this.isTerminal(context.state)
			) {
				continue;
			}
			this.transition(context, "canceled");
			this.retireContext(context);
			this.takeUnknownChunks(context.descriptor.requestId);
			canceled += 1;
		}
		return canceled;
	}

	/**
	 * Marks one stream as failed while retaining a terminal guard for late chunks.
	 * @param requestId Request identifier to fail.
	 * @param error Human-readable failure reason.
	 * @returns True when a registered stream was moved to failed.
	 */
	public fail(requestId: string, error: string): boolean {
		const context = this.contexts.get(requestId);
		this.takeUnknownChunks(requestId);
		if (!context || this.isTerminal(context.state)) return false;
		this.transition(context, "failed", error);
		this.retireContext(context);
		return true;
	}

	/**
	 * Routes a parsed WebSocket message without knowing which realm hosts the router.
	 * @param message Validated WebSocket message.
	 * @param byteLength Conservative raw byte size used by the unknown-stream mailbox.
	 * @returns Delivery decision made for the message.
	 */
	public route(
		message: QuerySocketMessage,
		byteLength = 0,
	): QueryStreamRouteResult {
		if (message.type !== DOCUMENT_CHUNK_MESSAGE_TYPE) {
			this.callbacks.onForwardMessage(message);
			return "forwarded";
		}

		const requestId = getQuerySocketRequestId(message);
		if (!requestId) {
			return "ignored";
		}

		const context = this.contexts.get(requestId);
		if (context) {
			return this.routeRegisteredChunk(context, message, byteLength);
		}
		this.sweepTerminalGuards();
		if (this.terminalGuards.has(requestId)) return "terminal";

		this.bufferUnknownChunk(requestId, message, byteLength);
		return "buffered";
	}

	/**
	 * Removes expired early chunks and updates mailbox accounting.
	 * @returns Number of chunks removed from the mailbox.
	 */
	public sweepExpired(): number {
		this.sweepTerminalGuards();
		const currentTime = this.now();
		const retained: BufferedUnknownChunk[] = [];
		let retainedBytes = 0;
		for (const chunk of this.unknownChunks) {
			if (chunk.expiresAt <= currentTime) continue;
			retained.push(chunk);
			retainedBytes += chunk.byteLength;
		}
		const removed = this.unknownChunks.length - retained.length;
		this.unknownChunks = retained;
		this.unknownMailboxBytes = retainedBytes;
		this.pruneUnknownNotifications();
		return removed;
	}

	/** Releases every registration and buffered document reference. */
	public clear(): void {
		this.contexts.clear();
		this.terminalGuards.clear();
		this.unknownChunks = [];
		this.notifiedUnknownRequests.clear();
		this.unknownMailboxBytes = 0;
	}

	/**
	 * Validates a caller-supplied stream descriptor before it becomes routing authority.
	 * @param descriptor Descriptor to validate.
	 */
	private validateDescriptor(descriptor: DocumentStreamDescriptor): void {
		if (descriptor.requestId.length === 0) {
			throw new TypeError("Document stream requestId cannot be empty.");
		}
		if (descriptor.operationId.length === 0) {
			throw new TypeError("Document stream operationId cannot be empty.");
		}
		if (
			!Number.isSafeInteger(descriptor.operationEpoch) ||
			descriptor.operationEpoch < 0
		) {
			throw new TypeError(
				"Document stream operationEpoch must be a non-negative integer.",
			);
		}
		if (
			descriptor.completionMode !== undefined &&
			descriptor.completionMode !== "last" &&
			descriptor.completionMode !== "explicit"
		) {
			throw new TypeError(
				"Document stream completionMode must be last or explicit.",
			);
		}
	}

	/**
	 * Applies deduplication and ownership to one registered document chunk.
	 * @param context Registered stream receiving the chunk.
	 * @param message Document chunk to deliver or discard.
	 * @param rawByteLength Conservative size of the original WebSocket frame.
	 * @returns Delivery decision made for the chunk.
	 */
	private routeRegisteredChunk(
		context: RegisteredStreamContext,
		message: QuerySocketMessage,
		rawByteLength = 0,
	): QueryStreamRouteResult {
		if (this.isTerminal(context.state) || context.state === "flushing") {
			return "terminal";
		}

		const sequence = getDocumentChunkSequence(message);
		if (
			sequence !== undefined &&
			context.descriptor.deduplicateSequences !== false
		) {
			if (context.seenSequences.has(sequence)) return "duplicate";
			context.seenSequences.add(sequence);
		}

		if (context.state === "registered") {
			this.transition(context, "streaming");
		}

		let result: QueryStreamRouteResult;
		switch (context.descriptor.owner) {
			case "persist":
				this.callbacks.onDocumentChunkReady({
					descriptor: context.descriptor,
					message,
					sequence,
					rawByteLength: this.normalizeByteLength(rawByteLength),
				});
				result = "document-ready";
				break;
			case "passthrough":
				this.callbacks.onForwardMessage(message);
				result = "forwarded";
				break;
			case "ignore":
				result = "ignored";
				break;
		}

		if (
			message.payload?.last === true &&
			context.descriptor.owner !== "persist" &&
			(context.descriptor.completionMode ?? "last") === "last"
		) {
			this.complete(context.descriptor.requestId);
		}
		return result;
	}

	/**
	 * Converts caller-provided frame sizes into safe non-negative integers.
	 * @param byteLength Candidate raw frame size.
	 * @returns Normalized byte count suitable for bounded accounting.
	 */
	private normalizeByteLength(byteLength: number): number {
		return Number.isFinite(byteLength)
			? Math.max(0, Math.floor(byteLength))
			: 0;
	}

	/**
	 * Adds an early chunk, evicts oldest data over the byte ceiling, and announces new requests.
	 * @param requestId Unknown request identifier.
	 * @param message Document chunk awaiting registration.
	 * @param byteLength Conservative raw payload size.
	 */
	private bufferUnknownChunk(
		requestId: string,
		message: QuerySocketMessage,
		byteLength: number,
	): void {
		this.sweepExpired();
		const safeByteLength = this.normalizeByteLength(byteLength);
		if (safeByteLength > this.unknownMailboxMaxBytes) return;

		this.unknownChunks.push({
			requestId,
			message,
			byteLength: safeByteLength,
			expiresAt: this.now() + this.unknownMailboxTtlMs,
		});
		this.unknownMailboxBytes += safeByteLength;
		this.evictMailboxOverflow();

		if (
			this.unknownChunks.some((chunk) => chunk.requestId === requestId) &&
			!this.notifiedUnknownRequests.has(requestId)
		) {
			this.notifiedUnknownRequests.add(requestId);
			this.callbacks.onUnknownDocumentStream?.(requestId, message);
		}
	}

	/** Evicts oldest unknown chunks until the configured byte ceiling is satisfied. */
	private evictMailboxOverflow(): void {
		let removeCount = 0;
		while (
			this.unknownMailboxBytes > this.unknownMailboxMaxBytes &&
			removeCount < this.unknownChunks.length
		) {
			this.unknownMailboxBytes -= this.unknownChunks[removeCount].byteLength;
			removeCount += 1;
		}
		if (removeCount > 0) {
			this.unknownChunks.splice(0, removeCount);
			this.pruneUnknownNotifications();
		}
	}

	/**
	 * Removes and returns buffered chunks for one newly registered request.
	 * @param requestId Request whose early chunks should be replayed.
	 * @returns Matching chunks in original arrival order.
	 */
	private takeUnknownChunks(requestId: string): BufferedUnknownChunk[] {
		this.sweepExpired();
		const matching: BufferedUnknownChunk[] = [];
		const retained: BufferedUnknownChunk[] = [];
		for (const chunk of this.unknownChunks) {
			if (chunk.requestId === requestId) {
				matching.push(chunk);
				this.unknownMailboxBytes -= chunk.byteLength;
			} else {
				retained.push(chunk);
			}
		}
		this.unknownChunks = retained;
		this.notifiedUnknownRequests.delete(requestId);
		return matching;
	}

	/** Removes notification markers for requests no longer present in the mailbox. */
	private pruneUnknownNotifications(): void {
		const retainedRequestIds = new Set(
			this.unknownChunks.map((chunk) => chunk.requestId),
		);
		for (const requestId of this.notifiedUnknownRequests) {
			if (!retainedRequestIds.has(requestId)) {
				this.notifiedUnknownRequests.delete(requestId);
			}
		}
	}

	/**
	 * Publishes a stream state transition only when the state actually changes.
	 * @param context Stream context to update.
	 * @param state New lifecycle state.
	 * @param error Optional failure detail.
	 */
	private transition(
		context: RegisteredStreamContext,
		state: DocumentStreamState,
		error?: string,
	): void {
		if (context.state === state) return;
		context.state = state;
		this.publishState(context, error);
	}

	/**
	 * Emits the current state through the optional observer callback.
	 * @param context Stream context whose state changed.
	 * @param error Optional failure detail.
	 */
	private publishState(
		context: RegisteredStreamContext,
		error?: string,
	): void {
		this.callbacks.onStreamStateChange?.({
			descriptor: context.descriptor,
			state: context.state,
			error,
		});
	}

	/**
	 * Replaces a terminal request context with a compact bounded late-chunk guard.
	 * @param context Terminal context whose descriptor and sequence set can be released.
	 */
	private retireContext(context: RegisteredStreamContext): void {
		if (!this.isTerminal(context.state)) return;
		this.contexts.delete(context.descriptor.requestId);
		context.seenSequences.clear();
		this.addTerminalGuard(context.descriptor.requestId, context.state);
	}

	/**
	 * Adds one terminal guard and evicts the oldest guards above the configured cap.
	 * @param requestId Request whose late chunks must be rejected.
	 * @param state Terminal lifecycle state retained for observers.
	 */
	private addTerminalGuard(
		requestId: string,
		state: TerminalStreamGuard["state"],
	): void {
		if (this.terminalGuardMaxEntries === 0) return;
		this.sweepTerminalGuards();
		this.terminalGuards.delete(requestId);
		this.terminalGuards.set(requestId, {
			state,
			expiresAt: this.now() + this.terminalGuardTtlMs,
		});
		while (this.terminalGuards.size > this.terminalGuardMaxEntries) {
			const oldestRequestId = this.terminalGuards.keys().next().value as
				| string
				| undefined;
			if (oldestRequestId === undefined) break;
			this.terminalGuards.delete(oldestRequestId);
		}
	}

	/** Removes expired terminal guards without touching active stream contexts. */
	private sweepTerminalGuards(): void {
		const currentTime = this.now();
		for (const [requestId, guard] of this.terminalGuards) {
			if (guard.expiresAt <= currentTime) this.terminalGuards.delete(requestId);
		}
	}

	/**
	 * Returns whether a state must reject every subsequent document chunk.
	 * @param state Lifecycle state to inspect.
	 * @returns True for completed, canceled, and failed streams.
	 */
	private isTerminal(
		state: DocumentStreamState,
	): state is TerminalStreamGuard["state"] {
		return state === "completed" || state === "canceled" || state === "failed";
	}
}

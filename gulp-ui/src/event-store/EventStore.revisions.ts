import { createEventStoreDatabaseName } from "./IndexedDbEventStore.backend";
import type {
	EventSourceSummary,
	EventStoreNamespace,
} from "./EventStore.types";

/** Cross-realm invalidation payload containing no canonical document data. */
export interface EventStoreRevisionMessage {
	type: "source-summary";
	summary: EventSourceSummary;
}

interface BroadcastChannelLike {
	onmessage: ((event: MessageEvent<EventStoreRevisionMessage>) => void) | null;
	postMessage(message: EventStoreRevisionMessage): void;
	close(): void;
}

type BroadcastChannelFactory = (name: string) => BroadcastChannelLike;

/**
 * Publishes compact source revisions between main, detached, and worker realms.
 * Exact documents remain in their operation-scoped IndexedDB database.
 */
export class EventStoreRevisionBus {
	private readonly channel: BroadcastChannelLike | null;

	/**
	 * Opens the namespace channel when BroadcastChannel is supported.
	 * @param namespace Shared coordinator and operation namespace.
	 * @param onSummary Callback invoked for compact revisions from other realms.
	 * @param factory Injectable channel factory used by focused tests.
	 */
	public constructor(
		namespace: EventStoreNamespace,
		onSummary: (summary: EventSourceSummary) => void,
		factory: BroadcastChannelFactory | undefined =
			typeof BroadcastChannel === "undefined"
				? undefined
				: (name) => new BroadcastChannel(name),
	) {
		this.channel = factory
			? factory(`${createEventStoreDatabaseName(namespace)}:revisions`)
			: null;
		if (this.channel) {
			this.channel.onmessage = (event) => {
				if (event.data?.type === "source-summary") {
					onSummary(event.data.summary);
				}
			};
		}
	}

	/**
	 * Broadcasts one committed source summary without retaining it on the bus.
	 * @param summary Source revision committed in IndexedDB.
	 * @returns Nothing.
	 */
	public publish(summary: EventSourceSummary): void {
		this.channel?.postMessage({ type: "source-summary", summary });
	}

	/** Closes the realm-local channel and drops its callback. */
	public close(): void {
		if (!this.channel) return;
		this.channel.onmessage = null;
		this.channel.close();
	}
}

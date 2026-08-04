import type { EventStoreCapacity } from "./EventStore.types";

const GIBIBYTE = 1024 ** 3;
const MINIMUM_RESERVED_BYTES = GIBIBYTE;
const RESERVED_QUOTA_RATIO = 0.2;
const INITIAL_CONSERVATIVE_BYTES_PER_EVENT = 160;
const INDEXED_DB_OVERHEAD_FACTOR = 1.5;
export const EVENT_CAPACITY_RECHECK_EVENTS = 100_000;
export const EVENT_CAPACITY_RECHECK_BYTES = 256 * 1024 ** 2;
const EVENT_CAPACITY_SAMPLE_LIMIT = 256;

export interface EventStoreCapacityInput {
	quotaBytes: number;
	usageBytes: number;
	currentEventCount: number;
	encodedBytesPerEventSamples?: readonly number[];
}

/** One simulated checkpoint used by capacity diagnostics and benchmarks. */
export interface EventStoreCapacitySimulationPoint {
	additionalEvents: number;
	estimatedAdditionalBytes: number;
	fits: boolean;
}

/**
 * Returns the nearest-rank p95 from finite, positive byte samples.
 * @param samples Encoded byte-per-event measurements.
 * @returns The p95 sample, or null when no valid sample exists.
 */
export function getEventStoreByteP95(
	samples: readonly number[],
): number | null {
	const sorted = [...samples]
		.filter((sample) => Number.isFinite(sample) && sample > 0)
		.sort((left, right) => left - right);
	if (sorted.length === 0) return null;
	const index = Math.max(0, Math.ceil(sorted.length * 0.95) - 1);
	return sorted[index];
}

/**
 * Calculates the quota-derived event limit without promising a device-independent count.
 * @param input Current origin estimate, retained count, and optional measured encoding costs.
 * @returns Conservative capacity available to the event store.
 */
export function calculateEventStoreCapacity(
	input: EventStoreCapacityInput,
): EventStoreCapacity {
	const quotaBytes = Math.max(0, input.quotaBytes);
	const usageBytes = Math.min(quotaBytes, Math.max(0, input.usageBytes));
	const reserveBytes = Math.min(
		quotaBytes,
		Math.max(MINIMUM_RESERVED_BYTES, quotaBytes * RESERVED_QUOTA_RATIO),
	);
	const availableBytes = Math.max(0, quotaBytes - usageBytes - reserveBytes);
	const measuredP95 = getEventStoreByteP95(
		input.encodedBytesPerEventSamples ?? [],
	);
	const conservativeBytesPerEvent =
		measuredP95 === null
			? INITIAL_CONSERVATIVE_BYTES_PER_EVENT
			: Math.max(1, Math.ceil(measuredP95 * INDEXED_DB_OVERHEAD_FACTOR));
	const maximumAdditionalEvents = Math.max(
		0,
		Math.floor(availableBytes / conservativeBytesPerEvent),
	);
	const currentEventCount = Math.max(0, Math.floor(input.currentEventCount));

	return {
		quotaBytes,
		usageBytes,
		reserveBytes,
		availableBytes,
		conservativeBytesPerEvent,
		currentEventCount,
		maximumAdditionalEvents,
		effectiveMaximumEvents: currentEventCount + maximumAdditionalEvents,
	};
}

/**
 * Reads the browser origin estimate and converts it into an event-store capacity.
 * @param currentEventCount Events already retained by active operation databases.
 * @param encodedBytesPerEventSamples Optional rolling codec measurements.
 * @param storageManager Browser storage manager, injectable for tests.
 * @returns Runtime capacity when the browser exposes a usable quota estimate.
 * @throws Error when quota telemetry is unavailable; callers must not treat it as exhausted quota.
 */
export async function estimateEventStoreCapacity(
	currentEventCount: number,
	encodedBytesPerEventSamples: readonly number[] = [],
	storageManager: Pick<StorageManager, "estimate"> | undefined =
		globalThis.navigator?.storage,
): Promise<EventStoreCapacity> {
	if (!storageManager) {
		throw new Error("Browser storage quota telemetry is unavailable.");
	}
	let estimate: StorageEstimate;
	try {
		estimate = await storageManager.estimate();
	} catch (error) {
		const detail = error instanceof Error ? ` ${error.message}` : "";
		throw new Error(`Browser storage quota telemetry is unavailable.${detail}`);
	}
	const estimatedQuota = estimate?.quota;
	if (
		typeof estimatedQuota !== "number" ||
		!Number.isFinite(estimatedQuota) ||
		estimatedQuota <= 0
	) {
		throw new Error("Browser storage quota telemetry is unavailable.");
	}
	return calculateEventStoreCapacity({
		quotaBytes: estimatedQuota,
		usageBytes: estimate.usage ?? 0,
		currentEventCount,
		encodedBytesPerEventSamples,
	});
}

/**
 * Simulates candidate dataset growth against one immutable capacity estimate.
 * @param capacity Current quota-derived admission snapshot.
 * @param additionalEventCounts Candidate query sizes to evaluate.
 * @returns Ordered checkpoints with byte estimates and admission results.
 */
export function simulateEventStoreCapacity(
	capacity: EventStoreCapacity,
	additionalEventCounts: readonly number[],
): EventStoreCapacitySimulationPoint[] {
	return additionalEventCounts.map((candidate) => {
		const additionalEvents = Math.max(0, Math.floor(candidate));
		const estimatedAdditionalBytes =
			additionalEvents * capacity.conservativeBytesPerEvent;
		return {
			additionalEvents,
			estimatedAdditionalBytes,
			fits:
				additionalEvents <= capacity.maximumAdditionalEvents &&
				estimatedAdditionalBytes <= capacity.availableBytes,
		};
	});
}

/** Bounded write-cost sampler and quota recheck scheduler owned by QueryWorker. */
export class EventStoreCapacityTracker {
	private readonly samples: number[] = [];
	private eventsSinceEstimate = 0;
	private bytesSinceEstimate = 0;
	private hasEstimate = false;

	/** Returns a copy of the bounded byte-per-event samples used for p95. */
	public get bytePerEventSamples(): readonly number[] {
		return [...this.samples];
	}

	/**
	 * Records one committed IndexedDB write without retaining document data.
	 * @param encodedBytes Bytes stored for page, LOD, manifest, and ID index.
	 * @param eventCount Active events represented by the committed write.
	 * @returns Nothing.
	 */
	public recordCommittedWrite(encodedBytes: number, eventCount: number): void {
		const safeBytes = Math.max(0, encodedBytes);
		const safeEvents = Math.max(0, Math.floor(eventCount));
		if (safeEvents > 0 && safeBytes > 0 && Number.isFinite(safeBytes)) {
			this.samples.push(safeBytes / safeEvents);
			if (this.samples.length > EVENT_CAPACITY_SAMPLE_LIMIT) {
				this.samples.splice(0, this.samples.length - EVENT_CAPACITY_SAMPLE_LIMIT);
			}
		}
		this.eventsSinceEstimate += safeEvents;
		this.bytesSinceEstimate += safeBytes;
	}

	/**
	 * Returns whether quota must be measured before accepting more stream data.
	 * Callers still measure unconditionally before starting every query.
	 * @returns True before the first estimate or after either write threshold.
	 */
	public shouldReestimate(): boolean {
		return (
			!this.hasEstimate ||
			this.eventsSinceEstimate >= EVENT_CAPACITY_RECHECK_EVENTS ||
			this.bytesSinceEstimate >= EVENT_CAPACITY_RECHECK_BYTES
		);
	}

	/** Marks a successful quota estimate and restarts both write thresholds. */
	public markEstimated(): void {
		this.hasEstimate = true;
		this.eventsSinceEstimate = 0;
		this.bytesSinceEstimate = 0;
	}

	/**
	 * Measures quota with the current p95 samples and resets threshold counters.
	 * @param currentEventCount Events already present across active databases.
	 * @param storageManager Browser storage manager, injectable for worker tests.
	 * @returns Current conservative admission snapshot.
	 */
	public async estimate(
		currentEventCount: number,
		storageManager?: Pick<StorageManager, "estimate">,
	): Promise<EventStoreCapacity> {
		const capacity = await estimateEventStoreCapacity(
			currentEventCount,
			this.samples,
			storageManager,
		);
		this.markEstimated();
		return capacity;
	}
}

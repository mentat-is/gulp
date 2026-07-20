export interface WorkerSourceSnapshotStatus {
	streamMissingEvents: boolean;
	wasKnownSource: boolean;
	graphSignature: string;
	previousGraphSignature?: string;
	sourceRevision: number;
	acknowledgedSourceRevision: number;
}

/**
 * Decides whether worker source synchronization must include an event snapshot.
 * @param status Current source membership, graph, and worker acknowledgement state.
 * @returns True when the worker needs an authoritative source event replacement.
 */
export function shouldStreamWorkerSourceSnapshot(
	status: WorkerSourceSnapshotStatus,
): boolean {
	if (!status.streamMissingEvents) return false;
	if (!status.wasKnownSource) return true;
	if (status.previousGraphSignature !== status.graphSignature) return true;

	return status.sourceRevision > status.acknowledgedSourceRevision;
}

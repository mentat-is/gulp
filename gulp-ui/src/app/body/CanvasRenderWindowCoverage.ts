import type { Source } from "@/entities/Source";
import type { EventSourceSummary } from "@/event-store/EventStore.types";
import type { QueryRenderWindowDescriptor } from "@/event-stream";
import type {
	RenderTimeRange,
	RenderWindowProjection,
} from "@/render-worker/RenderWorker.types";

interface CanvasCoveredSource {
	sourceId: Source.Id;
	offset: number;
	engine: QueryRenderWindowDescriptor["sources"][number]["engine"];
	frequency: number;
	settingsRevision: number;
	overrideRevision: number;
	coveredRange: RenderTimeRange;
	sourceAuthorityRevision: number;
	sourceGeneration: number;
	sourceRevision: number;
	sourceStatus: "building" | "complete" | "incomplete";
	loadedCount: number;
}

/** Scalar projection metadata retained after typed arrays move to RenderWorker. */
export interface CanvasRenderWindowCoverage {
	operationEpoch: number;
	frame: RenderTimeRange;
	width: number;
	scale: number;
	horizontalOverscanPixels: number;
	maximumRepresentatives: number | undefined;
	sources: ReadonlyMap<Source.Id, CanvasCoveredSource>;
}

/**
 * Captures only the scalar metadata required to reuse an accepted overscanned window.
 * @param descriptor QueryWorker request that produced the projection.
 * @param projection Accepted projection whose typed arrays are about to be transferred.
 * @returns Coverage snapshot, or null when request and response source sets differ.
 */
export function createCanvasRenderWindowCoverage(
	descriptor: QueryRenderWindowDescriptor,
	projection: RenderWindowProjection,
): CanvasRenderWindowCoverage | null {
	if (
		descriptor.operationEpoch !== projection.operationEpoch ||
		descriptor.viewportRevision !== projection.viewportRevision ||
		descriptor.projectionRevision !== projection.projectionRevision ||
		descriptor.sources.length !== projection.sources.length
	) {
		return null;
	}
	const projectionBySource = new Map(
		projection.sources.map((source) => [source.sourceId, source]),
	);
	const sources = new Map<Source.Id, CanvasCoveredSource>();
	for (const source of descriptor.sources) {
		const projected = projectionBySource.get(source.sourceId);
		if (!projected) return null;
		sources.set(source.sourceId, {
			sourceId: source.sourceId,
			offset: source.offset,
			engine: source.engine,
			frequency: source.frequency,
			settingsRevision: source.settingsRevision,
			overrideRevision: source.overrideRevision,
			coveredRange: { ...projected.coveredRange },
			sourceAuthorityRevision: projected.sourceAuthorityRevision,
			sourceGeneration: projected.sourceGeneration,
			sourceRevision: projected.sourceRevision,
			sourceStatus: projected.sourceStatus,
			loadedCount: projected.loadedCount,
		});
	}
	return {
		operationEpoch: descriptor.operationEpoch,
		frame: { ...descriptor.frame },
		width: descriptor.width,
		scale: descriptor.scale,
		horizontalOverscanPixels: descriptor.horizontalOverscanPixels,
		maximumRepresentatives: descriptor.maxRepresentatives,
		sources,
	};
}

/**
 * Checks whether the visible raw timestamp interval remains inside one source's
 * accepted horizontal overscan.
 */
function isVisibleRangeCovered(
	descriptor: QueryRenderWindowDescriptor,
	source: QueryRenderWindowDescriptor["sources"][number],
	coveredRange: RenderTimeRange,
): boolean {
	const frameRange = descriptor.frame.max - descriptor.frame.min;
	const virtualWidth = descriptor.width * descriptor.scale;
	if (!Number.isFinite(frameRange) || frameRange <= 0 || virtualWidth <= 0) {
		return false;
	}
	const first =
		descriptor.frame.min +
		(descriptor.scrollX / virtualWidth) * frameRange -
		source.offset;
	const last =
		descriptor.frame.min +
		((descriptor.scrollX + descriptor.width) / virtualWidth) * frameRange -
		source.offset;
	const frameMinimum = descriptor.frame.min - source.offset;
	const frameMaximum = descriptor.frame.max - source.offset;
	const visibleMinimum = Math.min(
		frameMaximum,
		Math.max(frameMinimum, Math.min(first, last)),
	);
	const visibleMaximum = Math.max(
		frameMinimum,
		Math.min(frameMaximum, Math.max(first, last)),
	);
	return (
		visibleMinimum >= coveredRange.min && visibleMaximum <= coveredRange.max
	);
}

/**
 * Determines whether RenderWorker can redraw a new pan from its current transferred
 * projection without requesting or cloning EventStore pages again.
 * @param coverage Last accepted scalar projection coverage.
 * @param descriptor Candidate request for the current viewport.
 * @param getSourceSummary Resolver for current durable source authority.
 * @returns True only for an unchanged complete source set inside horizontal overscan.
 */
export function canReuseCanvasRenderWindow(
	coverage: CanvasRenderWindowCoverage | null,
	descriptor: QueryRenderWindowDescriptor,
	getSourceSummary: (sourceId: Source.Id) => EventSourceSummary | undefined,
): boolean {
	if (
		!coverage ||
		coverage.operationEpoch !== descriptor.operationEpoch ||
		coverage.frame.min !== descriptor.frame.min ||
		coverage.frame.max !== descriptor.frame.max ||
		coverage.width !== descriptor.width ||
		coverage.scale !== descriptor.scale ||
		coverage.horizontalOverscanPixels !== descriptor.horizontalOverscanPixels ||
		coverage.maximumRepresentatives !== descriptor.maxRepresentatives ||
		coverage.sources.size !== descriptor.sources.length
	) {
		return false;
	}
	for (const source of descriptor.sources) {
		const covered = coverage.sources.get(source.sourceId);
		const summary = getSourceSummary(source.sourceId);
		if (
			!covered ||
			!summary ||
			covered.sourceStatus !== "complete" ||
			summary.status !== "complete" ||
			covered.offset !== source.offset ||
			covered.engine !== source.engine ||
			covered.frequency !== source.frequency ||
			covered.settingsRevision !== source.settingsRevision ||
			covered.overrideRevision !== source.overrideRevision ||
			covered.sourceAuthorityRevision !== summary.authorityRevision ||
			covered.sourceGeneration !== summary.generation ||
			covered.sourceRevision !== summary.revision ||
			covered.loadedCount !== summary.eventCount ||
			!isVisibleRangeCovered(descriptor, source, covered.coveredRange)
		) {
			return false;
		}
	}
	return true;
}

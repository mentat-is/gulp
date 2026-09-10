import { Application } from "@/context/Application.context";
import { scrollStore } from "@/store/scroll.store";
import { Algorhithm } from "@/class/TimelineAlgorithm";
import { Source } from "@/entities/Source";
import { DataStore } from "@/store/DataStore";
import { Navigator } from "./Navigator";
import { Stack } from "@/ui/Stack";
import s from "../Gulp.module.css";
import { memo, useEffect, useMemo, useRef, useSyncExternalStore } from "react";
import { Canvas } from "./Canvas";
import { MINUTE } from "@/dto";
import { debounce, DebouncedFunc } from "lodash";
import { Context } from "@/entities/Context";
import {
	cleanupTimelineAutosave,
	runTimelineAutosaveWhenIdle,
	scheduleTimelineAutosave,
	TIMELINE_AUTOSAVE_INTERACTION_QUIET_MS,
} from "./TimelineAutosave";
import { serializeWorkspaceSessionValue } from "@/class/WorkspaceSessionRepository";

export namespace Timeline {
	export interface Props {
		/** Prevents scheduling or flushing authenticated session autosave requests. */
		disableSessionAutosave?: boolean;
		/** Sizes the Canvas world from the nearest stable CSS size container. */
		stableCanvasWidth?: boolean;
	}
}

/** Canvas boundary retained across scale-store-only Timeline updates. */
const TimelineCanvas = memo(Canvas);
/** Navigator boundary retained across scale-store-only Timeline updates. */
const TimelineNavigator = memo(Navigator);

/**
 * Renders the timeline Canvas and controls without subscribing to live scroll updates.
 * @param props Timeline behavior options.
 * @returns Timeline view.
 */
export function Timeline({
	disableSessionAutosave = false,
	stableCanvasWidth = false,
}: Timeline.Props = {}) {
	const { app, Info, timeline } = Application.use();
	const timelineScale = useSyncExternalStore(
		Info.subscribeTimelineScale,
		Info.getTimelineScaleSnapshot,
		Info.getTimelineScaleSnapshot,
	);

	/**
	 * Focuses the timeline on a timestamp and optionally reveals a source row.
	 * @param timestamp Timeline timestamp to focus.
	 * @param onLeft Whether to align the timestamp to the left viewport edge.
	 * @param file_id Optional source identifier whose row must be revealed and centered.
	 * @returns Nothing.
	 */
	const focusEvent = (
		timestamp: number,
		onLeft = false,
		file_id?: Source.Id,
	): void => {
		const instance = getAlgothitmInstance();

		scrollStore.setScrollX(
			onLeft
				? instance.abs_x_from_timestamp(timestamp)
				: instance.center_scroll_from_timestamp(timestamp),
		);
		if (file_id) {
			const canvas = document.getElementById("canvas") as HTMLCanvasElement;
			if (!canvas) return;

			const target = app.target.files.find((f) => f.id === file_id);
			if (!target) return;

			// Reveal the source if it's currently unselected or excluded by the
			// sourcesWithNoEvents toggle, so focusing always lands on a visible row.
			const loadedCount =
				DataStore.getEventSourceSummary(file_id)?.eventCount ?? 0;
			const needsReveal = !target.selected || (target.total ?? 0) === 0;
			const files = needsReveal
				? app.target.files.map((f) =>
						f.id === file_id
							? {
									...f,
									selected: true,
									total: Math.max(f.total ?? 0, loadedCount, 1),
								}
							: f,
					)
				: app.target.files;

			if (needsReveal) Info.setInfoByKey(files, "target", "files");

			// Compute the row index against the same list the renderer uses so the
			// Y centering matches the actual rendered position.
			const synthApp = { ...app, target: { ...app.target, files } };
			const visible = Source.Entity.rendered(synthApp);
			const index = visible.findIndex((s) => s.id === file_id);
			if (index === -1) return;

			scrollStore.setScrollY(
				Source.Entity.getHeight(app, file_id, 0, index) -
					canvas.clientHeight / 2,
			);
		}
	};

	/**
	 * Ref that always holds the latest app state so the stable debounced
	 * callback can read fresh data without being recreated on every render.
	 */
	const appRef = useRef(app);
	appRef.current = app;
	const disableSessionAutosaveRef = useRef(disableSessionAutosave);
	disableSessionAutosaveRef.current = disableSessionAutosave;
	const persistedFingerprintsRef = useRef(new Map<string, string>());
	const autosaveInFlightRef = useRef(false);
	const autosaveRerunRequestedRef = useRef(false);
	const autosavePreparationPendingRef = useRef(false);
	const cancelAutosavePreparationRef = useRef<(() => void) | null>(null);
	const lastViewportInteractionAtRef = useRef(Number.NEGATIVE_INFINITY);
	const previousTimelineScaleRef = useRef(timelineScale);
	const debouncedSessionAutosaveRef = useRef<DebouncedFunc<() => void> | null>(
		null,
	);
	const selectedOperationId = app.target.operations.find(
		(operation) => operation.selected,
	)?.id;

	useEffect(() => {
		debouncedSessionAutosaveRef.current?.cancel();
		cancelAutosavePreparationRef.current?.();
		cancelAutosavePreparationRef.current = null;
		autosavePreparationPendingRef.current = false;
		autosaveRerunRequestedRef.current = false;
		lastViewportInteractionAtRef.current = Number.NEGATIVE_INFINITY;
		const initialSnapshot = Info.session_snapshot();
		if (selectedOperationId && initialSnapshot) {
			persistedFingerprintsRef.current.set(
				selectedOperationId,
				serializeWorkspaceSessionValue(initialSnapshot),
			);
		}
	}, [Info, selectedOperationId]);

	/**
	 * Checks whether the current app state contains persistable session data.
	 * @returns True when at least one source and one context are selected.
	 */
	const hasSessionData = (): boolean => {
		const currentApp = appRef.current;
		return (
			Source.Entity.selected(currentApp).length > 0 &&
			Context.Entity.selected(currentApp).length > 0
		);
	};

	const debouncedSessionAutosave: DebouncedFunc<() => void> = useMemo(() => {
		const persistLatestSnapshot = (): void => {
			if (autosaveInFlightRef.current) {
				autosaveRerunRequestedRef.current = true;
				return;
			}
			if (autosavePreparationPendingRef.current) return;

			autosavePreparationPendingRef.current = true;
			const cancelPreparation = runTimelineAutosaveWhenIdle(
				() => {
					autosavePreparationPendingRef.current = false;
					cancelAutosavePreparationRef.current = null;
					const snapshot = Info.session_snapshot(selectedOperationId);
					if (
						!snapshot ||
						snapshot.selected.files.length === 0 ||
						snapshot.selected.contexts.length === 0
					) {
						return;
					}
					const fingerprint = serializeWorkspaceSessionValue(snapshot);
					if (
						selectedOperationId &&
						persistedFingerprintsRef.current.get(selectedOperationId) ===
							fingerprint
					) {
						return;
					}

					autosaveInFlightRef.current = true;
					void Info.session_autosave(snapshot, selectedOperationId)
						.then((persisted) => {
							if (persisted && selectedOperationId) {
								persistedFingerprintsRef.current.set(
									selectedOperationId,
									fingerprint,
								);
							}
						})
						.finally(() => {
							autosaveInFlightRef.current = false;
							if (autosaveRerunRequestedRef.current) {
								autosaveRerunRequestedRef.current = false;
								debouncedSessionAutosaveRef.current?.();
							}
						});
				},
				() =>
					Date.now() - lastViewportInteractionAtRef.current <
					TIMELINE_AUTOSAVE_INTERACTION_QUIET_MS,
			);
			cancelAutosavePreparationRef.current =
				autosavePreparationPendingRef.current ? cancelPreparation : null;
		};

		return debounce(
			() => {
				persistLatestSnapshot();
			},
			10000,
			{ maxWait: 60000 },
		);
	}, [Info, selectedOperationId]);
	debouncedSessionAutosaveRef.current = debouncedSessionAutosave;

	useEffect(() => {
		if (previousTimelineScaleRef.current !== timelineScale) {
			lastViewportInteractionAtRef.current = Date.now();
			previousTimelineScaleRef.current = timelineScale;
		}
		if (disableSessionAutosave) {
			cancelAutosavePreparationRef.current?.();
			cancelAutosavePreparationRef.current = null;
			autosavePreparationPendingRef.current = false;
		}
		scheduleTimelineAutosave(
			debouncedSessionAutosave,
			disableSessionAutosave,
		);
	}, [
		timelineScale,
		app.timeline.frame.max,
		app.timeline.frame.min,
		app.timeline.filter,
		app.timeline.target,
		app.target.files,
		app.target.contexts,
		app.target.operations,
		app.hidden,
		debouncedSessionAutosave,
		disableSessionAutosave,
	]);

	useEffect(() => {
		if (disableSessionAutosave) return;
		return scrollStore.subscribe(() => {
			lastViewportInteractionAtRef.current = Date.now();
			debouncedSessionAutosave();
		});
	}, [debouncedSessionAutosave, disableSessionAutosave]);

	useEffect(() => {
		return () => {
			cleanupTimelineAutosave(
				debouncedSessionAutosave,
				disableSessionAutosaveRef.current,
				hasSessionData(),
			);
		};
	}, [debouncedSessionAutosave]);

	/**
	 * Creates a timestamp conversion helper from the current external scroll state.
	 * @returns Algorithm configured with the latest timeline viewport.
	 */
	const getAlgothitmInstance = () => {
		return new Algorhithm({
			frame: app.timeline.frame,
			scroll: {
				x: scrollStore.getX(),
				y: scrollStore.getY(),
			},
			width: Info.width,
			scale: timelineScale,
			viewportWidth: timeline.current?.clientWidth,
		});
	};

	// @ts-ignore
	window.focusCanvasOnEvent = focusEvent;

	return (
		<Stack
			id="timeline"
			className={s.timeline}
			gap={12}
			flex
			dir="column"
			ref={timeline}
		>
			<TimelineCanvas
				timeline={timeline}
				stableWidth={stableCanvasWidth}
			/>
			<TimelineNavigator timeline={timeline} />
		</Stack>
	);
}

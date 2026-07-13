import { Application } from "@/context/Application.context";
import { useScroll, scrollStore } from "@/store/scroll.store";
import { Algorhithm, getTimestamp } from "@/ui/utils";
import { Source } from "@/entities/Source";
import { Doc } from "@/entities/Doc";
import { Navigator } from "./Navigator";
import { Stack } from "@/ui/Stack";
import s from "../Gulp.module.css";
import { useEffect, useMemo, useRef } from "react";
import { Canvas } from "./Canvas";
import { MINUTE } from "@/dto";
import { debounce, DebouncedFunc } from "lodash";
import { Context } from "@/entities/Context";

export function Timeline() {
	const { app, Info, timeline } = Application.use();
	const { x: scrollX, y: scrollY } = useScroll();

	const focusEvent = (
		timestamp: number,
		onLeft = false,
		file_id?: Source.Id,
	) => {
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
			const loadedCount = Doc.Entity.get(app, file_id).length;
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
			// Y centering matches the actual rendered position. Reuse Source.Entity.selected
			// (which applies pin order and the timeline search filter) on a synthetic
			// app carrying the just-updated files.
			const synthApp = { ...app, target: { ...app.target, files } };
			const visible = Source.Entity.selected(synthApp);
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

	/**
	 * Returns true when the current app state contains meaningful session
	 * data (at least one selected source AND one selected context).
	 * Used as a guard to prevent autosave during the initial loading phase
	 * — e.g. while the SelectFiles banner is still open and the user has
	 * not yet picked files/contexts or loaded a saved session.
	 */
	const hasSessionData = (): boolean => {
		const currentApp = appRef.current;
		return (
			Source.Entity.selected(currentApp).length > 0 &&
			Context.Entity.selected(currentApp).length > 0
		);
	};

	const debouceSessionAutoSave: DebouncedFunc<() => void> = useMemo(() => {
		return debounce(() => {
			if (!hasSessionData()) {
				console.warn("session auto-save skipped: no sources/contexts selected");
				return;
			}
			console.warn("start session auto-save");
			Info.session_autosave();
		}, 10000);
	}, [Info]);

	useEffect(() => {
		debouceSessionAutoSave();
	}, [
		app.timeline.scale,
		app.timeline.frame.max,
		app.timeline.frame.min,
		app.timeline.filter,
		app.timeline.target,
		app.target.files,
		app.target.filters,
		app.hidden,
		debouceSessionAutoSave,
		Source.Entity.selected(app),
		Context.Entity.selected(app),
	]);

	useEffect(() => {
		return () => {
			if (hasSessionData()) {
				debouceSessionAutoSave.flush();
			} else {
				debouceSessionAutoSave.cancel();
			}
		};
	}, [debouceSessionAutoSave]);

	const getAlgothitmInstance = () => {
		return new Algorhithm({
			frame: app.timeline.frame,
			scroll: {
				x: scrollX,
				y: scrollY,
			},
			width: Info.width,
			scale: app.timeline.scale,
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
			<Canvas timeline={timeline} />
			<Navigator
				timeline={timeline}
				timestamp={getTimestamp(
					scrollX + (timeline.current?.clientWidth || 0),
					Info,
				)}
			/>
		</Stack>
	);
}

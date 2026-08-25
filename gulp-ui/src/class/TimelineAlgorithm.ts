export interface TimelineAlgorithmFrame {
	min: number;
	max: number;
}

export interface TimelineAlgorithmPoint {
	x: number;
	y: number;
}

export namespace Algorhithm {
	export interface Constructor {
		frame: TimelineAlgorithmFrame;
		scroll: TimelineAlgorithmPoint;
		width: number;
		scale: number;
		/** Visible clipping width used for centering commands. */
		viewportWidth?: number;
	}
}

/** Converts between timestamps and document-space timeline coordinates. */
export class Algorhithm implements Algorhithm.Constructor {
	frame: TimelineAlgorithmFrame = { min: 0, max: 0 };
	scroll: TimelineAlgorithmPoint = { x: 0, y: 0 };
	width = 1;
	scale = 1;
	viewportWidth?: number;

	/**
	 * Creates a timeline projection helper.
	 * @param constructor Frame, scroll, world width, scale, and optional viewport width.
	 */
	constructor(constructor: Algorhithm.Constructor) {
		Object.assign(this, constructor);
	}

	/**
	 * Projects a timestamp into absolute document-space pixels.
	 * @param timestamp Timeline timestamp to project.
	 * @returns Rounded document-space X coordinate.
	 */
	abs_x_from_timestamp = (timestamp: number): number =>
		Math.round(
			((timestamp - this.frame.min) / (this.frame.max - this.frame.min)) *
				this.width,
		);

	/**
	 * Projects a timestamp into the current scroll-relative coordinate system.
	 * @param timestamp Timeline timestamp to project.
	 * @param scroll Scroll position to subtract from document space.
	 * @returns Scroll-relative X coordinate.
	 */
	rel_x_from_timestamp = (
		timestamp: number,
		scroll: TimelineAlgorithmPoint = this.scroll,
	): number => (scroll ? this.abs_x_from_timestamp(timestamp) - scroll.x : -1);

	/**
	 * Centers a timestamp in the visible viewport without changing world geometry.
	 * @param timestamp Timeline timestamp to center.
	 * @returns Horizontal document-space scroll offset.
	 */
	center_scroll_from_timestamp = (timestamp: number): number =>
		Math.round(
			this.abs_x_from_timestamp(timestamp) -
				(this.viewportWidth ?? this.width / this.scale) / 2,
		);

	/**
	 * Resolves the timestamp beneath a scroll-relative pixel.
	 * @param relX Scroll-relative X coordinate.
	 * @param scroll Scroll position to add before projection.
	 * @returns Rounded timeline timestamp.
	 */
	timestamp_from_rel_x = (
		relX: number,
		scroll: TimelineAlgorithmPoint = this.scroll,
	): number => {
		const absX = relX + scroll.x;
		const ratio = absX / this.width;
		return Math.round(
			this.frame.min + ratio * (this.frame.max - this.frame.min),
		);
	};
}

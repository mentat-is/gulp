import type { XY } from "@/dto/XY.dto";

/** Complete display-tick state required by the imperative local pointer. */
export interface LocalPointerSnapshot extends XY {
	timestamp: number;
	isRightSide: boolean;
}

/**
 * Applies one local pointer snapshot directly to existing DOM nodes.
 * @param pointerElement Positioning element updated through a compositor transform.
 * @param labelElement Pointer label whose text changes with the timestamp.
 * @param snapshot Latest display-tick pointer snapshot.
 * @param label Localized label corresponding to the snapshot timestamp.
 * @param rightClassName Class used to orient pointers on the right half.
 * @returns Nothing.
 */
export function applyLocalPointerSnapshot(
	pointerElement: HTMLDivElement,
	labelElement: HTMLParagraphElement,
	snapshot: LocalPointerSnapshot,
	label: string,
	rightClassName: string,
): void {
	pointerElement.style.transform = `translate3d(${snapshot.x}px, ${snapshot.y}px, 0)`;
	if (rightClassName) {
		pointerElement.classList.toggle(rightClassName, snapshot.isRightSide);
	}
	if (labelElement.textContent !== label) labelElement.textContent = label;
}

/**
 * Applies horizontal timeline scroll to the timestamp-based remote pointer layer.
 * @param layerElement Remote pointer layer that follows timeline X scroll.
 * @param scrollX Current horizontal scroll position.
 * @returns Nothing.
 */
export function applyRemotePointerScrollX(
	layerElement: HTMLDivElement,
	scrollX: number,
): void {
	layerElement.style.setProperty("--remote-pointer-scroll-x", `${-scrollX}px`);
}

/**
 * Updates timestamp-derived remote pointer direction after an imperative X scroll.
 * @param layerElement Layer containing pointers with document-X data attributes.
 * @param scrollX Current horizontal timeline scroll.
 * @param viewportWidth Current pointer viewport width.
 * @param rightClassName Class that mirrors pointers displayed on the right half.
 * @returns Nothing.
 */
export function applyRemotePointerOrientations(
	layerElement: HTMLDivElement,
	scrollX: number,
	viewportWidth: number,
	rightClassName: string,
): void {
	if (!rightClassName) return;
	const midpoint = Math.max(1, viewportWidth) / 2;
	const pointerElements = layerElement.querySelectorAll<HTMLElement>(
		"[data-remote-pointer-document-x]",
	);
	for (const pointerElement of pointerElements) {
		const documentX = Number(pointerElement.dataset.remotePointerDocumentX);
		if (!Number.isFinite(documentX)) continue;
		pointerElement.classList.toggle(
			rightClassName,
			documentX - scrollX > midpoint,
		);
	}
}

/**
 * Applies vertical timeline scroll to the shared remote pointer layer.
 * @param layerElement Remote pointer layer that follows timeline Y scroll.
 * @param scrollY Current vertical scroll position.
 * @returns Nothing.
 */
export function applyRemotePointerScrollY(
	layerElement: HTMLDivElement,
	scrollY: number,
): void {
	layerElement.style.setProperty("--remote-pointer-scroll-y", `${-scrollY}px`);
}

/** Horizontal placement metadata for a collaborative pointer. */
export interface RemotePointerHorizontalPosition {
	positionX: number;
	viewportX: number;
	followsTimelineScroll: boolean;
}

/**
 * Resolves explicit viewport X values separately from timestamp-based document positions.
 * @param explicitX Optional remote viewport coordinate, including the valid value zero.
 * @param timestamp Remote pointer timestamp.
 * @param getPixelPosition Converts a timestamp into the current viewport coordinate.
 * @param scrollX Current horizontal timeline scroll.
 * @returns Pointer position and whether the timeline layer must scroll it.
 */
export function resolveRemotePointerHorizontalPosition(
	explicitX: number | undefined,
	timestamp: number,
	getPixelPosition: (timestamp: number) => number,
	scrollX: number,
): RemotePointerHorizontalPosition {
	if (explicitX !== undefined) {
		return {
			positionX: explicitX,
			viewportX: explicitX,
			followsTimelineScroll: false,
		};
	}

	const viewportX = getPixelPosition(timestamp);
	return {
		positionX: viewportX + scrollX,
		viewportX,
		followsTimelineScroll: true,
	};
}

/** Isolated external store for the local Canvas pointer position. */
export class PointerPositionStore {
	private readonly listeners = new Set<() => void>();
	private position: LocalPointerSnapshot = {
		x: 0,
		y: 0,
		timestamp: 0,
		isRightSide: false,
	};

	/**
	 * Subscribes one pointer-only React consumer.
	 * @param listener Callback invoked after a display-tick position update.
	 * @returns Function that removes the listener.
	 */
	public subscribe = (listener: () => void): (() => void) => {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	};

	/**
	 * Returns the stable current pointer snapshot for imperative consumers.
	 * @returns Current Canvas-relative pointer coordinates and display metadata.
	 */
	public getSnapshot = (): LocalPointerSnapshot => this.position;

	/**
	 * Publishes one coalesced pointer snapshot without rerendering Canvas.
	 * @param position Canvas-relative pointer coordinates and display metadata.
	 * @returns Nothing.
	 */
	public setPosition(position: LocalPointerSnapshot): void {
		if (
			position.x === this.position.x &&
			position.y === this.position.y &&
			position.timestamp === this.position.timestamp &&
			position.isRightSide === this.position.isRightSide
		) {
			return;
		}
		this.position = position;
		this.listeners.forEach((listener) => listener());
	}
}

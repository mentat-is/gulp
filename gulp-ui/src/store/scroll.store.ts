import { useSyncExternalStore } from "react";

type FunctionalUpdate = (previousValue: number) => number;

interface ScrollSnapshot {
	x: number;
	y: number;
}

/** External timeline scroll store with independent subscriptions for each axis. */
export class ScrollStore {
	private x = 0;
	private y = -26;
	private readonly listeners = new Set<() => void>();
	private readonly xListeners = new Set<() => void>();
	private readonly yListeners = new Set<() => void>();
	private snapshot: ScrollSnapshot = { x: 0, y: -26 };
	private readonly serverSnapshot: ScrollSnapshot = { x: 0, y: -26 };

	/**
	 * Subscribes to changes on either scroll axis.
	 * @param listener Callback invoked after either axis changes.
	 * @returns Function that removes the listener.
	 */
	public subscribe = (listener: () => void): (() => void) => {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	};

	/**
	 * Subscribes only to horizontal scroll changes.
	 * @param listener Callback invoked after the X axis changes.
	 * @returns Function that removes the listener.
	 */
	public subscribeX = (listener: () => void): (() => void) => {
		this.xListeners.add(listener);
		return () => this.xListeners.delete(listener);
	};

	/**
	 * Subscribes only to vertical scroll changes.
	 * @param listener Callback invoked after the Y axis changes.
	 * @returns Function that removes the listener.
	 */
	public subscribeY = (listener: () => void): (() => void) => {
		this.yListeners.add(listener);
		return () => this.yListeners.delete(listener);
	};

	/**
	 * Returns the stable combined client snapshot.
	 * @returns Current client-side scroll snapshot.
	 */
	public getSnapshot = (): ScrollSnapshot => this.snapshot;

	/**
	 * Returns the stable combined server snapshot.
	 * @returns Initial server-side scroll snapshot.
	 */
	public getServerSnapshot = (): ScrollSnapshot => this.serverSnapshot;

	/**
	 * Returns the current horizontal scroll position.
	 * @returns Current X-axis position.
	 */
	public getX = (): number => this.x;

	/**
	 * Returns the current vertical scroll position.
	 * @returns Current Y-axis position.
	 */
	public getY = (): number => this.y;

	/**
	 * Returns the initial horizontal server snapshot.
	 * @returns Initial server-side X-axis position.
	 */
	public getServerX = (): number => this.serverSnapshot.x;

	/**
	 * Returns the initial vertical server snapshot.
	 * @returns Initial server-side Y-axis position.
	 */
	public getServerY = (): number => this.serverSnapshot.y;

	/**
	 * Updates the horizontal scroll position and notifies X consumers only.
	 * @param x Next value or updater based on the current X position.
	 * @returns Nothing.
	 */
	public setScrollX = (x: number | FunctionalUpdate): void => {
		const nextX = typeof x === "function" ? x(this.x) : x;
		if (this.x === nextX) return;
		this.x = nextX;
		this.emitChange(true, false);
	};

	/**
	 * Updates the vertical scroll position and notifies Y consumers only.
	 * @param y Next value or updater based on the current Y position.
	 * @returns Nothing.
	 */
	public setScrollY = (y: number | FunctionalUpdate): void => {
		const nextY = typeof y === "function" ? y(this.y) : y;
		if (this.y === nextY) return;
		this.y = nextY;
		this.emitChange(false, true);
	};

	/**
	 * Updates both axes atomically and notifies only the axes that changed.
	 * @param x Next horizontal scroll position.
	 * @param y Next vertical scroll position.
	 * @returns Nothing.
	 */
	public setScroll = (x: number, y: number): void => {
		const xChanged = this.x !== x;
		const yChanged = this.y !== y;
		if (!xChanged && !yChanged) return;

		this.x = x;
		this.y = y;
		this.emitChange(xChanged, yChanged);
	};

	/**
	 * Publishes one combined snapshot and dispatches listeners by changed axis.
	 * @param xChanged Whether the horizontal value changed.
	 * @param yChanged Whether the vertical value changed.
	 * @returns Nothing.
	 */
	private emitChange(xChanged: boolean, yChanged: boolean): void {
		this.snapshot = { x: this.x, y: this.y };
		this.listeners.forEach((listener) => listener());
		if (xChanged) this.xListeners.forEach((listener) => listener());
		if (yChanged) this.yListeners.forEach((listener) => listener());
	}
}

export const scrollStore = new ScrollStore();

/**
 * Subscribes a React consumer to both scroll axes.
 * @returns Current horizontal and vertical scroll positions.
 */
export function useScroll(): ScrollSnapshot {
	return useSyncExternalStore(
		scrollStore.subscribe,
		scrollStore.getSnapshot,
		scrollStore.getServerSnapshot,
	);
}

/**
 * Subscribes a React consumer only to horizontal scroll changes.
 * @returns Current horizontal scroll position.
 */
export function useScrollX(): number {
	return useSyncExternalStore(
		scrollStore.subscribeX,
		scrollStore.getX,
		scrollStore.getServerX,
	);
}

/**
 * Subscribes a React consumer only to vertical scroll changes.
 * @returns Current vertical scroll position.
 */
export function useScrollY(): number {
	return useSyncExternalStore(
		scrollStore.subscribeY,
		scrollStore.getY,
		scrollStore.getServerY,
	);
}

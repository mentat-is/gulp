import { useSyncExternalStore } from 'react';

type FunctionalUpdate = (prev: number) => number;

class ScrollStore {
  private x: number = 0;
  private y: number = -26;
  private listeners = new Set<() => void>();
  private snapshot = { x: 0, y: -26 };
  private serverSnapshot = { x: 0, y: -26 };

  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  getSnapshot = () => this.snapshot;
  getServerSnapshot = () => this.serverSnapshot;

  getX = () => this.x;
  getY = () => this.y;

  setScrollX = (x: number | FunctionalUpdate) => {
    const nextX = typeof x === 'function' ? x(this.x) : x;
    if (this.x === nextX) return;
    this.x = nextX;
    this.emitChange();
  };

  setScrollY = (y: number | FunctionalUpdate) => {
    const nextY = typeof y === 'function' ? y(this.y) : y;
    if (this.y === nextY) return;
    this.y = nextY;
    this.emitChange();
  };

  setScroll = (x: number, y: number) => {
    if (this.x === x && this.y === y) return;
    this.x = x;
    this.y = y;
    this.emitChange();
  };

  private emitChange() {
    this.snapshot = { x: this.x, y: this.y };
    for (const listener of this.listeners) {
      listener();
    }
  }
}

export const scrollStore = new ScrollStore();

export function useScroll() {
  return useSyncExternalStore(scrollStore.subscribe, scrollStore.getSnapshot, scrollStore.getServerSnapshot);
}

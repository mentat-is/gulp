import { Note } from '@/entities/Note';
import { Link } from '@/entities/Link';
import { Source } from '@/entities/Source';
import { Context } from '@/entities/Context';
import { Doc } from '@/entities/Doc';
import { Highlight } from '@/entities/Highlight';
import { Glyph } from '@/entities/Glyph';

export class DataStore {
  // Pure mutable data structures. No React proxies.
  static notes: Note.Type[] = [];
  static links: Link.Type[] = [];
  
  // Manteniamo la Map per O(1) lookup durante il render, ma mutabile e fuori da React
  static events: Map<string, Doc.Type[]> = new Map(); 
  
  static highlights: Highlight.Type[] = [];
  static glyphs: Glyph.Type[] = [];

  // Atomic flag to signal requestAnimationFrame that data has changed
  static isDirty: boolean = false;

  private static listeners = new Set<() => void>();
  private static version = 0;
  private static pendingDirtyFrame: number | null = null;
  private static pendingDirtyTimeout: ReturnType<typeof setTimeout> | null = null;

  /**
   * Subscribes a listener to DataStore changes.
   * Returns an unsubscribe function.
   */
  static subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  /**
   * Returns the current version of the store for React's useSyncExternalStore.
   */
  static getSnapshot = () => this.version;

  /**
   * Signals that data has changed and notifies all subscribers.
   * @returns Nothing.
   */
  static markDirty(): void {
    this.cancelScheduledDirty();
    this.notifyDirtyListeners();
  }

  /**
   * Coalesces data-change notifications to the next animation frame.
   * @returns Nothing.
   */
  static markDirtySoon(): void {
    this.isDirty = true;
    if (this.pendingDirtyFrame !== null || this.pendingDirtyTimeout !== null) {
      return;
    }

    const notify = () => {
      this.pendingDirtyFrame = null;
      this.pendingDirtyTimeout = null;
      this.notifyDirtyListeners();
    };

    if (typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function') {
      this.pendingDirtyFrame = window.requestAnimationFrame(notify);
      return;
    }

    this.pendingDirtyTimeout = setTimeout(notify, 0);
  }

  /**
   * Cancels any deferred dirty notification before an immediate notification.
   * @returns Nothing.
   */
  private static cancelScheduledDirty(): void {
    if (this.pendingDirtyFrame !== null) {
      if (typeof window !== 'undefined' && typeof window.cancelAnimationFrame === 'function') {
        window.cancelAnimationFrame(this.pendingDirtyFrame);
      }
      this.pendingDirtyFrame = null;
    }

    if (this.pendingDirtyTimeout !== null) {
      clearTimeout(this.pendingDirtyTimeout);
      this.pendingDirtyTimeout = null;
    }
  }

  /**
   * Increments the external-store version and notifies subscribers.
   * @returns Nothing.
   */
  private static notifyDirtyListeners(): void {
    this.isDirty = true;
    this.version++;
    this.listeners.forEach(listener => listener());
  }
}

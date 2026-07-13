import { Source } from '@/entities/Source'
import { RenderEngine } from './RenderEngine'

/**
 * Defines the contract for all canvas render engines (DefaultEngine, HeightEngine, GraphEngine).
 * Each engine is a singleton that caches per-source pixel data and renders it to the canvas.
 */
export namespace Engine {
  export interface Interface<T> {
    /** Renders the given source at the specified Y position on the canvas. */
    render: (file: Source.Type, y: number, force?: boolean) => void
    /** Per-source cache of computed pixel data. Cleared on operation switch. */
    map: Map<Source.Id, T>
    /** Computes or retrieves cached pixel data for a source. */
    get: (file: Source.Type) => T
    /** Checks if the cache is valid for the given source (scale/scroll haven't changed). */
    is: (file: Source.Type) => boolean
    /**
     * Updates the internal RenderEngine reference without full constructor reconstruction.
     * Called by RenderEngine's singleton constructor to avoid allocating new engine
     * instances every frame — only the reference to the parent renderer is updated.
     */
    updateRenderer: (renderer: Engine.Constructor) => void
  }

  /** The RenderEngine type — passed to engine constructors and updateRenderer(). */
  export type Constructor = RenderEngine

  /** Union of available render engine names, used as keys in RenderEngine. */
  export type List = 'height' | 'graph' | 'default'
}

/**
 * Symbol-based metadata keys attached to engine Map instances.
 * Using Symbols prevents collisions with numeric Map keys (pixel positions/timestamps).
 */
export namespace Hardcode {
  /** Stores the timeline scale at which the cache was computed. Used for cache invalidation. */
  export const Scale = Symbol('Scale');

  export const Height = Symbol('Height')
  export interface Height {
    [Height]: Hardcode.Height
  }

  /** Stores the maximum height value across all entries, used for normalization. */
  export const MaxHeight = Symbol('MaxHeight')
  export interface MaxHeight {
    [MaxHeight]: Hardcode.Height
  }

  /** Stores the minimum height value across all entries. */
  export const MinHeight = Symbol('MinHeight')
  export interface MinHeight {
    [MinHeight]: Hardcode.Height
  }

  export const Length = Symbol('Length')
  export interface Length {
    [Length]: Hardcode.Length
  }

  /** Range symbols marking the start/end timestamps of cached data. */
  export const Start = Symbol('Start')
  export const End = Symbol('End')
  export interface StartEnd {
    [Start]: number
    [End]: number
  }
}

/** Symbol key for RenderEngine's internal caches (notes, range, flags). */
export const CacheKey = Symbol('Cache')

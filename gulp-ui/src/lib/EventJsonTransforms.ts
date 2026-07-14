/**
 * Client-side registry that lets bundled plugins post-process an event's JSON
 * before it is displayed. Transformers report the field names they actually
 * transformed via the shared `transformedFields` set — the event dialog trusts
 * that list for the toggle-chip bar, so it works regardless of whether the
 * underlying value lives at a flat top-level key or inside a nested object.
 */

export type EventJson = Record<string, unknown>;

export type EventJsonTransformer = (
  sourceId: string,
  json: EventJson,
  hiddenFields: Set<string>,
  transformedFields: Set<string>,
) => EventJson;

const transformers = new Map<string, EventJsonTransformer>();
const listeners = new Set<() => void>();

export interface ApplyResult {
  json: EventJson;
  /** Fields that were actually transformed (excluding those in `hiddenFields`). */
  transformedFields: Set<string>;
}

export const EventJsonTransforms = {
  register(name: string, fn: EventJsonTransformer): () => void {
    transformers.set(name, fn);
    EventJsonTransforms.notify();
    return () => {
      transformers.delete(name);
      EventJsonTransforms.notify();
    };
  },

  apply(
    sourceId: string,
    json: EventJson,
    hiddenFields: Set<string>,
  ): ApplyResult {
    const transformedFields = new Set<string>();
    let out = json;
    for (const fn of transformers.values()) {
      try {
        out = fn(sourceId, out, hiddenFields, transformedFields);
      } catch {
        /* a failing transformer must never break the dialog */
      }
    }
    return { json: out, transformedFields };
  },

  hasAny(): boolean {
    return transformers.size > 0;
  },

  subscribe(fn: () => void): () => void {
    listeners.add(fn);
    return () => {
      listeners.delete(fn);
    };
  },

  notify(): void {
    listeners.forEach((fn) => fn());
  },
};

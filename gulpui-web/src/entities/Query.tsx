import { Filter } from "./Filter"

export namespace Query {
  /**
   * Represents a structured query used for OpenSearch filtering.
   *
   * The actual filtering is driven by `source_config`, `text_filter`, `filters`,
   * and optionally `raw` (for manual mode). The `string` field is a **display-only
   * human-readable label** summarizing the applied query — it is never used
   * to build OpenSearch clauses.
   */
  export interface Type {
    /**
     * Human-readable summary of the applied query.
     * Display-only — never used for filtering or query construction.
     * Populated automatically via `Filter.Entity.describe()`.
     */
    string: string
    /** Wildcard text filter applied to the `event.original` field. */
    text_filter?: string
    /** Array of structured filter conditions and/or nested groups. */
    filters: Filter.Item[]
    /** Raw OpenSearch JSON used in manual mode. */
    raw?: any
    /** Whether this query was built in manual (JSON) mode. */
    isManual?: boolean
    /**
     * Structured source scoping: operation, source IDs, and time range.
     * Used by `Filter.Entity.query()` to generate `term`, `terms`, and `range` clauses.
     */
    source_config?: {
      operation_id: string;
      source_ids: string[];
      range: { min: number | string; max: number | string };
    }
    /**
     * Maps field names to their OpenSearch data type (e.g., "long", "keyword").
     * Used by `Filter.Entity.query()` to choose correct clause types
     * (e.g., `query_string` for numeric fields with wildcard patterns).
     */
    fieldTypeMap?: Record<string, string>
  }
}

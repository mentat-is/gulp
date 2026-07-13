import { useState, useEffect, useCallback, useRef } from 'react'
import { GulpIndexedDB } from '@/class/IndexedDB'

/** Singleton instance for table configuration persistence. */
const tableConfigDB = new GulpIndexedDB('gulp_DB', 'gulp_table_config')

/**
 * Stored schema for table column visibility configuration.
 * Includes the column snapshot at save time to detect schema drift.
 */
interface TableConfigEntry {
  /** Set of hidden column keys. */
  hiddenColumns: string[]
  /** Snapshot of all available columns when config was saved. */
  columnSnapshot: string[]
}

/**
 * Hook to manage persisted table column visibility configuration.
 *
 * Reads from IndexedDB on mount and writes on change. If the saved column
 * snapshot differs from the current column set, the stale config is discarded
 * and a fresh (empty) hidden set is used.
 *
 * @param persistId - Unique identifier for this table instance (derived from parent context).
 *                    When undefined, persistence is disabled (in-memory only).
 * @param allColumns - The full list of currently available columns.
 * @param enabled - Whether column visibility management is enabled at all.
 * @returns Object containing hiddenColumns set, toggle/reset helpers, and loading state.
 */
export function useTableConfig(
  persistId: string | undefined,
  allColumns: string[],
  enabled: boolean,
): {
  hiddenColumns: Set<string>
  toggleColumn: (column: string) => void
  resetConfig: () => void
  isLoaded: boolean
} {
  const [hiddenColumns, setHiddenColumns] = useState<Set<string>>(new Set())
  const [isLoaded, setIsLoaded] = useState(!enabled || !persistId)
  const columnsRef = useRef(allColumns)
  columnsRef.current = allColumns

  /**
   * Load persisted configuration from IndexedDB on mount.
   * Validates the saved column snapshot against the current set.
   */
  useEffect(() => {
    if (!enabled || !persistId) {
      setHiddenColumns(new Set())
      setIsLoaded(true)
      return
    }

    let cancelled = false

    const load = async () => {
      try {
        const entry: TableConfigEntry | undefined = await tableConfigDB.GetConfiguration(persistId)

        if (cancelled) return

        if (entry && entry.columnSnapshot && entry.hiddenColumns) {
          const savedSet = new Set(entry.columnSnapshot)
          const currentSet = new Set(columnsRef.current)

          // Compare saved column snapshot with current columns — discard if they differ
          const isSameSchema =
            savedSet.size === currentSet.size &&
            [...savedSet].every((col) => currentSet.has(col))

          if (isSameSchema) {
            setHiddenColumns(new Set(entry.hiddenColumns))
          } else {
            // Schema mismatch: remove stale config from IndexedDB
            await tableConfigDB.DeleteConfiguration(persistId)
            setHiddenColumns(new Set())
          }
        } else {
          setHiddenColumns(new Set())
        }
      } catch {
        // IndexedDB failure is non-critical; fall back to empty config
        setHiddenColumns(new Set())
      }

      if (!cancelled) {
        setIsLoaded(true)
      }
    }

    load()
    return () => { cancelled = true }
  }, [persistId, enabled])

  /**
   * Persists the current hidden columns to IndexedDB.
   */
  const persistConfig = useCallback(async (hidden: Set<string>) => {
    if (!persistId || !enabled) return

    const entry: TableConfigEntry = {
      hiddenColumns: [...hidden],
      columnSnapshot: [...columnsRef.current],
    }

    try {
      await tableConfigDB.UpdateConfiguration(entry, persistId)
    } catch {
      // Silent fail — persistence is best-effort
    }
  }, [persistId, enabled])

  /**
   * Toggles visibility of a specific column and persists the change.
   */
  const toggleColumn = useCallback((column: string) => {
    setHiddenColumns((prev) => {
      const next = new Set(prev)
      if (next.has(column)) {
        next.delete(column)
      } else {
        next.add(column)
      }
      persistConfig(next)
      return next
    })
  }, [persistConfig])

  /**
   * Resets all hidden columns to visible and removes persisted config.
   */
  const resetConfig = useCallback(async () => {
    setHiddenColumns(new Set())
    if (persistId && enabled) {
      try {
        await tableConfigDB.DeleteConfiguration(persistId)
      } catch {
        // Silent fail
      }
    }
  }, [persistId, enabled])

  return { hiddenColumns, toggleColumn, resetConfig, isLoaded }
}

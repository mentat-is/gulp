import { useState, useEffect, useCallback, useMemo, useRef } from 'react'
import { GulpIndexedDB } from '@/class/IndexedDB'

/** Singleton instance for table configuration persistence. */
const tableConfigDB = new GulpIndexedDB('gulp_DB', 'gulp_table_config')

/**
 * Stored schema for table column visibility and ordering configuration.
 * Includes the column snapshot at save time to detect schema drift.
 */
interface TableConfigEntry {
  /** Set of hidden column keys. */
  hiddenColumns: string[]
  /** Snapshot of all available columns when config was saved. */
  columnSnapshot: string[]
  /** User-defined order for every visible and hidden column. */
  columnOrder?: string[]
}

/** Controls how persisted settings react to a changed field catalog. */
export type TableConfigSchemaPolicy = 'reset' | 'reconcile'

/**
 * Checks whether a stored order is a complete permutation of the current schema.
 * @param order - Persisted value to validate.
 * @param columns - Current table schema.
 * @returns Whether the value contains every current column exactly once.
 */
function isValidColumnOrder(order: unknown, columns: string[]): order is string[] {
  if (!Array.isArray(order) || order.length !== columns.length) return false
  const orderSet = new Set(order)
  return orderSet.size === columns.length && columns.every((column) => orderSet.has(column))
}

/**
 * Reconciles a persisted order with an updated schema while retaining shared fields.
 * @param savedOrder - Complete order from the previously stored schema.
 * @param columns - Current field catalog in its default order.
 * @returns Current fields with retained fields first and new fields appended.
 */
export function reconcileTableColumns(
  savedOrder: string[],
  columns: string[],
): string[] {
  const currentColumns = new Set(columns)
  const reconciled = savedOrder.filter((column) => currentColumns.has(column))
  const retainedColumns = new Set(reconciled)
  columns.forEach((column) => {
    if (!retainedColumns.has(column)) reconciled.push(column)
  })
  return reconciled
}

/**
 * Moves one visible column relative to another without moving hidden slots.
 * @param order - Canonical order containing visible and hidden columns.
 * @param activeColumn - Visible column being moved.
 * @param overColumn - Visible column at the drop target.
 * @param hiddenColumns - Columns excluded from the sortable selected list.
 * @returns The reordered canonical list, or the original reference when unchanged.
 */
export function moveTableColumn(
  order: string[],
  activeColumn: string,
  overColumn: string,
  hiddenColumns: ReadonlySet<string> = new Set(),
): string[] {
  const visibleColumns = order.filter((column) => !hiddenColumns.has(column))
  const activeIndex = visibleColumns.indexOf(activeColumn)
  const overIndex = visibleColumns.indexOf(overColumn)
  if (activeIndex < 0 || overIndex < 0 || activeIndex === overIndex) return order

  const nextVisibleColumns = [...visibleColumns]
  const [movedColumn] = nextVisibleColumns.splice(activeIndex, 1)
  nextVisibleColumns.splice(overIndex, 0, movedColumn)

  let visibleIndex = 0
  return order.map((column) => {
    if (hiddenColumns.has(column)) return column
    const nextColumn = nextVisibleColumns[visibleIndex]
    visibleIndex += 1
    return nextColumn
  })
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
 * @param schemaPolicy - Whether changed schemas reset or reconcile saved settings.
 * @returns Visibility state, canonical order, mutation helpers, and loading state.
 */
export function useTableConfig(
  persistId: string | undefined,
  allColumns: string[],
  enabled: boolean,
  schemaPolicy: TableConfigSchemaPolicy = 'reset',
): {
  hiddenColumns: Set<string>
  orderedColumns: string[]
  toggleColumn: (column: string) => void
  selectAllColumns: () => void
  deselectAllColumns: () => void
  moveColumn: (activeColumn: string, overColumn: string) => void
  isLoaded: boolean
} {
  const [hiddenColumns, setHiddenColumns] = useState<Set<string>>(new Set())
  const [orderedColumns, setOrderedColumns] = useState<string[]>(allColumns)
  const [isLoaded, setIsLoaded] = useState(!enabled || !persistId)
  const columnsRef = useRef(allColumns)
  const hiddenColumnsRef = useRef(hiddenColumns)
  const orderedColumnsRef = useRef(orderedColumns)
  columnsRef.current = allColumns
  hiddenColumnsRef.current = hiddenColumns
  orderedColumnsRef.current = orderedColumns
  const schemaSignature = useMemo(
    () => JSON.stringify([...allColumns].sort()),
    [allColumns],
  )

  /**
   * Load persisted configuration from IndexedDB on mount.
   * Validates the saved column snapshot against the current set.
   */
  useEffect(() => {
    if (!enabled || !persistId) {
      setHiddenColumns(new Set())
      setOrderedColumns([...columnsRef.current])
      setIsLoaded(true)
      return
    }

    let cancelled = false
    setIsLoaded(false)

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
            const nextHiddenColumns = new Set(
              entry.hiddenColumns.filter((column) => currentSet.has(column)),
            )
            const nextColumnOrder = isValidColumnOrder(entry.columnOrder, columnsRef.current)
              ? [...entry.columnOrder]
              : [...columnsRef.current]
            hiddenColumnsRef.current = nextHiddenColumns
            orderedColumnsRef.current = nextColumnOrder
            setHiddenColumns(nextHiddenColumns)
            setOrderedColumns(nextColumnOrder)
          } else if (schemaPolicy === 'reconcile') {
            const savedOrder = isValidColumnOrder(
              entry.columnOrder,
              entry.columnSnapshot,
            )
              ? entry.columnOrder
              : entry.columnSnapshot
            const nextColumnOrder = reconcileTableColumns(
              savedOrder,
              columnsRef.current,
            )
            const nextHiddenColumns = new Set(
              entry.hiddenColumns.filter((column) => currentSet.has(column)),
            )
            hiddenColumnsRef.current = nextHiddenColumns
            orderedColumnsRef.current = nextColumnOrder
            setHiddenColumns(nextHiddenColumns)
            setOrderedColumns(nextColumnOrder)
            try {
              await tableConfigDB.UpdateConfiguration(
                {
                  hiddenColumns: [...nextHiddenColumns],
                  columnSnapshot: [...columnsRef.current],
                  columnOrder: [...nextColumnOrder],
                } satisfies TableConfigEntry,
                persistId,
              )
            } catch {
              // Reconciliation remains usable in memory when persistence fails.
            }
          } else {
            // Schema mismatch: remove stale config from IndexedDB
            await tableConfigDB.DeleteConfiguration(persistId)
            const nextColumnOrder = [...columnsRef.current]
            hiddenColumnsRef.current = new Set()
            orderedColumnsRef.current = nextColumnOrder
            setHiddenColumns(new Set())
            setOrderedColumns(nextColumnOrder)
          }
        } else {
          const nextColumnOrder = [...columnsRef.current]
          hiddenColumnsRef.current = new Set()
          orderedColumnsRef.current = nextColumnOrder
          setHiddenColumns(new Set())
          setOrderedColumns(nextColumnOrder)
        }
      } catch {
        // IndexedDB failure is non-critical; fall back to empty config
        const nextColumnOrder = [...columnsRef.current]
        hiddenColumnsRef.current = new Set()
        orderedColumnsRef.current = nextColumnOrder
        setHiddenColumns(new Set())
        setOrderedColumns(nextColumnOrder)
      }

      if (!cancelled) {
        setIsLoaded(true)
      }
    }

    load()
    return () => { cancelled = true }
  }, [persistId, enabled, schemaPolicy, schemaSignature])

  /**
   * Persists the current visibility and canonical column order to IndexedDB.
   * @param hidden - Columns currently excluded from the table.
   * @param order - Complete order for visible and hidden columns.
   * @returns A promise that settles after the best-effort write.
   */
  const persistConfig = useCallback(async (
    hidden: Set<string>,
    order: string[],
  ) => {
    if (!persistId || !enabled) return

    const entry: TableConfigEntry = {
      hiddenColumns: [...hidden],
      columnSnapshot: [...columnsRef.current],
      columnOrder: [...order],
    }

    try {
      await tableConfigDB.UpdateConfiguration(entry, persistId)
    } catch {
      // Silent fail — persistence is best-effort
    }
  }, [persistId, enabled])

  /**
   * Toggles visibility of a specific column and persists the change.
   * @param column - Column key to show or hide.
   * @returns Nothing.
   */
  const toggleColumn = useCallback((column: string) => {
    setHiddenColumns((prev) => {
      const next = new Set(prev)
      if (next.has(column)) {
        next.delete(column)
      } else {
        next.add(column)
      }
      hiddenColumnsRef.current = next
      void persistConfig(next, orderedColumnsRef.current)
      return next
    })
  }, [persistConfig])

  /**
   * Selects every column while preserving and persisting the canonical order.
   * @returns Nothing.
   */
  const selectAllColumns = useCallback(() => {
    const next = new Set<string>()
    hiddenColumnsRef.current = next
    setHiddenColumns(next)
    void persistConfig(next, orderedColumnsRef.current)
  }, [persistConfig])

  /**
   * Hides every available column and persists the resulting configuration.
   * @returns Nothing.
   */
  const deselectAllColumns = useCallback(() => {
    const hidden = new Set(columnsRef.current)
    hiddenColumnsRef.current = hidden
    setHiddenColumns(hidden)
    void persistConfig(hidden, orderedColumnsRef.current)
  }, [persistConfig])

  /**
   * Reorders one column and persists the complete canonical order.
   * @param activeColumn - Selected column being moved.
   * @param overColumn - Selected column at the drop target.
   * @returns Nothing.
   */
  const moveColumn = useCallback((activeColumn: string, overColumn: string) => {
    setOrderedColumns((previousOrder) => {
      const nextOrder = moveTableColumn(
        previousOrder,
        activeColumn,
        overColumn,
        hiddenColumnsRef.current,
      )
      if (nextOrder === previousOrder) return previousOrder
      orderedColumnsRef.current = nextOrder
      void persistConfig(hiddenColumnsRef.current, nextOrder)
      return nextOrder
    })
  }, [persistConfig])

  const resolvedColumnOrder = isValidColumnOrder(orderedColumns, allColumns)
    ? orderedColumns
    : allColumns

  return {
    hiddenColumns,
    orderedColumns: resolvedColumnOrder,
    toggleColumn,
    selectAllColumns,
    deselectAllColumns,
    moveColumn,
    isLoaded,
  }
}

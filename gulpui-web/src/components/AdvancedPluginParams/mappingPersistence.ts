import { GulpIndexedDB } from '@/class/IndexedDB'
import { MappingData } from './MappingPanel'

/** IndexedDB store dedicated to plugin custom mapping history. */
const customMappingsDB = new GulpIndexedDB('gulp_DB', 'gulp_plugin_custom_mappings')

/**
 * SavedPluginMapping describes a persisted custom mapping entry.
 */
export interface SavedPluginMapping {
  /** Plugin filename associated with the custom mapping. */
  plugin_filename: string
  /** Unique mapping identifier within the selected plugin. */
  mapping_id: string
  /** Full custom mapping payload to restore into the editor. */
  mapping: MappingData
  /** ISO timestamp indicating when the mapping was last saved. */
  updated_at: string
}

/**
 * SavedPluginMappingsStore describes the IndexedDB document stored per plugin.
 */
interface SavedPluginMappingsStore {
  /** Plugin filename used as the persistence scope. */
  plugin_filename: string
  /** Mapping history keyed by mapping_id to keep entries unique per plugin. */
  mappings: Record<string, SavedPluginMapping>
}

/**
 * Builds the IndexedDB key used to store mappings for one plugin.
 *
 * @param pluginFilename - The filename of the selected plugin.
 * @returns The stable persistence key for the plugin mapping store.
 */
function getPluginMappingsStorageKey(pluginFilename: string): string {
  return `plugin:${pluginFilename}:custom_mappings`
}

/**
 * Checks whether a value has the expected persisted mapping store shape.
 *
 * @param value - Unknown IndexedDB value to validate.
 * @returns True when the value can be safely treated as a mapping store.
 */
function isSavedPluginMappingsStore(value: unknown): value is SavedPluginMappingsStore {
  if (!value || typeof value !== 'object') return false

  const store = value as Partial<SavedPluginMappingsStore>
  return typeof store.plugin_filename === 'string' && !!store.mappings && typeof store.mappings === 'object'
}

/**
 * Sorts saved mappings by most recent update first.
 *
 * @param mappings - Saved mapping records to order.
 * @returns A new list ordered by updated_at descending.
 */
function sortSavedMappingsByUpdatedAt(mappings: SavedPluginMapping[]): SavedPluginMapping[] {
  return [...mappings].sort((left, right) => right.updated_at.localeCompare(left.updated_at))
}

/**
 * Reads all saved custom mappings for the selected plugin.
 *
 * @param pluginFilename - The filename of the selected plugin.
 * @returns Saved mappings associated with the plugin, newest first.
 */
export async function loadSavedPluginMappings(pluginFilename: string): Promise<SavedPluginMapping[]> {
  const savedStore = await customMappingsDB.GetConfiguration(getPluginMappingsStorageKey(pluginFilename))

  if (!isSavedPluginMappingsStore(savedStore)) {
    return []
  }

  return sortSavedMappingsByUpdatedAt(Object.values(savedStore.mappings))
}

/**
 * Persists all current custom mappings for a plugin, replacing entries with the same mapping_id.
 *
 * @param pluginFilename - The filename of the selected plugin.
 * @param mappings - Current custom mappings from the editor.
 * @returns Saved mappings associated with the plugin after the update.
 */
export async function persistPluginMappings(pluginFilename: string, mappings: MappingData[]): Promise<SavedPluginMapping[]> {
  const storageKey = getPluginMappingsStorageKey(pluginFilename)
  const previousStore = await customMappingsDB.GetConfiguration(storageKey)
  const existingMappings = isSavedPluginMappingsStore(previousStore) ? previousStore.mappings : {}
  const nextMappings: Record<string, SavedPluginMapping> = { ...existingMappings }
  const updatedAt = new Date().toISOString()

  mappings.forEach((mapping) => {
    const mappingId = mapping.id.trim()

    if (!mappingId) {
      return
    }

    nextMappings[mappingId] = {
      plugin_filename: pluginFilename,
      mapping_id: mappingId,
      mapping: {
        ...mapping,
        id: mappingId,
      },
      updated_at: updatedAt,
    }
  })

  const nextStore: SavedPluginMappingsStore = {
    plugin_filename: pluginFilename,
    mappings: nextMappings,
  }

  await customMappingsDB.UpdateConfiguration(nextStore, storageKey)

  return sortSavedMappingsByUpdatedAt(Object.values(nextMappings))
}

/**
 * Deletes one saved custom mapping for the selected plugin.
 *
 * @param pluginFilename - The filename of the selected plugin.
 * @param mappingId - Unique mapping identifier to remove from the plugin store.
 * @returns Saved mappings associated with the plugin after deletion.
 */
export async function deleteSavedPluginMapping(pluginFilename: string, mappingId: string): Promise<SavedPluginMapping[]> {
  const storageKey = getPluginMappingsStorageKey(pluginFilename)
  const previousStore = await customMappingsDB.GetConfiguration(storageKey)

  if (!isSavedPluginMappingsStore(previousStore)) {
    return []
  }

  const nextMappings: Record<string, SavedPluginMapping> = { ...previousStore.mappings }
  delete nextMappings[mappingId]

  const nextStore: SavedPluginMappingsStore = {
    plugin_filename: pluginFilename,
    mappings: nextMappings,
  }

  await customMappingsDB.UpdateConfiguration(nextStore, storageKey)

  return sortSavedMappingsByUpdatedAt(Object.values(nextMappings))
}

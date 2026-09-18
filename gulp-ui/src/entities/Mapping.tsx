import type { App } from "./App"

export namespace Mapping {
  export namespace Type {
    export interface Plugin {
      name: string
      methods: Method[]
    }

    export interface Method {
      name: string
      mappings: string[]
    }
  }

  export interface Raw {
    metadata: {
      plugin: string[]
    }
    filename: string
    mapping_ids: string[]
  }

  export class Entity {
    /**
     * Converts the mapping-file API payload into the plugin-oriented UI model.
     * Invalid files are reported and skipped without discarding valid files.
     *
     * @param raw - Untrusted mapping-file list returned by the backend.
     * @returns Valid mappings grouped by every plugin declared by each file.
     */
    public static parse(raw: unknown): Mapping.Type.Plugin[] {
      const plugins: Mapping.Type.Plugin[] = []

      if (!Array.isArray(raw)) {
        console.error(
          "[Mapping.parse] Failed to parse the mapping file list.",
          new TypeError("Expected the mapping file response to be an array."),
        )
        return plugins
      }

      raw.forEach((value, index) => {
        const mappingFileName = Entity.mappingFileName(value, index)

        try {
          if (typeof value !== "object" || value === null) {
            throw new TypeError("Expected a mapping file object.")
          }

          const mappingFile = value as Record<string, unknown>
          const metadata = mappingFile.metadata
          if (typeof metadata !== "object" || metadata === null) {
            throw new TypeError("Missing mapping metadata.")
          }

          const pluginNames = (metadata as Record<string, unknown>).plugin
          if (
            !Array.isArray(pluginNames) ||
            pluginNames.length === 0 ||
            !pluginNames.every(
              (pluginName) =>
                typeof pluginName === "string" && pluginName.trim().length > 0,
            )
          ) {
            throw new TypeError("Expected metadata.plugin to contain plugin filenames.")
          }

          const filename = mappingFile.filename
          if (typeof filename !== "string" || filename.trim().length === 0) {
            throw new TypeError("Missing mapping filename.")
          }

          const mappingIds = mappingFile.mapping_ids
          if (
            !Array.isArray(mappingIds) ||
            !mappingIds.every((mappingId) => typeof mappingId === "string")
          ) {
            throw new TypeError("Expected mapping_ids to be an array of strings.")
          }

          new Set(pluginNames).forEach((pluginName) => {
            let plugin = plugins.find((candidate) => candidate.name === pluginName)

            if (!plugin) {
              plugin = {
                name: pluginName,
                methods: [],
              }
              plugins.push(plugin)
            }

            plugin.methods.push({
              name: filename,
              mappings: [...mappingIds],
            })
          })
        } catch (error) {
          console.error(
            `[Mapping.parse] Failed to parse mapping file "${mappingFileName}".`,
            error,
          )
        }
      })

      return plugins
    }

    /**
     * Resolves a stable mapping-file label for validation errors.
     *
     * @param value - Untrusted mapping-file entry.
     * @param index - Entry position in the backend response.
     * @returns The mapping filename or a positional fallback label.
     */
    private static mappingFileName(value: unknown, index: number): string {
      if (typeof value === "object" && value !== null) {
        const filename = (value as Record<string, unknown>).filename
        if (typeof filename === "string" && filename.trim().length > 0) {
          return filename
        }
      }

      return `entry #${index + 1}`
    }

    public static plugins = (app: App.Type): Mapping.Type.Plugin['name'][] =>
      app.target.mappings.map((p) => p.name).sort((a, b) => a.localeCompare(b))

    public static methods = (
      app: App.Type,
      plugin?: Mapping.Type.Plugin['name'],
    ): Mapping.Type.Method['name'][] =>
      (app.target.mappings
        .find((p) => p.name === plugin)
        ?.methods.map((m) => m.name) || []).sort((a, b) => a.localeCompare(b))

    public static mappings = (
      app: App.Type,
      plugin?: Mapping.Type.Plugin['name'],
      method?: Mapping.Type.Method['name'],
    ): string[] =>
      (app.target.mappings
        .find((p) => p.name === plugin)
        ?.methods.find((m) => m.name === method)?.mappings || []).sort((a, b) => a.localeCompare(b))
  }
}

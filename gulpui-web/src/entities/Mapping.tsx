import { App } from "./App"

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
    public static parse(raw: Mapping.Raw[]): Mapping.Type.Plugin[] {
      const plugins: Mapping.Type.Plugin[] = []

      raw.forEach((r) => {
        const isPluginExist = plugins.find((p) => p.name === r.metadata.plugin[0])

        if (!isPluginExist) {
          plugins.push({
            name: r.metadata.plugin[0],
            methods: [],
          })
        }

        const shit = plugins.find(
          (p) => p.name === r.metadata.plugin[0],
        ) as Mapping.Type.Plugin

        shit.methods.push({
          name: r.filename,
          mappings: r.mapping_ids,
        })
      })

      return plugins
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
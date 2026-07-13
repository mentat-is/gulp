import { Arrayed } from "@/class/Info"
import { App } from "../App"
import { Context } from "../Context"
import { Doc } from "../Doc"
import { Operation } from "../Operation"
import { Source } from "../Source"

export class Parser {
  public static use = <K extends keyof App.Type['target']>(
    x: App.Type | App.Type['target'][K],
    expects: K,
  ): App.Type['target'][K] =>
    Array.isArray(x) ? (x as App.Type['target'][K]) : (x as App.Type)['target'][expects]

  public static useName = (
    unknown: Operation.Type | Context.Type | Source.Type | string,
  ): string => (typeof unknown === 'string' ? unknown : unknown.name)

  public static useId = (unknown: Doc.Type | string): string =>
    typeof unknown === 'string' ? unknown : unknown._id

  public static useUUID = <
    T extends Context.Type | Source.Type,
  >(
    unknown: T | string,
  ): Context.Id | Source.Id | Operation.Id => {
    if (typeof unknown === 'string') {
      return unknown as T['id']
    } else {
      return (unknown as T)?.id
    }
  }

  public static array = <K extends unknown>(unknown: Arrayed<K>): K[] =>
    Array.isArray(unknown) ? unknown : [unknown]

  public static isSingle = (arr: Array<any>) => arr.length === 1
}

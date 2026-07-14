import { Operation } from "./Operation";
import { App } from "./App";
import { Range } from "@/class/Info";
import { Glyph } from "./Glyph";
import { User } from "./User";
import { DataStore } from "@/store/DataStore";

type UUID = string;

export namespace Highlight {
  export const name = 'Highlight';
  const _ = Symbol(Highlight.name)
  export type Id = UUID & {
    readonly [_]: unique symbol
  }

  export interface Type {
    id: Highlight.Id
    type: 'highlight',
    owner_user_id: User.Id;
    description: string;
    time_range: Range,
    tags: string[],
    name: string;
    operation_id: Operation.Id;
    color: string
    glyph_id: Glyph.Id
    [key: string]: any;
  }

  export class Entity {
    static selected = (app: App.Type): Highlight.Type[] => {
      const operation = Operation.Entity.selected(app);
      if (!operation) {
        return [];
      }

      return DataStore.highlights.filter(h => h.operation_id === operation.id);
    }

    static computeDepths(highlights: Highlight.Type[]): number[] {
      const depths: number[] = [];

      for (let i = 0; i < highlights.length; i++) {
        const { time_range: [startA, endA] } = highlights[i];

        const usedDepths = new Set<number>();

        for (let j = 0; j < i; j++) {
          const { time_range: [startB, endB] } = highlights[j];
          const overlaps =
            !(endA <= startB || startA >= endB);

          if (overlaps) {
            usedDepths.add(depths[j]);
          }
        }

        let d = 0;
        while (usedDepths.has(d)) d++;
        depths.push(d);
      }

      return depths;
    }
  }
}

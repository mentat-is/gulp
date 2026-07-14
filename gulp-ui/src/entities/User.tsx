import { Permissions } from "@/banners/Permissions.banner"
type UUID = string
import { Group } from "./Group"
import { Glyph } from "./Glyph"

export namespace User {
  export const name = 'User'
  const _ = Symbol(User.name)
  export type Id = UUID & {
    readonly [_]: unique symbol
  }

  export interface Minified {
    token: string;
    id: User.Id;
    password: string;
    time_expire: number;
  }

  export interface Type {
    token: string;
    id: User.Id;
    password: string;
    time_expire: number;
    pwd_hash: '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918'
    permission: Permissions.Role[]
    time_last_login: number
    user_data: Record<string, any>
    type: 'user'
    name: string
    glyph_id: Glyph.Id
    groups: Group.Type[]
  }
}

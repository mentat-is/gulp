type UUID = string;

export namespace Group {
	export const name = "Group";
	const _ = Symbol(Group.name);
	export type Id = UUID & {
		readonly [_]: unique symbol;
	};

	export interface UserEntry {
		id?: string;
		user_id?: string;
		name?: string;
	}

	export interface Type extends Record<string, unknown> {
		id: Id | string;
		name?: string;
		glyph_id?: string | null;
		permission?: string[];
		description?: string;
		user?: Array<string | UserEntry>;
		users?: Array<string | UserEntry>;
	}
}

import { GulpDataset, MinMax } from "@/class/Info";
import { Context } from "./Context";
import { Doc } from "./Doc";
import { Link } from "./Link";
import { Note } from "./Note";
import { Operation } from "./Operation";
import { Source } from "./Source";
import { Pointers } from "@/components/Pointers";
import { generateUUID } from "@/ui/utils";
import { User } from "./User";
import { Query } from "./Query";
import { Request } from "./Request";
import { Glyph } from "./Glyph";
import { Highlight } from "./Highlight";
import { Mapping } from "./Mapping";
import { Internal } from "./addon/Internal";

export namespace App {
	export interface Type {
		target: {
			operations: Operation.Type[];
			contexts: Context.Type[];
			files: Source.Type[];
			events: Map<Source.Id, Doc.Type[]>;
			filters: Record<Source.Id, Query.Type>;
			notes: Note.Type[];
			links: Link.Type[];
			highlights: Highlight.Type[];
			glyphs: Glyph.Type[];
			mappings: Mapping.Type.Plugin[];
			plugins: GulpDataset.PluginList.Interface[];
		};
		general: {
			server: string;
			ws_id: string;
			glyphs_syncronized: boolean;
			requests: Request.Type[];
			user: User.Type | null;
			loadings: {
				byRequestId: Map<Request.Id, Source.Id>;
				byFileId: Map<Source.Id, Request.Id>;
			};
			tableViewSource: Source.Type | null;
			skippedAuth?: boolean;
		};
		timeline: {
			scale: number;
			frame: MinMax;
			target: Doc.Type | null;
			filter: string;
			cache: {
				data: Map<Source.Id, Doc.Type[]>;
				filters: Record<Source.Id, Query.Type>;
			};
			isScrollReversed: boolean;
			dialogSize: number;
			pointers: Pointers.Pointer[];
			renderVersion: number;
			renderedSourceIds?: Source.Id[];
		};
		settings: {
			[key: string]: any;
		};
		hidden: {
			notes: boolean;
			links: boolean;
			sourcesWithNoEvents: boolean;
			toasts: boolean;
		};
	}

	export const Base: App.Type = {
		general: {
			server: Internal.Settings.server,
			ws_id: generateUUID(),
			glyphs_syncronized: false,
			requests: [],
			user: null,
			loadings: {
				byRequestId: new Map(),
				byFileId: new Map(),
			},
			tableViewSource: null,
		},
		timeline: {
			scale: 1,
			target: null,
			filter: "",
			cache: {
				data: new Map<Source.Id, Doc.Type[]>(),
				filters: {},
			},
			frame: {
				min: 0,
				max: Date.now(),
			},
			isScrollReversed:
				localStorage.getItem("settings.__isScrollReversed") === "true",
			dialogSize: window.innerWidth / 3,
			pointers: [],
			renderVersion: 0,
		},
		target: {
			operations: [],
			contexts: [],
			files: [],
			events: new Map<Source.Id, Doc.Type[]>(),
			filters: {},
			notes: [],
			links: [],
			highlights: [],
			glyphs: [],
			mappings: [],
			plugins: [],
		},
		settings: {},
		hidden: {
			notes: false,
			links: false,
			sourcesWithNoEvents:
				localStorage.getItem("settings.__sourcesWithNoEventsHidden") !==
				"false",
			toasts: false,
		},
	};
}

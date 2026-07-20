import { CacheKey } from "@/class/Engine.dto";
import { Default } from "@/dto/Dataset";
import { Logger } from "@/dto/Logger.class";
type UUID = string;
import { Parser } from "./addon/Parser";
import { App } from "./App";
import { Source } from "./Source";
import { Doc } from "./Doc";
import { Context } from "./Context";
import { Operation } from "./Operation";
import { User } from "./User";
import { Glyph } from "./Glyph";
import { DataStore } from "@/store/DataStore";
import { Banner as UIBanner } from "@/ui/Banner";
import { Toggle } from "@/ui/Toggle";
import { Application } from "@/context/Application.context";
import { Button } from "@/ui/Button";
import { useState } from "react";
import { toast } from "sonner";
import { Internal } from "./addon/Internal";
import { Locale } from "@/locales";
import { Color } from "./Color";

export namespace Note {
	export const name = "Note";
	const _ = Symbol(Note.name);
	export type Id = UUID & {
		readonly [_]: unique symbol;
	};

	export interface Type {
		id: Note.Id;
		type: "note";
		operation_id: Operation.Id;
		tags: string[];
		name: string;
		context_id: Context.Id;
		color: string | null;
		source_id: Source.Id;
		doc: Doc.Type;
		time_pin: number;
		owner_user_id: User.Id;
		text: string;
		glyph_id: Glyph.Id;
		edits: Record<string, any>[];
		[key: string]: any;
	}

	export class Entity {
		private static readonly DEFAULT_UI_COLOR = "var(--accent)";

		public static icon = Internal.IconExtractor.activate<Note.Type | null>(
			Default.Icon.NOTE,
		);

		public static id = (_app: App.Type, id: Note.Id) =>
			DataStore.notes.find((n) => n.id === id) as Note.Type;

		/**
		 * Resolves the source id attached to a note, tolerating older payload shapes.
		 * @param note Note whose owning source should be resolved.
		 * @returns Source id from the note entity or its embedded document.
		 */
		public static sourceId = (
			note: Pick<Note.Type, "source_id" | "doc">,
		): Source.Id | undefined => note.source_id ?? note.doc?.["gulp.source_id"];

		public static selected = (app: App.Type): Note.Type[] => {
			const files = Source.Entity.selected(app).map((file) => file.id);

			return DataStore.notes.filter((note) => {
				const sourceId = Note.Entity.sourceId(note);
				return !!sourceId && files.includes(sourceId);
			});
		};

		public static event = (app: App.Type, note: Note.Type): Doc.Type =>
			Doc.Entity.id(app, note.doc._id);

		/**
		 * Resolves the note color used by DOM surfaces, falling back to the active theme accent.
		 * @param note Note whose display color should be resolved.
		 * @returns Persisted note color, or the theme-aware CSS accent variable when color is missing.
		 */
		public static color = (note: Pick<Note.Type, "color">): string => {
			return note.color?.trim() || Note.Entity.DEFAULT_UI_COLOR;
		};

		/**
		 * Resolves the note color for APIs that require concrete color values instead of CSS variables.
		 * @param note Note whose concrete color should be resolved.
		 * @returns Persisted note color, or the currently resolved theme accent color when color is missing.
		 */
		public static resolvedColor = (note: Pick<Note.Type, "color">): string => {
			return note.color?.trim() || Color.Themer.theme.FONT_ACCENT;
		};

		public static [CacheKey] = new Map<Source.Id, Note.Type[]>();

		/**
		 * Tracks whether the note-to-source index needs rebuilding.
		 * Set to false by invalidateCache(), checked by ensureIndexing().
		 */
		private static _cacheValid = false;

		/**
		 * Counts indexed notes without flattening the per-source arrays.
		 * @returns Total amount of notes currently present in the source index.
		 */
		public static indexSize = (): number => {
			let total = 0;
			Note.Entity[CacheKey].forEach((notes) => {
				total += notes.length;
			});
			return total;
		};

		/**
		 * Marks the note index as stale. This is O(1) — the actual rebuild is
		 * deferred to ensureIndexing() which runs lazily at first access.
		 * Call this whenever app.target.notes or app.target.files changes.
		 */
		public static invalidateCache = () => {
			Note.Entity._cacheValid = false;
			Note.Entity[CacheKey].clear();
		};

		/**
		 * Updates the source note index for one note when the index is already valid.
		 * @param note Normalized note to insert or replace.
		 * @param previousNote Previous note value when an existing note is being edited.
		 * @returns Nothing.
		 */
		public static upsertIndexedNote = (
			note: Note.Type,
			previousNote?: Note.Type,
		): void => {
			if (!Note.Entity._cacheValid) return;

			const sourceId = Note.Entity.sourceId(note);
			const previousSourceId = previousNote
				? Note.Entity.sourceId(previousNote)
				: undefined;
			if (!sourceId) return;

			if (previousNote && previousSourceId && previousSourceId !== sourceId) {
				Note.Entity.removeIndexedNote(previousNote);
			}

			const sourceNotes = Note.Entity[CacheKey].get(sourceId) ?? [];
			const existingIndex = sourceNotes.findIndex((item) => item.id === note.id);
			if (existingIndex !== -1) {
				sourceNotes[existingIndex] = note;
				sourceNotes.sort(
					(a, b) => Note.Entity.timestamp(b) - Note.Entity.timestamp(a),
				);
			} else {
				const insertIndex = Note.Entity.findNoteInsertIndex(sourceNotes, note);
				sourceNotes.splice(insertIndex, 0, note);
			}
			Note.Entity[CacheKey].set(sourceId, sourceNotes);
		};

		/**
		 * Removes one note from the source note index when the index is valid.
		 * @param note Note to remove from its source group.
		 * @returns Nothing.
		 */
		public static removeIndexedNote = (note: Note.Type): void => {
			if (!Note.Entity._cacheValid) return;

			const sourceId = Note.Entity.sourceId(note);
			if (!sourceId) return;

			const sourceNotes = Note.Entity[CacheKey].get(sourceId);
			if (!sourceNotes) return;

			const index = sourceNotes.findIndex((item) => item.id === note.id);
			if (index !== -1) {
				sourceNotes.splice(index, 1);
			}
		};

		/**
		 * Finds the descending timestamp insert position for a note.
		 * @param notes Source notes sorted by descending timestamp.
		 * @param note Note to insert.
		 * @returns Insert index that preserves descending timestamp order.
		 */
		private static findNoteInsertIndex = (
			notes: Note.Type[],
			note: Note.Type,
		): number => {
			const timestamp = Note.Entity.timestamp(note);
			let left = 0;
			let right = notes.length;

			while (left < right) {
				const mid = (left + right) >>> 1;
				if (Note.Entity.timestamp(notes[mid]) < timestamp) {
					right = mid;
				} else {
					left = mid + 1;
				}
			}

			return left;
		};

		public static normalize = (
			app: App.Type,
			notes: Note.Type[],
		): Note.Type[] => {
			for (let i = 0; i < notes.length; i++) {
				notes[i] = this.normalize_note(app, notes[i]);
			}
			return notes;
		};

		/**
		 * Resolves the source settings used to normalize a note document.
		 *
		 * @param app - Current application state containing loaded source metadata.
		 * @param note - Note whose source settings should be used for normalization.
		 * @returns Loaded source settings, or default settings when the source is not available yet.
		 */
		private static getNormalizationSettings = (
			app: App.Type,
			note: Note.Type,
		): Source.Type["settings"] => {
			const sourceId = Note.Entity.sourceId(note);
			const source = sourceId
				? (Source.Entity.id(app, sourceId) as Source.Type | undefined)
				: undefined;

			return source?.settings ?? Internal.Settings.default;
		};

		/**
		 * Normalizes the embedded note document using the owning source settings.
		 *
		 * @param app - Current application state containing loaded source metadata.
		 * @param note - Note payload returned by the API or collaboration socket.
		 * @returns The same note instance with a normalized doc payload.
		 */
		public static normalize_note = (
			app: App.Type,
			note: Note.Type,
		): Note.Type => {
			const sourceSettings = Note.Entity.getNormalizationSettings(app, note);
			note.doc = Doc.Entity.normalize(
				[note.doc],
				sourceSettings.field,
				sourceSettings.hash_function,
			)[0];
			return note;
		};

		/**
		 * Rebuilds the note-to-source index if it has been invalidated.
		 * Should be called before any render frame that needs note positions.
		 * Skips rebuild if the cache is already valid (no-op on repeated calls).
		 *
		 * @param app - Current application state containing notes and files
		 */
		public static ensureIndexing = (app: App.Type) => {
			if (Note.Entity._cacheValid) return;

			Note.Entity[CacheKey].clear();
			const sourceIds = new Set<Source.Id>();
			app.target.files.forEach((file) => {
				sourceIds.add(file.id);
				Note.Entity[CacheKey].set(file.id, []);
			});

			DataStore.notes.forEach((note) => {
				const sourceId = Note.Entity.sourceId(note);
				if (!sourceId || !sourceIds.has(sourceId)) return;
				const sourceNotes = Note.Entity[CacheKey].get(sourceId);
				if (sourceNotes) {
					sourceNotes.push(note);
				} else {
					Note.Entity[CacheKey].set(sourceId, [note]);
				}
			});
			Note.Entity[CacheKey].forEach((notes) => {
				notes.sort(
					(a, b) => Note.Entity.timestamp(b) - Note.Entity.timestamp(a),
				);
			});

			Note.Entity._cacheValid = true;
			Logger.log(`NOTES_INDEXES_REBUILT:${Note.Entity.indexSize()}`, Note);
		};

		/**
		 * @deprecated Use invalidateCache() + ensureIndexing() instead.
		 * Kept for backward compatibility with external callers.
		 */
		public static updateIndexing = (app: App.Type) => {
			Note.Entity._cacheValid = false;
			Note.Entity.ensureIndexing(app);
		};

		public static findByFile = (
			app: App.Type,
			file: Source.Type | Source.Id,
		): Note.Type[] => {
			const id = Parser.useUUID(file) as Source.Id;

			if (Note.Entity._cacheValid) {
				return Note.Entity[CacheKey].get(id) ?? [];
			}

			// When the global index is invalid, any per-source entry may have been
			// built from a previous DataStore.notes snapshot. Rebuild this source so
			// event-level views immediately reflect note creates, edits, and deletes.
			const fileNotes = DataStore.notes
				.filter((n) => Note.Entity.sourceId(n) === id)
				.sort((a, b) => Note.Entity.timestamp(b) - Note.Entity.timestamp(a));
			Note.Entity[CacheKey].set(id, fileNotes);
			return fileNotes;
		};

		public static timestamp = (note: Note.Type): number => {
			if (!note || !note.doc) {
				return 0;
			}

			const ts = note.doc.gulp_timestamp;
			return ts;
		};
	}

	export namespace Delete {
		export namespace Banner {
			export interface Props extends UIBanner.Props {
				note: Note.Type;
			}
		}
		export function Banner({ note, ...props }: Note.Delete.Banner.Props) {
			const { Info, destroyBanner } = Application.use();
			const { t } = Locale.use();
			const [loading, setLoading] = useState<boolean>(false);
			const [isSubmited, setIsSubmited] = useState<boolean>(false);

			const DeleteButton = () => (
				<Button
					loading={loading}
					icon="Trash2"
					variant="glass"
					onClick={deleteFile}
					disabled={!isSubmited}
				/>
			);

			const deleteFile = async () => {
				setLoading(true);
				await Info.note_delete(note);
				setLoading(false);
				if (props.back) {
					props.back();
				} else {
					destroyBanner();
				}
				toast(t("note.deleted", { name: note.name }));
			};

			return (
				<UIBanner
					title={t("note.deleteTitle")}
					done={<DeleteButton />}
					{...props}
				>
					<p>
						{t("note.deleteConfirm")} <code>{note.name}</code>
					</p>
					<Toggle
						option={[t("common.noDontDelete"), t("common.yesImSure")]}
						checked={isSubmited}
						onCheckedChange={setIsSubmited}
					/>
				</UIBanner>
			);
		}
	}
}

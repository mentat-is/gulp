import { MinMax } from "@/class/Info";
import { Info } from "./Info";
import { format } from "date-fns";
import { RulerDrawer } from "./Ruler.drawer";
import { DefaultEngine } from "../engines/Default.engine";
import { Engine, Hardcode, CacheKey } from "./Engine.dto";
import { HeightEngine } from "../engines/Height.engine";
import { GraphEngine } from "../engines/Graph.engine";
import { getCanvasIcon, CanvasIcon } from "@/ui/CanvasIcon";
import { Logger } from "@/dto/Logger.class";
import { Source } from "@/entities/Source";
import { Doc } from "@/entities/Doc";
import { Link } from "@/entities/Link";
import { Note } from "@/entities/Note";
import { Glyph } from "@/entities/Glyph";
import { Request } from "@/entities/Request";
import { Internal } from "@/entities/addon/Internal";
import { Color } from "@/entities/Color";
import { Operation } from "@/entities/Operation";
import { DataStore } from "@/store/DataStore";
import { Highlight } from "@/entities/Highlight";
import { Context } from "@/entities/Context";
import { stringToHexColor } from "@/ui/utils";
import { translate } from "@/locales/core";
import { requestStore } from "@/store/request.store";

const NOTE_SIZE = 32;
const NOTE_OFFSET = (NOTE_SIZE / 2) * -1;
const ALPHA_HIGHLIGHT = 29;

interface RenderEngineConstructor {
	ctx: CanvasRenderingContext2D;
	limits: MinMax;
	info: Info;
	scrollX: number;
	scrollY: number;
	getPixelPosition: (timestamp: number) => number;
	mouseX?: number;
	mouseY?: number;
	visibleSources?: Source.Type[];
}

export interface Status {
	codes: number[];
	timestamp: number;
	heights: number[];
}

export type StatusMap = Map<number, Status> & {
	[Hardcode.Scale]: number;
};
type Engines = {
	[key in Engine.List]: Engine.Interface<any>;
};

export interface Dot {
	x: number;
	y: number;
	color: string;
}

type Group = [index: number, count: number, centerX: number];

interface PositionedNote {
	x: number;
}

interface NoteGroupCache {
	key: string;
	groups: Group[];
}

interface LinkDocsCache {
	key: string;
	docs: Doc.Type[];
}

export interface RenderStats {
	visibleSources: number;
	renderedRows: number;
	visibleNotes: number;
	visibleLinks: number;
	interactiveRects: number;
	graphSamples: number;
	graphBuckets: number;
	renderedEventPixels: number;
}

export class RenderEngine implements RenderEngineConstructor, Engines {
	ctx!: CanvasRenderingContext2D;
	limits!: MinMax;
	info!: Info;
	getPixelPosition!: (timestamp: number) => number;
	scrollX!: number;
	scrollY!: number;
	mouseX?: number;
	mouseY?: number;
	segmentSize = 500;
	ruler!: RulerDrawer;
	private static instance: RenderEngine | null = null;
	default!: DefaultEngine;
	height!: HeightEngine;
	graph!: GraphEngine;
	shifted: Source.Type[] = [];
	visibleSources: Source.Type[] = [];
	visibleSourceIdSet = new Set<Source.Id>();
	visibleSourceById = new Map<Source.Id, Source.Type>();
	sourceRowIndexById = new Map<Source.Id, number>();
	stats: RenderStats = RenderEngine.emptyStats();

	// INTERACTIVE ELEMENTS IN CANVAS saved to manage clicks for notes and links
	public static interactiveNotes: Array<{
		notes: Note.Type[];
		rect: { x: number; y: number; w: number; h: number };
	}> = [];

	public static interactiveLinks: Array<{
		link: Link.Type;
		rect: { x: number; y: number; w: number; h: number };
	}> = [];

	// CACHE
	private static [CacheKey] = {
		notes: new Map() as Map<Source.Id, NoteGroupCache>,
		range: new Map() as Map<
			Source.Id,
			MinMax & {
				[Hardcode.Length]: number;
				field: keyof Doc.Type;
			}
		>,
		flags: new Map() as Map<
			Doc.Id,
			{
				// Y coordinate
				y: number;
				// timestamp
				t: number;
				// offset
				o: number;
			}
		>,
		links: new Map() as Map<Link.Id, LinkDocsCache>,
	};

	/**
	 * Creates or updates the singleton RenderEngine instance.
	 *
	 * SINGLETON PATTERN: If an instance already exists, updates its properties
	 * (ctx, limits, scroll, etc.) and calls `updateRenderer()` on each sub-engine
	 * instead of creating new DefaultEngine/HeightEngine/GraphEngine instances.
	 * This avoids 3 constructor calls + singleton checks per frame.
	 *
	 * On first call, initializes the singleton with canvas context settings
	 * (image smoothing, font) and creates the sub-engines.
	 */
	constructor({
		ctx,
		limits,
		info,
		getPixelPosition,
		scrollY,
		scrollX,
		mouseX,
		mouseY,
		visibleSources,
	}: RenderEngineConstructor) {
		if (RenderEngine.instance) {
			RenderEngine.instance.stats = RenderEngine.emptyStats();
			RenderEngine.instance.setVisibleSources(visibleSources || []);
			RenderEngine.instance.stats.visibleSources =
				RenderEngine.instance.visibleSources.length;
			RenderEngine.instance.ruler = new RulerDrawer({
				ctx,
				getPixelPosition,
				scrollX,
				scale: info.app.timeline.scale,
				selected: info.app.timeline.frame,
				width: info.width,
			});
			RenderEngine.instance.ctx = ctx;
			RenderEngine.instance.limits = limits;
			RenderEngine.instance.info = info;
			RenderEngine.instance.scrollX = scrollX;
			RenderEngine.instance.scrollY = scrollY;
			RenderEngine.instance.mouseX = mouseX;
			RenderEngine.instance.mouseY = mouseY;
			RenderEngine.instance.getPixelPosition = getPixelPosition;
			// Reuse engine singletons — only update their RenderEngine reference.
			// Avoids `new DefaultEngine()` etc. which would enter each constructor,
			// check the singleton, and return the existing instance anyway.
			RenderEngine.instance.default.updateRenderer(RenderEngine.instance);
			RenderEngine.instance.height.updateRenderer(RenderEngine.instance);
			RenderEngine.instance.graph.updateRenderer(RenderEngine.instance);
			return RenderEngine.instance;
		}
		this.mouseX = mouseX;
		this.mouseY = mouseY;
		this.stats = RenderEngine.emptyStats();
		this.setVisibleSources(visibleSources || []);
		this.stats.visibleSources = this.visibleSources.length;
		this.ruler = new RulerDrawer({
			ctx,
			getPixelPosition,
			scrollX,
			scale: info.app.timeline.scale,
			selected: info.app.timeline.frame,
			width: info.width,
		});
		this.ctx = ctx;
		this.ctx.imageSmoothingEnabled = false;
		this.ctx.imageSmoothingQuality = "high";
		this.ctx.font = "10px monospace";
		this.limits = limits;
		this.info = info;
		this.getPixelPosition = getPixelPosition;

		this.default = new DefaultEngine(this);
		this.height = new HeightEngine(this);
		this.graph = new GraphEngine(this);
		RenderEngine.instance = this;
		return this;
	}

	/**
	 * Creates a fresh render statistics object for one Canvas frame.
	 * @returns Zeroed render counters.
	 */
	private static emptyStats(): RenderStats {
		return {
			visibleSources: 0,
			renderedRows: 0,
			visibleNotes: 0,
			visibleLinks: 0,
			interactiveRects: 0,
			graphSamples: 0,
			graphBuckets: 0,
			renderedEventPixels: 0,
		};
	}

	/**
	 * Stores visible sources and builds O(1) lookup structures for the current frame.
	 * @param sources Sources selected for the current render frame.
	 * @returns Nothing.
	 */
	private setVisibleSources(sources: Source.Type[]): void {
		this.visibleSources = sources;
		this.visibleSourceIdSet = new Set<Source.Id>();
		this.visibleSourceById = new Map<Source.Id, Source.Type>();
		this.sourceRowIndexById = new Map<Source.Id, number>();

		sources.forEach((source, index) => {
			this.visibleSourceIdSet.add(source.id);
			this.visibleSourceById.set(source.id, source);
			this.sourceRowIndexById.set(source.id, index);
		});
	}

	public lines = (file: Source.Type, y?: number) => {
		// Derive a deterministic per-row tint from the context_id hash, but constrained
		// to the current theme accent colour so monochrome themes stay monochrome.
		const tintAlpha = Color.Themer.contextTintAlpha(file.context_id);
		const context = Source.Entity.context(this.info.app, file);
		const color = context.color
			? context.color
			: stringToHexColor(file.context_id);
		y =
			typeof y === "number"
				? y
				: Source.Entity.getHeight(
						this.info.app,
						file,
						this.scrollY,
						this.visibleSources.findIndex((s) => s.id === file.id),
					);

		// fill() paints fillRect(0, y-24, width, 48) which extends to y+24 — the exact
		// position of the separator. Draw fill first, then the separator on top so it
		// is not overwritten. Use reduced opacity so the line is lighter than the
		// fully-opaque text labels drawn afterwards by draw_info/locals.
		this.ctx.fillStyle = color;
		this.ctx.fillRect(0, y + 24, this.ctx.canvas.width, 1);

		this.fill(
			color,
			tintAlpha,
			y,
			!this.shifted.find((shiftedSource) => shiftedSource.id === file.id),
		);
	};

	public primary = (file: Source.Type, y?: number) => {
		y =
			typeof y === "number"
				? y
				: Source.Entity.getHeight(
						this.info.app,
						file,
						this.scrollY,
						this.visibleSources.findIndex((s) => s.id === file.id),
					);

		// Draw top-edge marker at y-25 with reduced opacity so it is visibly lighter
		// than the fully-opaque text labels drawn below it (topmost text is at ~y-10).
		const savedAlpha = this.ctx.globalAlpha;
		this.ctx.globalAlpha = 0.35;
		this.ctx.fillStyle = Color.Themer.theme.BORDER;
		this.ctx.fillRect(0, y - 25, this.ctx.canvas.width, 1);
		this.ctx.globalAlpha = savedAlpha;
	};

	public fill = (
		color: string,
		alpha: number,
		y: number,
		isShifted: boolean,
	) => {
		const base = isShifted ? alpha * 0.4 : alpha * 0.9;
		this.ctx.globalAlpha = Math.min(base, 0.18);
		this.ctx.fillStyle = color;
		this.ctx.fillRect(0, y - 24, this.ctx.canvas.width, 48);
		this.ctx.globalAlpha = 1;
	};

	/* LINK MANAGEMENT */

	/**
	 * Draws visible links, their event dots, and badges for the current Canvas frame.
	 * @returns Nothing.
	 */
	public drawLinks = () => {
		RenderEngine.interactiveLinks = [];
		const canvasWidth = this.ctx.canvas.width;
		let visibleLinks = 0;

		DataStore.links.forEach((link) => {
			const linkedDocs = this.resolveSortedLinkDocs(link);
			if (!linkedDocs) return;
			if (
				linkedDocs.some(
					(doc) => !this.visibleSourceIdSet.has(doc["gulp.source_id"]),
				)
			)
				return;

			const { dots } = this.calcDots(link, linkedDocs);

			const allOutsideLeft = dots.every((d) => d.x < -50);
			const allOutsideRight = dots.every((d) => d.x > canvasWidth + 50);
			if (allOutsideLeft || allOutsideRight) return;

			visibleLinks++;
			this.connection(dots);
			dots.forEach((dot) => this.dot(dot));
			this.renderLinkBadges(link, dots);
		});

		this.stats.visibleLinks += visibleLinks;
		this.stats.interactiveRects += RenderEngine.interactiveLinks.length;
	};

	/**
	 * Resolves and sorts link documents by timestamp ascending, caching the sorted
	 * result until the linked document IDs or document timestamps change.
	 * @param link Link whose root and connected documents should be resolved.
	 * @returns Sorted documents, or null when any linked document is unavailable.
	 */
	private resolveSortedLinkDocs(link: Link.Type): Doc.Type[] | null {
		const linkedDocIds = this.getUniqueLinkDocIds(link);
		const linkedDocs: Doc.Type[] = [];

		for (const docId of linkedDocIds) {
			const doc = Doc.Entity.id(this.info.app, docId);
			if (!doc) {
				RenderEngine[CacheKey].links.delete(link.id);
				return null;
			}
			linkedDocs.push(doc);
		}

		const cacheKey = this.buildLinkDocsCacheKey(linkedDocIds, linkedDocs);
		const cached = RenderEngine[CacheKey].links.get(link.id);
		if (cached?.key === cacheKey) {
			return cached.docs;
		}

		const sortedDocs = [...linkedDocs].sort((a, b) => {
			const timestampDiff = a.gulp_timestamp - b.gulp_timestamp;
			if (timestampDiff !== 0) return timestampDiff;
			return String(a._id).localeCompare(String(b._id));
		});

		RenderEngine[CacheKey].links.set(link.id, {
			key: cacheKey,
			docs: sortedDocs,
		});
		return sortedDocs;
	}

	/**
	 * Returns unique document IDs for a link, preserving the root document as the
	 * first candidate before connected documents are timestamp-sorted.
	 * @param link Link whose document IDs should be normalized.
	 * @returns Unique root and connected document IDs.
	 */
	private getUniqueLinkDocIds(link: Link.Type): Doc.Id[] {
		const uniqueDocIds: Doc.Id[] = [];
		const seenDocIds = new Set<Doc.Id>();

		for (const docId of [link.doc_id_from, ...link.doc_ids]) {
			if (seenDocIds.has(docId)) continue;
			seenDocIds.add(docId);
			uniqueDocIds.push(docId);
		}

		return uniqueDocIds;
	}

	/**
	 * Builds a stable cache key from link document IDs and the fields that affect
	 * connection order.
	 * @param linkedDocIds Ordered IDs stored on the link entity.
	 * @param linkedDocs Resolved documents matching the ordered IDs.
	 * @returns Cache key for sorted link document reuse.
	 */
	private buildLinkDocsCacheKey(
		linkedDocIds: Doc.Id[],
		linkedDocs: Doc.Type[],
	): string {
		let cacheKey = linkedDocIds.length.toString();

		for (let index = 0; index < linkedDocIds.length; index++) {
			const doc = linkedDocs[index];
			cacheKey += `|${linkedDocIds[index]}:${doc._id}:${doc.gulp_timestamp}:${doc["gulp.source_id"]}`;
		}

		return cacheKey;
	}

	/**
	 * Draws link badges on every connection segment, or on the root dot when a link
	 * contains only one document and therefore has no segment.
	 * @param link Link represented by the badge.
	 * @param dots Dots already sorted in timestamp ascending order.
	 * @returns Nothing.
	 */
	private renderLinkBadges(link: Link.Type, dots: Dot[]): void {
		if (dots.length === 1) {
			this.renderLinkBadge(link, dots[0].x, dots[0].y);
			return;
		}

		for (let index = 0; index < dots.length - 1; index++) {
			const start = dots[index];
			const end = dots[index + 1];

			this.renderLinkBadge(link, (start.x + end.x) / 2, (start.y + end.y) / 2);
		}
	}

	/**
	 * Draws an interactive badge for one link at the provided Canvas coordinates.
	 * @param link Link represented by the badge.
	 * @param x Horizontal Canvas coordinate for the badge center.
	 * @param y Vertical Canvas coordinate for the badge center.
	 * @returns Nothing.
	 */
	public renderLinkBadge = (link: Link.Type, x: number, y: number) => {
		const size = 24;
		const rectX = x - size / 2;
		const rectY = y - size / 2;
		const color = link.color || Color.Themer.theme.FONT_ACCENT;

		const isHovered =
			this.mouseX !== undefined &&
			this.mouseY !== undefined &&
			this.mouseX >= rectX &&
			this.mouseX <= rectX + size &&
			this.mouseY >= rectY &&
			this.mouseY <= rectY + size;

		this.ctx.save();

		// EFFETTO HOVER
		if (isHovered) {
			this.ctx.shadowColor = color;
			this.ctx.shadowBlur = 12;
			this.ctx.lineWidth = 2;
			this.ctx.strokeStyle = Color.Themer.theme.FONT_ACCENT;
		}

		this.drawRect(
			rectX,
			rectY,
			size,
			size,
			4,
			color,
			Color.Themer.theme.BACKGROUND_SECOND,
		);
		if (isHovered) this.ctx.stroke();

		const iconName = Link.Entity.icon(link);
		const icon = getCanvasIcon({ name: iconName, color });
		const iconSize = 16;
		this.ctx.drawImage(
			icon,
			x - iconSize / 2,
			y - iconSize / 2,
			iconSize,
			iconSize,
		);
		this.ctx.restore();

		RenderEngine.interactiveLinks.push({
			link,
			rect: { x: rectX, y: rectY, w: size, h: size },
		});
	};

	public connection = (dots: Dot[]) => {
		try {
			if (dots.length < 2) return;

			for (let i = 0; i < dots.length - 1; i++) {
				this.ctx.lineWidth = 2;
				const start = dots[i];
				const end = dots[i + 1] || dots[0];

				const gradient = this.ctx.createLinearGradient(
					start.x,
					start.y,
					end.x,
					end.y,
				);
				gradient.addColorStop(0, start.color);
				gradient.addColorStop(1, end.color);

				this.ctx.strokeStyle = gradient;

				this.ctx.beginPath();
				this.ctx.moveTo(start.x, start.y);
				this.ctx.lineTo(end.x, end.y);
				this.ctx.stroke();
			}
		} catch (_) {}
	};

	/* HIGHLIGHT MANAGEMENT */
	public highlight = (
		x: number,
		width: number,
		index: number,
		color: string,
	) => {
		if (color.startsWith("#")) {
			// Add 16% alpha (hex 29) if it's a 6-digit hex
			this.ctx.fillStyle = color.length === 7 ? color + ALPHA_HIGHLIGHT : color;
		} else {
			this.ctx.fillStyle = color;
		}
		const y = 32 * (index + 1);

		const height = this.ctx.canvas.height - 20 * 2 * (index + 1);

		this.ctx.fillRect(x, y, width, height);
	};

	public drawHighlights = () => {
		const highlights = Highlight.Entity.selected(this.info.app);
		const depths = Highlight.Entity.computeDepths(highlights);

		highlights.forEach((highlight, i) => {
			const xStart = this.getPixelPosition(highlight.time_range[0]);
			const xEnd = this.getPixelPosition(highlight.time_range[1]);
			const width = xEnd - xStart;

			this.highlight(xStart, width, depths[i], highlight.color);
		});
	};

	/**
	 * Computes the source-row base position for a flagged document in the current frame.
	 * @param doc Flagged document to position.
	 * @returns Row position and timestamp metadata, or null when its source is hidden.
	 */
	private getYForDoc = (
		doc: Doc.Type,
	): { y: number; t: number; o: number } | null => {
		const source = this.visibleSourceById.get(doc["gulp.source_id"]);
		const rowIndex = this.sourceRowIndexById.get(doc["gulp.source_id"]) ?? -1;
		if (!source || rowIndex === -1) {
			return null;
		}

		return {
			y: Source.Entity.getHeight(this.info.app, source, 0, rowIndex),
			t: doc.gulp_timestamp,
			o: source.settings.offset ?? 0,
		};
	};
	/**
	 * Draws green line on position of every flagged event for the current operation
	 */
	public highlightFlaggedDocuments = () => {
		const operation = Operation.Entity.selected(this.info.app);
		const flagged = Doc.Entity.flag.getList(operation?.id);
		const canvasWidth = this.ctx.canvas.width;

		for (const id of flagged) {
			const doc = Doc.Entity.id(this.info.app, id);
			if (!doc) continue;
			const source = this.visibleSourceById.get(doc["gulp.source_id"]);
			if (!source) continue;

			const position = this.getYForDoc(doc);
			if (!position) continue;
			const { y, t, o } = position;

			const x = this.getPixelPosition(t + o);
			//if event is outside canvas, skip it
			if (x < -10 || x > canvasWidth + 10) continue;
			const dy = y - 24 - this.scrollY;

			this.ctx.fillStyle = Color.Themer.theme.FONT_ACCENT;
			this.ctx.fillRect(x - 1, dy - 2, 1, 51);
			this.ctx.fillRect(x, dy - 4, 1, 55);
			this.ctx.fillRect(x + 1, dy - 2, 1, 51);
		}
	};

	/* NOTE MANAGEMENT */
	/**
	 * Draws one note badge, or a grouped note badge, at the provided note row.
	 * @param note Note data used for icon, label, color, source, and row placement.
	 * @param groupNotes Notes represented by this badge for interaction handling.
	 * @param centerX Optional Canvas x center used for grouped badge placement.
	 * @returns Nothing.
	 */
	public renderNote = (
		note: Note.Type,
		groupNotes: Note.Type[] = [note],
		centerX?: number,
	) => {
		const noteCenterX =
			typeof centerX === "number"
				? centerX
				: this.getPixelPosition(this.getNoteRenderTimestamp(note));
		const x = noteCenterX + NOTE_OFFSET;
		const rowIndex = this.sourceRowIndexById.get(note.source_id) ?? -1;
		const y =
			Source.Entity.getHeight(
				this.info.app,
				note.source_id,
				this.scrollY,
				rowIndex,
			) + NOTE_OFFSET;

		// Hit detection per l'Hover
		const isHovered =
			this.mouseX !== undefined &&
			this.mouseY !== undefined &&
			this.mouseX >= x &&
			this.mouseX <= x + NOTE_SIZE &&
			this.mouseY >= y &&
			this.mouseY <= y + NOTE_SIZE;

		this.ctx.save();

		const noteBorder = Color.Themer.theme.BORDER;
		const noteFill = Color.Themer.theme.BACKGROUND_SECOND;
		const noteAccent = Note.Entity.resolvedColor(note);
		const noteIcon = Color.Themer.theme.FONT_ACCENT;

		// EFFETTO HOVER
		if (isHovered) {
			this.ctx.shadowColor = noteBorder;
			this.ctx.shadowBlur = 12; // Effetto Glow
			this.ctx.lineWidth = 2;
			this.ctx.strokeStyle = noteBorder;
		}

		// Main
		this.drawRect(x, y, NOTE_SIZE, NOTE_SIZE, 5, noteBorder, noteFill);
		// Accent
		this.drawRect(
			x,
			y + NOTE_SIZE - 4,
			NOTE_SIZE,
			4,
			4,
			noteBorder,
			noteAccent,
		);

		if (isHovered) this.ctx.stroke();

		const icon = getCanvasIcon({
			name: Note.Entity.icon(note),
			color: noteIcon,
		});
		const iconSize = 16;
		const iconX = x + (NOTE_SIZE - iconSize) / 2;
		const iconY = y + (NOTE_SIZE - iconSize) / 2 - 1;

		this.ctx.drawImage(icon, iconX, iconY, iconSize, iconSize);

		this.drawTextLabel(note.name, x + NOTE_SIZE / 2, y + 26, 120, noteBorder);
		this.ctx.restore();

		RenderEngine.interactiveNotes.push({
			notes: groupNotes,
			rect: { x, y, w: NOTE_SIZE, h: NOTE_SIZE },
		});
	};

	private getVisibleNotes = (
		file: Source.Type,
	): {
		notes: Note.Type[];
		min: number;
		max: number;
	} => {
		const notes = Note.Entity.findByFile(this.info.app, file.id);
		const min = this.getTimestamp(-128) - file.settings.offset;
		const max =
			this.getTimestamp(this.ctx.canvas.width + 128) - file.settings.offset;

		// Since notes are sorted in descending order, we need to find:
		// - first note <= max (leftmost visible note)
		// - last note >= min (rightmost visible note)
		const start = this.binarySearchDesc(notes, max, true); // Find first note <= max
		const end = this.binarySearchDesc(notes, min, false); // Find last note >= min

		if (start === -1 || end === -1 || start > end) {
			return { notes: [], min, max };
		}

		return { notes: notes.slice(start, end + 1), min, max };
	};

	/**
	 * Calculates non-overlapping visible note badge groups for one source row.
	 * @param file Source identifier whose visible notes should be grouped.
	 * @returns Visible notes with group start, count, and rendered x center.
	 */
	private calculateNotesGroups(file: Source.Type): {
		notes: Note.Type[];
		groups: Group[];
	} {
		const visibleNotes = this.getVisibleNotes(file);
		const notes = visibleNotes.notes;
		const cacheKey = [
			this.info.app.timeline.scale,
			Math.floor(visibleNotes.min),
			Math.ceil(visibleNotes.max),
			this.scrollX,
			file.settings.offset,
			DataStore.notes.length,
		].join(":");

		if (notes.length === 0) {
			RenderEngine[CacheKey].notes.set(file.id, {
				key: cacheKey,
				groups: [],
			});
			return {
				notes: [],
				groups: [],
			};
		}

		const cachedGroups = RenderEngine[CacheKey].notes.get(file.id);
		if (cachedGroups?.key === cacheKey) {
			return {
				notes,
				groups: cachedGroups.groups,
			};
		}

		const groups: Group[] = [];
		const positionedNotes = this.resolveNotePositions(notes);

		for (let i = 0; i < notes.length; ) {
			const groupEndIdx = this.findGroupEndIndexDirect(positionedNotes, i);
			groups.push([
				i,
				groupEndIdx - i + 1,
				this.getPositionedNoteGroupCenterX(positionedNotes, i, groupEndIdx),
			]);
			i = groupEndIdx + 1;
		}

		RenderEngine[CacheKey].notes.set(file.id, {
			key: cacheKey,
			groups,
		});
		return { notes, groups };
	}

	/**
	 * Resolves Canvas x centers for notes once so grouping does not repeatedly convert timestamps.
	 * @param notes Visible notes sorted by descending timestamp.
	 * @returns Notes paired with their Canvas x center.
	 */
	private resolveNotePositions(notes: Note.Type[]): PositionedNote[] {
		const positionedNotes = new Array<PositionedNote>(notes.length);

		for (let index = 0; index < notes.length; index++) {
			positionedNotes[index] = {
				x: this.getPixelPosition(this.getNoteRenderTimestamp(notes[index])),
			};
		}

		return positionedNotes;
	}

	/**
	 * Resolves the rendered note timestamp with the owning source offset applied.
	 * @param note Note whose document timestamp should be drawn.
	 * @returns Timeline timestamp after source offset.
	 */
	private getNoteRenderTimestamp(note: Note.Type): number {
		const source = this.visibleSourceById.get(note.source_id);
		return Note.Entity.timestamp(note) + (source?.settings.offset ?? 0);
	}

	/**
	 * Computes the midpoint where a grouped note badge should be rendered.
	 * @param positionedNotes Notes paired with their Canvas x centers.
	 * @param startIdx First note index in the group.
	 * @param endIdx Last note index in the group.
	 * @returns Canvas x center exactly halfway between first and last grouped note.
	 */
	private getPositionedNoteGroupCenterX(
		positionedNotes: PositionedNote[],
		startIdx: number,
		endIdx: number,
	): number {
		return (positionedNotes[startIdx].x + positionedNotes[endIdx].x) / 2;
	}

	/**
	 * Finds the final index for a non-overlapping note badge group.
	 * @param positionedNotes Visible notes paired with their Canvas x centers.
	 * @param startIdx First candidate note index for the group.
	 * @returns Last note index whose event center remains within one badge width.
	 */
	private findGroupEndIndexDirect(
		positionedNotes: PositionedNote[],
		startIdx: number,
	): number {
		let endIdx = startIdx;
		const startNoteX = positionedNotes[startIdx].x;

		for (let i = startIdx + 1; i < positionedNotes.length; i++) {
			const candidateGroupWidth = Math.abs(startNoteX - positionedNotes[i].x);

			if (candidateGroupWidth <= NOTE_SIZE) {
				endIdx = i;
			} else {
				break;
			}
		}

		return endIdx;
	}

	// Fixed binary search for descending order
	private binarySearchDesc(
		notes: Note.Type[],
		timestamp: number,
		findFirst: boolean,
	): number {
		let left = 0,
			right = notes.length - 1,
			result = -1;

		while (left <= right) {
			const mid = Math.floor((left + right) / 2);
			const noteTime = Note.Entity.timestamp(notes[mid]);

			if (findFirst) {
				// Finding first note <= timestamp (leftmost)
				if (noteTime <= timestamp) {
					result = mid;
					right = mid - 1; // Look for earlier index (smaller timestamp)
				} else {
					left = mid + 1; // Current note too large, go right
				}
			} else {
				// Finding last note >= timestamp (rightmost)
				if (noteTime >= timestamp) {
					result = mid;
					left = mid + 1; // Look for later index (larger timestamp)
				} else {
					right = mid - 1; // Current note too small, go left
				}
			}
		}

		return result;
	}

	/**
	 * Draws all visible note badges for the provided source rows.
	 * @param files Source rows currently visible in the Canvas viewport.
	 * @returns Nothing.
	 */
	public drawNotes = (files: Source.Type[]): void => {
		if (!files || !files.length) return;
		RenderEngine.interactiveNotes = [];
		files.forEach((file) => {
			const { notes, groups } = this.calculateNotesGroups(file);
			if (!notes.length) {
				return;
			}

			groups.forEach((group) => {
				if (!group || group.length < 2 || !notes[group[0]]) return;
				const [groupIndex, groupCount, groupCenterX] = group;

				const groupNotes = notes
					.slice(groupIndex, groupIndex + groupCount)
					.filter((n) => Doc.Entity.id(this.info.app, n.doc._id));
				if (groupNotes.length === 0) return;
				this.stats.visibleNotes += groupNotes.length;

				if (groupNotes.length === 1) {
					this.renderNote(groupNotes[0]);
				} else {
					this.renderNote(
						{
							...groupNotes[0],
							name: `${groupNotes.length}`,
							color: Color.Themer.theme.FONT_ACCENT,
							glyph_id: Glyph.getIdByName("Status"),
						},
						groupNotes,
						groupCenterX,
					);
				}
			});
		});
		this.stats.interactiveRects += RenderEngine.interactiveNotes.length;
	};

	/* CANVAS DRAW Utility*/

	public dot = ({ x, y, color }: Dot) => {
		this.ctx.fillStyle = Color.Themer.theme.FONT_ACCENT;
		this.ctx.beginPath();
		if (typeof this.ctx.roundRect == "function") {
			this.ctx.roundRect(x - 4, y - 4, 8, 8, [999]);
		}

		this.ctx.fill();
		this.ctx.fillStyle = color;
		this.ctx.beginPath();
		if (typeof this.ctx.roundRect == "function") {
			this.ctx.roundRect(x - 3, y - 3, 6, 6, [999]);
		}

		this.ctx.fill();
	};

	public calcDots = (
		link: Link.Type,
		linkedDocs?: Doc.Type[],
	): {
		dots: Dot[];
	} => {
		const dots: Dot[] = [];
		const docs =
			linkedDocs ??
			this.getUniqueLinkDocIds(link)
				.map((id) => Doc.Entity.id(this.info.app, id))
				.filter(Boolean);

		docs.forEach((e) => {
			const source = this.visibleSourceById.get(e["gulp.source_id"]);
			const index = this.sourceRowIndexById.get(e["gulp.source_id"]);
			if (!source || typeof index === "undefined") {
				return;
			}

			const x = this.getPixelPosition(
				e.gulp_timestamp + (source.settings.offset || 0),
			);
			const y = index * 48 + 20 - this.scrollY || 0;
			const color = link.color || Color.Themer.theme.FONT_ACCENT;

			dots.push({
				x,
				y,
				color: color.endsWith("48") ? color.slice(-2) : color,
			});
		});

		return { dots };
	};

	public locals = (file: Source.Type, y?: number) => {
		y =
			typeof y === "number"
				? y
				: Source.Entity.getHeight(
						this.info.app,
						file,
						this.scrollY,
						this.visibleSources.findIndex((s) => s.id === file.id),
					);

		const right =
			this.getPixelPosition(file.timestamp.max + file.settings.offset) + 12;
		const left =
			this.getPixelPosition(file.timestamp.min + file.settings.offset) - 12;
		const line = {
			one: y - 6,
			two: y + 4,
			three: y + 14,
		};

		this.ctx.fillStyle = Color.Themer.theme.FONT_ACCENT;
		this.ctx.fillRect(
			this.getPixelPosition(file.timestamp.max + file.settings.offset) + 1,
			y - 24,
			1,
			48 - 1,
		);
		this.ctx.fillRect(
			this.getPixelPosition(file.timestamp.min + file.settings.offset) - 1,
			y - 24,
			1,
			48 - 1,
		);

		const events = Doc.Entity.get(this.info.app, file.id).length.toString();

		this.ctx.fillStyle = Color.Themer.theme.FONT_ACCENT;

		this.ctx.textAlign = "left";
		this.ctx.fillStyle = Color.Themer.theme.FONT_ACCENT;
		this.ctx.fillText(file.total.toString(), right, line.one);
		this.ctx.fillStyle = Color.Themer.theme.FONT_SECOND;
		this.ctx.fillText(
			format(file.timestamp.max + file.settings.offset, "dd.MM.yyyy"),
			right,
			line.two,
		);
		this.ctx.fillStyle = Color.Themer.theme.FONT_SECOND;
		this.ctx.fillText(events, right, line.three);

		this.ctx.textAlign = "right";
		this.ctx.fillStyle = Color.Themer.theme.FONT_ACCENT;
		this.ctx.fillText(file.total.toString(), left, line.one);
		this.ctx.fillStyle = Color.Themer.theme.FONT_SECOND;
		this.ctx.fillText(
			format(file.timestamp.min + file.settings.offset, "dd.MM.yyyy"),
			left,
			line.two,
		);
		this.ctx.fillStyle = Color.Themer.theme.FONT_SECOND;
		this.ctx.fillText(events, left, line.three);

		if (requestStore.hasLoadingForFile(file.id)) {
			this.loading(file);
		}
	};

	private drawRect(
		x: number,
		y: number,
		w: number,
		h: number,
		r: number,
		accent: string,
		color = Color.Themer.theme.BACKGROUND_SECOND,
	) {
		this.ctx.fillStyle = color;
		this.ctx.strokeStyle = accent;
		this.ctx.lineWidth = 1;

		this.ctx.beginPath();
		if (typeof this.ctx.roundRect === "function") {
			this.ctx.roundRect(x, y, w, h, r);
		} else {
			this.ctx.rect(x, y, w, h);
		}
		this.ctx.fill();
		this.ctx.stroke();
	}

	private drawTextLabel(
		text: string,
		x: number,
		y: number,
		maxWidth: number,
		accent: string,
	) {
		this.ctx.font = '10px "GeistMono", sans-serif';
		const textWidth = this.ctx.measureText(text).width;
		const padding = 5;
		const labelWidth = Math.min(
			Math.max(textWidth + padding * 2, NOTE_SIZE),
			maxWidth,
		);
		const labelHeight = 20;

		const labelX = x - labelWidth / 2;

		this.drawRect(
			labelX,
			y + NOTE_SIZE - labelHeight - 2,
			labelWidth,
			labelHeight,
			5,
			accent,
		);

		this.ctx.fillStyle = Color.Themer.theme.FONT_ACCENT;
		this.ctx.textAlign = "center";
		this.ctx.textBaseline = "middle";

		let displayText = text;
		if (textWidth > maxWidth - padding * 2) {
			while (
				this.ctx.measureText(displayText + "...").width >
					maxWidth - padding * 2 &&
				displayText.length > 0
			) {
				displayText = displayText.slice(0, -1);
			}
			displayText += "...";
		}

		this.ctx.fillText(displayText, x, y + NOTE_SIZE - labelHeight / 2 - 2);
	}

	public getTimestamp = (x: number): number => {
		const visibleWidth = this.ctx.canvas.width * this.info.app.timeline.scale;
		const pixelOffset = x + this.scrollX;
		return (
			this.info.app.timeline.frame.min +
			(pixelOffset / visibleWidth) *
				(this.info.app.timeline.frame.max - this.info.app.timeline.frame.min)
		);
	};

	/**
	 * Finds the grouped notes closest to a Canvas x coordinate.
	 * @param file Source row whose note groups should be searched.
	 * @param x Canvas x coordinate to match against grouped badge centers.
	 * @param padding Maximum accepted distance from the grouped badge center.
	 * @returns Notes in the closest matching group, or an empty list.
	 */
	public static getNotesByX = (
		file: Source.Type,
		x: number,
		padding = 16,
	): Note.Type[] => {
		if (!RenderEngine.instance) {
			return [];
		}

		const { notes, groups } =
			RenderEngine.instance.calculateNotesGroups(file);
		if (groups.length === 0) {
			return [];
		}

		let bestGroup: [number, number] | null = null;
		let bestDistance = Infinity;

		for (const [groupIndex, groupCount, groupCenterX] of groups) {
			const distance = Math.abs(groupCenterX - x);

			if (distance < bestDistance) {
				bestDistance = distance;
				bestGroup = [groupIndex, groupCount];
			} else {
				break;
			}
		}

		if (bestGroup && bestDistance <= padding) {
			const [groupIndex, groupCount] = bestGroup;
			return notes.slice(groupIndex, groupIndex + groupCount);
		}

		return [];
	};

	public loading = (file: Source.Type, y?: number) => {
		this.ctx.beginPath();
		this.ctx.strokeStyle = Color.Themer.theme.FONT_ACCENT;
		if (this.ctx.setLineDash) {
			this.ctx.setLineDash([5, 5]);
		}

		const height =
			typeof y === "number"
				? y
				: Source.Entity.getHeight(
						this.info.app,
						file,
						this.scrollY,
						this.visibleSources.findIndex((s) => s.id === file.id),
					);
		this.ctx.moveTo(0, height);
		this.ctx.lineTo(2000, height);
		this.ctx.stroke();
		if (this.ctx.setLineDash) {
			this.ctx.setLineDash([]);
		}
	};

	public draw_info = (file: Source.Type, y?: number) => {
		y =
			(typeof y === "number"
				? y
				: Source.Entity.getHeight(
						this.info.app,
						file,
						this.scrollY,
						this.visibleSources.findIndex((s) => s.id === file.id),
					)) + 4;
		const x = 10;
		const lineHeight = 14;

		const requestType = Source.Entity.getRequestType(this.info.app, file);
		const suffix = !requestType
			? ""
			: requestType === Request.Prefix.INGESTION
				? translate("renderEngine.ingestingSuffix")
				: translate("renderEngine.loadingSuffix");

		const context = Source.Entity.context(this.info.app, file);
		const lines: Array<{ text: string; dy: number; color: string }> = [
			{
				text: file.name + suffix,
				dy: 0,
				color: Color.Themer.theme.FONT_ACCENT,
			},
			{
				text: Source.Entity.events(this.info.app, file).length.toString(),
				dy: lineHeight,
				color: Color.Themer.theme.FONT_ACCENT,
			},
			{
				text:
					`${file.total.toString()} | ${context?.name || "Unknown"}` + suffix,
				dy: -lineHeight,
				color: Color.Themer.theme.FONT_SECOND,
			},
		];

		this.ctx.font = "12px sans-serif";

		this.ctx.textAlign = "left";
		for (const { text, dy, color } of lines) {
			this.ctx.fillStyle = color;
			this.ctx.fillText(text, x, y + dy);
		}
	};

	public target = (targetFile?: Source.Type, targetY?: number) => {
		if (!this.info.app.timeline.target) return;

		const file = Source.Entity.id(
			this.info.app,
			this.info.app.timeline.target["gulp.source_id"],
		);

		if (!file) return;
		if (targetFile && targetFile.id !== file.id) return;

		const index = this.sourceRowIndexById.get(file.id) ?? -1;

		if (index === -1) return;

		const rowY =
			typeof targetY === "number"
				? targetY - 1
				: Source.Entity.getHeight(this.info.app, file, this.scrollY, index) - 1;

		this.ctx.save();
		this.ctx.globalAlpha = 0.7;
		this.ctx.strokeStyle = Color.Themer.getTargetGuideColor();
		this.ctx.lineWidth = 1;
		this.ctx.lineCap = "round";
		if (this.ctx.setLineDash) {
			this.ctx.setLineDash([1, 3]);
		}
		this.ctx.beginPath();
		this.ctx.moveTo(0, rowY + 0.5);
		this.ctx.lineTo(this.ctx.canvas.width, rowY + 0.5);
		this.ctx.stroke();
		this.ctx.restore();
	};

	public targetMarker = () => {
		if (!this.info.app.timeline.target) return;

		const file = Source.Entity.id(
			this.info.app,
			this.info.app.timeline.target["gulp.source_id"],
		);

		if (!file) return;

		const index = this.sourceRowIndexById.get(file.id) ?? -1;

		if (index === -1) return;

		const targetX = this.getPixelPosition(
			this.info.app.timeline.target.gulp_timestamp + file.settings.offset,
		);

		this.ctx.save();
		this.ctx.globalAlpha = 0.7;
		this.ctx.strokeStyle = Color.Themer.getTargetGuideColor();
		this.ctx.lineWidth = 1;
		this.ctx.lineCap = "round";
		if (this.ctx.setLineDash) {
			this.ctx.setLineDash([1, 3]);
		}
		this.ctx.beginPath();
		this.ctx.moveTo(targetX + 0.5, 0);
		this.ctx.lineTo(targetX + 0.5, this.ctx.canvas.height);
		this.ctx.stroke();
		this.ctx.restore();
	};

	/* MEMORY MANAGEMENT */

	/** Clears a specific render cache (notes, range, or flags). Used to force recalculation. */
	public static reset = (key: keyof (typeof RenderEngine)[typeof CacheKey]) => {
		RenderEngine[CacheKey][key].clear();
	};

	/**
	 * Clears note grouping cache for one source after its notes change.
	 * @param sourceId Source identifier whose note groups should be recalculated.
	 * @returns Nothing.
	 */
	public static resetSourceNotes = (sourceId: Source.Id): void => {
		RenderEngine[CacheKey].notes.delete(sourceId);
	};

	/**
	 * Clears render caches that are scoped to one source after its events change.
	 * @param sourceId Source identifier whose cached render data should be invalidated.
	 * @returns Nothing.
	 */
	public static resetSource = (sourceId: Source.Id): void => {
		RenderEngine[CacheKey].notes.delete(sourceId);
		RenderEngine[CacheKey].range.delete(sourceId);
		RenderEngine[CacheKey].flags.clear();
		RenderEngine[CacheKey].links.clear();
		RenderEngine.interactiveNotes = [];
		RenderEngine.interactiveLinks = [];

		if (DefaultEngine.instance) {
			DefaultEngine.instance.clearSourceCache(sourceId);
		}
		if (HeightEngine.instance) {
			HeightEngine.instance.map.delete(sourceId);
			HeightEngine.instance.cacheKeys.delete(sourceId);
		}
		if (GraphEngine.instance) {
			GraphEngine.instance.clearSourceCache(sourceId);
		}
	};

	/**
	 * Comprehensive memory cleanup — clears ALL render-related caches.
	 * Must be called when switching operations to prevent memory leaks.
	 *
	 * Clears:
	 * - RenderEngine's note, range, flag, link, and interactive hit-region caches
	 * - DefaultEngine event and color-override indexes
	 * - HeightEngine and GraphEngine derived event caches
	 *
	 * @returns Nothing.
	 */
	public static clearAllCaches = (): void => {
		RenderEngine[CacheKey].notes.clear();
		RenderEngine[CacheKey].range.clear();
		RenderEngine[CacheKey].flags.clear();
		RenderEngine[CacheKey].links.clear();
		RenderEngine.interactiveNotes = [];
		RenderEngine.interactiveLinks = [];

		if (DefaultEngine.instance) {
			DefaultEngine.instance.clearCache();
		}
		if (HeightEngine.instance) {
			HeightEngine.instance.map.clear();
			HeightEngine.instance.cacheKeys.clear();
		}
		if (GraphEngine.instance) {
			GraphEngine.instance.clearCache();
		}
	};
}

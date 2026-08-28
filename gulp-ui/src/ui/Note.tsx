import { Application } from "@/context/Application.context";
import { DisplayGroupDialog } from "@/dialogs/Group.dialog";
import { Point as UIPoint } from "./Point";
import { Icon } from "@/ui/Icon";
import s from "./styles/Note.module.css";
import { cn } from "@/ui/utils";
import { formatTimestampToReadableString, stringToHexColor } from "./utils";
import { Stack } from "./Stack";
import { Badge } from "./Badge";
import { Banner as UIBanner } from "./Banner";
import { Button } from "./Button";
import { Doc } from "@/entities/Doc";
import { Context } from "@/entities/Context";
import { Source } from "@/entities/Source";
import { DisplayEventDialog } from "@/dialogs/Event.dialog";
import { Note } from "@/entities/Note";
import { DataStore } from "@/store/DataStore";
import { useMemo, useState } from "react";
import { useCallback } from "react";
import { toast } from "sonner";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/ui/Tooltip";
import { Locale } from "@/locales";

export namespace NotePoint {
	export interface Props extends Omit<
		UIPoint.Props,
		"icon" | "accent" | "name"
	> {
		notes: Note.Type[];
	}

	export namespace Combination {
		export interface Props extends Omit<Stack.Props, "onClick"> {
			note: Note.Type;
			onTargetClick?: (note: Note.Type) => void;
			onDeleteClick?: (note: Note.Type) => void;
		}
	}

	export interface FetchEventBannerProps {
		note?: Note.Type;
		notes?: readonly Note.Type[];
		onIncluded?: (events: Doc.Type[]) => void;
	}

	/**
	 * Offers one explicit fetch action for note targets outside the active query.
	 * Partial results remain included and a subsequent attempt requests only missing IDs.
	 * @param props Single or grouped notes and an optional completion callback.
	 * @returns Localized fetch banner.
	 */
	export function FetchEventBanner({
		note,
		notes,
		onIncluded,
	}: FetchEventBannerProps) {
		const { Info, spawnDialog, destroyBanner } = Application.use();
		const { t } = Locale.use();
		const [loading, setLoading] = useState<boolean>(false);
		const targets = useMemo(
			() => notes ?? (note ? [note] : []),
			[note, notes],
		);
		const uniqueTargetCount = new Set(targets.map((target) => target.doc._id)).size;

		/** Fetches unresolved targets and closes the banner only after full resolution. */
		const fetch = async (): Promise<void> => {
			setLoading(true);
			try {
				const result = await Info.includeNoteEvents(targets);
				if (result.stale) return;
				if (result.missingDocumentIds.length > 0) {
					toast.error(
						t(
							result.missingDocumentIds.length === 1
								? "notePoint.fetchFailed"
								: "notePoint.fetchFailedMany",
							{ count: result.missingDocumentIds.length },
						),
					);
					return;
				}
				destroyBanner();
				if (onIncluded) {
					onIncluded(result.events);
				} else if (result.events.length === 1) {
					spawnDialog(<DisplayEventDialog event={result.events[0]} />);
				} else if (result.events.length > 1) {
					spawnDialog(
						<DisplayGroupDialog events={result.events} groupKind="notes" />,
					);
				}
			} catch {
				toast.error(
					t(
						uniqueTargetCount === 1
							? "notePoint.fetchFailed"
							: "notePoint.fetchFailedMany",
						{ count: uniqueTargetCount },
					),
				);
			} finally {
				setLoading(false);
			}
		};

		return (
			<UIBanner
				title={t(
					uniqueTargetCount === 1
						? "notePoint.fetchTitle"
						: "notePoint.fetchTitleMany",
					{ count: uniqueTargetCount },
				)}
				done={
					<Button
						loading={loading}
						icon="MagnifyingGlass"
						variant="glass"
						onClick={fetch}
					/>
				}
			>
				<p>
					{uniqueTargetCount === 1 ? (
						<>
							{t("notePoint.fetchDescriptionBefore")}{" "}
							<code>{targets[0]?.name ?? targets[0]?.doc._id}</code>{" "}
							{t("notePoint.fetchDescriptionAfter")}
						</>
					) : (
						t("notePoint.fetchDescriptionMany", {
							count: uniqueTargetCount,
						})
					)}
				</p>
			</UIBanner>
		);
	}

	export function Combination({
		className,
		style,
		note,
		onTargetClick,
		onDeleteClick,
		...props
	}: Combination.Props) {
		const { app } = Application.use();
		const { t } = Locale.use();
		const context = Context.Entity.id(app, note.context_id);
		const sourceId = Note.Entity.sourceId(note);
		const source = sourceId
			? (Source.Entity.id(app, sourceId) as Source.Type | undefined)
			: undefined;
		const contextName = context?.name ?? t("collab.deletedContext");
		const sourceName = source?.name ?? t("collab.deletedSource");
		const noteColor = Note.Entity.color(note);

		return (
			<Stack
				className={cn(
					s.combination,
					note.tags.includes("auto") && s.hidden,
					className,
				)}
				style={{
					...style,
					background: "transparent",
				}}
				{...props}
			>
				<p>{formatTimestampToReadableString(note.doc.gulp_timestamp)}</p>
				<Icon
					name={Note.Entity.icon(note)}
					style={{ color: noteColor }}
					size={16}
				/>
				<p style={{ color: noteColor }}>{note.name}</p>
				<Tooltip>
					<TooltipTrigger asChild>
						<span style={{ cursor: "help", opacity: 0.9 }}>{note.text}</span>
					</TooltipTrigger>
					<TooltipContent>{note.text}</TooltipContent>
				</Tooltip>
				<Stack className={s.badge_wrapper}>
					<Badge
						value={`${contextName} / ${sourceName}`}
						style={{
							background: stringToHexColor(note.context_id),
							border: `1px solid ${stringToHexColor(note.context_id)}40`,
						}}
					/>
					{note.tags.map((t) => (
						<Badge
							value={t}
							icon={
								isTagAreSeverityIndicator(t)
									? NotePoint.getIconFromNoteSeverity(note)
									: undefined
							}
							variant={
								(isTagAreSeverityIndicator(t)
									? NotePoint.getColorFromNoteSeverity(note)
									: "gray-subtle") as any
							}
						/>
					))}
				</Stack>

				<Button
					icon="MagnifyingGlassSmall"
					onClick={() => onTargetClick?.(note)}
					variant="glass"
				/>
				<Button
					icon="Trash2"
					onClick={() => onDeleteClick?.(note)} //{() => Info.note_delete(note)}
					variant="glass"
				/>
			</Stack>
		);
	}

	export function Point({ notes, ...props }: NotePoint.Props) {
		const { spawnBanner, spawnDialog } = Application.use();

		/** Opens a fully resolved single-note or grouped-note target. */
		const openEvents = useCallback(
			(events: Doc.Type[]): void => {
				if (events.length === 1) {
					spawnDialog(<DisplayEventDialog event={events[0]} />);
				} else if (events.length > 1) {
					spawnDialog(
						<DisplayGroupDialog events={events} groupKind="notes" />,
					);
				}
			},
			[spawnDialog],
		);

		/**
		 * Opens the event or group dialog for the notes represented by this point.
		 *
		 * @returns Event dialog for one note target or notes group dialog for many.
		 */
		const handleClick = useCallback(async (): Promise<void> => {
			const ids = [...new Set(notes.map((n) => n.doc._id))];
			const sourceId = Note.Entity.sourceId(notes[0]);
			if (!sourceId) return;

			let events: Doc.Type[] = [];
			try {
				events = (await DataStore.Events.getByIds(ids)).filter(
					(event) => event["gulp.source_id"] === sourceId,
				);
			} catch {
				// The explicit inclusion flow remains available if the local query store is unavailable.
			}
			if (events.length !== ids.length) {
				spawnBanner(
					<FetchEventBanner notes={notes} onIncluded={openEvents} />,
				);
				return;
			}
			openEvents(events);
		}, [notes, openEvents, spawnBanner]);

		return (
			<UIPoint
				onClick={() => void handleClick()}
				icon={notes.length > 1 ? "Status" : Note.Entity.icon(notes[0])}
				accent={notes.length > 1 ? "#e8e8e8" : Note.Entity.color(notes[0])}
				name={(notes.length > 1 ? notes.length : notes[0].name).toString()}
				{...props}
			/>
		);
	}

	export type Severity =
		| "critical"
		| "high"
		| "medium"
		| "low"
		| "informational";

	export const SeverityToColorMap: Record<Severity, string> = {
		critical: "red",
		high: "amber",
		medium: "blue",
		low: "green",
		informational: "gray",
	} as const;

	export type SeverityToColorMap = typeof SeverityToColorMap;

	export const SeverityToIconMap: Record<Severity, Icon.Name> = {
		critical: "Warning",
		high: "DataPoint",
		medium: "DataPointMedium",
		low: "DataPointLow",
		informational: "Information",
	} as const;

	export type SeverityToIconMap = typeof SeverityToIconMap;

	/**
	 * O(1) complexity
	 */
	export const getSeverityFromNote = (note: Note.Type): Severity => {
		const target = note.tags.find((tag) => isTagAreSeverityIndicator(tag));
		if (!target) {
			return "informational";
		}

		const splited = target.split("_");
		if (splited.length !== 2) {
			return "informational";
		}

		const key: Severity =
			splited[1] in SeverityToColorMap
				? (splited[1] as Severity)
				: "informational";

		return key;
	};

	export const getIconFromNoteSeverity = (note: Note.Type) =>
		SeverityToIconMap[getSeverityFromNote(note)];

	export const getColorFromNoteSeverity = (note: Note.Type) =>
		SeverityToColorMap[getSeverityFromNote(note)];

	export const isTagAreSeverityIndicator = (tag: string): boolean =>
		tag.startsWith("severity_");
}

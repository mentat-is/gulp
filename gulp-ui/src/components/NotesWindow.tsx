import "@/global.css";
import {
	useState,
	useMemo,
	useRef,
	useCallback,
	useEffect,
	useSyncExternalStore,
} from "react";
import { Application } from "@/context/Application.context";
import { Source } from "@/entities/Source";
import { Note } from "@/entities/Note";
import { Select } from "@/ui/Select";
import { Stack } from "@/ui/Stack";
import { Input } from "@/ui/Input";
import { Banner as UIBanner } from "@/ui/Banner";
import { DataStore } from "@/store/DataStore";
import { Checkbox } from "@/ui/Checkbox";
import { Button } from "@/ui/Button";
import { Toggle } from "@/ui/Toggle";
import { formatTimestampToReadableString } from "../ui/utils";
import { WindowBridge } from "@/lib/WindowBridge";
import { Table } from "./Table";
import { Icon } from "@/ui/Icon";
import { Locale } from "@/locales";

import s from "./styles/NotesWindow.module.css";

interface FloatingWindowProps {
	onClose: () => void;
}

interface BulkDeleteNotesBannerProps {
	noteIds: Note.Id[];
	onDeleted: () => void;
}

interface NotesTableRow extends Record<string, unknown> {
	id: Note.Id;
	_id: Note.Id;
	timestamp: string;
	title: string;
	text: string;
	contextName: string;
	contextColor: string;
	sourceName: string;
	sourceColor: string;
	tags: string;
	color: string;
	rawNote: Note.Type;
	icon: Icon.Name;
}

const NOTES_TABLE_VIRTUALIZATION = {
	threshold: 20,
	overscan: 8,
	estimatedRowHeight: 28,
} as const;

/**
 * Resolves the source id attached to a note, tolerating older payload shapes.
 * @param note Note whose owning source should be resolved.
 * @returns Source id from the note entity or its embedded document.
 */
function getNoteSourceId(note: Note.Type): Source.Id {
	return note.source_id ?? note.doc["gulp.source_id"];
}

/**
 * Builds a stable signature for the sources selected by the user.
 * @param sources Source list from the current application state.
 * @returns Sorted source ID signature used to detect selection changes.
 */
function BulkDeleteNotesBanner({
	noteIds,
	onDeleted,
}: BulkDeleteNotesBannerProps) {
	const { Info, destroyBanner } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState(false);
	const [isSubmitted, setIsSubmitted] = useState(false);

	const confirmDelete = async () => {
		setLoading(true);
		await Info.notes_delete_bulk(noteIds);
		setLoading(false);
		onDeleted();
		destroyBanner();
	};

	return (
		<UIBanner
			title={t("notes.deleteTitle")}
			done={
				<Button
					loading={loading}
					icon="Trash2"
					variant="glass"
					onClick={confirmDelete}
					disabled={!isSubmitted}
				/>
			}
		>
			<p>{t("notes.deleteConfirm", { count: noteIds.length })}</p>
			<Toggle
				option={[t("common.noDontDelete"), t("common.yesImSure")]}
				checked={isSubmitted}
				onCheckedChange={setIsSubmitted}
			/>
		</UIBanner>
	);
}

export function NotesWindow({ onClose }: FloatingWindowProps) {
	const { app, Info, spawnBanner, banner } = Application.use();
	const { t } = Locale.use();
	const dataStoreVersion = useSyncExternalStore(
		DataStore.subscribe,
		DataStore.getSnapshot,
	);
	const visibleSourceSignature = useSyncExternalStore(
		Info.subscribeVisibleSourceIds,
		Info.getVisibleSourceSignature,
		Info.getVisibleSourceSignature,
	);
	const [search, setSearch] = useState("");
	const [showOnlyVisibleSources, setShowOnlyVisibleSources] =
		useState<boolean>(false);
	const [selectedNoteIds, setSelectedNoteIds] = useState<Set<Note.Id>>(
		new Set(),
	);

	/**
	 * Collects note tags from the current DataStore snapshot.
	 * @returns Lowercase tag values available for filtering.
	 */
	const getAvailableTags = useCallback(() => {
		const tags = new Set<string>();
		DataStore.notes.forEach((note) => {
			note.tags.forEach((tag) => tags.add(tag.toLowerCase()));
		});
		return [...tags.values()];
	}, [dataStoreVersion]);

	const [selectedTags, setSelectedTags] = useState<Set<string>>(new Set());

	const availableTags = useMemo(
		() => getAvailableTags(),
		[getAvailableTags],
	);

	const sortedAvailableTags = useMemo(
		() => [...availableTags].sort(),
		[availableTags],
	);

	useEffect(() => {
		const currentAvailable = new Set(availableTags.map((t) => t.toLowerCase()));
		setSelectedTags((prev) => {
			const next = new Set(
				[...prev].filter((tag) => currentAvailable.has(tag.toLowerCase())),
			);
			return next.size === prev.size ? prev : next;
		});
	}, [availableTags]);

	useEffect(() => {
		const currentNoteIds = new Set(DataStore.notes.map((note) => note.id));
		setSelectedNoteIds((prev) => {
			const next = new Set([...prev].filter((id) => currentNoteIds.has(id)));
			return next.size === prev.size ? prev : next;
		});
	}, [dataStoreVersion]);

	const windowRef = useRef<HTMLDivElement>(null);

	/**
	 * Memoizes the source IDs most recently reported as rendered by the Canvas.
	 * @returns Set containing Canvas-rendered source IDs used by the notes filter.
	 */
	const visibleSourceIds = useMemo(() => {
		return Info.getVisibleSourceIds();
	}, [Info, visibleSourceSignature]);

	const sourceById = useMemo(
		() => new Map(app.target.files.map((source) => [source.id, source])),
		[app.target.files],
	);
	const contextById = useMemo(
		() => new Map(app.target.contexts.map((context) => [context.id, context])),
		[app.target.contexts],
	);
	const selectedTagValues = useMemo(
		() => [...selectedTags],
		[selectedTags],
	);

	const sortedNotes = useMemo(() => {
		const lowerSearch = search.toLowerCase();

		const filtered = DataStore.notes
			.filter((n) => {
				const sourceId = getNoteSourceId(n);
				const source = sourceById.get(sourceId);
				const context = contextById.get(n.context_id);
				const sourceName = source?.name.toLowerCase() ?? "";
				const contextName = context?.name.toLowerCase() ?? "";

				return (
					!search ||
					n.name.toLowerCase().includes(lowerSearch) ||
					n.text.toLowerCase().includes(lowerSearch) ||
					sourceName.includes(lowerSearch) ||
					contextName.includes(lowerSearch) ||
					n.tags.some((t) => t.toLowerCase() === lowerSearch)
				);
			})
			.filter(
				(n) =>
					!selectedTagValues.length ||
					selectedTagValues.every((tag) =>
						n.tags.map((t) => t.toLowerCase()).includes(tag),
					),
			)
			.filter(
				(n) => !showOnlyVisibleSources || visibleSourceIds.has(getNoteSourceId(n)),
			);

		return filtered.sort((a, b) => {
			const byContext = a.context_id.localeCompare(b.context_id);
			return byContext !== 0
				? byContext
				: getNoteSourceId(a).localeCompare(getNoteSourceId(b));
		});
	}, [
		search,
		contextById,
		sourceById,
		selectedTagValues,
		showOnlyVisibleSources,
		visibleSourceIds,
		dataStoreVersion,
	]);

	const isAllSelected = useMemo(() => {
		return (
			sortedNotes.length > 0 &&
			sortedNotes.every((n) => selectedNoteIds.has(n.id))
		);
	}, [sortedNotes, selectedNoteIds]);

	const selectAllLabel = useMemo(() => {
		if (isAllSelected) return t("notes.deselectAll", { count: selectedNoteIds.size });
		return t("notes.selectAll", { count: sortedNotes.length });
	}, [isAllSelected, selectedNoteIds.size, sortedNotes.length, t]);

	const handleSelectAll = useCallback(
		(checked: boolean) => {
			if (checked) {
				setSelectedNoteIds(new Set(sortedNotes.map((n) => n.id)));
			} else {
				setSelectedNoteIds(new Set());
			}
		},
		[sortedNotes],
	);

	const handleBulkDelete = async () => {
		if (selectedNoteIds.size === 0) return;
		const deletedIds = [...selectedNoteIds];
		spawnBanner(
			<BulkDeleteNotesBanner
				noteIds={deletedIds}
				onDeleted={() => setSelectedNoteIds(new Set())}
			/>,
			"table",
		);
	};

	const targetNoteButtonHandler = useCallback((note: Note.Type) => {
		const bridge = WindowBridge.create(WindowBridge.generateId(), () => {});
		bridge.send(WindowBridge.MessageType.TARGET_NOTE, {
			docId: note.doc._id,
			operationId: note.operation_id,
		});
		bridge.destroy();
	}, []);

	const handleDelete = useCallback(
		(note: Note.Type) => {
			spawnBanner(<Note.Delete.Banner note={note} />, "table");
		},
		[spawnBanner],
	);

	/**
	 * Transforms sortedNotes into a flat data format suited for the generic Table component.
	 */
	const tableValues = useMemo<NotesTableRow[]>(() => {
		return sortedNotes.map((note) => {
			const context = contextById.get(note.context_id);
			const source = sourceById.get(getNoteSourceId(note));
			const sourceColor = source?.color || "var(--accent)";
			const noteColor = Note.Entity.color(note);
			return {
				id: note.id,
				_id: note.id,
				timestamp: formatTimestampToReadableString(note.doc.gulp_timestamp),
				title: note.name,
				text: note.text,
				contextName: context?.name || "",
				contextColor: context?.color || "var(--accent)",
				sourceName: source?.name || "",
				sourceColor,
				tags: note.tags.join(", ") || "-",
				color: noteColor,
				rawNote: note,
				icon: Note.Entity.icon(note),
			};
		});
	}, [sortedNotes, contextById, sourceById]);

	/**
	 * Maps selected note IDs to matching indices in the sorted tableValues list.
	 */
	const selectedrows = useMemo(() => {
		const indices = new Set<number>();
		tableValues.forEach((item, index) => {
			if (selectedNoteIds.has(item.id)) {
				indices.add(index);
			}
		});
		return indices;
	}, [tableValues, selectedNoteIds]);

	/**
	 * Handles individual row selection toggle, converting index to note ID.
	 *
	 * @param index - Index of the toggled row
	 * @param selected - New selection state of the checkbox
	 */
	const handleRowSelect = useCallback(
		(index: number, selected: boolean) => {
			const item = tableValues[index];
			if (!item) return;
			setSelectedNoteIds((prev) => {
				const next = new Set(prev);
				if (selected) {
					next.add(item.id);
				} else {
					next.delete(item.id);
				}
				return next;
			});
		},
		[tableValues],
	);

	/**
	 * Actions available per-row in the notes table.
	 */
	const tableActions = useMemo<Table.Action<NotesTableRow>[]>(
		() => [
			{
				icon: "MagnifyingGlassSmall",
				label: t("notes.targetNote"),
				onClick: (row) => {
					targetNoteButtonHandler(row.rawNote);
				},
			},
			{
				icon: "Trash2",
				label: t("note.deleteTitle"),
				onClick: (row) => {
					handleDelete(row.rawNote);
				},
			},
		],
		[targetNoteButtonHandler, handleDelete, t],
	);

	/**
	 * Column configuration for the unified Table component, using custom render functions for title and context.
	 */
	const columns = useMemo<Table.ColumnDefinition<NotesTableRow>[]>(
		() => [
			{
				key: "title",
				label: t("common.title"),
				width: 180,
				render: (value, row) => (
					<span
						className={s.titleValue}
						style={{ color: row.color }}
					>
						<Icon
							name={row.icon}
							color="currentColor"
							size={12}
							className={s.titleIcon}
						/>
						{row.title}
					</span>
				),
			},
			{
				key: "timestamp",
				label: t("common.timestamp"),
				width: 190,
			},
			{
				key: "text",
				label: t("common.text"),
				width: 280,
			},
			{
				key: "contextName",
				label: t("common.context"),
				width: 160,
				render: (value, row) => (
					<span
						className={s.colorValue}
						style={{ color: row.contextColor }}
					>
						{row.contextName}
					</span>
				),
			},
			{
				key: "sourceName",
				label: t("common.source"),
				width: 180,
				render: (value, row) => (
					<span
						className={s.colorValue}
						style={{ color: row.sourceColor }}
					>
						{row.sourceName}
					</span>
				),
			},
			{
				key: "tags",
				label: t("common.tags"),
				width: 180,
			},
		],
		[t],
	);

	return (
		<div className={s.main}>
			<div className={s.header}>
				<h3>{t("notes.title")}</h3>
			</div>
			<div className={s.content}>
				<Stack
					dir="column"
					flex={false}
					ai="stretch"
				>
					<Input
						placeholder={t("notes.searchPlaceholder")}
						icon="MagnifyingGlass"
						variant="highlighted"
						value={search}
						onChange={(e) => setSearch(e.target.value)}
					/>
					<Select.Multi.Root
						value={[...selectedTags]}
						onValueChange={(values) => setSelectedTags(new Set(values))}
					>
						<Select.Trigger>
							<Select.Multi.Value
								icon={["DataPointMedium", "DataPoint"]}
								placeholder={t("notes.selectTags")}
								text={(len) =>
									typeof len === "number" ? t("notes.selectedTags", { count: len }) : len
								}
							/>
						</Select.Trigger>
						<Select.Content container={windowRef.current ?? undefined}>
							{sortedAvailableTags.map((tag) => (
								<Select.Item
									key={tag}
									value={tag}
								>
									{tag}
								</Select.Item>
							))}
						</Select.Content>
					</Select.Multi.Root>
				</Stack>
				<Stack
					gap={10}
					ai="center"
					flex={false}
					style={{ padding: "0 12px" }}
				>
					<Checkbox
						style={{ height: 20, width: 20 }}
						checked={showOnlyVisibleSources}
						onCheckedChange={(checked) =>
							setShowOnlyVisibleSources(checked === true)
						}
					/>
					<span
						className={s.helperText}
						onClick={() => setShowOnlyVisibleSources((value) => !value)}
					>
						{t("notes.visibleSourcesOnly")}
					</span>
				</Stack>
				<div
					className={s.result}
				>
					<Table
						values={tableValues}
						columns={columns}
						includeIndex={false}
						selectable={true}
						selectedrows={selectedrows}
						onrowselect={handleRowSelect}
						onSelectAll={handleSelectAll}
						actions={tableActions}
						highlightedId={app.timeline.target?._id}
						columnVisibility={true}
						persistId="notes_table"
						virtualization={NOTES_TABLE_VIRTUALIZATION}
					/>
				</div>
			</div>
			<div className={s.footer}>
				<Stack jc="flex-end">
					<Button
						variant="secondary"
						disabled={selectedNoteIds.size === 0}
						onClick={handleBulkDelete}
						icon="Trash2"
					>
						{t("notes.deleteSelected", { count: selectedNoteIds.size })}
					</Button>
				</Stack>
			</div>
			{banner?.target === "table" ? banner.node : null}
			<div
				ref={windowRef}
				className={s.portalContainer}
			/>
		</div>
	);
}

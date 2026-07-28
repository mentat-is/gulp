import "@/global.css";
import {
	useState,
	useMemo,
	useRef,
	useCallback,
	useEffect,
	useSyncExternalStore,
} from "react";
import { toast } from "sonner";
import { Application } from "@/context/Application.context";
import { Source } from "@/entities/Source";
import { Note } from "@/entities/Note";
import { Select } from "@/ui/Select";
import { Stack } from "@/ui/Stack";
import { Input } from "@/ui/Input";
import { Banner as UIBanner } from "@/ui/Banner";
import { DataStore, type NotesSyncChange } from "@/store/DataStore";
import { Checkbox } from "@/ui/Checkbox";
import { Button } from "@/ui/Button";
import { Toggle } from "@/ui/Toggle";
import { Popover } from "@/ui/Popover";
import { Notification } from "@/ui/Notification";
import { formatTimestampToReadableString } from "../ui/utils";
import { WindowBridge } from "@/lib/WindowBridge";
import { Table } from "./Table";
import { Icon } from "@/ui/Icon";
import { Locale } from "@/locales";
import {
	acknowledgeNotesChangeSequence,
	advanceNotesChangeSequence,
	buildNotesListFilters,
	getCanvasRenderedSourceIdSet,
	getNoteSourceId,
	getSearchMatchingNoteIds,
	noteMatchesNotesWindowFilter,
} from "@/utils";

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

const NOTES_PAGE_SIZES = [20, 50, 100] as const;
const NOTES_SEARCH_DEBOUNCE_MSEC = 250;

/**
 * Renders the confirmation banner for deleting notes selected across pages.
 * @param props Selected note identifiers and success callback.
 * @returns Bulk-delete confirmation banner.
 */
function BulkDeleteNotesBanner({
	noteIds,
	onDeleted,
}: BulkDeleteNotesBannerProps) {
	const { Info, destroyBanner } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState(false);
	const [isSubmitted, setIsSubmitted] = useState(false);

	/**
	 * Deletes all selected notes and refreshes the parent table.
	 * @returns Promise resolved after local note state is synchronized.
	 */
	const confirmDelete = async (): Promise<void> => {
		setLoading(true);
		try {
			await Info.notes_delete_bulk(noteIds);
			onDeleted();
			destroyBanner();
		} finally {
			setLoading(false);
		}
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

/**
 * Displays a paginated, filterable Notes table for the selected operation.
 * @param props Detached-window lifecycle properties.
 * @returns Notes window content.
 */
export function NotesWindow({ onClose: _onClose }: FloatingWindowProps) {
	const {
		Info,
		app,
		spawnBanner,
		banner,
		detachedStatus,
	} = Application.use();
	const { t } = Locale.use();
	const dataStoreVersion = useSyncExternalStore(
		DataStore.subscribeOverlays,
		DataStore.getOverlaysSnapshot,
	);
	const [search, setSearch] = useState("");
	const [appliedSearch, setAppliedSearch] = useState("");
	const [selectedTags, setSelectedTags] = useState<Set<string>>(new Set());
	const [showOnlyVisibleSources, setShowOnlyVisibleSources] = useState(false);
	const [selectedNoteIds, setSelectedNoteIds] = useState<Set<Note.Id>>(
		new Set(),
	);
	const [queryNotes, setQueryNotes] = useState<Note.Type[]>(() => [
		...DataStore.notes,
	]);
	const [pageNotes, setPageNotes] = useState<Note.Type[]>([]);
	const [totalHits, setTotalHits] = useState(0);
	const [pageSize, setPageSize] = useState(50);
	const [currentPage, setCurrentPage] = useState(1);
	const [pendingNotesChangeSequence, setPendingNotesChangeSequence] =
		useState<number | null>(null);
	const [loading, setLoading] = useState(false);
	const [refreshRevision, setRefreshRevision] = useState(0);
	const refreshAcknowledgementRef = useRef<number | null>(null);
	const notesRequestControllerRef = useRef<AbortController | null>(null);
	const windowRef = useRef<HTMLDivElement>(null);
	const selectedOperationId = app.target.operations.find(
		(operation) => operation.selected,
	)?.id;

	const selectedSourceIds = useMemo(
		() =>
			app.target.files
				.filter((source) => source.selected)
				.map((source) => source.id)
				.sort((left, right) => left.localeCompare(right)),
		[app.target.files],
	);
	const selectedSourceIdsSignature = selectedSourceIds.join(",");

	const canvasRenderedSourceIds = useMemo(
		() => getCanvasRenderedSourceIdSet(app),
		[
			app.target.files,
			app.target.contexts,
			app.timeline.filter,
			app.timeline.renderVersion,
			app.timeline.renderedSourceIds,
			app.hidden.sourcesWithNoEvents,
		],
	);

	const filteredSourceIds = useMemo(
		() =>
			showOnlyVisibleSources
				? selectedSourceIds.filter((sourceId) =>
						canvasRenderedSourceIds.has(sourceId),
					)
				: selectedSourceIds,
		[selectedSourceIdsSignature, showOnlyVisibleSources, canvasRenderedSourceIds],
	);
	const filteredSourceIdSet = useMemo(
		() => new Set(filteredSourceIds),
		[filteredSourceIds],
	);
	const sourceById = useMemo(
		() => new Map(app.target.files.map((source) => [source.id, source])),
		[app.target.files],
	);
	const contextById = useMemo(
		() => new Map(app.target.contexts.map((context) => [context.id, context])),
		[app.target.contexts],
	);
	const selectedTagValues = useMemo(
		() => [...selectedTags].sort((left, right) => left.localeCompare(right)),
		[selectedTags],
	);

	const availableTags = useMemo(() => {
		const tags = new Set<string>();
		queryNotes.forEach((note) => {
			note.tags.forEach((tag) => tags.add(tag.toLowerCase()));
		});
		return [...tags].sort((left, right) => left.localeCompare(right));
	}, [queryNotes]);

	const searchMatchingIds = useMemo(
		() =>
			getSearchMatchingNoteIds(
				queryNotes,
				appliedSearch,
				sourceById,
				contextById,
			),
		[queryNotes, appliedSearch, sourceById, contextById],
	);

	const allFilteredNoteIds = useMemo(
		() =>
			DataStore.notes
				.filter((note) =>
					noteMatchesNotesWindowFilter(note, {
						search: appliedSearch,
						selectedTags: selectedTagValues,
						sourceIds: filteredSourceIdSet,
						sourceById,
						contextById,
					}),
				)
				.map((note) => note.id),
		[
			dataStoreVersion,
			appliedSearch,
			selectedTagValues,
			filteredSourceIdSet,
			sourceById,
			contextById,
		],
	);

	const { pageFilter, countFilter } = useMemo(
		() =>
			buildNotesListFilters({
				searchMatchingIds,
				sourceIds: filteredSourceIds,
				selectedTags: selectedTagValues,
				pageSize,
				currentPage,
			}),
		[
			searchMatchingIds,
			filteredSourceIds,
			selectedTagValues,
			pageSize,
			currentPage,
		],
	);

	const queryCanReturnNotes =
		filteredSourceIds.length > 0 &&
		(searchMatchingIds === undefined || searchMatchingIds.length > 0);
	const totalPages = Math.max(1, Math.ceil(totalHits / pageSize));

	/**
	 * Acknowledges one explicit refresh without hiding changes that arrived later.
	 * @param sequence Realtime sequence captured when the refresh started.
	 * @returns Nothing.
	 */
	const acknowledgeSuccessfulRefresh = useCallback(
		(sequence: number | null): void => {
			if (sequence === null) return;
			setPendingNotesChangeSequence((pendingSequence) =>
				acknowledgeNotesChangeSequence(pendingSequence, sequence),
			);
		},
		[],
	);

	/**
	 * Fetches the active note page and matching total while rejecting stale results.
	 * @returns Promise resolved after the latest request updates table state.
	 */
	const fetchNotesPage = useCallback(async (): Promise<void> => {
		const refreshSequence = refreshAcknowledgementRef.current;
		refreshAcknowledgementRef.current = null;
		notesRequestControllerRef.current?.abort();
		if (!queryCanReturnNotes) {
			notesRequestControllerRef.current = null;
			setPageNotes([]);
			setTotalHits(0);
			setLoading(false);
			acknowledgeSuccessfulRefresh(refreshSequence);
			return;
		}

		const requestController = new AbortController();
		notesRequestControllerRef.current = requestController;
		setPageNotes([]);
		setLoading(true);
		try {
			const [notes, count] = await Promise.all([
				Info.notes_list(pageFilter, requestController.signal),
				Info.notes_count(countFilter, requestController.signal),
			]);
			if (
				requestController.signal.aborted ||
				notesRequestControllerRef.current !== requestController
			) {
				return;
			}

			const nextTotalPages = Math.max(1, Math.ceil(count / pageSize));
			setTotalHits(count);
			if (currentPage > nextTotalPages) {
				acknowledgeSuccessfulRefresh(refreshSequence);
				setCurrentPage(nextTotalPages);
				return;
			}
			setPageNotes(notes);
			acknowledgeSuccessfulRefresh(refreshSequence);
		} catch {
			if (!requestController.signal.aborted) {
				toast.error(t("tableView.fetchFailed"));
			}
		} finally {
			if (notesRequestControllerRef.current === requestController) {
				notesRequestControllerRef.current = null;
				setLoading(false);
			}
		}
	}, [
		Info,
		pageFilter,
		countFilter,
		queryCanReturnNotes,
		pageSize,
		currentPage,
		t,
		refreshRevision,
		acknowledgeSuccessfulRefresh,
	]);

	useEffect(() => {
		fetchNotesPage();
	}, [fetchNotesPage]);

	useEffect(
		() => () => notesRequestControllerRef.current?.abort(),
		[],
	);

	useEffect(() => {
		const timeout = window.setTimeout(() => {
			setAppliedSearch(search);
			setCurrentPage(1);
			setSelectedNoteIds(new Set());
		}, NOTES_SEARCH_DEBOUNCE_MSEC);
		return () => window.clearTimeout(timeout);
	}, [search]);

	useEffect(() => {
		const availableTagSet = new Set(availableTags);
		setSelectedTags((previousTags) => {
			const nextTags = new Set(
				[...previousTags].filter((tag) =>
					availableTagSet.has(tag.toLowerCase()),
				),
			);
			return nextTags.size === previousTags.size ? previousTags : nextTags;
		});
	}, [availableTags]);

	useEffect(() => {
		const currentNoteIds = new Set(DataStore.notes.map((note) => note.id));
		setSelectedNoteIds((previousIds) => {
			const nextIds = new Set(
				[...previousIds].filter((noteId) => currentNoteIds.has(noteId)),
			);
			return nextIds.size === previousIds.size ? previousIds : nextIds;
		});
	}, [dataStoreVersion]);

	useEffect(() => {
		/**
		 * Separates silent authoritative snapshots from realtime list changes.
		 * @param change Note synchronization event delivered in this window realm.
		 * @returns Nothing.
		 */
		const handleNotesSyncChange = (change: NotesSyncChange): void => {
			if (change.operationId !== selectedOperationId) return;

			if (change.mutation === "snapshot-ready") {
				setQueryNotes([...DataStore.notes]);
				return;
			}
			setPendingNotesChangeSequence((pendingSequence) =>
				advanceNotesChangeSequence(pendingSequence, change),
			);
		};

		return DataStore.subscribeNotesSyncChanges(handleNotesSyncChange);
	}, [selectedOperationId]);

	useEffect(() => {
		const notesSnapshot = [...DataStore.notes];
		setQueryNotes(notesSnapshot);
		refreshAcknowledgementRef.current = null;
		setPendingNotesChangeSequence(null);
		setCurrentPage(1);
		setSelectedNoteIds(new Set());
		setRefreshRevision((revision) => revision + 1);
	}, [selectedOperationId, selectedSourceIdsSignature, detachedStatus]);

	/**
	 * Acknowledges the latest canonical notes and reloads the first result page.
	 * @returns Nothing.
	 */
	const refreshNotes = useCallback((): void => {
		const notesSnapshot = [...DataStore.notes];
		setQueryNotes(notesSnapshot);
		refreshAcknowledgementRef.current =
			pendingNotesChangeSequence;
		setCurrentPage(1);
		setRefreshRevision((revision) => revision + 1);
	}, [pendingNotesChangeSequence]);

	/**
	 * Applies a new tag selection and resets query-dependent selection state.
	 * @param values Selected lowercase tag values.
	 * @returns Nothing.
	 */
	const handleSelectedTagsChange = useCallback((values: string[]): void => {
		setSelectedTags(new Set(values));
		setCurrentPage(1);
		setSelectedNoteIds(new Set());
	}, []);

	/**
	 * Toggles the rendered-source restriction and resets pagination.
	 * @returns Nothing.
	 */
	const toggleVisibleSources = useCallback((): void => {
		setShowOnlyVisibleSources((visibleOnly) => !visibleOnly);
		setCurrentPage(1);
		setSelectedNoteIds(new Set());
	}, []);

	/**
	 * Selects or deselects every note displayed on the current page.
	 * @param checked Whether current-page note IDs should be selected.
	 * @returns Nothing.
	 */
	const handleSelectPage = useCallback(
		(checked: boolean): void => {
			setSelectedNoteIds((previousIds) => {
				const nextIds = new Set(previousIds);
				pageNotes.forEach((note) => {
					if (checked) nextIds.add(note.id);
					else nextIds.delete(note.id);
				});
				return nextIds;
			});
		},
		[pageNotes],
	);

	/**
	 * Selects every filtered DataStore note or clears the complete selection.
	 * @param checked Whether all filtered note IDs should be selected.
	 * @returns Nothing.
	 */
	const handleSelectAllFiltered = useCallback(
		(checked: boolean): void => {
			setSelectedNoteIds(
				checked ? new Set(allFilteredNoteIds) : new Set(),
			);
		},
		[allFilteredNoteIds],
	);

	/**
	 * Opens the confirmation banner for the current cross-page selection.
	 * @returns Nothing.
	 */
	const handleBulkDelete = useCallback((): void => {
		if (selectedNoteIds.size === 0) return;
		spawnBanner(
			<BulkDeleteNotesBanner
				noteIds={[...selectedNoteIds]}
				onDeleted={() => {
					setSelectedNoteIds(new Set());
				}}
			/>,
			"table",
		);
	}, [selectedNoteIds, spawnBanner]);

	/**
	 * Targets the event associated with a note in the main operation window.
	 * @param note Note whose event should be targeted.
	 * @returns Nothing.
	 */
	const targetNoteButtonHandler = useCallback((note: Note.Type): void => {
		const bridge = WindowBridge.create(WindowBridge.generateId(), () => {});
		bridge.send(WindowBridge.MessageType.TARGET_NOTE, {
			docId: note.doc._id,
			operationId: note.operation_id,
		});
		bridge.destroy();
	}, []);

	/**
	 * Opens the existing row-delete confirmation.
	 * @param note Note selected by the row action.
	 * @returns Nothing.
	 */
	const handleDelete = useCallback(
		(note: Note.Type): void => {
			spawnBanner(
				<Note.Delete.Banner
					note={note}
				/>,
				"table",
			);
		},
		[spawnBanner],
	);

	const tableValues = useMemo<NotesTableRow[]>(
		() =>
			pageNotes.map((note) => {
				const context = contextById.get(note.context_id);
				const source = sourceById.get(getNoteSourceId(note));
				return {
					id: note.id,
					_id: note.id,
					timestamp: formatTimestampToReadableString(
						note.doc.gulp_timestamp,
					),
					title: note.name,
					text: note.text,
					contextName: context?.name || "",
					contextColor: context?.color || "var(--accent)",
					sourceName: source?.name || "",
					sourceColor: source?.color || "var(--accent)",
					tags: note.tags.join(", ") || "-",
					color: Note.Entity.color(note),
					rawNote: note,
					icon: Note.Entity.icon(note),
				};
			}),
		[pageNotes, contextById, sourceById],
	);

	const selectedRows = useMemo(() => {
		const indices = new Set<number>();
		tableValues.forEach((item, index) => {
			if (selectedNoteIds.has(item.id)) indices.add(index);
		});
		return indices;
	}, [tableValues, selectedNoteIds]);

	const isAllFilteredSelected =
		allFilteredNoteIds.length > 0 &&
		allFilteredNoteIds.every((noteId) => selectedNoteIds.has(noteId));

	/**
	 * Converts a table row checkbox index into its stable note ID.
	 * @param index Current-page row index.
	 * @param selected Whether the row should be selected.
	 * @returns Nothing.
	 */
	const handleRowSelect = useCallback(
		(index: number, selected: boolean): void => {
			const item = tableValues[index];
			if (!item) return;
			setSelectedNoteIds((previousIds) => {
				const nextIds = new Set(previousIds);
				if (selected) nextIds.add(item.id);
				else nextIds.delete(item.id);
				return nextIds;
			});
		},
		[tableValues],
	);

	const tableActions = useMemo<Table.Action<NotesTableRow>[]>(
		() => [
			{
				icon: "MagnifyingGlassSmall",
				label: t("notes.targetNote"),
				onClick: (row) => targetNoteButtonHandler(row.rawNote),
			},
			{
				icon: "Trash2",
				label: t("note.deleteTitle"),
				onClick: (row) => handleDelete(row.rawNote),
			},
		],
		[targetNoteButtonHandler, handleDelete, t],
	);

	const columns = useMemo<Table.ColumnDefinition<NotesTableRow>[]>(
		() => [
			{
				key: "title",
				label: t("common.title"),
				width: 180,
				render: (_value, row) => (
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
				label: t("notes.eventTimestamp"),
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
				render: (_value, row) => (
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
				render: (_value, row) => (
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
				{pendingNotesChangeSequence !== null && (
					<Notification
						variant="warning"
						icon="Warning"
						className={s.newNotesWarning}
						jc="space-between"
						gap={8}
					>
						<span className={s.newNotesMessage}>
							{t("notes.changesAvailable")}
						</span>
						<Button
							variant="secondary"
							icon="RotateCcw"
							onClick={refreshNotes}
						>
							{t("common.refresh")}
						</Button>
					</Notification>
				)}

				<div className={s.filters}>
					<Input
						placeholder={t("notes.searchPlaceholder")}
						icon="MagnifyingGlass"
						variant="highlighted"
						value={search}
						onChange={(event) => setSearch(event.target.value)}
					/>
					<Select.Multi.Root
						value={[...selectedTags]}
						onValueChange={handleSelectedTagsChange}
					>
						<Select.Trigger>
							<Select.Multi.Value
								icon={["DataPointMedium", "DataPoint"]}
								placeholder={t("notes.selectTags")}
								text={(length) =>
									typeof length === "number"
										? t("notes.selectedTags", { count: length })
										: length
								}
							/>
						</Select.Trigger>
						<Select.Content container={windowRef.current ?? undefined}>
							{availableTags.map((tag) => (
								<Select.Item
									key={tag}
									value={tag}
								>
									{tag}
								</Select.Item>
							))}
						</Select.Content>
					</Select.Multi.Root>
					<Stack
						gap={10}
						ai="center"
						flex={false}
						className={s.visibleSourceFilter}
					>
						<Checkbox
							checked={showOnlyVisibleSources}
							onCheckedChange={toggleVisibleSources}
						/>
						<span
							className={s.helperText}
							onClick={toggleVisibleSources}
						>
							{t("notes.visibleSourcesOnly")}
						</span>
					</Stack>
				</div>

				<div className={s.actionBar}>
					<div className={s.actionBarLeft}>
						<Stack
							gap={10}
							ai="center"
							flex={false}
							className={s.selectionControl}
						>
							<Checkbox
								checked={isAllFilteredSelected}
								onCheckedChange={(checked) =>
									handleSelectAllFiltered(checked === true)
								}
							/>
							<span
								className={s.helperText}
								onClick={() =>
									handleSelectAllFiltered(!isAllFilteredSelected)
								}
							>
								{t("notes.selectAllFiltered", {
									count: allFilteredNoteIds.length,
								})}
							</span>
						</Stack>

						<Popover.Root>
							<Popover.Trigger asChild>
								<Button
									variant="secondary"
									icon="ChevronDown"
									disabled={selectedNoteIds.size === 0}
								>
									{t("common.action")}
								</Button>
							</Popover.Trigger>
							<Popover.Content
								align="start"
								sideOffset={4}
								className={s.popoverContent}
								container={windowRef.current}
							>
								<div
									className={s.menuItem}
									onClick={handleBulkDelete}
								>
									<Icon
										name="Trash2"
										size={14}
									/>
									<span>
										{t("notes.deleteSelected", {
											count: selectedNoteIds.size,
										})}
									</span>
								</div>
							</Popover.Content>
						</Popover.Root>

					</div>

					<div className={s.actionBarRight}>
						<span className={s.helperText}>
							{t("tableView.showingRange", {
								from:
									totalHits > 0 ? pageSize * (currentPage - 1) + 1 : 0,
								to: Math.min(pageSize * currentPage, totalHits),
								total: totalHits,
							})}
						</span>
						<Stack
							gap={8}
							ai="center"
						>
							<Select.Root
								value={pageSize.toString()}
								onValueChange={(value) => {
									setPageSize(Number(value));
									setCurrentPage(1);
								}}
							>
								<Select.Trigger className={s.pageSelect}>
									<Select.Value />
								</Select.Trigger>
								<Select.Content container={windowRef.current ?? undefined}>
									{NOTES_PAGE_SIZES.map((size) => (
										<Select.Item
											key={size}
											value={size.toString()}
										>
											{size}
										</Select.Item>
									))}
								</Select.Content>
							</Select.Root>
							<span className={s.helperText}>
								{t("tableView.perPage")}
							</span>
						</Stack>
						<span className={s.helperText}>
							{t("tableView.pageOf", {
								page: currentPage,
								total: totalPages,
							})}
						</span>
						<Stack gap={4}>
							<Button
								variant="secondary"
								icon="ChevronLeft"
								disabled={currentPage <= 1 || loading}
								onClick={() =>
									setCurrentPage((page) => Math.max(1, page - 1))
								}
								title={t("tableView.previousPage")}
							/>
							<Button
								variant="secondary"
								icon="ChevronRight"
								disabled={currentPage >= totalPages || loading}
								onClick={() =>
									setCurrentPage((page) =>
										Math.min(totalPages, page + 1),
									)
								}
								title={t("tableView.nextPage")}
							/>
						</Stack>
					</div>
				</div>

				<div className={s.result}>
					{loading ? (
						<p className={s.placeholder}>{t("tableView.loadingData")}</p>
					) : tableValues.length > 0 ? (
						<Table
							values={tableValues}
							columns={columns}
							includeIndex={false}
							selectable={true}
							selectedrows={selectedRows}
							onrowselect={handleRowSelect}
							onSelectAll={handleSelectPage}
							actions={tableActions}
							highlightedId={app.timeline.target?._id}
							columnVisibility={true}
							persistId="notes_table_paginated_v2"
							virtualization={NOTES_TABLE_VIRTUALIZATION}
						/>
					) : (
						<p className={s.placeholder}>{t("tableView.noData")}</p>
					)}
				</div>
			</div>
			{banner?.target === "table" ? banner.node : null}
			<div
				ref={windowRef}
				className={s.portalContainer}
			/>
		</div>
	);
}

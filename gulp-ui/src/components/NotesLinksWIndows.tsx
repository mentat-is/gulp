import "@/global.css";
import {
	type MouseEvent as ReactMouseEvent,
	useState,
	useMemo,
	useRef,
	useCallback,
	useEffect,
	useSyncExternalStore,
} from "react";
import { toast } from "sonner";
import { Application } from "@/context/Application.context";
import { Doc } from "@/entities/Doc";
import { Note } from "@/entities/Note";
import { Link } from "@/entities/Link";
import { Select } from "@/ui/Select";
import { Stack } from "@/ui/Stack";
import { Input } from "@/ui/Input";
import { Banner as UIBanner } from "@/ui/Banner";
import {
	DataStore,
	type LinksRealtimeSyncChange,
	type NotesSyncChange,
} from "@/store/DataStore";
import { Checkbox } from "@/ui/Checkbox";
import { Button } from "@/ui/Button";
import { Toggle } from "@/ui/Toggle";
import { Notification } from "@/ui/Notification";
import { formatTimestampToReadableString } from "../ui/utils";
import { WindowBridge } from "@/lib/WindowBridge";
import { Table } from "./Table";
import { Icon } from "@/ui/Icon";
import { TableHeader } from "@/ui/TableHeader";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/ui/Tabs";
import { Locale } from "@/locales";
import { DisplayGroupDialog } from "@/dialogs/Group.dialog";
import {
	acknowledgeNotesChangeSequence,
	advanceNotesChangeSequence,
	buildLinksListFilters,
	applyNotesLinksWIndowsUpdateDelta,
	buildNotesListFilters,
	getCanvasRenderedSourceIdSet,
	getRelatedLinkEvents,
	getNoteSourceId,
	getSearchMatchingLinkIds,
	getSearchMatchingNoteIds,
	noteMatchesNotesLinksWIndowsFilter,
} from "@/utils";

import s from "./styles/NotesLinksWIndows.module.css";

interface FloatingWindowProps {
	onClose: () => void;
}

interface BulkDeleteNotesBannerProps {
	noteIds: Note.Id[];
	onDeleted: () => void;
}

interface BulkDeleteLinksBannerProps {
	linkIds: Link.Id[];
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

interface LinksTableRow extends Record<string, unknown> {
	id: Link.Id;
	_id: Link.Id;
	title: string;
	description: string;
	tags: string;
	originEventId: Doc.Id;
	color: string;
	rawLink: Link.Type;
	icon: Icon.Name;
}

interface NotesQuerySnapshot {
	operationId: Note.Type["operation_id"] | undefined;
	notes: Note.Type[];
}

interface LinksQuerySnapshot {
	operationId: Link.Type["operation_id"] | undefined;
	links: Link.Type[];
}

interface NotesPageRequest {
	pageFilter: Note.ListFilter;
	countFilter: Note.ListFilter;
	queryCanReturnNotes: boolean;
	pageSize: number;
	currentPage: number;
}

interface LinksPageRequest {
	pageFilter: Link.ListFilter;
	countFilter: Link.ListFilter;
	queryCanReturnLinks: boolean;
	pageSize: number;
	currentPage: number;
}

const NOTES_TABLE_VIRTUALIZATION = {
	threshold: 20,
	overscan: 8,
	estimatedRowHeight: 28,
} as const;

const TABLE_PAGE_SIZES = [20, 50, 100] as const;
const TABLE_SEARCH_DEBOUNCE_MSEC = 250;
const EMPTY_NOTE_IDS: Note.Id[] = [];
const EMPTY_LINK_IDS: Link.Id[] = [];

/**
 * Keeps an array reference stable while its ordered primitive values are equal.
 * @param values Current semantic array values.
 * @returns Previous array reference for an equivalent value sequence.
 */
function useStableSemanticArray<T>(values: T[]): T[] {
	const stableValuesRef = useRef(values);
	const previousValues = stableValuesRef.current;
	const valuesChanged =
		previousValues.length !== values.length ||
		values.some((value, index) => !Object.is(value, previousValues[index]));
	if (valuesChanged) {
		stableValuesRef.current = values;
	}
	return stableValuesRef.current;
}

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
 * Renders the confirmation banner for deleting links selected across pages.
 * @param props Selected link identifiers and success callback.
 * @returns Bulk-delete confirmation banner.
 */
function BulkDeleteLinksBanner({
	linkIds,
	onDeleted,
}: BulkDeleteLinksBannerProps) {
	const { Info, destroyBanner } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState(false);
	const [isSubmitted, setIsSubmitted] = useState(false);

	/**
	 * Deletes all selected links and closes the confirmation banner.
	 * @returns Promise resolved after link state has been synchronized.
	 */
	const confirmDelete = async (): Promise<void> => {
		setLoading(true);
		try {
			await Info.links_delete_bulk(linkIds);
			onDeleted();
			destroyBanner();
		} finally {
			setLoading(false);
		}
	};

	return (
		<UIBanner
			title={t("links.deleteSelected", { count: linkIds.length })}
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
			<p>{t("links.deleteConfirm", { count: linkIds.length })}</p>
			<Toggle
				option={[t("common.noDontDelete"), t("common.yesImSure")]}
				checked={isSubmitted}
				onCheckedChange={setIsSubmitted}
			/>
		</UIBanner>
	);
}

/**
 * Displays paginated, filterable Notes and Links tables for the selected operation.
 * @param props Detached-window lifecycle properties.
 * @returns Notes & Links window content.
 */
export function NotesLinksWIndows({ onClose: _onClose }: FloatingWindowProps) {
	const {
		Info,
		app,
		spawnBanner,
		banner,
		detachedClientKind,
		detachedCoordinatorId,
		detachedSelectedEventId,
		setDetachedNotesRenderedSync,
	} = Application.use();
	const { t } = Locale.use();
	const infoRef = useRef(Info);
	infoRef.current = Info;
	const translateRef = useRef(t);
	translateRef.current = t;
	const dataStoreVersion = useSyncExternalStore(
		DataStore.subscribeOverlays,
		DataStore.getOverlaysSnapshot,
	);
	const selectedOperationId = app.target.operations.find(
		(operation) => operation.selected,
	)?.id;
	const operationLinks = useMemo(
		() =>
			DataStore.links.filter(
				(link) => link.operation_id === selectedOperationId,
			),
		[dataStoreVersion, selectedOperationId],
	);
	const [linksQuerySnapshot, setLinksQuerySnapshot] =
		useState<LinksQuerySnapshot>(() => ({
			operationId: selectedOperationId,
			links: [...operationLinks],
		}));
	const queryLinks =
		linksQuerySnapshot.operationId === selectedOperationId
			? linksQuerySnapshot.links
			: operationLinks;
	const queryLinksRef = useRef<Link.Type[]>(queryLinks);
	queryLinksRef.current = queryLinks;
	const [activeTab, setActiveTab] = useState<"notes" | "links">("notes");
	const [linkSearch, setLinkSearch] = useState("");
	const [appliedLinkSearch, setAppliedLinkSearch] = useState("");
	const [linkPageSize, setLinkPageSize] = useState(50);
	const [linkCurrentPage, setLinkCurrentPage] = useState(1);
	const [pageLinks, setPageLinks] = useState<Link.Type[]>([]);
	const [linkTotalHits, setLinkTotalHits] = useState(0);
	const [linkLoading, setLinkLoading] = useState(false);
	const [linksCollectionRevision, setLinksCollectionRevision] = useState(0);
	const [pendingLinksChangeSequence, setPendingLinksChangeSequence] =
		useState<number | null>(null);
	const pendingLinksChangeSequenceRef = useRef<number | null>(null);
	const linksRefreshAcknowledgementRef = useRef<number | null>(null);
	const linksRequestControllerRef = useRef<AbortController | null>(null);
	const [selectedLinkIds, setSelectedLinkIds] = useState<Set<Link.Id>>(
		new Set(),
	);
	const [search, setSearch] = useState("");
	const [appliedSearch, setAppliedSearch] = useState("");
	const [selectedTags, setSelectedTags] = useState<Set<string>>(new Set());
	const [showOnlyVisibleSources, setShowOnlyVisibleSources] = useState(false);
	const [showOnlyFlaggedEvents, setShowOnlyFlaggedEvents] = useState(false);
	const [, setFlagsRevision] = useState(0);
	const [selectedNoteIds, setSelectedNoteIds] = useState<Set<Note.Id>>(
		new Set(),
	);
	const [querySnapshot, setQuerySnapshot] = useState<NotesQuerySnapshot>(() => ({
		operationId: selectedOperationId,
		notes: [...DataStore.notes],
	}));
	const queryNotes =
		querySnapshot.operationId === selectedOperationId
			? querySnapshot.notes
			: DataStore.notes;
	const queryNotesRef = useRef<Note.Type[]>(queryNotes);
	queryNotesRef.current = queryNotes;
	const [pageNotes, setPageNotes] = useState<Note.Type[]>([]);
	const pageNotesRef = useRef<Note.Type[]>(pageNotes);
	pageNotesRef.current = pageNotes;
	const [totalHits, setTotalHits] = useState(0);
	const [pageSize, setPageSize] = useState(50);
	const [currentPage, setCurrentPage] = useState(1);
	const [pendingNotesChangeSequence, setPendingNotesChangeSequence] =
		useState<number | null>(null);
	const pendingNotesChangeSequenceRef = useRef<number | null>(null);
	const [loading, setLoading] = useState(false);
	const [refreshRevision, setRefreshRevision] = useState(0);
	const refreshAcknowledgementRef = useRef<number | null>(null);
	const notesRequestControllerRef = useRef<AbortController | null>(null);
	const windowRef = useRef<HTMLDivElement>(null);
	const [linkedEventsPopup, setLinkedEventsPopup] = useState<{
		events: Doc.Type[];
		anchor: { x: number; y: number };
		operationId: Link.Type["operation_id"];
	} | null>(null);

	const selectedSourceIdValues = useMemo(
		() =>
			app.target.files
				.filter((source) => source.selected)
				.map((source) => source.id)
				.sort((left, right) => left.localeCompare(right)),
		[app.target.files],
	);
	const selectedSourceIds = useStableSemanticArray(selectedSourceIdValues);
	const queryScopeKey = JSON.stringify([
		selectedOperationId ?? null,
		selectedSourceIds,
	]);
	const currentPageScopeRef = useRef(queryScopeKey);
	const queryCurrentPage =
		currentPageScopeRef.current === queryScopeKey ? currentPage : 1;
	const queryLinkCurrentPage =
		currentPageScopeRef.current === queryScopeKey ? linkCurrentPage : 1;

	const searchMatchingLinkIdValues = useMemo(() => {
		const matchingIds = getSearchMatchingLinkIds(
			queryLinks,
			appliedLinkSearch,
		);
		return matchingIds?.sort((left, right) => left.localeCompare(right));
	}, [queryLinks, appliedLinkSearch]);
	const stableSearchMatchingLinkIds = useStableSemanticArray(
		searchMatchingLinkIdValues ?? EMPTY_LINK_IDS,
	);
	const searchMatchingLinkIds =
		searchMatchingLinkIdValues === undefined
			? undefined
			: stableSearchMatchingLinkIds;
	const allFilteredLinkIds = useMemo(
		() =>
			getSearchMatchingLinkIds(operationLinks, appliedLinkSearch) ??
			operationLinks.map((link) => link.id),
		[operationLinks, appliedLinkSearch],
	);
	const {
		pageFilter: linkPageFilter,
		countFilter: linkCountFilter,
	} = useMemo(
		() =>
			buildLinksListFilters({
				searchMatchingIds: searchMatchingLinkIds,
				pageSize: linkPageSize,
				currentPage: queryLinkCurrentPage,
			}),
		[
			searchMatchingLinkIds,
			linkPageSize,
			queryLinkCurrentPage,
		],
	);
	const queryCanReturnLinks =
		activeTab === "links" &&
		Boolean(selectedOperationId) &&
		(searchMatchingLinkIds === undefined ||
			searchMatchingLinkIds.length > 0);
	const linksRequestKey = JSON.stringify([
		activeTab,
		selectedOperationId ?? null,
		searchMatchingLinkIds ?? null,
		linkPageSize,
		queryLinkCurrentPage,
		linksCollectionRevision,
	]);
	const linksPageRequestRef = useRef<LinksPageRequest>({
		pageFilter: linkPageFilter,
		countFilter: linkCountFilter,
		queryCanReturnLinks,
		pageSize: linkPageSize,
		currentPage: queryLinkCurrentPage,
	});
	linksPageRequestRef.current = {
		pageFilter: linkPageFilter,
		countFilter: linkCountFilter,
		queryCanReturnLinks,
		pageSize: linkPageSize,
		currentPage: queryLinkCurrentPage,
	};

	const canvasRenderedSourceIds = useMemo(
		() =>
			showOnlyVisibleSources
				? getCanvasRenderedSourceIdSet(app, selectedOperationId)
				: null,
		[
			showOnlyVisibleSources,
			selectedOperationId,
			app.target.files,
			app.target.contexts,
			app.timeline.filter,
			app.timeline.renderVersion,
			app.timeline.renderedSourceIds,
			app.hidden.sourcesWithNoEvents,
		],
	);

	const filteredSourceIdValues =
		showOnlyVisibleSources && canvasRenderedSourceIds
			? [...canvasRenderedSourceIds].sort((left, right) =>
					left.localeCompare(right),
				)
			: selectedSourceIds;
	const filteredSourceIds = useStableSemanticArray(filteredSourceIdValues);
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
	const flaggedDocumentIdValues =
		showOnlyFlaggedEvents && selectedOperationId
			? Array.from(Doc.Entity.flag.getList(selectedOperationId)).sort(
					(left, right) => left.localeCompare(right),
				)
			: [];
	const flaggedDocumentIds = useStableSemanticArray(flaggedDocumentIdValues);
	const flaggedDocumentIdSet = useMemo(
		() => new Set(flaggedDocumentIds),
		[flaggedDocumentIds],
	);

	const availableTags = useMemo(() => {
		const tags = new Set<string>();
		queryNotes.forEach((note) => {
			note.tags.forEach((tag) => tags.add(tag.toLowerCase()));
		});
		return [...tags].sort((left, right) => left.localeCompare(right));
	}, [queryNotes]);
	const availableTagSet = useMemo(
		() => new Set(availableTags),
		[availableTags],
	);
	const selectedTagValues = useMemo(
		() =>
			[...selectedTags]
				.filter((tag) => availableTagSet.has(tag.toLowerCase()))
				.sort((left, right) => left.localeCompare(right)),
		[selectedTags, availableTagSet],
	);

	const searchMatchingIdValues = useMemo(
		() => {
			const matchingIds = getSearchMatchingNoteIds(
				queryNotes,
				appliedSearch,
				sourceById,
				contextById,
			);
			return matchingIds?.sort((left, right) =>
				left.localeCompare(right),
			);
		},
		[queryNotes, appliedSearch, sourceById, contextById],
	);
	const stableSearchMatchingIds = useStableSemanticArray(
		searchMatchingIdValues ?? EMPTY_NOTE_IDS,
	);
	const searchMatchingIds =
		searchMatchingIdValues === undefined
			? undefined
			: stableSearchMatchingIds;

	const allFilteredNoteIds = useMemo(
		() =>
			DataStore.notes
				.filter((note) =>
					noteMatchesNotesLinksWIndowsFilter(note, {
						search: appliedSearch,
						selectedTags: selectedTagValues,
						sourceIds: filteredSourceIdSet,
						documentIds: showOnlyFlaggedEvents
							? flaggedDocumentIdSet
							: undefined,
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
			showOnlyFlaggedEvents,
			flaggedDocumentIdSet,
			sourceById,
			contextById,
		],
	);

	const { pageFilter, countFilter } = useMemo(
		() =>
			buildNotesListFilters({
				searchMatchingIds,
				sourceIds: filteredSourceIds,
				documentIds: showOnlyFlaggedEvents
					? flaggedDocumentIds
					: undefined,
				selectedTags: selectedTagValues,
				pageSize,
				currentPage: queryCurrentPage,
			}),
		[
			searchMatchingIds,
			filteredSourceIds,
			showOnlyFlaggedEvents,
			flaggedDocumentIds,
			selectedTagValues,
			pageSize,
			queryCurrentPage,
		],
	);

	const queryCanReturnNotes =
		Boolean(selectedOperationId) &&
		filteredSourceIds.length > 0 &&
		(!showOnlyFlaggedEvents || flaggedDocumentIds.length > 0) &&
		(searchMatchingIds === undefined || searchMatchingIds.length > 0);
	const totalPages = Math.max(1, Math.ceil(totalHits / pageSize));
	const notesRequestKey = JSON.stringify([
		selectedOperationId ?? null,
		showOnlyVisibleSources,
		filteredSourceIds,
		showOnlyFlaggedEvents,
		flaggedDocumentIds,
		searchMatchingIds ?? null,
		selectedTagValues,
		pageSize,
		queryCurrentPage,
		refreshRevision,
	]);
	const notesPageRequestRef = useRef<NotesPageRequest>({
		pageFilter,
		countFilter,
		queryCanReturnNotes,
		pageSize,
		currentPage: queryCurrentPage,
	});
	notesPageRequestRef.current = {
		pageFilter,
		countFilter,
		queryCanReturnNotes,
		pageSize,
		currentPage: queryCurrentPage,
	};

	/**
	 * Acknowledges one explicit refresh without hiding changes that arrived later.
	 * @param sequence Realtime sequence captured when the refresh started.
	 * @returns Nothing.
	 */
	const acknowledgeSuccessfulRefresh = useCallback(
		(sequence: number | null): void => {
			if (sequence === null) return;
			setPendingNotesChangeSequence((pendingSequence) => {
				const nextSequence = acknowledgeNotesChangeSequence(
					pendingSequence,
					sequence,
				);
				pendingNotesChangeSequenceRef.current = nextSequence;
				return nextSequence;
			});
		},
		[],
	);

	/**
	 * Fetches the active note page and matching total while rejecting stale results.
	 * @returns Promise resolved after the latest request updates table state.
	 */
	const fetchNotesPage = useCallback(async (): Promise<void> => {
		const {
			pageFilter: requestPageFilter,
			countFilter: requestCountFilter,
			queryCanReturnNotes: requestCanReturnNotes,
			pageSize: requestPageSize,
			currentPage: requestPage,
		} = notesPageRequestRef.current;
		const refreshSequence = refreshAcknowledgementRef.current;
		refreshAcknowledgementRef.current = null;
		notesRequestControllerRef.current?.abort();
		if (!requestCanReturnNotes) {
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
				infoRef.current.notes_list(
					requestPageFilter,
					requestController.signal,
				),
				infoRef.current.notes_count(
					requestCountFilter,
					requestController.signal,
				),
			]);
			if (
				requestController.signal.aborted ||
				notesRequestControllerRef.current !== requestController
			) {
				return;
			}

			const nextTotalPages = Math.max(
				1,
				Math.ceil(count / requestPageSize),
			);
			setTotalHits(count);
			if (requestPage > nextTotalPages) {
				acknowledgeSuccessfulRefresh(refreshSequence);
				setCurrentPage(nextTotalPages);
				return;
			}
			setPageNotes(notes);
			pageNotesRef.current = notes;
			acknowledgeSuccessfulRefresh(refreshSequence);
		} catch {
			if (!requestController.signal.aborted) {
				toast.error(translateRef.current("tableView.fetchFailed"));
			}
		} finally {
			if (notesRequestControllerRef.current === requestController) {
				notesRequestControllerRef.current = null;
				setLoading(false);
			}
		}
	}, [acknowledgeSuccessfulRefresh]);

	useEffect(() => {
		fetchNotesPage();
	}, [fetchNotesPage, notesRequestKey]);

	useEffect(
		() => () => notesRequestControllerRef.current?.abort(),
		[],
	);

	useEffect(
		() =>
			DataStore.subscribeRenderChanges((change) => {
				if (change.type !== "links" || change.mutation !== "replace") {
					return;
				}
				const linksSnapshot = DataStore.links.filter(
					(link) => link.operation_id === selectedOperationId,
				);
				queryLinksRef.current = linksSnapshot;
				setLinksQuerySnapshot({
					operationId: selectedOperationId,
					links: linksSnapshot,
				});
				setLinksCollectionRevision(change.collectionRevision);
			}),
		[selectedOperationId],
	);

	useEffect(() => {
		/**
		 * Keeps updates live while holding create/delete changes behind a refresh.
		 * @param change Realtime Links synchronization event for this window realm.
		 * @returns Nothing.
		 */
		const handleLinksSyncChange = (
			change: LinksRealtimeSyncChange,
		): void => {
			if (change.operationId !== selectedOperationId) return;
			if (change.mutation === "update") {
				const linksSnapshot = DataStore.links.filter(
					(link) => link.operation_id === selectedOperationId,
				);
				queryLinksRef.current = linksSnapshot;
				setLinksQuerySnapshot({
					operationId: selectedOperationId,
					links: linksSnapshot,
				});
				setLinksCollectionRevision((revision) => revision + 1);
				return;
			}
			const nextSequence = Math.max(
				pendingLinksChangeSequenceRef.current ?? 0,
				change.sequence,
			);
			pendingLinksChangeSequenceRef.current = nextSequence;
			setPendingLinksChangeSequence(nextSequence);
		};

		return DataStore.subscribeLinksSyncChanges(handleLinksSyncChange);
	}, [selectedOperationId]);

	/**
	 * Clears only the Links changes covered by one successful explicit refresh.
	 * @param sequence Realtime sequence captured when refresh started.
	 * @returns Nothing.
	 */
	const acknowledgeSuccessfulLinksRefresh = useCallback(
		(sequence: number | null): void => {
			if (sequence === null) return;
			setPendingLinksChangeSequence((pendingSequence) => {
				const nextSequence =
					pendingSequence !== null && pendingSequence <= sequence
					? null
					: pendingSequence;
				pendingLinksChangeSequenceRef.current = nextSequence;
				return nextSequence;
			});
		},
		[],
	);

	/**
	 * Fetches the active link page and matching total while rejecting stale results.
	 * @returns Promise resolved after the latest request updates table state.
	 */
	const fetchLinksPage = useCallback(async (): Promise<void> => {
		const {
			pageFilter: requestPageFilter,
			countFilter: requestCountFilter,
			queryCanReturnLinks: requestCanReturnLinks,
			pageSize: requestPageSize,
			currentPage: requestPage,
		} = linksPageRequestRef.current;
		const refreshSequence = linksRefreshAcknowledgementRef.current;
		linksRefreshAcknowledgementRef.current = null;
		linksRequestControllerRef.current?.abort();
		if (!requestCanReturnLinks) {
			linksRequestControllerRef.current = null;
			setPageLinks([]);
			setLinkTotalHits(0);
			setLinkLoading(false);
			acknowledgeSuccessfulLinksRefresh(refreshSequence);
			return;
		}

		const requestController = new AbortController();
		linksRequestControllerRef.current = requestController;
		setPageLinks([]);
		setLinkLoading(true);
		try {
			const [links, count] = await Promise.all([
				infoRef.current.links_list(
					requestPageFilter,
					requestController.signal,
				),
				infoRef.current.links_count(
					requestCountFilter,
					requestController.signal,
				),
			]);
			if (
				requestController.signal.aborted ||
				linksRequestControllerRef.current !== requestController
			) {
				return;
			}

			const nextTotalPages = Math.max(
				1,
				Math.ceil(count / requestPageSize),
			);
			setLinkTotalHits(count);
			if (requestPage > nextTotalPages) {
				acknowledgeSuccessfulLinksRefresh(refreshSequence);
				setLinkCurrentPage(nextTotalPages);
				return;
			}
			setPageLinks(links);
			acknowledgeSuccessfulLinksRefresh(refreshSequence);
		} catch {
			if (!requestController.signal.aborted) {
				toast.error(translateRef.current("tableView.fetchFailed"));
			}
		} finally {
			if (linksRequestControllerRef.current === requestController) {
				linksRequestControllerRef.current = null;
				setLinkLoading(false);
			}
		}
	}, [acknowledgeSuccessfulLinksRefresh]);

	useEffect(() => {
		fetchLinksPage();
	}, [fetchLinksPage, linksRequestKey]);

	useEffect(
		() => () => linksRequestControllerRef.current?.abort(),
		[],
	);

	useEffect(() => {
		if (search === appliedSearch) return;
		const timeout = window.setTimeout(() => {
			setAppliedSearch(search);
			setCurrentPage(1);
			setSelectedNoteIds(new Set());
		}, TABLE_SEARCH_DEBOUNCE_MSEC);
		return () => window.clearTimeout(timeout);
	}, [search, appliedSearch]);

	useEffect(() => {
		if (linkSearch === appliedLinkSearch) return;
		const timeout = window.setTimeout(() => {
			setAppliedLinkSearch(linkSearch);
			setLinkCurrentPage(1);
			setSelectedLinkIds(new Set());
		}, TABLE_SEARCH_DEBOUNCE_MSEC);
		return () => window.clearTimeout(timeout);
	}, [linkSearch, appliedLinkSearch]);

	useEffect(() => {
		/**
		 * Invalidates and refreshes flagged-document state changed by another window.
		 * @param event Browser storage event for the shared flagged-event list.
		 * @returns Nothing.
		 */
		const handleFlagStorageChange = (event: StorageEvent): void => {
			if (event.key !== Doc.Entity.flag.KEY) return;
			Doc.Entity.flag.invalidateCache();
			setFlagsRevision((revision) => revision + 1);
		};

		window.addEventListener("storage", handleFlagStorageChange);
		return () => window.removeEventListener("storage", handleFlagStorageChange);
	}, []);

	useEffect(() => {
		setSelectedTags((previousTags) => {
			const nextTags = new Set(
				[...previousTags].filter((tag) =>
					availableTagSet.has(tag.toLowerCase()),
				),
			);
			return nextTags.size === previousTags.size ? previousTags : nextTags;
		});
	}, [availableTagSet]);

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
				const notesSnapshot = [...DataStore.notes];
				queryNotesRef.current = notesSnapshot;
				setQuerySnapshot({
					operationId: selectedOperationId,
					notes: notesSnapshot,
				});
				return;
			}
			if (change.mutation === "update") {
				const updateResult = applyNotesLinksWIndowsUpdateDelta({
					queryNotes: queryNotesRef.current,
					pageNotes: pageNotesRef.current,
					updatedNotes: change.notes,
					filter: {
						search: appliedSearch,
						selectedTags: selectedTagValues,
						sourceIds: filteredSourceIdSet,
						documentIds: showOnlyFlaggedEvents
							? flaggedDocumentIdSet
							: undefined,
						sourceById,
						contextById,
					},
				});
				if (updateResult.queryNotes !== queryNotesRef.current) {
					queryNotesRef.current = updateResult.queryNotes;
					setQuerySnapshot({
						operationId: selectedOperationId,
						notes: updateResult.queryNotes,
					});
				}
				if (updateResult.pageNotes !== pageNotesRef.current) {
					pageNotesRef.current = updateResult.pageNotes;
					setPageNotes(updateResult.pageNotes);
				}
				if (updateResult.requiresRefresh) {
					setRefreshRevision((revision) => revision + 1);
				}
				return;
			}
			const nextSequence = advanceNotesChangeSequence(
				pendingNotesChangeSequenceRef.current,
				change,
			);
			pendingNotesChangeSequenceRef.current = nextSequence;
			setPendingNotesChangeSequence(nextSequence);
		};

		return DataStore.subscribeNotesSyncChanges(handleNotesSyncChange);
	}, [
		selectedOperationId,
		appliedSearch,
		selectedTagValues,
		filteredSourceIdSet,
		showOnlyFlaggedEvents,
		flaggedDocumentIdSet,
		sourceById,
		contextById,
	]);

	useEffect(() => {
		if (currentPageScopeRef.current === queryScopeKey) return;
		currentPageScopeRef.current = queryScopeKey;
		const notesSnapshot = [...DataStore.notes];
		queryNotesRef.current = notesSnapshot;
		setQuerySnapshot({
			operationId: selectedOperationId,
			notes: notesSnapshot,
		});
		refreshAcknowledgementRef.current = null;
		pendingNotesChangeSequenceRef.current = null;
		setPendingNotesChangeSequence(null);
		setCurrentPage(1);
		setSelectedNoteIds(new Set());
		const linksSnapshot = DataStore.links.filter(
			(link) => link.operation_id === selectedOperationId,
		);
		queryLinksRef.current = linksSnapshot;
		setLinksQuerySnapshot({
			operationId: selectedOperationId,
			links: linksSnapshot,
		});
		linksRefreshAcknowledgementRef.current = null;
		pendingLinksChangeSequenceRef.current = null;
		setPendingLinksChangeSequence(null);
		setLinkCurrentPage(1);
		setSelectedLinkIds(new Set());
	}, [queryScopeKey, selectedOperationId]);

	useEffect(() => {
		const availableLinkIds = new Set(allFilteredLinkIds);
		setSelectedLinkIds((previousIds) => {
			const retainedIds = new Set(
				[...previousIds].filter((linkId) => availableLinkIds.has(linkId)),
			);
			return retainedIds.size === previousIds.size
				? previousIds
				: retainedIds;
		});
	}, [allFilteredLinkIds]);

	/**
	 * Clears selection, acknowledges canonical notes, and reloads the first page.
	 * @returns Nothing.
	 */
	const refreshNotes = useCallback((): void => {
		const notesSnapshot = [...DataStore.notes];
		queryNotesRef.current = notesSnapshot;
		setQuerySnapshot({
			operationId: selectedOperationId,
			notes: notesSnapshot,
		});
		refreshAcknowledgementRef.current =
			pendingNotesChangeSequenceRef.current;
		setSelectedNoteIds(new Set());
		setCurrentPage(1);
		setRefreshRevision((revision) => revision + 1);
	}, [selectedOperationId]);

	/**
	 * Acknowledges canonical Links and reloads the first backend page.
	 * @returns Nothing.
	 */
	const refreshLinks = useCallback((): void => {
		const linksSnapshot = DataStore.links.filter(
			(link) => link.operation_id === selectedOperationId,
		);
		queryLinksRef.current = linksSnapshot;
		setLinksQuerySnapshot({
			operationId: selectedOperationId,
			links: linksSnapshot,
		});
		linksRefreshAcknowledgementRef.current =
			pendingLinksChangeSequenceRef.current;
		setSelectedLinkIds(new Set());
		setLinkCurrentPage(1);
		setLinksCollectionRevision((revision) => revision + 1);
	}, [selectedOperationId]);

	/**
	 * Reloads the current Notes page after a deletion initiated by this window.
	 * @returns Nothing.
	 */
	const reloadCurrentNotesPage = useCallback((): void => {
		const notesSnapshot = [...DataStore.notes];
		queryNotesRef.current = notesSnapshot;
		setQuerySnapshot({
			operationId: selectedOperationId,
			notes: notesSnapshot,
		});
		refreshAcknowledgementRef.current =
			pendingNotesChangeSequenceRef.current;
		setSelectedNoteIds(new Set());
		setRefreshRevision((revision) => revision + 1);
	}, [selectedOperationId]);

	/**
	 * Reloads the current Links page after a deletion initiated by this window.
	 * @returns Nothing.
	 */
	const reloadCurrentLinksPage = useCallback((): void => {
		const linksSnapshot = DataStore.links.filter(
			(link) => link.operation_id === selectedOperationId,
		);
		queryLinksRef.current = linksSnapshot;
		setLinksQuerySnapshot({
			operationId: selectedOperationId,
			links: linksSnapshot,
		});
		linksRefreshAcknowledgementRef.current =
			pendingLinksChangeSequenceRef.current;
		setSelectedLinkIds(new Set());
		setLinksCollectionRevision((revision) => revision + 1);
	}, [selectedOperationId]);

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
		setShowOnlyVisibleSources((visibleOnly) => {
			const nextVisibleOnly = !visibleOnly;
			setDetachedNotesRenderedSync(nextVisibleOnly);
			return nextVisibleOnly;
		});
		setCurrentPage(1);
		setSelectedNoteIds(new Set());
	}, [setDetachedNotesRenderedSync]);

	/**
	 * Toggles the flagged-event restriction and resets pagination.
	 * @returns Nothing.
	 */
	const toggleFlaggedEvents = useCallback((): void => {
		setShowOnlyFlaggedEvents((flaggedOnly) => !flaggedOnly);
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
				onDeleted={reloadCurrentNotesPage}
			/>,
			"table",
		);
	}, [selectedNoteIds, spawnBanner, reloadCurrentNotesPage]);

	/**
	 * Applies the Links search and resets its query-dependent table state.
	 * @param value Current search field value.
	 * @returns Nothing.
	 */
	const handleLinkSearchChange = useCallback((value: string): void => {
		setLinkSearch(value);
	}, []);

	/**
	 * Selects or deselects every link displayed on the current page.
	 * @param checked Whether current-page link IDs should be selected.
	 * @returns Nothing.
	 */
	const handleSelectLinkPage = useCallback(
		(checked: boolean): void => {
			setSelectedLinkIds((previousIds) => {
				const nextIds = new Set(previousIds);
				pageLinks.forEach((link) => {
					if (checked) nextIds.add(link.id);
					else nextIds.delete(link.id);
				});
				return nextIds;
			});
		},
		[pageLinks],
	);

	/**
	 * Selects every filtered link or clears the complete Links selection.
	 * @param checked Whether all filtered link IDs should be selected.
	 * @returns Nothing.
	 */
	const handleSelectAllFilteredLinks = useCallback(
		(checked: boolean): void => {
			setSelectedLinkIds(
				checked ? new Set(allFilteredLinkIds) : new Set(),
			);
		},
		[allFilteredLinkIds],
	);

	/**
	 * Opens bulk-delete confirmation for the current Links selection.
	 * @returns Nothing.
	 */
	const handleBulkDeleteLinks = useCallback((): void => {
		if (selectedLinkIds.size === 0) return;
		spawnBanner(
			<BulkDeleteLinksBanner
				linkIds={[...selectedLinkIds]}
				onDeleted={reloadCurrentLinksPage}
			/>,
			"table",
		);
	}, [selectedLinkIds, spawnBanner, reloadCurrentLinksPage]);

	/**
	 * Targets an event in the main operation window.
	 * @param docId Event document identifier.
	 * @param operationId Operation containing the event.
	 * @returns Nothing.
	 */
	const navigateToEvent = useCallback(
		(
			docId: Doc.Id,
			operationId: Note.Type["operation_id"],
		): void => {
			WindowBridge.sendControlMessage(
				WindowBridge.MessageType.NAVIGATE_TO_EVENT,
				{
					docId,
					operationId,
				},
				detachedCoordinatorId ?? undefined,
			);
		},
		[detachedCoordinatorId],
	);

	/**
	 * Targets the event associated with a note in the main operation window.
	 * @param note Note whose event should be targeted.
	 * @returns Nothing.
	 */
	const targetNoteButtonHandler = useCallback((note: Note.Type): void => {
		navigateToEvent(note.doc._id, note.operation_id);
	}, [navigateToEvent]);

	/**
	 * Targets the event from which a link was created in the main operation window.
	 * @param link Link whose origin event should be targeted.
	 * @returns Nothing.
	 */
	const targetLinkOriginHandler = useCallback((link: Link.Type): void => {
		navigateToEvent(link.doc_id_from, link.operation_id);
	}, [navigateToEvent]);

	/**
	 * Opens the origin and destination events currently available for a link.
	 * @param link Link whose events should be displayed.
	 * @param triggerEvent Action click used to anchor the event list.
	 * @returns Promise resolved after available events have been loaded.
	 */
	const handleOpenLinkedEvents = useCallback(
		async (
			link: Link.Type,
			triggerEvent: ReactMouseEvent<HTMLButtonElement>,
		): Promise<void> => {
			const rect = triggerEvent.currentTarget.getBoundingClientRect();
			const linkedEvents = await getRelatedLinkEvents(link);
			if (linkedEvents.length === 0) return;

			setLinkedEventsPopup({
				events: linkedEvents,
				anchor: { x: rect.left, y: rect.top },
				operationId: link.operation_id,
			});
		},
		[],
	);

	/**
	 * Opens the existing complete-link deletion confirmation.
	 * @param link Link selected by the row action.
	 * @returns Nothing.
	 */
	const handleDeleteLink = useCallback(
		(link: Link.Type): void => {
			spawnBanner(
				<Link.Delete.Banner
					link={link}
					onDeleted={reloadCurrentLinksPage}
				/>,
				"table",
			);
		},
		[spawnBanner, reloadCurrentLinksPage],
	);

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
					onDeleted={reloadCurrentNotesPage}
				/>,
				"table",
			);
		},
		[spawnBanner, reloadCurrentNotesPage],
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
						<span
							className={s.titleText}
							data-table-tooltip-trigger="true"
							data-table-tooltip-text={row.title}
						>
							{row.title}
						</span>
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

	const linkTableValues = useMemo<LinksTableRow[]>(
		() =>
			pageLinks.map((link) => ({
				id: link.id,
				_id: link.id,
				title: String(link.name ?? ""),
				description: link.description || "-",
				tags: link.tags.join(", ") || "-",
				originEventId: link.doc_id_from,
				color: link.color || "var(--accent)",
				rawLink: link,
				icon: Link.Entity.icon(link),
			})),
		[pageLinks],
	);

	const selectedLinkRows = useMemo(() => {
		const indices = new Set<number>();
		linkTableValues.forEach((item, index) => {
			if (selectedLinkIds.has(item.id)) indices.add(index);
		});
		return indices;
	}, [linkTableValues, selectedLinkIds]);

	const isAllFilteredLinksSelected =
		allFilteredLinkIds.length > 0 &&
		allFilteredLinkIds.every((linkId) => selectedLinkIds.has(linkId));

	/**
	 * Converts a Links row checkbox index into its stable link ID.
	 * @param index Current-page row index.
	 * @param selected Whether the row should be selected.
	 * @returns Nothing.
	 */
	const handleLinkRowSelect = useCallback(
		(index: number, selected: boolean): void => {
			const item = linkTableValues[index];
			if (!item) return;
			setSelectedLinkIds((previousIds) => {
				const nextIds = new Set(previousIds);
				if (selected) nextIds.add(item.id);
				else nextIds.delete(item.id);
				return nextIds;
			});
		},
		[linkTableValues],
	);

	const linkTableActions = useMemo<Table.Action<LinksTableRow>[]>(
		() => [
			{
				icon: "MagnifyingGlassSmall",
				label: t("eventDialog.showLinkedEvents"),
				onClick: (row, _index, actionEvent) => {
					void handleOpenLinkedEvents(row.rawLink, actionEvent);
				},
			},
			{
				icon: "Trash2",
				label: t("link.deleteTitle"),
				onClick: (row) => handleDeleteLink(row.rawLink),
			},
		],
		[handleDeleteLink, handleOpenLinkedEvents, t],
	);

	const linkTableColumns = useMemo<Table.ColumnDefinition<LinksTableRow>[]>(
		() => [
			{
				key: "title",
				label: t("common.title"),
				width: 220,
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
						<span
							className={s.titleText}
							data-table-tooltip-trigger="true"
							data-table-tooltip-text={row.title}
						>
							{row.title}
						</span>
					</span>
				),
			},
			{
				key: "description",
				label: t("common.description"),
				width: 360,
			},
			{
				key: "tags",
				label: t("common.tags"),
				width: 180,
			},
			{
				key: "originEventId",
				label: t("common.event"),
				width: 300,
				render: (_value, row) => (
					<Button
						variant="tertiary"
						className={s.originEventLink}
						title={t("tableView.targetEvent")}
						onClick={() => targetLinkOriginHandler(row.rawLink)}
					>
						{row.originEventId}
					</Button>
				),
			},
		],
		[t, targetLinkOriginHandler],
	);

	return (
		<Tabs
			value={activeTab}
			onValueChange={(value) =>
				setActiveTab(value as "notes" | "links")
			}
			className={s.main}
		>
			<div className={s.header}>
				<TabsList className={s.tabsList}>
					<TabsTrigger value="notes">
						<Icon
							name="StickyNote"
							size={14}
						/>
						{t("notes.title")}
					</TabsTrigger>
					<TabsTrigger value="links">
						<Icon
							name="GitPullRequestCreate"
							size={14}
						/>
						{t("eventDialog.links")}
					</TabsTrigger>
				</TabsList>
			</div>
			<TabsContent
				value="notes"
				className={s.content}
			>
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
					<div className={s.filterOptionsRow}>
						<Stack
							gap={10}
							ai="center"
							flex={false}
							className={s.filterOption}
						>
							<Checkbox
								id="notes-visible-sources-only"
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
						<Stack
							gap={10}
							ai="center"
							flex={false}
							className={s.filterOption}
						>
							<Checkbox
								id="notes-flagged-events-only"
								checked={showOnlyFlaggedEvents}
								onCheckedChange={toggleFlaggedEvents}
							/>
							<span
								className={s.helperText}
								onClick={toggleFlaggedEvents}
							>
								{t("filterFile.flaggedOnly")}
							</span>
							<Icon name="Flag" />
						</Stack>
					</div>
				</div>

				<TableHeader
					allSelected={isAllFilteredSelected}
					selectionLabel={t("notes.selectAllFiltered", {
						count: allFilteredNoteIds.length,
					})}
					onSelectionChange={handleSelectAllFiltered}
					actionsDisabled={selectedNoteIds.size === 0}
					actions={[
						{
							id: "delete",
							icon: "Trash2",
							label: t("notes.deleteSelected", {
								count: selectedNoteIds.size,
							}),
							onSelect: handleBulkDelete,
						},
					]}
					pagination={{
						currentPage,
						totalItems: totalHits,
						pageSize,
						pageSizeOptions: TABLE_PAGE_SIZES,
						disabled: loading,
						onPageChange: setCurrentPage,
						onPageSizeChange: (value) => {
							setPageSize(value);
							setCurrentPage(1);
						},
					}}
					container={windowRef.current}
				/>

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
							highlightedId={
								detachedClientKind === "notes"
									? detachedSelectedEventId ?? undefined
									: app.timeline.target?._id
							}
							columnVisibility={true}
							persistId="notes_table_paginated_v2"
							virtualization={NOTES_TABLE_VIRTUALIZATION}
						/>
					) : (
						<p className={s.placeholder}>{t("tableView.noData")}</p>
					)}
				</div>
			</TabsContent>
			<TabsContent
				value="links"
				className={s.content}
			>
				{pendingLinksChangeSequence !== null && (
					<Notification
						variant="warning"
						icon="Warning"
						className={s.newNotesWarning}
						jc="space-between"
						gap={8}
					>
						<span className={s.newNotesMessage}>
							{t("links.changesAvailable")}
						</span>
						<Button
							variant="secondary"
							icon="RotateCcw"
							onClick={refreshLinks}
						>
							{t("common.refresh")}
						</Button>
					</Notification>
				)}
				<div className={s.filters}>
					<Input
						placeholder={t("common.search")}
						icon="MagnifyingGlass"
						variant="highlighted"
						value={linkSearch}
						onChange={(event) =>
							handleLinkSearchChange(event.target.value)
						}
					/>
				</div>
				<TableHeader
					allSelected={isAllFilteredLinksSelected}
					selectionLabel={t("links.selectAllFiltered", {
						count: allFilteredLinkIds.length,
					})}
					onSelectionChange={handleSelectAllFilteredLinks}
					actionsDisabled={selectedLinkIds.size === 0}
					actions={[
						{
							id: "delete",
							icon: "Trash2",
							label: t("links.deleteSelected", {
								count: selectedLinkIds.size,
							}),
							onSelect: handleBulkDeleteLinks,
						},
					]}
					pagination={{
						currentPage: linkCurrentPage,
						totalItems: linkTotalHits,
						pageSize: linkPageSize,
						pageSizeOptions: TABLE_PAGE_SIZES,
						disabled: linkLoading,
						onPageChange: setLinkCurrentPage,
						onPageSizeChange: (value) => {
							setLinkPageSize(value);
							setLinkCurrentPage(1);
						},
					}}
					container={windowRef.current}
				/>
				<div className={s.result}>
					{linkLoading ? (
						<p className={s.placeholder}>{t("tableView.loadingData")}</p>
					) : linkTableValues.length > 0 ? (
						<Table
							values={linkTableValues}
							columns={linkTableColumns}
							includeIndex={false}
							selectable={true}
							selectedrows={selectedLinkRows}
							onrowselect={handleLinkRowSelect}
							onSelectAll={handleSelectLinkPage}
							actions={linkTableActions}
							columnVisibility={true}
							persistId="notes_window_links_paginated_v2"
							virtualization={NOTES_TABLE_VIRTUALIZATION}
						/>
					) : (
						<p className={s.placeholder}>{t("tableView.noData")}</p>
					)}
				</div>
			</TabsContent>
			{linkedEventsPopup && (
				<DisplayGroupDialog
					events={linkedEventsPopup.events}
					anchor={linkedEventsPopup.anchor}
					groupKind="links"
					onClose={() => setLinkedEventsPopup(null)}
					onSelectEvent={(event) =>
						navigateToEvent(event._id, linkedEventsPopup.operationId)
					}
					preserveTimelineTarget
				/>
			)}
			{banner?.target === "table" ? banner.node : null}
			<div
				ref={windowRef}
				className={s.portalContainer}
			/>
		</Tabs>
	);
}

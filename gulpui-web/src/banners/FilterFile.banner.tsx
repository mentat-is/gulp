import s from "./styles/FilterFileBanner.module.css";
import { Banner } from "@/ui/Banner";
import { Application } from "@/context/Application.context";
import {
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
	SetStateAction,
	memo,
} from "react";
import { fws } from "@/ui/utils";
import { Separator } from "@/ui/Separator";
import { Preview } from "./Preview.banner";
import { Popover } from "@/ui/Popover";
import { OpenSearchQueryBuilder } from "@/components/QueryBuilder";
import {
	Tooltip,
	TooltipContent,
	TooltipProvider,
	TooltipTrigger,
} from "@/ui/Tooltip";
import { Checkbox } from "@/ui/Checkbox";
import { Button } from "@/ui/Button";
import { Stack } from "@/ui/Stack";
import { Query } from "@/entities/Query";
import { Source } from "@/entities/Source";
import { Filter } from "@/entities/Filter";
import { Operation } from "@/entities/Operation";
import { Doc } from "@/entities/Doc";
import { Glyph } from "@/entities/Glyph";
import { toast } from "sonner";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/ui/Tabs";
import { Textarea } from "@/ui/Textarea";
import { Label } from "@/ui/Label";
import { Internal } from "@/entities/addon/Internal";
import { Locale } from "@/locales";

interface FilterFileBannerProps extends Banner.Props {
	sources: Source.Type[];
	query?: Query.Type;
	keys?: string[];
	create_notes?: boolean;
	notes_color?: string;
	notes_tags?: string[];
	notes_glyph_id?: Glyph.Id;
	name?: string;
}

import { QueriesHistory } from "@/components/QueriesHistory";
import { Icon } from "@/ui/Icon";

/**
 * FilterFileBanner — Modal for building and applying OpenSearch queries/filters
 * to specific source files within an operation.
 *
 * Supports two modes:
 * - **Builder mode**: Structured UI with text filter, conditions, and source selection.
 * - **Manual mode**: Raw JSON editor for advanced OpenSearch queries.
 *
 * The component manages query state locally and persists it to Info's filter cache
 * on submit. The `Query.Type.string` field is used exclusively as a display label
 * (generated via `Filter.Entity.describe()`), never for actual filtering.
 */
export function FilterFileBanner({
	sources: initSources,
	query: initQuery,
	keys: initKeys,
	create_notes: initCreateNotes,
	notes_color,
	notes_tags,
	notes_glyph_id,
	name,
	...props
}: FilterFileBannerProps) {
	const { app, Info, spawnBanner, destroyBanner } = Application.use();
	const { t } = Locale.use();

	const [loading, setLoading] = useState(false);
	const [files, setFiles] = useState<Source.Type[]>(initSources);
	const [flaggedOnly, setFlaggedOnly] = useState(false);
	const [createNotesChecked, setCreateNotesChecked] = useState(false);

	// -- State Management --

	// Builder Mode State: the structured query object
	const [query, setQuery] = useState<Query.Type>(
		initQuery ?? { string: "", filters: [] },
	);

	// Manual Mode State: raw JSON string for the manual editor
	const [manualContent, setManualContent] = useState("");
	const [isManual, setIsManual] = useState<boolean>(!!initQuery?.isManual);

	// Available field keys and their OpenSearch types for the query builder
	const [keys, setKeys] = useState<string[]>(initKeys ?? []);
	const [fieldTypeMap, setFieldTypeMap] = useState<Record<string, string>>({});

	// Refs for tracking file selection changes
	const isFirstRun = useRef(true);
	const prevFileIds = useRef(
		files
			.map((f) => f.id)
			.sort()
			.join(","),
	);

	/**
	 * Creates a clean base Query.Type scoped to the given files and current timeline frame.
	 * Used as the starting point when no persisted filter exists, or when resetting.
	 */
	const getCleanBase = useCallback(
		(targetFiles: Source.Type[]): Query.Type => {
			if (targetFiles.length === 0) return { string: "", filters: [] };
			const first = targetFiles[0];
			const min = Internal.Transformator.toNanos(
				app.timeline.frame.min,
			).toString();
			const max = Internal.Transformator.toNanos(
				app.timeline.frame.max,
			).toString();

			return {
				string: "",
				text_filter: "",
				source_config: {
					operation_id: first.operation_id,
					source_ids: targetFiles.map((f) => f.id),
					range: { min, max },
				},
				filters: [],
			};
		},
		[app],
	);

	/**
	 * Converts a Query.Type into a JSON string for the manual editor.
	 * If the query has raw JSON (manual mode), uses that directly.
	 * Otherwise, generates the OpenSearch query via Filter.Entity.query().
	 */
	const getManualContentFromQuery = useCallback(
		(q: Query.Type): string => {
			if (q.raw) return JSON.stringify(q.raw, null, 2);
			return JSON.stringify(
				Filter.Entity.query({ ...q, fieldTypeMap }),
				null,
				2,
			);
		},
		[fieldTypeMap],
	);

	/**
	 * Resets the query state to a clean base for the given files.
	 * Updates both the builder state and the manual editor content.
	 */
	const resetToCleanBase = useCallback(
		(targetFiles: Source.Type[]) => {
			const cleanBase = getCleanBase(targetFiles);
			setQuery(cleanBase);
			setManualContent(
				JSON.stringify(
					Filter.Entity.query({ ...cleanBase, fieldTypeMap }),
					null,
					2,
				),
			);
		},
		[getCleanBase, fieldTypeMap],
	);

	const fileIds = useMemo(() => files.map((f) => f.id), [files]);

	/**
	 * Handles source file selection changes from the multi-select component.
	 * When files change, updates the query's source_config to match the new selection.
	 * In manual mode, resets the query entirely to avoid stale JSON.
	 */
	const handleSourceChange = useCallback(
		(action: SetStateAction<Source.Id[]>) => {
			let newIds: Source.Id[] = [];
			if (typeof action === "function") {
				newIds = action(files.map((f) => f.id));
			} else {
				newIds = action;
			}

			const newFiles = newIds.map((id) => Source.Entity.id(app, id));
			setFiles(newFiles);

			const newIdsStr = newIds.sort().join(",");
			if (newIdsStr !== prevFileIds.current) {
				if (isManual) {
					toast.info(t("filterFile.queryResetSource"));
					resetToCleanBase(newFiles);
				} else {
					toast.info(t("filterFile.queryUpdatedSource"));
					setQuery((prev) => {
						const cleanBase = getCleanBase(newFiles);
						const updated = { ...prev, source_config: cleanBase.source_config };
						setManualContent(
							JSON.stringify(
								Filter.Entity.query({ ...updated, fieldTypeMap }),
								null,
								2,
							),
						);
						return updated;
					});
				}
				prevFileIds.current = newIdsStr;
			}
		},
		[app, files, isManual, resetToCleanBase, fieldTypeMap, t],
	);

	/**
	 * Initialization effect — runs once on mount.
	 *
	 * Priority order:
	 * 1. If `initQuery` was provided (e.g. from "Last filters" or preview back-navigation),
	 *    merge it with the current files' source_config and use directly.
	 * 2. Otherwise, scan all selected files for a persisted non-empty query in the cache.
	 * 3. If nothing found, start with a clean base.
	 */
	useEffect(() => {
		if (isFirstRun.current) {
			isFirstRun.current = false;

			// Case 1: Caller provided a query — honour it directly
			if (initQuery) {
				const cleanBase = getCleanBase(files);
				const merged = { ...initQuery, source_config: cleanBase.source_config };
				setQuery(merged);
				setIsManual(!!initQuery.isManual);
				setManualContent(getManualContentFromQuery(merged));
				prevFileIds.current = files
					.map((f) => f.id)
					.sort()
					.join(",");
				return;
			}

			// Case 2: Scan all selected files for a persisted query that has
			// any user-applied filter state (text_filter, conditions, raw, or fieldTypeMap).
			// We do NOT check `string` because it's a display-only label.
			let startingQuery: Query.Type | undefined;
			for (const file of files) {
				const persisted = Info.getQuery(file) as Query.Type;
				if (
					persisted &&
					(persisted.filters?.length ||
						persisted.text_filter ||
						persisted.raw ||
						persisted.isManual ||
						persisted.fieldTypeMap)
				) {
					startingQuery = persisted;
					break;
				}
			}

			if (startingQuery) {
				const cleanBase = getCleanBase(files);
				const updatedStartingQuery = {
					...startingQuery,
					source_config: cleanBase.source_config,
				};
				setQuery(updatedStartingQuery);
				setIsManual(!!startingQuery.isManual);
				setManualContent(getManualContentFromQuery(updatedStartingQuery));
				prevFileIds.current = files
					.map((f) => f.id)
					.sort()
					.join(",");
			} else {
				// Case 3: No persisted filter found — start clean
				resetToCleanBase(initSources);
				prevFileIds.current = initSources
					.map((f) => f.id)
					.sort()
					.join(",");
			}
		}
	}, [
		files,
		initQuery,
		initSources,
		Info,
		getManualContentFromQuery,
		resetToCleanBase,
		getCleanBase,
	]);

	/**
	 * Loads available field keys and their types for the query builder dropdowns.
	 * Fetches from each selected file's event_keys and merges results.
	 */
	useEffect(() => {
		if (initKeys?.length) {
			setKeys(initKeys);
			return;
		}

		let cancelled = false;
		(async () => {
			const set = new Set<string>();
			const map: Record<string, string> = {};
			await Promise.all(
				files.map(async (file) => {
					const fileKeys = await Info.event_keys(file);
					Object.entries(fileKeys).forEach(([k, t]) => {
						set.add(k);
						map[k] = t as string;
					});
				}),
			);
			if (!cancelled) {
				const sortedKeys = [...set].sort((a, b) => a.localeCompare(b));
				setKeys(sortedKeys);
				setFieldTypeMap(map);
			}
		})();
		return () => {
			cancelled = true;
		};
	}, [Info, files, initKeys]);

	/** Updates the filters array within the query state. */
	const setFilters = useCallback(
		(action: Filter.Item[] | ((prev: Filter.Item[]) => Filter.Item[])) =>
			setQuery((q) => ({
				...q,
				filters: typeof action === "function" ? action(q.filters) : action,
			})),
		[],
	);

	/**
	 * Assembles the final Query.Type for submission or preview.
	 *
	 * Merges builder state with manual JSON, attaches fieldTypeMap for correct
	 * clause generation, and generates the human-readable display label.
	 *
	 * @returns The final Query.Type, or null if manual JSON is invalid.
	 */
	const getFinalQuery = useCallback((): Query.Type | null => {
		const builderState = { ...query };
		let manualState = undefined;
		try {
			manualState = JSON.parse(manualContent);
		} catch {
			if (isManual) {
				toast.error(t("filterFile.invalidJson"));
				return null;
			}
		}

		const finalQuery: Query.Type = {
			...builderState,
			raw: manualState,
			isManual,
			fieldTypeMap,
		};

		// Generate the display label from the actual query state
		finalQuery.string = Filter.Entity.describe(finalQuery, app);

		return finalQuery;
	}, [isManual, manualContent, query, fieldTypeMap, app, t]);

	/**
	 * Submit handler — applies the current query to the selected files.
	 *
	 * Steps:
	 * 1. Build the final query (with display label)
	 * 2. Cache current events for undo support
	 * 3. Persist the query via Info.setQuery()
	 * 4. Trigger refetch for all selected files
	 * 5. Re-render and close the banner
	 */
	const submit = useCallback(async () => {
		const finalQuery = getFinalQuery();
		if (!finalQuery) return;

		setLoading(true);
		try {
			Info.filters_cache(files);
			Info.setQuery(files, finalQuery);
			await Info.refetch({
				ids: files.map((f) => f.id),
				addToHistory: true,
				create_notes: createNotesChecked,
				notes_color,
				notes_tags,
				notes_glyph_id,
				name,
			});
			Info.render();
			props.back ? props.back() : destroyBanner();
		} finally {
			setLoading(false);
		}
	}, [
		Info,
		files,
		getFinalQuery,
		createNotesChecked,
		notes_color,
		notes_tags,
		notes_glyph_id,
		name,
		props,
		destroyBanner,
	]);

	/**
	 * Builds the OpenSearch query for preview display, optionally adding
	 * a flagged-events filter (terms clause on doc IDs).
	 */
	const queryWithFlaggedEvents = useMemo(() => {
		const baseQuery = Filter.Entity.query({ ...query, fieldTypeMap });
		if (flaggedOnly) {
			const operation = Operation.Entity.selected(app);
			const flaggedEvents = Doc.Entity.flag.getDocIds(app, operation?.id);
			if (flaggedEvents.length > 0) {
				if (!baseQuery.bool) baseQuery.bool = { must: [] };
				if (!baseQuery.bool.must) baseQuery.bool.must = [];
				baseQuery.bool.must.push({ terms: { id: flaggedEvents } });
			}
		}
		return baseQuery;
	}, [query, flaggedOnly, app, fieldTypeMap]);

	const Done = useMemo(
		() => (
			<Button
				icon="Check"
				variant="secondary"
				loading={loading}
				onClick={submit}
			/>
		),
		[loading, submit],
	);

	const QueryStringPart = useMemo(
		() => (
			<OpenSearchQueryBuilder.Query.String
				style={fws}
				textFilter={query.text_filter || ""}
				setTextFilter={(text_filter) =>
					setQuery((q) => ({ ...q, text_filter }))
				}
				reset={() => setQuery((q) => ({ ...q, text_filter: "" }))}
			/>
		),
		[query.text_filter],
	);

	const AddCondition = useMemo(
		() => (
			<OpenSearchQueryBuilder.Query.Add
				filters={query.filters}
				setFilters={setFilters}
			/>
		),
		[query.filters, setFilters],
	);

	const QueryConditions = useMemo(
		() => (
			<OpenSearchQueryBuilder.Query.Filters
				filters={query.filters}
				setFilters={setFilters}
				keys={keys}
			/>
		),
		[query.filters, setFilters, keys],
	);

	const [isPreviewLoading, setIsPreviewLoading] = useState(false);

	/**
	 * Preview button handler — executes the current query in preview mode
	 * and opens a results banner. Passes the full query state back so
	 * the user can return to this banner with their configuration intact.
	 */
	const previewCurrentFilterButtonClickHandler = useCallback(() => {
		const finalQuery = getFinalQuery();
		if (!finalQuery) return;

		setIsPreviewLoading(true);
		Info.preview_query({ ...finalQuery, fieldTypeMap })
			.then(({ docs, total_hits }) => {
				if (total_hits > 0) {
					spawnBanner(
						<Preview.Banner
							total={total_hits}
							values={docs}
							fixed
							back={() =>
								spawnBanner(
									<FilterFileBanner
										sources={files}
										query={finalQuery}
										keys={keys}
										create_notes={initCreateNotes}
										notes_color={notes_color}
										notes_tags={notes_tags}
										notes_glyph_id={notes_glyph_id}
										name={name}
										{...props}
									/>,
								)
							}
						/>,
					);
				}
			})
			.finally(() => setIsPreviewLoading(false));
	}, [
		Info,
		getFinalQuery,
		files,
		keys,
		spawnBanner,
		props,
		initCreateNotes,
		name,
		notes_color,
		notes_glyph_id,
		notes_tags,
		fieldTypeMap,
	]);

	// Load last queries from server history on mount
	const [lastQueriesList, setLastQueriesList] = useState<Query.Type[]>([]);

	useEffect(() => {
		Info.getLastQueries().then(setLastQueriesList);
	}, [Info]);

	/**
	 * Handler for applying a query from the "Last filters" popover.
	 *
	 * Resolves source_ids from the history query against available sources in the app.
	 * If matching sources are found and `applySource` is true, auto-selects them in the file picker.
	 * Updates query state, manual mode, and editor content to reflect the applied filter.
	 *
	 * @param q - The historical query to apply
	 * @param applySource - Whether to change the source selection to match the filter's sources
	 */
	const handleApplyLastFilter = useCallback(
		(q: Query.Type, applySource: boolean = true) => {
			let filesToUse = files;
			let queryToApply = { ...q };

			if (applySource && q.source_config?.source_ids?.length) {
				// Apply the filter's sources
				const matchedFiles: Source.Type[] = [];
				for (const id of q.source_config.source_ids) {
					try {
						const file = Source.Entity.id(app, id as Source.Id);
						if (file) matchedFiles.push(file);
					} catch {
						/* source no longer exists, skip */
					}
				}

				if (matchedFiles.length > 0) {
					setFiles(matchedFiles);
					filesToUse = matchedFiles;
					prevFileIds.current = matchedFiles
						.map((f) => f.id)
						.sort()
						.join(",");
				}
			} else {
				// Keep current files: update query's source_config to match currently selected sources
				const cleanBase = getCleanBase(files);
				queryToApply = {
					...q,
					source_config: cleanBase.source_config
						? {
								operation_id:
									q.source_config?.operation_id ??
									cleanBase.source_config.operation_id,
								range: q.source_config?.range ?? cleanBase.source_config.range,
								source_ids: files.map((f) => f.id),
							}
						: undefined,
				};
			}

			// Apply the full query state
			setQuery(queryToApply);
			setIsManual(!!queryToApply.isManual);
			setManualContent(getManualContentFromQuery(queryToApply));
			Info.setQuery(filesToUse, queryToApply);
		},
		[app, getManualContentFromQuery, Info, files],
	);

	/** Resets both builder and manual mode to a clean generated query from current sources. */
	const handleManualReset = () => {
		resetToCleanBase(files);
		toast.success(t("filterFile.resetGeneratedSuccess"));
	};

	/** Formats the manual JSON editor content with proper indentation. */
	const handleBeautify = useCallback(() => {
		try {
			setManualContent(JSON.stringify(JSON.parse(manualContent), null, 2));
		} catch {
			toast.error(t("filterFile.failedParseJson"));
		}
	}, [manualContent, t]);

	return (
		<Banner
			title={t("filterFile.title")}
			done={Done}
			side={
				!isManual ? (
					<OpenSearchQueryBuilder.Preview query={queryWithFlaggedEvents} />
				) : null
			}
			subtitle={
				<QueriesHistory
					list={lastQueriesList}
					onSelect={handleApplyLastFilter}
				/>
			}
			className={s.banner}
			{...props}
		>
			<TooltipProvider>
				{useMemo(
					() => (
						<Source.Select.Multi
							selected={fileIds}
							setSelected={handleSourceChange}
							placeholder={t("filterFile.selectSourcesPlaceholder")}
						/>
					),
					[fileIds, handleSourceChange],
				)}

				<Tabs
					value={String(isManual)}
					onValueChange={(v) => setIsManual(v === "true")}
				>
					<Stack
						dir="row"
						jc="space-between"
					>
						<TabsList>
							<TabsTrigger value="false">{t("common.builder")}</TabsTrigger>
							<TabsTrigger value="true">{t("common.manual")}</TabsTrigger>
						</TabsList>
						{!isManual && (
							<Stack style={{ margin: "8px 0" }}>
								<Checkbox
									id="isFlagedEventOnly"
									checked={flaggedOnly}
									onCheckedChange={(v) => setFlaggedOnly(!!v)}
								/>
								<Label
									htmlFor="isFlagedEventOnly"
									value={t("filterFile.flaggedOnly")}
									cursor="pointer"
								/>
								<Icon name="Flag" />
							</Stack>
						)}
					</Stack>
					<Separator style={{ margin: "8px 0" }} />
					<TabsContent value="false">
						<Stack
							dir="column"
							ai="stretch"
						>
							{QueryStringPart}
							{AddCondition}
							<Separator />
							{QueryConditions}
						</Stack>
					</TabsContent>
					<TabsContent value="true">
						<Textarea
							className={s.manualTextarea}
							value={manualContent}
							onChange={(e) => setManualContent(e.target.value)}
							placeholder={t("filterFile.manualPlaceholder")}
						/>
					</TabsContent>
				</Tabs>
				<Stack
					ai="center"
					jc="flex-start"
					dir="row"
				>
					<Button
						variant="secondary"
						loading={isPreviewLoading}
						onClick={previewCurrentFilterButtonClickHandler}
						icon="PreviewDocument"
					>
						{t("filterFile.previewCurrent")}
					</Button>

					{isManual && (
						<>
							<Button
								variant="secondary"
								icon="RefreshCw"
								onClick={handleManualReset}
							>
								{t("filterFile.resetGenerated")}
							</Button>
							<Button
								variant="secondary"
								icon="Wand"
								onClick={handleBeautify}
							>
								{t("filterFile.beautify")}
							</Button>
						</>
					)}
				</Stack>
				{initCreateNotes !== undefined &&
					initCreateNotes !== null &&
					initCreateNotes !== false && (
						<>
							<Separator style={{ margin: "8px 0" }} />
							<Stack style={{ margin: "8px 0" }}>
								<Checkbox
									id="create_notes"
									checked={createNotesChecked}
									onCheckedChange={(v) => setCreateNotesChecked(!!v)}
								/>
								<Label
									htmlFor="create_notes"
									value={t("filterFile.createNoteForFlagged")}
									cursor="pointer"
								/>
							</Stack>
						</>
					)}
			</TooltipProvider>
		</Banner>
	);
}

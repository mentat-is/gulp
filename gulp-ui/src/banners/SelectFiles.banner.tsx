import { Application } from "@/context/Application.context";
import { Banner as UIBanner } from "@/ui/Banner";
import { Checkbox } from "@/ui/Checkbox";
import s from "./styles/SelectFilesBanner.module.css";
import { Label } from "@/ui/Label";
import { useEffect, useMemo, useReducer, useState, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { Frame } from "./Frame.banner";
import { UploadBanner } from "./Upload.banner";
import { Separator } from "@/ui/Separator";
import { Preview } from "./Preview.banner";
import { FilterFileBanner } from "./FilterFile.banner";
import { cn } from "@/ui/utils";
import { Refractor } from "@/ui/utils";
import { SetState } from "@/class/API";
import { toast } from "sonner";
import { Stack } from "@/ui/Stack";
import { Badge } from "@/ui/Badge";
import { Button } from "@/ui/Button";
import { Input } from "@/ui/Input";
import { Spinner } from "@/ui/Spinner";
import { Skeleton } from "@/ui/Skeleton";
import { Progress } from "@/ui/Progress";
import {
	Tooltip,
	TooltipContent,
	TooltipProvider,
	TooltipTrigger,
} from "@/ui/Tooltip";
import { Context } from "@/entities/Context";
import { Source } from "@/entities/Source";
import { Operation } from "@/entities/Operation";
import { Request } from "@/entities/Request";
import { Select } from "@/ui/Select";
import { Session } from "./Session.banner";
import { DisplayEventDialog } from "@/dialogs/Event.dialog";
import { useParams } from "react-router-dom";
import { Internal } from "@/entities/addon/Internal";
import { Locale } from "@/locales";
import { WindowBridge } from "@/lib/WindowBridge";
import { requestStore } from "@/store/request.store";

export namespace SelectFiles {
	export namespace Banner {
		export type Props = UIBanner.Props & {
			showSession?: boolean;
		};
	}

	/**
	 * Renders the source/context selection banner with session loading, upload progress,
	 * and virtualized source rows.
	 *
	 * @param props - Banner options and the optional session selector flag.
	 * @returns The source selection banner.
	 */
	export function Banner({ showSession = true, fixed: _fixed, ...props }: Banner.Props) {
		const { app, Info, spawnBanner, spawnDialog, destroyBanner } = Application.use();
		const { t } = Locale.use();
		const { operation_id: routeOpId } = useParams<{ operation_id: string }>();
		const operation = Operation.Entity.selected(app);
		const operation_id = routeOpId || operation?.id;

		const [sessions, setSessions] = useState<Internal.Session.Data[]>([]);
		const [openSelectSession, setOpenSelectSession] = useState(false);

		const activeSessions = useMemo(() => {
			if (!operation_id) return [];
			return sessions.filter(
				(session) =>
					session.selected.operations &&
					session.selected.operations === operation_id,
			);
		}, [sessions, operation_id]);

		/**
		 * Reloads the sessions list from the backend for the current user.
		 */
		const reloadSessionsList = () => {
			Info.session_list(app.general.user).then((res) => {
				if (res) {
					setSessions(res);
				}
			});
		};

		useEffect(() => {
			let isMounted = true;
			Info.session_list(app.general.user).then((res) => {
				if (isMounted && res) {
					setSessions(res);
				}
			});
			return () => {
				isMounted = false;
			};
		}, [app.general.user]);

		/**
		 * Loads a specific saved session, clears local selections, and launches the operation.
		 * @param name - The name of the session to load.
		 */
		const load_session = async (name: string) => {
			const session = sessions.find((s) => s.name === name);
			if (!session) return;

			await Info.session_load(session);
			setSelectedContexts(new Set());
			setSelectedFiles(new Set());
			spawnBanner(null);

			if (session.timeline.target) {
				spawnDialog(<DisplayEventDialog event={session.timeline.target} />);
			}
		};

		const [filter, setFilter] = useState("");
		const [refreshToken, debug] = useReducer((i) => ++i, 0);
		const [loading, setLoading] = useState(false);
		const [showEmptySources, setShowEmptySources] = useState(false);
		const [selectedContexts, setSelectedContexts] = useState<Set<Context.Id>>(
			new Set(Context.Entity.selected(app).map((c) => c.id)),
		);
		const [selectedFiles, setSelectedFiles] = useState<Set<Source.Id>>(
			new Set(Source.Entity.selected(app).map((c) => c.id)),
		);

		useEffect(() => {
			const timer = setInterval(debug, 1000);

			return () => clearInterval(timer);
		}, []);

		useEffect(() => {
			const operation = Operation.Entity.selected(app);
			if (!operation) return;
			Info.resync_ingestion_state(operation.id);
		}, []);

		const update = <T,>(values: Set<T>, vault: SetState<Set<T>>) =>
			vault(new Set(values));

		function all(select: boolean) {
			const operation = Operation.Entity.selected(app);
			if (!operation) {
				toast.error(t("operationSelect.notSelected"), {
					richColors: true,
				});
				return;
			}

			const method = select ? "add" : "delete";

			app.target.contexts
				.filter((context) => context.operation_id === operation.id)
				.forEach((context) => {
					const contextMatches =
						!filter ||
						context.name.toLowerCase().includes(filter.toLowerCase());

					Context.Entity.sources(app, context).forEach((file) => {
						if (!showEmptySources && !file.total) return;
						const fileMatches =
							!filter || file.name.toLowerCase().includes(filter.toLowerCase());
						if (contextMatches || fileMatches) {
							selectedFiles[method](file.id);
						}
					});
				});

			app.target.contexts
				.filter((context) => context.operation_id === operation.id)
				.forEach((context) => {
					const files = Context.Entity.sources(app, context).filter(
						(file) => showEmptySources || file.total,
					);

					if (files.some((file) => selectedFiles.has(file.id))) {
						selectedContexts.add(context.id);
					} else {
						selectedContexts.delete(context.id);
					}
				});

			update(selectedFiles, setSelectedFiles);
			update(selectedContexts, setSelectedContexts);
		}

		function setContext(contextId: Context.Id, select: boolean) {
			const method = select ? "add" : "delete";
			const context = Context.Entity.id(app, contextId);
			if (!context) return;

			const contextMatches =
				!filter || context.name.toLowerCase().includes(filter.toLowerCase());

			const files = Context.Entity.sources(app, context).filter(
				(file) => showEmptySources || file.total,
			);

			files.forEach((file) => {
				const fileMatches =
					!filter || file.name.toLowerCase().includes(filter.toLowerCase());
				if (contextMatches || fileMatches) {
					selectedFiles[method](file.id);
				}
			});

			if (files.some((file) => selectedFiles.has(file.id))) {
				selectedContexts.add(contextId);
			} else {
				selectedContexts.delete(contextId);
			}

			update(selectedFiles, setSelectedFiles);
			update(selectedContexts, setSelectedContexts);
		}

		function setFile(target: Source.Id, select: boolean) {
			const method = select ? "add" : "delete";

			selectedFiles[method](target);

			const file = Source.Entity.id(app, target);

			const files = Context.Entity.sources(app, file.context_id);

			if (files.some((file) => selectedFiles.has(file.id))) {
				selectedContexts.add(file.context_id);
			} else {
				selectedContexts.delete(file.context_id);
			}

			update(selectedContexts, setSelectedContexts);
			update(selectedFiles, setSelectedFiles);
		}

		const hasData =
			app.target.operations.length > 0 || app.target.contexts.length > 0;
		const hasRenderableData = hasData || Info.activeUploads.size > 0;

		/**
		 * Persists the selected contexts and sources, then replays that active
		 * selection to any detached windows.
		 *
		 * @returns Nothing.
		 */
		const save = () => {
			const contexts = Refractor.array(
				...app.target.contexts.map((context) => ({
					...context,
					selected: selectedContexts.has(context.id),
				})),
			);
			const files = Refractor.array(
				...app.target.files.map((file) => ({
					...file,
					selected: selectedFiles.has(file.id),
				})),
			);

			Info.setInfoByKey(contexts, "target", "contexts");
			Info.setInfoByKey(files, "target", "files");

			/**
			 * Replays the just-saved source/context selection to detached tabs.
			 *
			 * @returns Nothing.
			 */
			const replayDetachedSelection = () => {
				if (!operation_id) return;

				WindowBridge.broadcastMainContext(
					{
						...app,
						target: {
							...app.target,
							contexts,
							files,
						},
					},
					"active",
					operation_id as Operation.Id,
				);
			};

			replayDetachedSelection();
			Info.session_autosave();
			window.dispatchEvent(new CustomEvent(WindowBridge.DETACHED_REPLAY_EVENT));
			setTimeout(() => {
				replayDetachedSelection();
				window.dispatchEvent(new CustomEvent(WindowBridge.DETACHED_REPLAY_EVENT));
				spawnBanner(
					<Frame.Banner
						//was fixed //(disable close button)
						onClose={Info.notes_reload}
						back={() => spawnBanner(<SelectFiles.Banner />)}
					/>,
				);
			}, 10);
		};

		const reloadClickHandler = async () => {
			setLoading(true);
			await Info.sync();
			setLoading(false);
		};

		const filteredContexts = useMemo(() => {
			const term = filter.toLowerCase();
			return Operation.Entity.contexts(app).filter((ctx) => {
				if (!filter) return true;
				const contextMatches = ctx.name.toLowerCase().includes(term);
				const sourceMatches = Context.Entity.sources(app, ctx)
					.filter((file) => showEmptySources || file.total)
					.some((f) => f.name.toLowerCase().includes(term));
				return contextMatches || sourceMatches;
			});
		}, [app.target.contexts, app.target.files, filter, showEmptySources]);

		const selectionSummary = useMemo(() => {
			const contexts = app.target.contexts.filter(
				(context) => !operation_id || context.operation_id === operation_id,
			);
			const contextIds = new Set(contexts.map((context) => context.id));
			const files = app.target.files.filter(
				(file) =>
					contextIds.has(file.context_id) && (showEmptySources || file.total),
			);
			const selectedContextCount = contexts.filter((context) =>
				selectedContexts.has(context.id),
			).length;
			const selectedFileCount = files.filter((file) =>
				selectedFiles.has(file.id),
			).length;

			return {
				contexts: t("selectFiles.selectedOfTotal", {
					label: t("common.context"),
					selected: selectedContextCount,
					total: contexts.length,
				}),
				files: t("selectFiles.selectedOfTotal", {
					label: t("common.sources"),
					selected: selectedFileCount,
					total: files.length,
				}),
			};
		}, [app.target.contexts, app.target.files, operation_id, selectedContexts, selectedFiles, showEmptySources, t]);

		const SearchInput = useMemo(() => {
			return (
				<Input
					icon="Search"
					placeholder={t("selectFiles.searchPlaceholder")}
					variant="highlighted"
					value={filter}
					onChange={(e) => setFilter(e.target.value)}
				/>
			);
		}, [setFilter, filter]);

		type UploadProgressItem = {
			type: "upload";
			reqId: Request.Id;
			filename: string;
			percent: number;
		};

		type VirtualItemType =
			| UploadProgressItem
			| { type: "context"; context: Context.Type; hasFiles: boolean }
			| {
					type: "file";
					file: Source.Type;
					context: Context.Type;
					isLast: boolean;
			  };

		const items = useMemo(() => {
			const arr: VirtualItemType[] = [...Info.activeUploads.entries()].map(
				([reqId, { filename, percent }]) => ({
					type: "upload",
					reqId,
					filename,
					percent,
				}),
			);
			const term = filter.toLowerCase();

			filteredContexts.forEach((context) => {
				const contextMatches =
					!filter || context.name.toLowerCase().includes(term);
				const files = Context.Entity.sources(app, context).filter((file) => {
					if (!showEmptySources && !file.total) return false;
					if (contextMatches) return true;
					return file.name.toLowerCase().includes(term);
				});

				arr.push({ type: "context", context, hasFiles: files.length > 0 });

				files.forEach((file, index) => {
					arr.push({
						type: "file",
						file,
						context,
						isLast: index === files.length - 1,
					});
				});
			});
			return arr;
		}, [filteredContexts, filter, app.target.files, Info.activeUploads.size, refreshToken]);

		const parentRef = useRef<HTMLDivElement>(null);
		const rowVirtualizer = useVirtualizer({
			count: items.length,
			getScrollElement: () => parentRef.current,
			estimateSize: (index) => {
				const item = items[index];
				if (item.type === "context") return 50;
				if (item.type === "upload") return 42;
				return 36;
			},
			overscan: 10,
		});

		/**
		 * Builds the absolute transform used by the virtualizer for a visible row.
		 *
		 * @param start - The row offset calculated by the virtualizer.
		 * @returns The row transform style.
		 */
		const getVirtualRowStyle = (start: number) => ({
			transform: `translateY(${start}px)`,
		});

		return (
			<UIBanner
				title={t("source.selectSources")}
				className={s.banner}
				onClose={destroyBanner}
				done={
					<Button
						icon="Check"
						variant="glass"
						disabled={!selectedContexts.size || !selectedFiles.size}
						onClick={save}
					/>
				}
				option={
					<Button
						icon="Upload"
						variant="tertiary"
						title={t("operationView.menu.uploadFiles")}
						aria-label={t("operationView.menu.uploadFiles")}
						onClick={() => spawnBanner(<UploadBanner />)}
					/>
				}
				{...props}
			>
				{showSession && activeSessions.length > 0 && (
					<Stack
						dir="column"
						gap={6}
						ai="stretch"
						className={s.sessionSection}
					>
						<Label value={t("session.label")} />
						<Select.Root
							open={openSelectSession}
							onOpenChange={setOpenSelectSession}
							onValueChange={load_session}
						>
							<Select.Trigger>
								<Select.Icon name="Status" />
								{t("session.select")}
							</Select.Trigger>
							<Select.Content>
								{activeSessions.map((session) => (
									<Select.Item
										key={session.name}
										value={session.name}
										style={{ color: session.color }}
									>
										<Select.Icon name={session.icon} />
										{session.name}
									</Select.Item>
								))}
								<Button
									variant="tertiary"
									className={s.sessionManageButton}
									onClick={() =>
										spawnBanner(
											<Session.Delete.Banner
												onClose={reloadSessionsList}
												back={() =>
													spawnBanner(
														<SelectFiles.Banner showSession={showSession} />,
													)
												}
											/>,
										)
									}
									icon="Wrench"
								>
									{t("session.manage")}
								</Button>
							</Select.Content>
						</Select.Root>
					</Stack>
				)}
				{SearchInput}
				<Stack className={s.selectionSummary}>
					<Badge
						mono
						size="sm"
						variant="gray-subtle"
						value={selectionSummary.contexts}
					/>
					<Badge
						mono
						size="sm"
						variant="gray-subtle"
						value={selectionSummary.files}
					/>
				</Stack>
				<Stack className={s.emptySourcesToggle}>
					<Checkbox
						id="select-files-show-empty-sources"
						checked={showEmptySources}
						onCheckedChange={(checked) => {
							const next = !!checked;
							setShowEmptySources(next);
							if (next) void Info.sync({ includeEmptySources: true });
						}}
					/>
					<Label
						htmlFor="select-files-show-empty-sources"
						cursor="pointer"
						value={t("selectFiles.showEmptySources")}
					/>
				</Stack>
				<Stack>
					<Button
						onClick={() => all(true)}
						variant="secondary"
						className={s.actionButton}
						icon="FilePlus"
					>
						{t("common.selectAll")}
					</Button>
					<Button
						onClick={() => all(false)}
						variant="secondary"
						className={s.actionButton}
						icon="FileMinus"
					>
						{t("common.deselectAll")}
					</Button>
				</Stack>
				<div
					className={s.wrapper}
				>
					<Skeleton
						show={!hasRenderableData}
						width="full"
						className={s.skeleton}
					>
						{items.length > 0 ? (
							<div
								ref={parentRef}
								className={s.scrollArea}
							>
								<div
									className={s.virtualList}
									style={{ height: `${rowVirtualizer.getTotalSize()}px` }}
								>
									{rowVirtualizer.getVirtualItems().map((virtualRow) => {
										const item = items[virtualRow.index];
										if (!item) return null;

										if (item.type === "context") {
											return (
												<div
													key={virtualRow.key}
													data-index={virtualRow.index}
													ref={rowVirtualizer.measureElement}
													className={s.virtualRow}
													style={getVirtualRowStyle(virtualRow.start)}
												>
													<ContextHeading
														context={item.context}
														selectedContexts={selectedContexts}
														setContext={setContext}
														noFiles={!item.hasFiles}
													/>
												</div>
											);
										}

										if (item.type === "upload") {
											return (
												<div
													key={virtualRow.key}
													data-index={virtualRow.index}
													ref={rowVirtualizer.measureElement}
													className={s.virtualRow}
													style={getVirtualRowStyle(virtualRow.start)}
												>
													<Stack className={s.uploadingRow}>
														<Spinner size={16} />
														<Label value={item.filename} />
														<Progress
															value={item.percent}
															className={s.uploadProgress}
														/>
													</Stack>
												</div>
											);
										}

										return (
											<div
												key={virtualRow.key}
												data-index={virtualRow.index}
												ref={rowVirtualizer.measureElement}
												className={s.virtualRow}
												style={getVirtualRowStyle(virtualRow.start)}
											>
												<div
													className={cn(s.fileWrapper, item.isLast && s.last)}
												>
													<FlatFileComponent
														file={item.file}
														selectedFiles={selectedFiles}
														setFile={setFile}
														isLast={item.isLast}
														onPreviewBack={() =>
															spawnBanner(<SelectFiles.Banner />)
														}
													/>
												</div>
											</div>
										);
									})}
								</div>
							</div>
						) : (
							<p className={s.noData}>
								{t("selectFiles.noData")}
							</p>
						)}
					</Skeleton>
				</div>
				<Stack>
					<Button
						onClick={reloadClickHandler}
						variant="secondary"
						className={s.actionButton}
						icon="RefreshClockwise"
						loading={loading}
					>
						{t("common.refresh")}
					</Button>
				</Stack>
			</UIBanner>
		);
	}
}

/**
 * Renders a context heading with optional selection and delete controls.
 *
 * @param props - Context heading data and callbacks.
 * @returns The context heading row.
 */
export function ContextHeading({
	context,
	selectedContexts,
	setContext,
	noFiles,
	showCheckbox = true,
	onDeleteBack,
}: ContextHeadingProps) {
	const { spawnBanner } = Application.use();
	const { t } = Locale.use();

	/**
	 * Returns to the caller's source/context view after a context is deleted.
	 *
	 * @returns Nothing.
	 */
	const handleDeleteBack = () => {
		if (onDeleteBack) {
			onDeleteBack();
			return;
		}

		spawnBanner(<SelectFiles.Banner />);
	};

	return (
		<Stack
			dir="column"
			ai="stretch"
			gap={0}
			className={cn(s.branchHeader, noFiles && s.noFiles)}
		>
			<Stack
				className={s.contextHeading}
				gap={8}
			>
				{showCheckbox && (
					<Checkbox
						style={{ height: 20, width: 20 }}
						checked={selectedContexts?.has(context.id) || false}
						onCheckedChange={(checked) => setContext(context.id, !!checked)}
						id={context.name}
					/>
				)}
				<Label value={context.name} />
				<hr style={{ flex: 1 }} />
				<Badge
					size="sm"
					value={t("common.delete")}
					style={{ border: "1px solid var(--red-400)", borderRadius: "2px" }}
					variant="red-subtle"
					icon="Trash2"
					onClick={() =>
						spawnBanner(
							<Context.Delete.Banner
								context={context}
								back={handleDeleteBack}
							/>,
						)
					}
				/>
			</Stack>
			{!noFiles && <Separator className={s.separator} />}
		</Stack>
	);
}

interface ContextHeadingProps {
	/** Context rendered in the section heading. */
	context: Context.Type;
	/** Set of currently selected context IDs. */
	selectedContexts: Set<Context.Id>;
	/** Toggles the context and matching sources. */
	setContext: (contextId: Context.Id, select: boolean) => void;
	/** Whether the context has no visible source rows. */
	noFiles: boolean;
	/** Whether to show the selection checkbox. */
	showCheckbox?: boolean;
	/** Callback invoked after deleting a context. */
	onDeleteBack?: () => void;
}

export function FlatFileComponent({
	file,
	selectedFiles,
	setFile,
	isLast,
	showFilter = true,
	showCheckbox = true,
	onPreviewBack,
}: any) {
	return (
		<Stack
			dir="column"
			ai="stretch"
			jc="flex-start"
			className={cn(s.fileBranch, isLast && s.last)}
		>
			<FileComponent
				file={file}
				selectedFiles={selectedFiles}
				setFile={setFile}
				showFilter={showFilter}
				showCheckbox={showCheckbox}
				onPreviewBack={onPreviewBack}
			/>
		</Stack>
	);
}

interface FileComponentProps {
	file: Source.Type;
	selectedFiles: Set<Source.Id>;
	setFile: (file: Source.Id, select: boolean) => void;
	showFilter?: boolean;
	showCheckbox?: boolean;
	onPreviewBack?: () => void;
}

function FileComponent({
	file,
	setFile,
	selectedFiles,
	showFilter = true,
	showCheckbox = true,
	onPreviewBack,
}: FileComponentProps) {
	const { app, Info, spawnBanner } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState<boolean>(false);

	const previewButtonClickHandler = () => {
		setLoading(true);
		Info.preview_file(file).then(({ docs, total_hits }) =>
			spawnBanner(
				<Preview.Banner
					total={total_hits}
					values={docs}
					fixed
					back={() => {
						setLoading(false);
						if (onPreviewBack) onPreviewBack();
					}}
				/>,
			),
		);
	};

	const progressDefault = Info.ingestionProgress.get(file.id) || 0;
	const [progress, setProgress] = useState<number>(progressDefault);

	useEffect(() => {
		const interval = setInterval(() => {
			const isIngesting =
				Source.Entity.getRequestType(Info.app, file.id) ===
				Request.Prefix.INGESTION;

			if (isIngesting) {
				const reqId = requestStore.getRequestIdByFile(file.id);
				const p =
					Info.ingestionProgress.get(file.id) ||
					(reqId ? Info.ingestionProgress.get(reqId as any) : 0) ||
					0;
				setProgress((prev) => (prev !== p ? p : prev));
			} else {
				setProgress((prev) => (prev !== 0 ? 0 : prev));
			}
		}, 1000);

		return () => clearInterval(interval);
	}, [file.id, Info]);

	const FileIsTooBig = () => {
		const total = Math.max(file.total, progress);
		if (total < 500_000) {
			return null;
		}

		return (
			<Badge
				size="sm"
				variant="amber-subtle"
				icon="Warning"
				value={t("selectFiles.fileTooBig")}
			/>
		);
	};

	return (
		<Stack
			className={s.file}
			key={file.id}
		>
			{showCheckbox && (
				<Checkbox
					id={file.name}
					checked={selectedFiles?.has(file.id) || false}
					onCheckedChange={(checked) => setFile(file.id, !!checked)}
				/>
			)}
			{Source.Entity.getRequestType(app, file) === Request.Prefix.INGESTION && (
				<TooltipProvider>
					<Tooltip>
						<TooltipTrigger asChild>
							<span>
								<Spinner size={16} />
							</span>
						</TooltipTrigger>
						<TooltipContent>{t("selectFiles.processing")}</TooltipContent>
					</Tooltip>
				</TooltipProvider>
			)}
			<Label value={file.name} />
			<FileIsTooBig />
			<Badge
				size="sm"
				className={s.amount}
				variant="gray-subtle"
				value={Math.max(file.total, progress)}
			/>
			{showFilter && (
				<Button
					shape="icon"
					icon="Filter"
					variant="secondary"
					className={s.smallButton}
					onClick={() =>
						spawnBanner(
							<FilterFileBanner
								sources={[file]}
								fixed
								back={() => {
									if (onPreviewBack) onPreviewBack();
								}}
							/>,
						)
					}
				/>
			)}
			<Button
				shape="icon"
				icon="PreviewEye"
				variant="secondary"
				loading={loading}
				className={s.smallButton}
				onClick={previewButtonClickHandler}
			/>
			<Badge
				size="sm"
				style={{
					border: "1px solid var(--red-400)",
					borderRadius: "2px",
					padding: 4,
				}}
				variant="red-subtle"
				icon="Trash2"
				onClick={() =>
					spawnBanner(
						<Source.Delete.Banner
							source={file}
							back={() => {
								if (onPreviewBack) onPreviewBack();
							}}
						/>,
					)
				}
			/>
		</Stack>
	);
}

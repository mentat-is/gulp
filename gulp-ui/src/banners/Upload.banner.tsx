import { Application } from "@/context/Application.context";
import { Banner } from "@/ui/Banner";
import { Select } from "@/ui/Select";
import {
	useEffect,
	useState,
	useCallback,
	useMemo,
	ChangeEvent,
	useRef,
} from "react";
import s from "./styles/UploadBanner.module.css";
import { MinMax, MinMaxBase } from "@/class/Info";
import { formatBytes, Refractor } from "@/ui/utils";
import { SelectFiles } from "./SelectFiles.banner";
import { Popover } from "@/ui/Popover";
import { Icon } from "@/ui/Icon";
import { Toggle } from "@/ui/Toggle";
import { Separator } from "@/ui/Separator";
import { cn } from "@/ui/utils";
import { Default } from "@/dto/Dataset";
import { toast } from "sonner";
import { SetState } from "@/class/API";
import { Progress as UIProgress } from "@/ui/Progress";
import { Table } from "@/components/Table";
import { CustomParameters } from "@/components/CustomParameters";
import { Input } from "@/ui/Input";
import { Checkbox } from "@/ui/Checkbox";
import { Label } from "@/ui/Label";
import { Stack } from "@/ui/Stack";
import { Button } from "@/ui/Button";
import { Badge } from "@/ui/Badge";
import { Spinner } from "@/ui/Spinner";
import { Context } from "@/entities/Context";
import { Operation } from "@/entities/Operation";
import { Doc } from "@/entities/Doc";
import { Mapping } from "@/entities/Mapping";
import React from "react";
import {
	Tooltip,
	TooltipContent,
	TooltipProvider,
	TooltipTrigger,
} from "@/ui/Tooltip";
import { AdvancedPluginParams } from "@/components/AdvancedPluginParams";
import { Internal } from "@/entities/addon/Internal";
import { Locale } from "@/locales";
import {
	getFilePathsWithoutPlugin,
	NO_METHOD_SELECTED,
	NO_MAPPING_SELECTED,
	resolveOptionalMethod,
	resolveOptionalMapping,
} from "./Upload.validation";
import {
	detectFilePlugin,
	MAX_FILE_SIGNATURE_LENGTH,
} from "./Upload.detection";

export namespace FileEntity {
	export interface IngestOptions {
		context: Context.Id | string;
		file: any;
		frame?: MinMax;
		settings: FileEntity.Settings;
		setProgress?: (num: number) => void;
		preview_mode?: boolean;
	}

	export interface Settings {
		plugin?: string;
		method?: string;
		mapping?: string;
		offset: number;
		custom_parameters: Record<string, any>;
		plugin_params?: Record<string, any>;
		store_file?: boolean;
		[key: string]: any;
	}
}

import { useVirtualizer } from "@tanstack/react-virtual";

const filePath = (file: File) =>
	(file as File & { path?: string }).path || file.webkitRelativePath || file.name;

const FilesList = React.memo(function FilesList(props: {
	files: File[];
	setFiles: SetState<File[]>;
	settings: Record<string, FileEntity.Settings>;
	progress: Record<string, number>;
	updateSettings: (
		filename: string,
		update: Partial<FileEntity.Settings>,
	) => void;
}) {
	const { files, settings, progress, updateSettings } = props;
	const parentRef = useRef<HTMLDivElement>(null);

	const rowVirtualizer = useVirtualizer({
		count: files.length,
		getScrollElement: () => parentRef.current,
		estimateSize: () => 40,
		overscan: 10,
	});

	return (
		<div
			ref={parentRef}
			style={{
				height: "100%",
				width: "100%",
				overflow: "auto",
			}}
		>
			<div
				style={{
					height: `${rowVirtualizer.getTotalSize()}px`,
					width: "100%",
					position: "relative",
				}}
			>
				{rowVirtualizer.getVirtualItems().map((virtualRow) => {
					const file = files[virtualRow.index];
					const path = filePath(file);
					return (
						<FilePreview
							key={virtualRow.key}
							data-index={virtualRow.index}
							ref={rowVirtualizer.measureElement}
							style={{
								position: "absolute",
								top: 0,
								left: 0,
								width: "100%",
								transform: `translateY(${virtualRow.start}px)`,
							}}
							file={file}
							setFiles={props.setFiles}
							settings={settings[path] || { custom_parameters: {} }}
							progress={progress[path]}
							updateSettings={(update) => updateSettings(path, update)}
						/>
					);
				})}
			</div>
		</div>
	);
});

export const ContextSelector = ({
	app,
	context,
	setContext,
}: {
	app: any;
	context: string;
	setContext: (value: string) => void;
}) => {
	const contexts = Operation.Entity.contexts(app);
	const { t } = Locale.use();

	return (
		<Stack
			dir="column"
			gap={6}
			ai="flex-start"
		>
			<Label value={t("common.context")} />
			<Select.Root
				value={context}
				onValueChange={setContext}
				disabled={!contexts.length}
			>
				<Select.Trigger>
					<Icon
						name={Context.Entity.icon(Context.Entity.findByName(app, context)!)}
					/>
					{context || t("upload.selectContext")}
				</Select.Trigger>
				<Select.Content>
					{contexts.map((c) => (
						<Select.Item
							key={c.name}
							value={c.name}
						>
							<Icon name={Context.Entity.icon(c)} />
							{c.name}
						</Select.Item>
					))}
				</Select.Content>
			</Select.Root>
		</Stack>
	);
};

export const FilePreview = React.memo(
	React.forwardRef<
		HTMLDivElement,
		{
			file: File;
			settings: FileEntity.Settings;
			updateSettings: (update: Partial<FileEntity.Settings>) => void;
			progress: number | undefined;
			setFiles: SetState<File[]>;
			style?: React.CSSProperties;
			"data-index"?: number;
		}
	>(
		(
			{
				file,
				settings,
				updateSettings,
				progress,
				setFiles,
				style,
				"data-index": dataIndex,
			},
			ref,
		) => {
			const { Info, app } = Application.use();
			const { t } = Locale.use();
			const path = filePath(file);
			const [preview, setPreview] = useState<Doc.Type[] | null>(null);
			const [isPreviewLoading, setIsPreviewLoading] = useState(false);

			const methods = useMemo(
				() => Mapping.Entity.methods(app, settings.plugin),
				[app, settings.plugin],
			);

			const mappings = useMemo(
				() => Mapping.Entity.mappings(app, settings.plugin, settings.method),
				[app, settings.plugin, settings.method],
			);

			/**
			 * We removed the useEffects that were calling updateSettings during render.
			 * Plugin, Method, and Mapping defaults are now calculated either at file addition
			 * time or inside the specific selector components to avoid global re-render loops.
			 */

			const handleSettingsUpdate = (update: Partial<FileEntity.Settings>) => {
				const finalUpdate = { ...update };

				if (update.plugin) {
					const currentMethods = Mapping.Entity.methods(app, update.plugin);
					if (currentMethods.length === 1) {
						finalUpdate.method = currentMethods[0];
						const currentMappings = Mapping.Entity.mappings(
							app,
							update.plugin,
							currentMethods[0],
						);
						if (currentMappings.length === 1) {
							finalUpdate.mapping = currentMappings[0];
						}
					} else {
						finalUpdate.method = undefined;
						finalUpdate.mapping = undefined;
					}
				} else if (update.method) {
					const currentMappings = Mapping.Entity.mappings(
						app,
						settings.plugin,
						update.method,
					);
					if (currentMappings.length === 1) {
						finalUpdate.mapping = currentMappings[0];
					} else {
						finalUpdate.mapping = undefined;
					}
				}

				updateSettings(finalUpdate);
			};

			/**
			 * Loads preview rows for the current file before ingestion starts.
			 *
			 * @returns A promise that resolves when the preview state has been updated.
			 */
			const loadPreview = async () => {
				setIsPreviewLoading(true);
				setPreview(null);

				try {
					const nextPreview = await Info.file_ingest_preview({
						preview_mode: true,
						context: "preview",
						file,
						settings,
					});

					setPreview(nextPreview);
				} catch {
					toast.error(t("upload.previewLoadFailed"));
					setPreview([]);
				} finally {
					setIsPreviewLoading(false);
				}
			};

			const setCustomParameters = (update: any) => {
				const next =
					typeof update === "function"
						? update(settings.custom_parameters)
						: update;
				updateSettings({ custom_parameters: next });
			};

			const fileOffsetInputChangeHandler = (
				event: ChangeEvent<HTMLInputElement>,
			) => {
				const offset = parseInt(event.currentTarget.value);
				if (Number.isNaN(offset)) {
					return;
				}

				updateSettings({ offset });
			};

			return (
				<Stack
					ref={ref}
					style={style}
					data-index={dataIndex}
					className={cn(
						s.filePreview,
						!settings.plugin && s.filePreviewMissingPlugin,
					)}
					aria-invalid={!settings.plugin}
					gap={0}
					flex={0}
					pos={style?.position || "relative"}
				>
					{progress && <Progress value={progress} />}
					<Icon name={Default.Icon.SOURCE} />
					<TooltipProvider>
						<Tooltip>
							<TooltipTrigger asChild>
								<p className={s.filename}>{file.name}</p>
							</TooltipTrigger>
							<TooltipContent>{path}</TooltipContent>
						</Tooltip>
					</TooltipProvider>
					<Popover.Root onOpenChange={(o) => o && loadPreview()}>
						<Popover.Trigger asChild>
							<Button
								icon="PreviewDocument"
								variant="tertiary"
								disabled={!settings.plugin}
								aria-disabled={!settings.plugin}
								onClick={(event) => {
									if (!settings.plugin) event.preventDefault();
								}}
							/>
						</Popover.Trigger>
						<Popover.Content
							style={{ maxHeight: "50vh", maxWidth: "50vw", overflow: "auto" }}
						>
							{isPreviewLoading || preview === null ? (
								<Spinner
									style={{ width: "fit-content", whiteSpace: "nowrap" }}
								/>
							) : (
								<Table
									style={{ overflow: "visible", width: "fit-content" }}
									values={preview}
								/>
							)}
						</Popover.Content>
					</Popover.Root>
					<Popover.Root>
						<Popover.Trigger asChild>
							<Button
								icon="Settings"
								variant="tertiary"
							/>
						</Popover.Trigger>
						<Popover.Content style={{ overflow: "auto", maxHeight: "50vh" }}>
							<Stack
								dir="column"
								ai="stretch"
							>
								<Input
									value={settings.offset ?? 0}
									onChange={fileOffsetInputChangeHandler}
									icon="LayoutShift"
									variant="highlighted"
									placeholder={t("common.zeroMilliseconds")}
									label={t("advancedParams.offset")}
								/>
								<PluginSelector
									settings={settings}
									updateSettings={handleSettingsUpdate}
								/>
								<MethodSelector
									settings={settings}
									updateSettings={handleSettingsUpdate}
									methods={methods}
								/>
								<MappingSelector
									settings={settings}
									updateSettings={handleSettingsUpdate}
									mappings={mappings}
								/>

								<CustomParameters.Editor
									plugin={
										app.target.plugins.find(
											(p) => p.filename === settings.plugin,
										)!
									}
									customParameters={settings.custom_parameters}
									setCustomParameters={setCustomParameters}
								/>
								<AdvancedPluginParams
									plugin={
										app.target.plugins.find(
											(p) => p.filename === settings.plugin,
										)!
									}
									pluginParams={settings}
									showStoreFile={true}
									updatePluginParams={(pluginParams) =>
										updateSettings({
											...settings,
											...pluginParams,
										})
									}
								/>
							</Stack>
						</Popover.Content>
					</Popover.Root>
					<Button
						icon="X"
						variant="tertiary"
						shape="icon"
						onClick={() =>
							setFiles((sources) =>
								sources.filter((source) => filePath(source) !== path),
							)
						}
					/>
				</Stack>
			);
		},
	),
);

const Progress = ({ value }: { value: number }) => {
	return (
		<Stack
			className={s.progress}
			pos="absolute"
		>
			<UIProgress
				className={s.bar}
				background="var(--green-800)"
				color="var(--green-800)"
				value={value}
			/>
		</Stack>
	);
};

const PluginSelector = ({
	settings,
	updateSettings,
}: {
	settings: FileEntity.Settings;
	updateSettings: (update: Partial<FileEntity.Settings>) => void;
}) => {
	const { app } = Application.use();
	const { t } = Locale.use();

	const plugins = Mapping.Entity.plugins(app);

	const cutExtension = useCallback((str: string) => {
		return str.split(".").slice(0, -1).join("");
	}, []);
	const value = settings.plugin ? cutExtension(settings.plugin) : "";
	const text =
		value || (plugins.length > 0 ? t("upload.selectPlugin") : t("upload.noPlugins"));

	return (
		<Stack
			dir="column"
			gap={6}
			ai="flex-start"
			data-input
		>
			<Label value={t("upload.plugin")} />
			<Select.Root
				value={settings.plugin || ""}
				onValueChange={(plugin) => updateSettings({ plugin })}
			>
				<Select.Trigger className={s.select} title={text}>
					<Icon name="Puzzle" />
					<span>{text}</span>
				</Select.Trigger>
				<Select.Content>
					{plugins.map((p) => (
						<Select.Item
							key={p}
							value={p}
							title={cutExtension(p)}
						>
							{cutExtension(p)}
						</Select.Item>
					))}
				</Select.Content>
			</Select.Root>
		</Stack>
	);
};

export const MethodSelector = ({
	settings,
	updateSettings,
	methods,
}: {
	settings: FileEntity.Settings;
	updateSettings: (update: Partial<FileEntity.Settings>) => void;
	methods: string[];
}) => {
	const { t } = Locale.use();
	const text =
		settings.method || (methods.length > 0 ? t("upload.selectMethod") : "-");

	return methods.length > 0 ? (
		<Stack
			dir="column"
			gap={6}
			ai="flex-start"
			data-input
		>
			<Label value={t("upload.mappingFile")} />
			<Select.Root
				value={settings.method || ""}
				onValueChange={(method) => {
					const resolvedMethod = resolveOptionalMethod(method);
					updateSettings({
						method: resolvedMethod,
						...(resolvedMethod ? {} : { mapping: undefined }),
					});
				}}
			>
				<Select.Trigger className={s.select} title={text}>
					<Icon name="ChevronRight" />
					<span>{text}</span>
				</Select.Trigger>
				<Select.Content>
					<Select.Item value={NO_METHOD_SELECTED}>
						{t("upload.clearSelection")}
					</Select.Item>
					<Select.Separator />
					{methods.map((m) => (
						<Select.Item
							key={m}
							value={m}
							title={m}
						>
							{m}
						</Select.Item>
					))}
				</Select.Content>
			</Select.Root>
		</Stack>
	) : null;
};

export const MappingSelector = ({
	settings,
	updateSettings,
	mappings,
}: {
	settings: FileEntity.Settings;
	updateSettings: (update: Partial<FileEntity.Settings>) => void;
	mappings: string[];
}) => {
	const { t } = Locale.use();
	const text =
		settings.mapping ||
		(mappings.length > 0
			? t("upload.selectMapping")
			: settings.method
				? t("upload.noMappings")
				: "-");

	return mappings.length > 0 ? (
		<Stack
			dir="column"
			gap={6}
			ai="flex-start"
			data-input
		>
			<Label value={t("advancedParams.mappingId")} />
			<Select.Root
				value={settings.mapping || ""}
				onValueChange={(mapping) =>
					updateSettings({
						mapping: resolveOptionalMapping(mapping),
					})
				}
			>
				<Select.Trigger className={s.select} title={text}>
					<Icon name="ChevronRight" />
					<span>{text}</span>
				</Select.Trigger>
				<Select.Content>
					<Select.Item value={NO_MAPPING_SELECTED}>
						{t("upload.clearSelection")}
					</Select.Item>
					<Select.Separator />
					{mappings.map((m) => (
						<Select.Item
							key={m}
							value={m}
							title={m}
						>
							{m}
						</Select.Item>
					))}
				</Select.Content>
			</Select.Root>
		</Stack>
	) : null;
};

export const FrameSelector = ({
	isCustomFrame,
	setFrame,
}: {
	isCustomFrame: boolean;
	setFrame: SetState<FileEntity.IngestOptions["frame"]>;
}) => {
	const { t } = Locale.use();

	if (!isCustomFrame) {
		return null;
	}

	const frameInputChangeHandler = (
		event: ChangeEvent<HTMLInputElement>,
		type: keyof MinMax,
	) => {
		const value = event.target.valueAsDate;

		if (!value) {
			toast.error(t("upload.invalidDate"), {
				richColors: true,
			});
			return;
		}

		setFrame((f) => ({
			...f!,
			[type]: value.valueOf(),
		}));
	};

	const frameMinInputChangeHandler = (event: ChangeEvent<HTMLInputElement>) =>
		frameInputChangeHandler(event, "min");

	const frameMaxInputChangeHandler = (event: ChangeEvent<HTMLInputElement>) =>
		frameInputChangeHandler(event, "max");

	return (
		<Stack
			gap={12}
			className={s.frame_selector}
		>
			<Input
				variant="highlighted"
				type="date"
				icon="CalendarArrowUp"
				onChange={frameMinInputChangeHandler}
			/>
			<Input
				variant="highlighted"
				type="date"
				icon="CalendarArrowDown"
				onChange={frameMaxInputChangeHandler}
			/>
		</Stack>
	);
};

export const ApplySettinsForAllFiles = ({
	settings,
	updateSettings,
	setSettings,
}: {
	settings: Record<string, FileEntity.Settings>;
	updateSettings: any;
	setSettings: (s: FileEntity.Settings) => void;
}) => {
	const { app } = Application.use();
	const { t } = Locale.use();
	const [isOpen, setIsOpen] = useState(false);

	/**
	 * Initialization of settings.all is now handled in the parent component
	 * to ensure consistency and avoid direct mutations during render.
	 */

	const methods = Mapping.Entity.methods(app, settings.all?.plugin);
	const mappings = Mapping.Entity.mappings(
		app,
		settings.all?.plugin,
		settings.all?.method,
	);

	const handleAllSettingsUpdate = (update: Partial<FileEntity.Settings>) => {
		const finalUpdate = { ...update };

		if (update.plugin) {
			const methods = Mapping.Entity.methods(app, update.plugin);
			if (methods.length === 1) {
				finalUpdate.method = methods[0];
				const mappings = Mapping.Entity.mappings(
					app,
					update.plugin,
					methods[0],
				);
				if (mappings.length === 1) {
					finalUpdate.mapping = mappings[0];
				}
			} else {
				finalUpdate.method = undefined;
				finalUpdate.mapping = undefined;
			}
		} else if (update.method) {
			const mappings = Mapping.Entity.mappings(
				app,
				settings.all?.plugin,
				update.method,
			);
			if (mappings.length === 1) {
				finalUpdate.mapping = mappings[0];
			} else {
				finalUpdate.mapping = undefined;
			}
		}

		updateSettings("all", finalUpdate);
	};

	return (
		<Popover.Root
			open={isOpen}
			onOpenChange={setIsOpen}
		>
			<Popover.Trigger asChild>
				<Button
					style={{ width: "100%" }}
					variant="secondary"
					icon="Settings"
				>
					{t("upload.selectSettingsForAll")}
				</Button>
			</Popover.Trigger>
			<Popover.Content className={s.allSettingsPopover}>
				<Stack
					className={s.allSettings}
					gap={0}
				>
					<PluginSelector
						settings={settings?.all || {}}
						updateSettings={handleAllSettingsUpdate}
					/>
					<Separator
						orientation="vertical"
						style={{ height: 32 }}
					/>
					<MethodSelector
						settings={settings?.all || {}}
						updateSettings={handleAllSettingsUpdate}
						methods={methods}
					/>
					<Separator
						orientation="vertical"
						style={{ height: 32 }}
					/>
					<MappingSelector
						settings={settings?.all || {}}
						updateSettings={handleAllSettingsUpdate}
						mappings={mappings}
					/>
					<Separator
						orientation="vertical"
						style={{ height: 32 }}
					/>
					<Button
						variant="tertiary"
						style={{ borderRadius: 2 }}
						icon="Check"
						disabled={!settings.all?.plugin}
						onClick={() => {
							setSettings(settings.all);
							setIsOpen(false);
						}}
					>
						{t("common.apply")}
					</Button>
				</Stack>
			</Popover.Content>
		</Popover.Root>
	);
};

export function UploadBanner() {
	const { Info, app, spawnBanner } = Application.use();
	const { t } = Locale.use();
	const [files, setFiles] = useState<File[]>([]);
	const [context, setContext] =
		useState<FileEntity.IngestOptions["context"]>("");
	const [loading, setLoading] = useState(false);
	const [newContext, setNewContext] = useState(true);
	const [customFrame, setCustomFrame] = useState(false);
	const [frame, setFrame] =
		useState<FileEntity.IngestOptions["frame"]>(MinMaxBase);

	useEffect(() => {
		setContext("");
	}, [newContext]);

	const [settings, setSettings] = useState<Record<string, FileEntity.Settings>>(
		{
			all: {
				offset: 0,
				custom_parameters: {},
			},
		},
	);

	const updateSettings = useCallback(
		(filename: string, update: Partial<FileEntity.Settings>) => {
			setSettings((prev) => {
				const current = prev[filename] || { offset: 0, custom_parameters: {} };
				return {
					...prev,
					[filename]: { ...current, ...update },
				};
			});
		},
		[],
	);

	/**
	 * Identifies an available plugin by magic bytes, then by file extension.
	 * @param file Target file for analysis.
	 * @param availablePlugins Ingestion plugins exposed by the current backend.
	 * @returns Detected plugin identifier or undefined.
	 */
	const detectFileType = useCallback(
		async (file: File, availablePlugins: string[]) => {
			const buffer = await readFileChunk(file);
			return detectFilePlugin(buffer, file.name, availablePlugins);
		},
		[],
	);

	/**
	 * Provisions default ingestion parameters based on detected file characteristics.
	 * @param file File entity to initialize.
	 * @param appState Current application context for mapping resolution.
	 */
	const initializeFileSettings = async (file: File, appState: any) => {
		const availablePlugins = Mapping.Entity.plugins(appState);
		const plugin = await detectFileType(file, availablePlugins);
		const methods = Mapping.Entity.methods(appState, plugin);
		const method = methods.length === 1 ? methods[0] : undefined;
		const mappings = method
			? Mapping.Entity.mappings(appState, plugin, method)
			: [];
		const mapping = mappings.length === 1 ? mappings[0] : undefined;

		return {
			plugin,
			method,
			mapping,
			offset: 0,
			custom_parameters: {},
		};
	};

	const readFileChunk = (file: File): Promise<ArrayBuffer> => {
		return new Promise((resolve, reject) => {
			const reader = new FileReader();
			reader.onload = () => resolve(reader.result as ArrayBuffer);
			reader.onerror = reject;
			reader.readAsArrayBuffer(file.slice(0, MAX_FILE_SIGNATURE_LENGTH));
		});
	};

	const [progress, setProgress] = useState<Record<string, number>>({});

	const setFileProgressConstrustor = (file: File) => (num: number) => {
		setProgress((p) => ({
			...p,
			[filePath(file)]: num,
		}));
	};

	/**
	 * Dispatches standard ingestion requests for all selected files.
	 *
	 * @returns A promise that resolves after ingestion requests have been queued.
	 */
	const handleSubmit = useCallback(async () => {
		const missingPluginPaths = getFilePathsWithoutPlugin(
			files.map(filePath),
			settings,
		);
		if (missingPluginPaths.length > 0) {
			toast.error(
				t("upload.selectPluginsForFiles", {
					files: missingPluginPaths.join(", "),
				}),
				{ richColors: true },
			);
			return;
		}

		setLoading(true);

		const frameConfig = customFrame ? frame : undefined;

		for (const file of files) {
			const baseOptions = {
				context,
				file,
				setProgress: setFileProgressConstrustor(file),
				frame: frameConfig,
			};

			Info.file_ingest({
				...baseOptions,
				settings: settings[filePath(file)],
			});
		}

		setLoading(false);
		spawnBanner(<SelectFiles.Banner />);
	}, [files, settings, context, customFrame, frame, Info, spawnBanner, t]);

	const DoneButton = useMemo(() => {
		return (
			<Button
				variant="glass"
				onClick={handleSubmit}
				icon="Check"
				className={s.done}
				disabled={!context || !files.length}
				loading={loading}
			/>
		);
	}, [context, handleSubmit, files, loading]);

	/**
	 * Applies a specific set of settings to all selected files in a single batch.
	 * @param newSettings The settings object to apply.
	 */
	const updateAllSettings = (newSettings: FileEntity.Settings) => {
		setSettings((prev) => {
			const updated = { ...prev };
			files.forEach((file) => {
				const path = filePath(file);
				updated[path] = {
					...updated[path],
					...newSettings,
					custom_parameters: {
						...(updated[path]?.custom_parameters || {}),
						...(newSettings.custom_parameters || {}),
					},
				};
			});
			return updated;
		});
	};

	const fileInputRef = useRef<HTMLInputElement>(null);
	const directoryInputRef = useRef<HTMLInputElement>(null);

	const addFilesButtonClickHandler = () => {
		if (fileInputRef.current) {
			fileInputRef.current.click();
		}
	};

	const addDirectoryButtonClickHandler = () => {
		if (directoryInputRef.current) {
			directoryInputRef.current.setAttribute("webkitdirectory", "");
			directoryInputRef.current.click();
		}
	};

	const handleFileChange = async (
		event: React.ChangeEvent<HTMLInputElement>,
	) => {
		const rawFiles = event.target.files;
		if (!rawFiles) return;

		const newFilesList = [...(rawFiles || [])];

		// We add new files and calculate their settings in one go to avoid O(N^2) loops in useEffects
		const newSettingsUpdates: Record<string, FileEntity.Settings> = {};
		const filesToAdd: File[] = [];

		for (const file of newFilesList) {
			const path = filePath(file);
			if (files.every((f) => filePath(f) !== path)) {
				filesToAdd.push(file);
				newSettingsUpdates[path] = await initializeFileSettings(file, app);
			}
		}

		if (filesToAdd.length > 0) {
			setFiles((prev) => [...prev, ...filesToAdd]);
			setSettings((prev) => ({ ...prev, ...newSettingsUpdates }));
		}
	};

	return (
		<Banner
			className={s.banner}
			title={t("upload.title")}
			done={DoneButton}
		>
			<Stack jc="space-between">
				<Toggle
					option={[t("upload.ingestEverything"), t("upload.useLimits")]}
					checked={customFrame}
					onCheckedChange={setCustomFrame}
				/>
				<Stack>
					<Button
						icon="Upload"
						variant="secondary"
						title={t("operationView.menu.uploadFiles")}
						aria-label={t("operationView.menu.uploadFiles")}
						onClick={addFilesButtonClickHandler}
					>
						{t("operationView.menu.uploadFiles")}
					</Button>
					<Button
						icon="FolderPlus"
						variant="secondary"
						title={t("upload.addDirectory")}
						aria-label={t("upload.addDirectory")}
						onClick={addDirectoryButtonClickHandler}
					>
						{t("upload.addDirectory")}
					</Button>
				</Stack>
			</Stack>
			<FrameSelector
				setFrame={setFrame}
				isCustomFrame={customFrame}
			/>
			{files.length ? (
				<Stack
					dir="column"
					className={cn(s.files, !files.length && s.fill)}
					onClick={() =>
						files.length === 0 ? addFilesButtonClickHandler() : null
					}
					ai="stretch"
				>
					<Stack
						className={s.inner}
						gap={0}
						dir="column"
					>
						<FilesList
							files={files}
							setFiles={setFiles}
							settings={settings}
							progress={progress}
							updateSettings={updateSettings}
						/>
					</Stack>
				</Stack>
			) : null}
			<input
				ref={fileInputRef}
				type="file"
				multiple
				style={{ display: "none" }}
				onChange={handleFileChange}
			/>
			<input
				ref={directoryInputRef}
				type="file"
				multiple
				style={{ display: "none" }}
				onChange={handleFileChange}
			/>
			<Stack>
				<Badge
					className={s.fileCounter}
					mono
					size="sm"
					variant="gray-subtle"
					value={t("upload.selectedFiles", { count: files.length })}
				/>
				<ApplySettinsForAllFiles
					settings={settings}
					updateSettings={updateSettings}
					setSettings={updateAllSettings}
				/>
				<Button
					variant="secondary"
					icon="Cross"
					onClick={() => setFiles([])}
				>
					{t("upload.clearSelection")}
				</Button>
			</Stack>
			<Stack
				dir="column"
				ai="stretch"
			>
				{newContext ? (
					<Input
						variant="highlighted"
						label={t("common.context")}
						icon={Default.Icon.CONTEXT}
						value={context}
						onChange={(e) => setContext(e.target.value)}
						placeholder={t("globalQuery.contextNamePlaceholder")}
						className={s.reset_font}
					/>
				) : (
					<ContextSelector
						app={app}
						context={context}
						setContext={setContext}
					/>
				)}
				<Stack
					ai="center"
					gap={4}
				>
					<Checkbox
						id="newContext"
						checked={newContext}
						onCheckedChange={(e) => setNewContext(!!e)}
					/>
					<Label
						htmlFor="newContext"
						value={t("upload.createNewContext")}
						cursor="pointer"
					/>
				</Stack>
			</Stack>
		</Banner>
	);
}

import { GulpDataset, MinMax, MinMaxBase } from "@/class/Info";
import { Application } from "@/context/Application.context";
import { Default } from "@/dto/Dataset";
import { Banner as UIBanner } from "@/ui/Banner";
import { Select } from "@/ui/Select";
import { Label } from "@/ui/Label";
import { Icon } from "@/ui/Icon";
import { generateUUID } from "@/ui/utils";
import {
	Tooltip,
	TooltipContent,
	TooltipProvider,
	TooltipTrigger,
} from "@/ui/Tooltip";
import { format } from "date-fns";
import {
	ChangeEvent,
	CSSProperties,
	useCallback,
	useEffect,
	useMemo,
	useState,
} from "react";
import s from "./styles/EnrichmentBanner.module.css";
import { CustomParameters } from "@/components/CustomParameters";
import { Checkbox } from "@/ui/Checkbox";
import { Button } from "@/ui/Button";
import { Skeleton } from "@/ui/Skeleton";
import { Input } from "@/ui/Input";
import { Stack } from "@/ui/Stack";
import { Doc } from "@/entities/Doc";
import { Source } from "@/entities/Source";
import type { App } from "@/entities/App";
import { Internal } from "@/entities/addon/Internal";
import { Locale } from "@/locales";

// --- CONSTANTS ---

const DISABLED_STYLE: CSSProperties = {
	pointerEvents: "none",
	color: "var(--second)",
};

// --- UTILITIES ---

/**
 * Automatically detects the type of a string value (IP, Domain, Email, MAC, Hash, URL).
 * @param value The string to test
 * @returns detected type name or null
 */
const detectType = (value: string): string | null => {
	const v = value.trim();

	const ipRegex =
		/^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)|(?:[A-F0-9]{1,4}:){7}[A-F0-9]{1,4}$/i;
	if (ipRegex.test(v)) return "ip";

	const domainRegex =
		/^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$/i;
	if (domainRegex.test(v)) return "domain";

	const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
	if (emailRegex.test(v)) return "mail";

	const macRegex = /^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$/;
	if (macRegex.test(v)) return "mac";

	const md5Regex = /^[a-fA-F0-9]{32}$/;
	const sha1Regex = /^[a-fA-F0-9]{40}$/;
	const sha256Regex = /^[a-fA-F0-9]{64}$/;
	if (md5Regex.test(v) || sha1Regex.test(v) || sha256Regex.test(v))
		return "hash";

	const urlRegex =
		/^(http:\/\/|https:\/\/)?([\da-z\.-]+)\.([a-z0-9\.]{2,10})(:\d+)?([\/\w \.-]*)*\/?$/i;
	if (urlRegex.test(v)) return "url";

	return null;
};

/**
 * Transforms an array of field objects into a record of key-value pairs.
 * @param fields Array of {key, value} objects
 * @returns Record of key/values
 */
const prepareFormattedFields = (
	fields: { key: string; value: string | null }[],
): Record<string, string | null> => {
	return fields.reduce(
		(acc, f) => {
			if (!f.key) return acc;
			acc[f.key] = f.value;
			return acc;
		},
		{} as Record<string, string | null>,
	);
};

/**
 * Formats a source label with its context name when the context is available.
 * @param app Current application state used to resolve the source context.
 * @param file Source whose name should be displayed.
 * @returns Source label in the "source (context)" format, or the source name alone.
 */
const formatSourceContextLabel = (app: App.Type, file: Source.Type): string => {
	const contextName = Source.Entity.context(app, file)?.name;
	return contextName ? `${file.name} (${contextName})` : file.name;
};

// --- SUB-COMPONENTS ---

interface FieldRowProps {
	field: { id: string; key: string; value: string | null };
	index: number;
	onUpdate: (index: number, updates: Partial<FieldRowProps["field"]>) => void;
	onDelete: (id: string) => void;
}

/**
 * Renders a single row for field key/value configuration.
 */
function FieldRow({ field, index, onUpdate, onDelete }: FieldRowProps) {
	const { t } = Locale.use();

	return (
		<Stack
			gap={8}
			ai="center"
			className={s.fieldRow}
		>
			{/* Key column */}
			<Stack
				gap={4}
				ai="center"
				className={s.fieldColumn}
			>
				<Label
					value={t("common.key")}
					className={s.label}
				/>
				<TooltipProvider>
					<Tooltip>
						<TooltipTrigger asChild>
							<Input
								placeholder={t("enrichment.fieldKeyPlaceholder")}
								value={field.key}
								onChange={(e) => onUpdate(index, { key: e.target.value })}
							/>
						</TooltipTrigger>
						{field.key && (
							<TooltipContent side="top">{field.key}</TooltipContent>
						)}
					</Tooltip>
				</TooltipProvider>
			</Stack>

			{/* Value column */}
			<Stack
				gap={4}
				ai="center"
				className={s.fieldColumn}
			>
				<Label
					value={t("common.value")}
					className={s.label}
				/>
				<TooltipProvider>
					<Tooltip>
						<TooltipTrigger asChild>
							<Input
								placeholder={t("enrichment.fieldValuePlaceholder")}
								disabled={field.value === null}
								value={field.value ?? ""}
								onChange={(e) => onUpdate(index, { value: e.target.value })}
							/>
						</TooltipTrigger>
						{field.value !== null && field.value !== "" && (
							<TooltipContent side="top">{field.value}</TooltipContent>
						)}
					</Tooltip>
				</TooltipProvider>
			</Stack>

			{/* Null/Value column */}
			<Stack
				gap={4}
				ai="center"
				className={s.actionColumn}
			>
				<Label
					value={t("enrichment.toggleValue")}
					className={s.label}
				/>
				<Checkbox
					checked={field.value !== null}
					onCheckedChange={(checked) =>
						onUpdate(index, { value: checked ? "" : null })
					}
				/>
			</Stack>

			{/* Delete column */}
			<Stack
				gap={4}
				ai="center"
				className={s.actionColumn}
			>
				<Label
					value={t("common.delete")}
					className={s.label}
				/>
				<Button
					variant="tertiary"
					icon="Trash"
					onClick={() => onDelete(field.id)}
				/>
			</Stack>
		</Stack>
	);
}

interface TimeRangeSelectorProps {
	event?: Doc.Type;
	frame: MinMax;
	setFrame: (updater: (prev: MinMax) => MinMax) => void;
	isLoaded: boolean;
}

/**
 * Renders either the specific event ID or date-time inputs for range enrichment.
 */
function TimeRangeSelector({
	event,
	frame,
	setFrame,
	isLoaded,
}: TimeRangeSelectorProps) {
	const { t } = Locale.use();
	const handleMinChange = useCallback(
		(e: ChangeEvent<HTMLInputElement>) => {
			const value = e.target.value;
			setFrame((prev) => ({ ...prev, min: new Date(value).getTime() }));
		},
		[setFrame],
	);

	const handleMaxChange = useCallback(
		(e: ChangeEvent<HTMLInputElement>) => {
			const value = e.target.value;
			setFrame((prev) => ({ ...prev, max: new Date(value).getTime() }));
		},
		[setFrame],
	);

	const formatDate = (timestamp: number) => {
		return isNaN(timestamp) ? "" : format(timestamp, "yyyy-MM-dd'T'HH:mm");
	};

	if (event) {
		return (
			<Skeleton
				width="full"
				className={s.skeleton}
				show={!isLoaded}
			>
				<Input
					value={event._id}
					readOnly
					variant="highlighted"
					style={DISABLED_STYLE}
					icon={Default.Icon.EVENT}
				/>
			</Skeleton>
		);
	}

	return (
		<Stack
			dir="column"
			gap={8}
		>
			<Stack
				dir="column"
				gap={4}
			>
				<Label
					value={t("enrichment.startDate")}
					className={s.label}
				/>
				<Input
					key="min-date"
					onChange={handleMinChange}
					value={formatDate(frame.min)}
					variant="highlighted"
					icon="CalendarArrowUp"
					type="datetime-local"
				/>
			</Stack>
			<Stack
				dir="column"
				gap={4}
			>
				<Label
					value={t("enrichment.endDate")}
					className={s.label}
				/>
				<Input
					key="max-date"
					onChange={handleMaxChange}
					value={formatDate(frame.max)}
					variant="highlighted"
					icon="CalendarArrowDown"
					type="datetime-local"
				/>
			</Stack>
		</Stack>
	);
}

// --- MAIN COMPONENT ---

export namespace Enrichment {
	export interface Props extends UIBanner.Props {
		event?: Doc.Type;
		source?: Source.Type;
		onEnrichment?: (event: Doc.Type) => void;
		enrichmentField?: { key: string; value: string };
	}

	/**
	 * Renders the enrichment workflow for an event, a fixed source, or a user-selected source.
	 * @param props.event Optional event to enrich directly.
	 * @param props.source Optional source to preselect and lock for source-level enrichment.
	 * @param props.onEnrichment Callback invoked with the enriched event after single-event enrichment.
	 * @param props.enrichmentField Optional field/value pair used to prefill enrichment fields.
	 * @returns The enrichment banner UI.
	 */
	export function Banner({
		event,
		source,
		onEnrichment,
		enrichmentField,
		...props
	}: Enrichment.Props) {
		const { Info, app, destroyBanner } = Application.use();
		const { t } = Locale.use();

		// --- STATE ---
		const lockedSource = useMemo(
			() =>
				event
					? Source.Entity.id(app, event["gulp.source_id"])
					: (source ?? null),
			[app, event, source],
		);
		const [file, setFile] = useState<Source.Type | null>(lockedSource);
		const [allEnrichmentPlugins, setAllEnrichmentPlugins] =
			useState<GulpDataset.PluginList.Interface[]>();
		const [plugins, setPlugins] =
			useState<GulpDataset.PluginList.Interface[]>();
		const [plugin, setPlugin] = useState<GulpDataset.PluginList.Interface>();
		const [availableTypes, setAvailableTypes] = useState<string[]>([]);
		const [selectedType, setSelectedType] = useState<string>("Other");
		const [customParameters, setCustomParameters] = useState<
			Record<string, any>
		>({});
		const [loading, setLoading] = useState<boolean>(false);
		const [isShowOnlyEnriched, setIsShowOnlyEnriched] = useState<boolean>(true);
		const [fields, setFields] = useState<
			{ id: string; key: string; value: string | null }[]
		>([]);
		const [frame, setFrame] = useState<MinMax>(MinMaxBase);

		// --- EFFECTS: Initialization & Filtering ---

		// Keep the source fixed for event and settings-triggered enrichments.
		useEffect(() => {
			if (!lockedSource) return;
			setFile(lockedSource);
		}, [lockedSource]);

		// Reset values so the shared editor can initialize the selected plugin defaults.
		useEffect(() => {			
			setCustomParameters({});
		}, [plugin]);

		// Load and filter enrichment plugins
		useEffect(() => {
			Info.plugin_list().then((allPlugins) => {
				if (!allPlugins) return;
				const enrichmentPlugins = allPlugins.filter((p) =>
					p.type.includes("enrichment"),
				);
				setAllEnrichmentPlugins(enrichmentPlugins);

				const types = new Set<string>();
				enrichmentPlugins.forEach((p) => {
					const data = p.data as Record<string, any>;
					if (data) {
						Object.keys(data).forEach((k) => types.add(k));
					}
				});
				const available = Array.from(types).sort();
				setAvailableTypes(available);

				if (!enrichmentField) {
					setSelectedType("Other");
				} else {
					let autoDetected: string | null = null;
					const detectedRegexType = detectType(enrichmentField.value);

					for (const p of enrichmentPlugins) {
						const data = p.data as Record<
							string,
							{ ecs_fields?: string[]; regexp?: string }
						>;
						if (!data) continue;
						for (const [typeName, config] of Object.entries(data)) {
							if (config.ecs_fields?.includes(enrichmentField.key)) {
								autoDetected = typeName;
								break;
							}
							if (config.regexp) {
								try {
									if (
										new RegExp(config.regexp, "i").test(enrichmentField.value)
									) {
										autoDetected = typeName;
										break;
									}
								} catch (e) {
									/* ignore */
								}
							}

							const hasNoEcsFields =
								!config.ecs_fields ||
								config.ecs_fields.filter((f) => f !== "").length === 0;
							if (
								hasNoEcsFields &&
								!config.regexp &&
								detectedRegexType === typeName
							) {
								autoDetected = typeName;
								break;
							}
						}
						if (autoDetected) break;
					}

					if (autoDetected && available.includes(autoDetected)) {
						setSelectedType(autoDetected);
					} else {
						setSelectedType("Other");
					}
				}
			});
		}, [enrichmentField, Info]);

		// Filter plugins based on selectedType
		useEffect(() => {
			if (!allEnrichmentPlugins) return;

			let filtered = allEnrichmentPlugins;
			if (selectedType !== "Other") {
				filtered = allEnrichmentPlugins.filter((p) => {
					const data = p.data as Record<string, any>;
					return data && !!data[selectedType];
				});
			}

			setPlugins(filtered);

			setPlugin((prev) => {
				if (prev && !filtered.find((f) => f.filename === prev.filename)) {
					return undefined;
				}
				return prev;
			});
		}, [selectedType, allEnrichmentPlugins]);

		// Initialize fields from props
		useEffect(() => {
			if (enrichmentField) {
				setFields([
					{
						id: generateUUID(),
						key: enrichmentField.key,
						value: enrichmentField.value,
					},
				]);
			}
		}, [enrichmentField]);

		// Initialize time frame from source
		useEffect(() => {
			if (!file) return;
			setFrame({
				min: Internal.Transformator.toTimestamp(
					file.nanotimestamp.min,
					"floor",
				),
				max: Internal.Transformator.toTimestamp(file.nanotimestamp.max, "ceil"),
			});
		}, [file]);

		// --- HANDLERS ---

		const handleUpdateField = useCallback(
			(
				index: number,
				updates: Partial<{ key: string; value: string | null }>,
			) => {
				setFields((prev) => {
					const next = [...prev];
					next[index] = { ...next[index], ...updates };
					return next;
				});
			},
			[],
		);

		const handleDeleteField = useCallback((id: string) => {
			setFields((prev) => prev.filter((f) => f.id !== id));
		}, []);

		const handleAddField = useCallback(() => {
			setFields((prev) => [
				...prev,
				{ id: generateUUID(), key: "", value: null },
			]);
		}, []);

		/**
		 * Submits enrichment for either the selected single event or the selected source range.
		 * @returns A promise that resolves when the enrichment request finishes or is queued.
		 */
		const submit = async () => {
			if (!file || !plugin) return;
			const formattedFields = prepareFormattedFields(fields);
			setLoading(true);

			try {
				if (event) {
					const enriched = await Info.enrich_single_id(
						plugin.filename,
						event,
						customParameters,
						formattedFields,
					);
					if (enriched) {
						if (onEnrichment) onEnrichment(enriched);
						destroyBanner();
					}
				} else {
					const enrichmentStarted = await Info.enrichment(
						plugin.filename,
						file,
						frame,
						customParameters,
						isShowOnlyEnriched,
						formattedFields,
					);
					if (enrichmentStarted) {
						destroyBanner();
					}
				}
			} finally {
				setLoading(false);
			}
		};

		// --- UI SEGMENTS ---

		const FileSelection = useMemo(() => {
			if (lockedSource && file) {
				return (
					<Skeleton
						show={!plugins}
						width="full"
						className={s.skeleton}
					>
						<Input
							icon={Source.Entity.icon(file)}
							readOnly
							value={formatSourceContextLabel(app, file)}
							variant="highlighted"
							style={DISABLED_STYLE}
						/>
					</Skeleton>
				);
			}

			return (
				<Select.Root
					value={file?.id}
					onValueChange={(id) =>
						setFile(Source.Entity.id(app, id as unknown as Source.Id))
					}
				>
					<Select.Trigger>
						<Stack gap={16}>
							<Icon
								variant="dimmed"
								name={file ? Source.Entity.icon(file) : Default.Icon.SESSION}
							/>
							{file
								? formatSourceContextLabel(app, file)
								: t("enrichment.selectSource")}
						</Stack>
					</Select.Trigger>
					<Select.Content>
						{Source.Entity.selected(app).map((f) => (
							<Select.Item
								key={f.id}
								value={f.id}
							>
								{formatSourceContextLabel(app, f)}
							</Select.Item>
						))}
					</Select.Content>
				</Select.Root>
			);
		}, [plugins, file, lockedSource, app, t]);

		const TypeSelection = useMemo(() => {
			if (!allEnrichmentPlugins)
				return (
					<Skeleton
						className={s.skeleton}
						width="full"
					/>
				);

			return (
				<Select.Root
					value={selectedType}
					onValueChange={(val) => setSelectedType(val)}
				>
					<Select.Trigger>
						<Stack gap={16}>
							<Icon
								variant="dimmed"
								name="Filter"
							/>
							{selectedType === "Other"
								? t("enrichment.observableTypeOther")
								: t("enrichment.observableType", { type: selectedType })}
						</Stack>
					</Select.Trigger>
					<Select.Content>
						{availableTypes.map((type) => (
							<Select.Item
								key={type}
								value={type}
							>
								{type}
							</Select.Item>
						))}
						<Select.Item value="Other">{t("common.other")}</Select.Item>
					</Select.Content>
				</Select.Root>
			);
		}, [selectedType, availableTypes, allEnrichmentPlugins, t]);

		const PluginSelection = useMemo(() => {
			if (!plugins)
				return (
					<Skeleton
						className={s.skeleton}
						width="full"
					/>
				);

			return (
				<Select.Root
					value={plugin?.filename}
					onValueChange={(filename) =>
						setPlugin(plugins.find((p) => p.filename === filename))
					}
				>
					<Select.Trigger>
						<Stack gap={16}>
							<Icon
								variant="dimmed"
								name="Puzzle"
							/>
							{plugin ? plugin.filename : t("enrichment.selectPlugin")}
						</Stack>
					</Select.Trigger>
					<Select.Content>
						{plugins.map((p) => (
							<Select.Item
								key={p.filename}
								value={p.filename}
							>
								{p.filename}
							</Select.Item>
						))}
					</Select.Content>
				</Select.Root>
			);
		}, [plugin, plugins, t]);

		const doneElement = (
			<Button
				disabled={!file || !plugin}
				variant="glass"
				icon="Check"
				onClick={submit}
				loading={loading}
			/>
		);

		return (
			<UIBanner
				title={
					event
						? t("enrichment.eventTitle")
						: t("operationView.menu.dataEnrichment")
				}
				done={doneElement}
				loading={!plugins}
				{...props}
			>
				{TypeSelection}
				{PluginSelection}
				{FileSelection}
				<TimeRangeSelector
					event={event}
					frame={frame}
					setFrame={setFrame}
					isLoaded={!!plugins}
				/>

				<CustomParameters.Editor
					customParameters={customParameters}
					setCustomParameters={setCustomParameters}
					plugin={plugin}
				/>

				<Stack
					className={s.fieldsSection}
					gap={8}
					dir="column"
				>
					<Stack
						ai="center"
						jc="space-between"
					>
						<Label value={t("common.fields")} />
						{!enrichmentField && (
							<Button
								variant="tertiary"
								icon="Plus"
								onClick={handleAddField}
							/>
						)}
					</Stack>

					<EnrichmentTooltip />

					{fields.map((field, index) => (
						<FieldRow
							key={field.id}
							field={field}
							index={index}
							onUpdate={handleUpdateField}
							onDelete={handleDeleteField}
						/>
					))}
				</Stack>

				<Stack
					ai="center"
					gap={4}
				>
					<Checkbox
						id="isShowOnlyEnriched"
						checked={isShowOnlyEnriched}
						onCheckedChange={(v) => setIsShowOnlyEnriched(!!v)}
					/>
					<Label
						htmlFor="isShowOnlyEnriched"
						value={t("enrichment.showOnlyEnriched")}
						cursor="pointer"
					/>
				</Stack>
			</UIBanner>
		);
	}
}

/**
 * Help tooltip for enrichment fields documentation.
 */
function EnrichmentTooltip() {
	const { t } = Locale.use();

	return (
		<TooltipProvider>
			<Tooltip>
				<TooltipTrigger asChild>
					<p
						className={s.helperText}
					>
						{t("enrichment.tooltipSummary")}
					</p>
				</TooltipTrigger>
				<TooltipContent className={s.tooltip}>
					<p>
						{t("enrichment.tooltipIntro")} <br />-{" "}
						{t("enrichment.tooltipNullValue")} <br />
						{t("common.example")}:{" "}
						<code>{`{ "host.name": null, "ip.address": null }`}</code>{" "}
						{t("enrichment.tooltipNullExample")}
						<br />
						<br />- {t("enrichment.tooltipSetValue")} <br />
						{t("common.example")}:{" "}
						<code>{`{ "host.name": "example.com", "ip.address": "8.8.8.8" }`}</code>{" "}
						{t("enrichment.tooltipSetExample")}
						<br />
						<br />- {t("enrichment.tooltipMix")}
						<br />
						<br />
						{t("enrichment.tooltipDotNotation")}
					</p>
				</TooltipContent>
			</Tooltip>
		</TooltipProvider>
	);
}

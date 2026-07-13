import { useEffect, useState, useMemo } from "react";
import { Popover } from "@/ui/Popover";
import * as Dialog from "@radix-ui/react-dialog";
import { Button } from "@/ui/Button";
import { Stack } from "@/ui/Stack";
import { Label } from "@/ui/Label";
import { Input } from "@/ui/Input";
import { Checkbox } from "@/ui/Checkbox";
import { toast } from "sonner";
import { Separator } from "@/ui/Separator";
import { Select } from "@/ui/Select";
import { Application } from "@/context/Application.context";
import { Mapping } from "@/entities/Mapping";
import { Icon } from "@/ui/Icon";
import { MethodSelector, MappingSelector } from "@/banners/Upload.banner";
import { CustomParameters } from "./CustomParameters";
import { SummaryTable } from "./AdvancedPluginParams/SummaryTable";
import { MappingPanel, MappingData } from "./AdvancedPluginParams/MappingPanel";
import {
	SigmaMappingPanel,
	SigmaMappingData,
} from "./AdvancedPluginParams/SigmaMappingPanel";
import { LastMappingsPanel } from "./AdvancedPluginParams/LastMappingsPanel";
import {
	deleteSavedPluginMapping,
	loadSavedPluginMappings,
	persistPluginMappings,
	SavedPluginMapping,
} from "./AdvancedPluginParams/mappingPersistence";

import { Textarea } from "@/ui/Textarea";
import { cn } from "@/ui/utils";
import s from "./styles/AdvancedPluginParams.module.css";
import { Locale } from "@/locales";

/**
 * AdvancedPluginParams component provides a comprehensive UI for configuring
 * complex plugin ingestion and query parameters beyond basic settings.
 * It manages state for global overrides, mapping configurations, sigma mappings,
 * and custom parameters, providing a unified modal interface.
 */
interface AdvancedPluginParamsProps {
	pluginParams: Record<string, any>;
	updatePluginParams: (params: Record<string, any>) => void;
	plugin?: any;
	showStoreFile?: boolean;
	customParamsMode?: "auto" | "textarea";
	triggerText?: string;
	triggerIcon?: any;
	triggerVariant?: any;
	applyText?: string;
	onReset?: () => void;
}

export function AdvancedPluginParams({
	pluginParams,
	updatePluginParams,
	plugin,
	showStoreFile,
	customParamsMode = "auto",
	triggerText,
	triggerIcon = "Code",
	triggerVariant = "tertiary",
	applyText,
	onReset,
}: AdvancedPluginParamsProps) {
	const { app, Info } = Application.use();
	const { t } = Locale.use();
	const [open, setOpen] = useState(false);

	// Global Settings
	const [overrideChunkSize, setOverrideChunkSize] = useState("");
	const [overrideAllowUnmappedFields, setOverrideAllowUnmappedFields] =
		useState(false);
	const [timestampOffsetMsec, setTimestampOffsetMsec] = useState("");
	const [storeFile, setStoreFile] = useState(false);

	// Mapping Parameters
	const [mappingFile, setMappingFile] = useState("");
	const [mappingId, setMappingId] = useState("");
	const [additionalMappingFiles, setAdditionalMappingFiles] = useState("");
	const [mappings, setMappings] = useState<MappingData[]>([]);
	const [additionalMappings, setAdditionalMappings] = useState<MappingData[]>(
		[],
	);
	const [sigmaMappings, setSigmaMappings] = useState<SigmaMappingData[]>([]);
	const [savedMappings, setSavedMappings] = useState<SavedPluginMapping[]>([]);
	const [savedMappingsLoading, setSavedMappingsLoading] = useState(false);

	const methods = useMemo(
		() => (plugin ? Mapping.Entity.methods(app, plugin.filename) : []),
		[app, plugin],
	);
	const mappingsList = useMemo(
		() =>
			plugin && mappingFile
				? Mapping.Entity.mappings(app, plugin.filename, mappingFile)
				: [],
		[app, plugin, mappingFile],
	);

	// Custom Parameters
	const [customParams, setCustomParams] = useState<any>({});

	// Panel States
	const [isMappingPanelOpen, setIsMappingPanelOpen] = useState(false);
	const [isLastMappingsPanelOpen, setIsLastMappingsPanelOpen] = useState(false);
	const [editingMapping, setEditingMapping] = useState<MappingData | null>(
		null,
	);
	const [editingMappingIndex, setEditingMappingIndex] = useState<number | null>(
		null,
	);
	const [editingMappingType, setEditingMappingType] = useState<
		"mappings" | "additional"
	>("mappings");

	const [isSigmaPanelOpen, setIsSigmaPanelOpen] = useState(false);
	const [editingSigma, setEditingSigma] = useState<SigmaMappingData | null>(
		null,
	);
	const [editingSigmaIndex, setEditingSigmaIndex] = useState<number | null>(
		null,
	);

	// Auto-selection logic: automatically sets the first available mapping file
	// when a plugin is selected and no manual choice has been made.
	useEffect(() => {
		if (!plugin) return;

		if (methods.length === 0) {
			setMappingFile("");
		} else if (methods.length === 1) {
			setMappingFile(methods[0]);
		} else if (mappingFile && !methods.includes(mappingFile)) {
			setMappingFile("");
		}
	}, [plugin, methods]);

	// Auto-selection logic: automatically sets the first available mapping ID
	// when a mapping file is selected and no manual choice has been made.
	useEffect(() => {
		if (!plugin) return;

		if (mappingsList.length === 0 || !mappingFile) {
			setMappingId("");
		} else if (mappingsList.length === 1) {
			setMappingId(mappingsList[0]);
		} else if (mappingId && !mappingsList.includes(mappingId)) {
			setMappingId("");
		}
	}, [plugin, mappingFile, mappingsList]);

	// Initializing local state from the passed pluginParams prop when the modal opens.
	// This ensures the UI reflects current configuration values.
	useEffect(() => {
		if (open) {
			setOverrideChunkSize(
				pluginParams?.override_chunk_size !== undefined
					? String(pluginParams.override_chunk_size)
					: "",
			);
			setOverrideAllowUnmappedFields(
				pluginParams?.override_allow_unmapped_fields || false,
			);
			setTimestampOffsetMsec(
				pluginParams?.offset !== undefined && pluginParams?.offset !== 0
					? String(pluginParams.offset)
					: "",
			);
			setStoreFile(pluginParams?.store_file || false);

			const currentMethods = plugin
				? Mapping.Entity.methods(app, plugin.filename)
				: [];

			const mappingParams = pluginParams?.mapping_parameters || {};

			let initMappingFile =
				pluginParams?.method || mappingParams.mapping_file || "";
			if (!initMappingFile && currentMethods.length === 1) {
				initMappingFile = currentMethods[0];
			}
			setMappingFile(initMappingFile);

			let initMappingId =
				pluginParams?.mapping || mappingParams.mapping_id || "";
			if (!initMappingId) {
				const currentMappings =
					plugin && initMappingFile
						? Mapping.Entity.mappings(app, plugin.filename, initMappingFile)
						: [];
				if (currentMappings.length === 1) {
					initMappingId = currentMappings[0];
				}
			}
			setMappingId(initMappingId);

			setAdditionalMappingFiles(
				pluginParams?.additional_mapping_files ||
					mappingParams.additional_mapping_files ||
					"",
			);

			const parseMappings = (obj: Record<string, any> = {}): MappingData[] => {
				return Object.entries(obj).map(
					([id, data]) => ({ id, ...data }) as MappingData,
				);
			};

			setMappings(
				parseMappings(pluginParams?.mappings || mappingParams.mappings),
			);
			setAdditionalMappings(
				parseMappings(
					pluginParams?.additional_mappings ||
						mappingParams.additional_mappings,
				),
			);

			const parsedSigma: SigmaMappingData[] = [];
			const sigmaMappingsSource =
				pluginParams?.sigma_mappings || mappingParams.sigma_mappings;
			if (sigmaMappingsSource) {
				Object.entries(sigmaMappingsSource).forEach(
					([name, data]: [string, any]) => {
						parsedSigma.push({
							name,
							service_field: data.service_field,
							service_values: data.service_values || [],
						});
					},
				);
			}
			setSigmaMappings(parsedSigma);

			let initCustomParams = pluginParams?.custom_parameters || {};
			if (customParamsMode === "textarea") {
				if (
					typeof initCustomParams === "object" &&
					Object.keys(initCustomParams).length > 0
				) {
					initCustomParams = JSON.stringify(initCustomParams, null, 2);
				} else if (typeof initCustomParams === "object") {
					initCustomParams = "";
				} else {
					initCustomParams = String(initCustomParams);
				}
			}
			setCustomParams(initCustomParams);
		}
	}, [open, pluginParams, plugin, app]);

	const isCustomParamsJsonValid = useMemo(() => {
		if (customParamsMode !== "textarea") return true;
		if (typeof customParams !== "string" || customParams.trim() === "")
			return true;
		try {
			const parsed = JSON.parse(customParams);
			return (
				typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
			);
		} catch {
			return false;
		}
	}, [customParams, customParamsMode]);

	/**
	 * Inserts or replaces a mapping entry while keeping mapping_id unique.
	 *
	 * @param currentMappings - Existing mapping entries from the target list.
	 * @param data - Mapping entry to insert or update.
	 * @param editingIndex - Explicit edit index when updating from the editor.
	 * @returns A new mapping list with a single entry for the provided mapping_id.
	 */
	const upsertMappingById = (
		currentMappings: MappingData[],
		data: MappingData,
		editingIndex: number | null,
	): MappingData[] => {
		const normalizedData = {
			...data,
			id: data.id.trim(),
		};
		const existingIndex = currentMappings.findIndex((mapping, index) => {
			if (editingIndex !== null && index === editingIndex) {
				return false;
			}

			return mapping.id === normalizedData.id;
		});

		if (editingIndex !== null) {
			const nextMappings = currentMappings.filter(
				(_, index) => index !== existingIndex,
			);
			nextMappings[
				editingIndex > existingIndex && existingIndex !== -1
					? editingIndex - 1
					: editingIndex
			] = normalizedData;
			return nextMappings;
		}

		if (existingIndex !== -1) {
			const nextMappings = [...currentMappings];
			nextMappings[existingIndex] = normalizedData;
			return nextMappings;
		}

		return [...currentMappings, normalizedData];
	};

	/**
	 * Saves a mapping from the edit panel into the selected mapping collection.
	 *
	 * @param data - Mapping object created or updated in the mapping editor.
	 */
	const handleSaveMapping = (data: MappingData) => {
		if (editingMappingType === "mappings") {
			setMappings((currentMappings) =>
				upsertMappingById(currentMappings, data, editingMappingIndex),
			);
			setMappingId(data.id.trim());
		} else {
			setAdditionalMappings((currentMappings) =>
				upsertMappingById(currentMappings, data, editingMappingIndex),
			);
		}
	};

	/**
	 * Loads saved custom mappings for the selected plugin into the Last Mappings panel.
	 *
	 * @returns A promise that resolves after the saved mappings are loaded or a failure is handled.
	 */
	const openLastMappingsPanel = async () => {
		if (!plugin?.filename) {
			toast.error(t("advancedParams.selectPluginBeforeLoadingSavedMappings"));
			return;
		}

		setSavedMappingsLoading(true);
		setIsLastMappingsPanelOpen(true);

		try {
			const savedPluginMappings = await loadSavedPluginMappings(
				plugin.filename,
			);
			setSavedMappings(savedPluginMappings);
		} catch {
			setSavedMappings([]);
			toast.error(t("advancedParams.failedLoadSavedMappings"));
		} finally {
			setSavedMappingsLoading(false);
		}
	};

	/**
	 * Restores a saved custom mapping into the active mapping list.
	 *
	 * @param savedMapping - Saved mapping selected from IndexedDB history.
	 * @returns void
	 */
	const loadSavedMapping = (savedMapping: SavedPluginMapping) => {
		setMappings((currentMappings) =>
			upsertMappingById(currentMappings, savedMapping.mapping, null),
		);
		setMappingId(savedMapping.mapping_id);
		setIsLastMappingsPanelOpen(false);
		toast.success("Saved mapping loaded");
	};

	/**
	 * Deletes a saved custom mapping from IndexedDB for the selected plugin.
	 *
	 * @param savedMapping - Persisted mapping record selected for deletion.
	 * @returns A promise that resolves when the saved mappings state is refreshed.
	 */
	const deleteSavedMapping = async (savedMapping: SavedPluginMapping) => {
		if (!plugin?.filename) {
			toast.error("Select a plugin before deleting saved mappings");
			return;
		}

		try {
			const savedPluginMappings = await deleteSavedPluginMapping(
				plugin.filename,
				savedMapping.mapping_id,
			);
			setSavedMappings(savedPluginMappings);
			toast.success("Saved mapping deleted");
		} catch {
			toast.error("Failed to delete saved mapping");
		}
	};

	const handleSaveSigma = (data: SigmaMappingData) => {
		if (editingSigmaIndex !== null) {
			const newArr = [...sigmaMappings];
			newArr[editingSigmaIndex] = data;
			setSigmaMappings(newArr);
		} else {
			setSigmaMappings([...sigmaMappings, data]);
		}
	};

	/**
	 * Transforms mapping editor rows into the keyed backend mapping object.
	 *
	 * @param mappingEntries - Mapping rows currently present in the editor.
	 * @returns Backend-ready mapping object keyed by mapping_id.
	 */
	const buildMappingsObject = (
		mappingEntries: MappingData[],
	): Record<string, Record<string, unknown>> => {
		const mappingsObject: Record<string, Record<string, unknown>> = {};

		mappingEntries.forEach((mappingEntry) => {
			const { id, ...mappingData } = mappingEntry;
			const mappingId = id.trim();

			if (mappingId) {
				mappingsObject[mappingId] = mappingData as Record<string, unknown>;
			}
		});

		return mappingsObject;
	};

	/**
	 * Uploads a single mapping row to the backend mapping-file endpoint.
	 *
	 * @param mapping - Mapping row selected from the mapping summary table.
	 * @returns A promise that resolves after the upload attempt is handled.
	 */
	const uploadMapping = async (mapping: MappingData): Promise<void> => {
		if (!plugin?.filename) {
			toast.error("Select a plugin before uploading mappings");
			return;
		}

		const mappingsObject = buildMappingsObject([mapping]);

		if (Object.keys(mappingsObject).length === 0) {
			toast.error("Mapping ID is required before upload");
			return;
		}

		try {
			await Info.mapping_file_upload(
				{
					metadata: {
						plugin: [plugin.filename],
					},
					mappings: mappingsObject,
				},
				false,
			);
			toast.success("Mapping uploaded");
		} catch {
			toast.error("Failed to upload mapping");
		}
	};

	/**
	 * Orchestrates the transformation of UI state into a flat 'settings' object structure.
	 * It handles conditional deletions of empty parameters, number conversions,
	 * mapping persistence, and object transformations.
	 *
	 * @returns A promise that resolves after the configuration is persisted and applied.
	 */
	const apply = async () => {
		try {
			// Transforming list-based UI states back into keyed objects used by the backend.
			const mappingsObj = buildMappingsObject(mappings);
			const additionalMappingsObj = buildMappingsObject(additionalMappings);

			const sigmaObj: Record<string, any> = {};
			sigmaMappings.forEach((s) => {
				sigmaObj[s.name] = {
					service_field: s.service_field,
					service_values: s.service_values,
				};
			});

			const payload: Record<string, any> = {
				...pluginParams,
			};

			// Explicitly setting or deleting parameters based on UI input values.
			if (overrideChunkSize)
				payload.override_chunk_size = Number(overrideChunkSize);
			else delete payload.override_chunk_size;

			if (overrideAllowUnmappedFields)
				payload.override_allow_unmapped_fields = true;
			else delete payload.override_allow_unmapped_fields;

			if (storeFile) payload.store_file = true;
			else delete payload.store_file;

			if (timestampOffsetMsec) payload.offset = Number(timestampOffsetMsec);
			else delete payload.offset;

			if (mappingFile) payload.method = mappingFile;
			else delete payload.method;

			if (mappingId) payload.mapping = mappingId;
			else delete payload.mapping;

			if (additionalMappingFiles)
				payload.additional_mapping_files = additionalMappingFiles;
			else delete payload.additional_mapping_files;

			if (Object.keys(mappingsObj).length > 0) payload.mappings = mappingsObj;
			else delete payload.mappings;

			if (Object.keys(additionalMappingsObj).length > 0)
				payload.additional_mappings = additionalMappingsObj;
			else delete payload.additional_mappings;

			if (Object.keys(sigmaObj).length > 0) payload.sigma_mappings = sigmaObj;
			else delete payload.sigma_mappings;

			if (customParamsMode === "textarea") {
				let parsed = customParams;
				if (typeof customParams === "string" && customParams.trim() !== "") {
					try {
						parsed = JSON.parse(customParams);
					} catch {
						parsed = customParams;
					}
				}
				if (
					parsed !== "" &&
					parsed !== undefined &&
					(typeof parsed !== "object" || Object.keys(parsed).length > 0)
				) {
					payload.custom_parameters = parsed;
				} else {
					delete payload.custom_parameters;
				}
			} else {
				if (
					customParams &&
					typeof customParams === "object" &&
					Object.keys(customParams).length > 0
				)
					payload.custom_parameters = customParams;
				else delete payload.custom_parameters;
			}

			// Removing legacy/internal keys that might have leaked into the state object.
			delete payload.timestamp_offset_msec;
			delete payload.mapping_parameters;

			if (plugin?.filename && mappings.length > 0) {
				const savedPluginMappings = await persistPluginMappings(
					plugin.filename,
					mappings,
				);
				setSavedMappings(savedPluginMappings);
			}

			updatePluginParams(payload);
			toast.success(t("advancedParams.applied"));
			setOpen(false);
		} catch {
			toast.error(t("advancedParams.applyFailed"));
		}
	};

	const handleReset = () => {
		if (onReset) {
			onReset();
		} else {
			updatePluginParams({});
			toast.success(t("advancedParams.resetSuccess"));
		}
		setOpen(false);
	};

	return (
		<Dialog.Root
			open={open}
			onOpenChange={setOpen}
		>
			<Dialog.Trigger asChild>
				<Button
					variant={triggerVariant as any}
					icon={triggerIcon as any}
					className={s.fullWidth}
				>
					{triggerText ?? t("advancedParams.trigger")}
				</Button>
			</Dialog.Trigger>

			<Dialog.Portal>
				<Dialog.Overlay className={s.overlayL1} />
				<Dialog.Content
					aria-describedby={undefined}
					className={s.contentL1}
				>
					<Stack
						dir="column"
						gap={24}
						ai="stretch"
					>
						<Stack
							dir="row"
							jc="space-between"
							ai="center"
						>
							<Dialog.Title className={cn(s.titleBase, s.titleL)}>
								{t("advancedParams.title")}
							</Dialog.Title>
							<Button
								variant="tertiary"
								icon="Trash"
								onClick={handleReset}
								className={s.resetButton}
							>
								{t("advancedParams.resetDelete")}
							</Button>
						</Stack>

						<Separator />

						{/* Global Settings */}
						<Stack
							dir="column"
							gap={12}
							ai="stretch"
						>
							<Label
								value={t("advancedParams.globalSettings")}
								className={s.labelBold}
							/>
							<Stack
								dir="column"
								gap={12}
								ai="stretch"
							>
								<Input
									icon="ChartColumn"
									variant="highlighted"
									label={t("advancedParams.overrideChunkSize")}
									placeholder="e.g. 1000"
									type="number"
									value={overrideChunkSize}
									onChange={(e) => setOverrideChunkSize(e.target.value)}
								/>
								<Input
									icon="LayoutShift"
									variant="highlighted"
									placeholder={t("common.zeroMilliseconds")}
									label={t("advancedParams.offset")}
									type="number"
									value={timestampOffsetMsec}
									onChange={(e) => setTimestampOffsetMsec(e.target.value)}
								/>
							</Stack>
							<Stack
								ai="center"
								gap={8}
								className={s.fullWidth}
								style={{ padding: "8px 0" }}
							>
								<Checkbox
									id="override_allow_unmapped_fields"
									checked={overrideAllowUnmappedFields}
									onCheckedChange={(c) => setOverrideAllowUnmappedFields(!!c)}
								/>
								<Label
									htmlFor="override_allow_unmapped_fields"
									value={t("advancedParams.overrideAllowUnmapped")}
									cursor="pointer"
								/>
							</Stack>
							{showStoreFile && (
								<Stack
									ai="center"
									gap={8}
									className={s.fullWidth}
									style={{ padding: "8px 0" }}
								>
									<Checkbox
										id="store_file"
										checked={storeFile}
										onCheckedChange={(c) => setStoreFile(!!c)}
									/>
									<Label
										htmlFor="store_file"
										value={t("advancedParams.storeFile")}
										cursor="pointer"
									/>
								</Stack>
							)}
						</Stack>

						<Separator />

						{/* Mapping Parameters */}
						<Stack
							dir="column"
							gap={16}
							ai="stretch"
						>
							<Label
								value={t("advancedParams.mappingParameters")}
								className={s.labelBold}
							/>

							<Stack
								dir="column"
								gap={16}
								ai="stretch"
							>
								{methods.length > 0 && (
									<MethodSelector
										settings={{
											method: mappingFile,
											offset: 0,
											custom_parameters: {},
										}}
										updateSettings={(update) => {
											if (update.method !== undefined) {
												setMappingFile(update.method);
												setMappingId("");
											}
										}}
										methods={methods}
									/>
								)}
								{mappingsList.length > 0 && (
									<MappingSelector
										settings={{
											mapping: mappingId,
											offset: 0,
											custom_parameters: {},
										}}
										updateSettings={(update) => {
											if (update.mapping !== undefined) {
												setMappingId(update.mapping);
											}
										}}
										mappings={mappingsList}
									/>
								)}
							</Stack>

							<Stack
								dir="row"
								jc="space-between"
								ai="center"
								className={s.fullWidth}
								style={{ marginTop: 8 }}
							>
								<Label value={t("advancedParams.mappings")} />
								<Stack
									dir="row"
									gap={8}
									ai="center"
								>
									<Button
										variant="secondary"
										onClick={openLastMappingsPanel}
										disabled={!plugin?.filename}
									>
										{t("advancedParams.lastMappings")}
									</Button>
									<Button
										variant="secondary"
										onClick={() => {
											setEditingMappingType("mappings");
											setEditingMapping(null);
											setEditingMappingIndex(null);
											setIsMappingPanelOpen(true);
										}}
									>
										{t("advancedParams.addMapping")}
									</Button>
								</Stack>
							</Stack>
							<SummaryTable
								columns={[
									{ key: "id", label: t("advancedParams.mappingId") },
									{ key: "agent_type", label: t("advancedParams.agentType") },
									{ key: "description", label: t("common.description") },
								]}
								data={mappings}
								actions={[
									{
										icon: "Save",
										label: "Upload mapping",
										variant: "glass",
										onClick: (item) => uploadMapping(item),
									},
								]}
								onEdit={(item, index) => {
									setEditingMappingType("mappings");
									setEditingMapping(item);
									setEditingMappingIndex(index);
									setIsMappingPanelOpen(true);
								}}
								onDelete={(_, index) =>
									setMappings(mappings.filter((__, i) => i !== index))
								}
							/>

							<Input
								label={t("advancedParams.additionalMappingFiles")}
								placeholder="filename1/mapping_id1, filename2/mapping_id2"
								value={additionalMappingFiles}
								onChange={(e) => setAdditionalMappingFiles(e.target.value)}
							/>

							<Stack
								dir="row"
								jc="space-between"
								ai="center"
								className={s.fullWidth}
								style={{ marginTop: 8 }}
							>
								<Label value={t("advancedParams.additionalMappings")} />
								<Button
									variant="secondary"
									onClick={() => {
										setEditingMappingType("additional");
										setEditingMapping(null);
										setEditingMappingIndex(null);
										setIsMappingPanelOpen(true);
									}}
								>
									{t("advancedParams.addAdditionalMapping")}
								</Button>
							</Stack>
							<SummaryTable
								columns={[
									{ key: "id", label: t("advancedParams.mappingId") },
									{ key: "agent_type", label: t("advancedParams.agentType") },
									{ key: "description", label: t("common.description") },
								]}
								data={additionalMappings}
								onEdit={(item, index) => {
									setEditingMappingType("additional");
									setEditingMapping(item);
									setEditingMappingIndex(index);
									setIsMappingPanelOpen(true);
								}}
								onDelete={(_, index) =>
									setAdditionalMappings(
										additionalMappings.filter((__, i) => i !== index),
									)
								}
							/>

							<Stack
								dir="row"
								jc="space-between"
								ai="center"
								className={s.fullWidth}
								style={{ marginTop: 8 }}
							>
								<Label value={t("advancedParams.sigmaMappings")} />
								<Button
									variant="secondary"
									onClick={() => {
										setEditingSigma(null);
										setEditingSigmaIndex(null);
										setIsSigmaPanelOpen(true);
									}}
								>
									{t("advancedParams.addSigmaMapping")}
								</Button>
							</Stack>
							<SummaryTable
								columns={[
									{ key: "name", label: t("common.name") },
									{
										key: "service_field",
										label: t("advancedParams.serviceField"),
									},
									{
										key: "service_values",
										label: t("advancedParams.serviceValues"),
										render: (val) =>
											Array.isArray(val) ? val.join(", ") : val,
									},
								]}
								data={sigmaMappings}
								onEdit={(item, index) => {
									setEditingSigma(item);
									setEditingSigmaIndex(index);
									setIsSigmaPanelOpen(true);
								}}
								onDelete={(_, index) =>
									setSigmaMappings(sigmaMappings.filter((__, i) => i !== index))
								}
							/>
						</Stack>

						<Separator />

						{/* Custom Parameters */}
						<Stack
							dir="column"
							gap={8}
							ai="stretch"
						>
							<Label
								value={t("advancedParams.customParameters")}
								className={s.labelBold}
							/>
							<Stack
								dir="column"
								gap={8}
								ai="stretch"
							>
								{customParamsMode === "auto" && plugin && (
									<CustomParameters.Editor
										plugin={plugin}
										customParameters={customParams}
										setCustomParameters={(update) => {
											setCustomParams((prev: any) => {
												const next =
													typeof update === "function" ? update(prev) : update;
												return { ...next };
											});
										}}
									/>
								)}
								{customParamsMode === "textarea" && (
									<Textarea
										value={typeof customParams === "string" ? customParams : ""}
										onChange={(e) => setCustomParams(e.target.value)}
										placeholder={t(
											"advancedParams.customParametersPlaceholder",
										)}
										style={{ minHeight: 150 }}
										error={!isCustomParamsJsonValid}
									/>
								)}
							</Stack>
						</Stack>

						<Stack
							dir="row"
							gap={8}
							className={s.fullWidth}
							style={{ marginTop: 16 }}
						>
							<Button
								variant="glass"
								className={s.fullWidth}
								onClick={apply}
								icon="Check"
								disabled={!isCustomParamsJsonValid}
							>
								{applyText ?? t("advancedParams.applyChanges")}
							</Button>
						</Stack>
					</Stack>

					<MappingPanel
						open={isMappingPanelOpen}
						setOpen={setIsMappingPanelOpen}
						onSave={handleSaveMapping}
						initialData={editingMapping}
					/>
					<LastMappingsPanel
						open={isLastMappingsPanelOpen}
						setOpen={setIsLastMappingsPanelOpen}
						savedMappings={savedMappings}
						loading={savedMappingsLoading}
						onLoad={loadSavedMapping}
						onDelete={deleteSavedMapping}
					/>
					<SigmaMappingPanel
						open={isSigmaPanelOpen}
						setOpen={setIsSigmaPanelOpen}
						onSave={handleSaveSigma}
						initialData={editingSigma}
					/>
				</Dialog.Content>
			</Dialog.Portal>
		</Dialog.Root>
	);
}

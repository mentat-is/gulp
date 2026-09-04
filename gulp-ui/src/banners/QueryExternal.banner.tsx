import type { GulpDataset } from "@/class/Info";
import { Application } from "@/context/Application.context";
import { Extension } from "@/context/Extension.context";
import { Banner as UIBanner } from "@/ui/Banner";
import { Select } from "@/ui/Select";
import { Icon } from "@/ui/Icon";
import {
	type Dispatch,
	type SetStateAction,
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import s from "./styles/QueryExternalBanner.module.css";
import { Label } from "@/ui/Label";
import { Preview } from "./Preview.banner";
import { SelectFiles } from "./SelectFiles.banner";
import { Skeleton } from "@/ui/Skeleton";
import { Button } from "@/ui/Button";
import { Stack } from "@/ui/Stack";
import { Textarea } from "@/ui/Textarea";
import { toast } from "sonner";
import { AdvancedPluginParams } from "@/components/AdvancedPluginParams";
import { Locale } from "@/locales";
import { CustomParameters } from "@/components/CustomParameters";

export namespace QueryExternal {
	export const PluginSelection = ({
		options,
		selectedOption,
		onSelect,
	}: {
		options: GulpDataset.PluginList.Interface[] | null;
		selectedOption: GulpDataset.PluginList.Interface | null;
		onSelect: (name: string) => void;
	}) => {
		const { t } = Locale.use();
		if (!options) return <Skeleton />;

		return (
			<Select.Root
				value={selectedOption?.filename}
				onValueChange={onSelect}
				disabled={!options}
			>
				<Select.Trigger>
					<Icon name="Puzzle" />
					{selectedOption
						? selectedOption.display_name
						: t("queryExternal.selectPlugin")}
				</Select.Trigger>
				<Select.Content>
					{options.map((option) => (
						<Select.Item
							key={option.filename}
							value={option.filename}
						>
							<Icon name="Puzzle" />
							{option.display_name}
						</Select.Item>
					))}
				</Select.Content>
			</Select.Root>
		);
	};

	/**
	 * Renders external-plugin parameters through the shared typed editor.
	 * @param props.selectedOption Currently selected external query plugin.
	 * @param props.params Current typed custom parameter values.
	 * @param props.setParams State setter for typed custom parameter values.
	 * @param props.onValidityChange Callback reporting aggregate editor validity.
	 * @returns The shared custom parameter editor, or null without a plugin.
	 */
	export const DynamicRequestBuilder = ({
		selectedOption,
		params,
		setParams,
		onValidityChange,
	}: {
		selectedOption: GulpDataset.PluginList.Interface | null;
		params: CustomParameters.ValueMap;
		setParams: Dispatch<SetStateAction<CustomParameters.ValueMap>>;
		onValidityChange?: (isValid: boolean) => void;
	}) => {
		if (!selectedOption) return null;

		return (
			<CustomParameters.Editor
				plugin={selectedOption}
				customParameters={params}
				setCustomParameters={setParams}
				onValidityChange={onValidityChange}
			/>
		);
	};

	/**
	 * Determines whether external-query actions must be disabled.
	 * @param selectedOption Currently selected plugin.
	 * @param areCustomParametersValid Aggregate custom parameter validity.
	 * @returns True when the query cannot be submitted safely.
	 */
	export function isSubmissionDisabled(
		selectedOption: GulpDataset.PluginList.Interface | null,
		areCustomParametersValid: boolean,
	): boolean {
		return !selectedOption || !areCustomParametersValid;
	}

	/**
	 * Converts a restored query payload into the editable JSON text representation.
	 * @param query Query payload or an already serialized query string.
	 * @returns The serialized query without double-encoding restored string values.
	 */
	export function restoredQueryText(query: unknown): string {
		if (!query) return "";
		return typeof query === "string" ? query : JSON.stringify(query, null, 2);
	}

	/** Request fields prepared by either the default editor or a dedicated UI plugin. */
	export interface PreparedRequest {
		q: unknown;
		plugin_params: Record<string, unknown>;
		q_options: Record<string, unknown>;
	}

	/** Props supplied to a UI plugin mounted in the Query External slot. */
	export interface PluginEditorProps extends Extension.ComponentProps {
		backendPlugin: GulpDataset.PluginList.Interface;
		value: PreparedRequest;
		onChange: (value: PreparedRequest) => void;
		onValidityChange: (isValid: boolean) => void;
	}

	/**
	 * Creates an isolated request value suitable for a controlled query editor.
	 * @param initialRequest Optional request restored after banner navigation.
	 * @returns A shallow copy of the restored request or a new empty request.
	 */
	export function createPreparedRequest(
		initialRequest?: PreparedRequest,
	): PreparedRequest {
		if (!initialRequest) {
			return {
				q: "",
				plugin_params: { custom_parameters: {} },
				q_options: {},
			};
		}

		return {
			q: initialRequest.q,
			plugin_params: { ...initialRequest.plugin_params },
			q_options: { ...initialRequest.q_options },
		};
	}

	/**
	 * Returns Query External UI extensions associated with one backend plugin.
	 * @param extensions Loaded UI extension registry.
	 * @param backendPlugin Selected backend external-query plugin.
	 * @returns Exact manifest matches in deterministic slot order.
	 */
	export function getPluginEditors(
		extensions: Record<string, Extension.Interface>,
		backendPlugin: GulpDataset.PluginList.Interface | null,
	): Extension.Interface[] {
		if (!backendPlugin) return [];

		return Extension.getBySlot(extensions, Extension.Slot.QueryExternal).filter(
			(extension) => extension.plugin === backendPlugin.filename,
		);
	}

	/**
	 * Reads the fallback editor's custom parameters from the shared request.
	 * @param request Current prepared request.
	 * @returns The typed custom-parameter value map, or an empty map when absent.
	 */
	function getCustomParameters(
		request: PreparedRequest,
	): CustomParameters.ValueMap {
		const customParameters = request.plugin_params.custom_parameters;
		if (
			typeof customParameters !== "object" ||
			customParameters === null ||
			Array.isArray(customParameters)
		) {
			return {};
		}

		return customParameters as CustomParameters.ValueMap;
	}

	/**
	 * Renders the standard Query External request form as the extension fallback.
	 * @param props Controlled editor contract owned by the Query External banner.
	 * @returns The existing custom, advanced, query, and query-options controls.
	 */
	export function DefaultRequestEditor({
		backendPlugin,
		value,
		onChange,
		onValidityChange,
	}: PluginEditorProps) {
		const { t } = Locale.use();
		const [qText, setQText] = useState(restoredQueryText(value.q));
		const [qOptionsText, setQOptionsText] = useState(
			Object.keys(value.q_options).length > 0
				? JSON.stringify(value.q_options, null, 2)
				: "",
		);
		const [showAdvanced, setShowAdvanced] = useState(false);
		const customParameters = getCustomParameters(value);

		/** Applies typed custom-parameter updates to the shared request. */
		const setCustomParameters: Dispatch<
			SetStateAction<CustomParameters.ValueMap>
		> = useCallback(
			(update) => {
				const nextCustomParameters =
					typeof update === "function"
						? update(customParameters)
						: update;
				onChange({
					...value,
					plugin_params: {
						...value.plugin_params,
						custom_parameters: nextCustomParameters,
					},
				});
			},
			[customParameters, onChange, value],
		);

		/** Replaces advanced plugin parameters in the shared request. */
		const updatePluginParams = useCallback(
			(nextPluginParams: Record<string, unknown>): void => {
				onChange({ ...value, plugin_params: nextPluginParams });
			},
			[onChange, value],
		);

		return (
			<>
				<DynamicRequestBuilder
					selectedOption={backendPlugin}
					params={customParameters}
					setParams={setCustomParameters}
					onValidityChange={onValidityChange}
				/>
				<AdvancedPluginParams
					plugin={backendPlugin}
					pluginParams={value.plugin_params}
					updatePluginParams={updatePluginParams}
				/>
				<Stack
					dir="column"
					className={s.wrapper}
					ai="stretch"
					gap={16}
				>
					<Button
						onClick={() => setShowAdvanced(!showAdvanced)}
						variant="tertiary"
						icon={showAdvanced ? "ChevronUp" : "ChevronDown"}
					>
						{t("queryExternal.advancedOptions")}
					</Button>
					{showAdvanced && (
						<>
							<Stack
								dir="column"
								gap={8}
								ai="stretch"
							>
								<Label value={t("queryExternal.queryJson")} />
								<Textarea
									value={qText}
									onChange={(event) => setQText(event.target.value)}
									onBlur={(event) => {
										const nextQuery = event.currentTarget.value.trim();
										setQText(nextQuery);
										onChange({ ...value, q: nextQuery });
									}}
									placeholder='{"query": {"match_all": {}}}'
									className={s.jsonInput}
								/>
								<span className={s.description}>
									{t("queryExternal.queryLanguageHint")}
								</span>
							</Stack>
							<Stack
								dir="column"
								gap={8}
								ai="stretch"
							>
								<Label value={t("queryExternal.queryOptionsJson")} />
								<Textarea
									value={qOptionsText}
									onChange={(event) => setQOptionsText(event.target.value)}
									onBlur={(event) => {
										const nextOptionsText = event.currentTarget.value.trim();
										try {
											const parsedOptions: unknown = nextOptionsText
												? JSON.parse(nextOptionsText)
												: {};
											if (
												typeof parsedOptions !== "object" ||
												parsedOptions === null ||
												Array.isArray(parsedOptions)
											) {
												throw new Error("Query options must be an object.");
											}
											const nextOptions = parsedOptions as Record<string, unknown>;
											onChange({ ...value, q_options: nextOptions });
											setQOptionsText(
												Object.keys(nextOptions).length > 0
													? JSON.stringify(nextOptions, null, 2)
													: "",
											);
										} catch {
											setQOptionsText(
												Object.keys(value.q_options).length > 0
													? JSON.stringify(value.q_options, null, 2)
													: "",
											);
										}
									}}
									placeholder='{"limit": 1000, "fields": ["@timestamp", "event.id"]}'
									className={s.jsonInput}
								/>
								<span className={s.description}>
									{t("queryExternal.queryOptionsHint")}
								</span>
							</Stack>
						</>
					)}
				</Stack>
				<p className={s.hint}>
					{t("queryExternal.requiredParams")} <Icon name="Asterisk" />
				</p>
			</>
		);
	}

	export namespace Banner {
		export interface Props extends UIBanner.Props {
			preselectedOption?: GulpDataset.PluginList.Interface;
			initialRequest?: PreparedRequest;
		}
	}

	/**
	 * Renders the external query workflow with a dedicated UI plugin or fallback editor.
	 * @param props.preselectedOption Optional plugin restored by back navigation.
	 * @param props.initialRequest Optional complete request restored by back navigation.
	 * @returns The external query banner.
	 */
	export const Banner = ({
		preselectedOption,
		initialRequest,
		...props
	}: QueryExternal.Banner.Props) => {
		const { Info, spawnBanner, app } = Application.use();
		const { extensions, ready: extensionsReady } = Extension.use();
		const { t } = Locale.use();
		const [options, setOptions] = useState<
			GulpDataset.PluginList.Interface[] | null
		>(app.target.plugins.filter((i) => i.type.includes("external")));
		const [selectedOption, setSelectedOption] = useState(
			preselectedOption ?? null,
		);
		const selectedPluginFilenameRef = useRef<string | null>(
			preselectedOption?.filename ?? null,
		);
		const [loading, setLoading] = useState<number>(0);
		const [preparedRequest, setPreparedRequest] = useState<PreparedRequest>(
			() => createPreparedRequest(initialRequest),
		);
		const [editorValidation, setEditorValidation] = useState<{
			key: string;
			isValid: boolean;
		} | null>(null);

		useEffect(() => {
			setOptions(app.target.plugins.filter((i) => i.type.includes("external")));
		}, [app.target.plugins]);

		const matchingPluginEditors = useMemo(
			() => getPluginEditors(extensions, selectedOption),
			[extensions, selectedOption],
		);
		const selectedPluginEditor = matchingPluginEditors[0] ?? null;
		const editorKey =
			selectedOption && extensionsReady
				? `${selectedOption.filename}:${selectedPluginEditor?.filename ?? "fallback"}`
				: null;
		const isEditorValid =
			editorKey !== null && editorValidation?.key === editorKey
				? editorValidation.isValid
				: editorKey !== null && selectedPluginEditor === null;

		useEffect(() => {
			if (!extensionsReady || matchingPluginEditors.length < 2) return;
			console.warn(
				`Multiple Query External UI plugins target ${selectedOption?.filename}. Using ${selectedPluginEditor?.filename}.`,
			);
		}, [
			extensionsReady,
			matchingPluginEditors,
			selectedOption?.filename,
			selectedPluginEditor?.filename,
		]);

		/** Accepts request updates only from the currently selected backend plugin. */
		const handleEditorChange = useCallback(
			(nextRequest: PreparedRequest): void => {
				if (
					!selectedOption ||
					selectedPluginFilenameRef.current !== selectedOption.filename
				) {
					return;
				}
				setPreparedRequest(createPreparedRequest(nextRequest));
			},
			[selectedOption],
		);

		/** Records validity only for the currently mounted request editor. */
		const handleEditorValidityChange = useCallback(
			(isValid: boolean): void => {
				if (
					!editorKey ||
					!selectedOption ||
					selectedPluginFilenameRef.current !== selectedOption.filename
				) {
					return;
				}
				setEditorValidation({ key: editorKey, isValid });
			},
			[editorKey, selectedOption],
		);

		/**
		 * Executes or previews the prepared external query when its editor is valid.
		 * @param preview Whether to request preview results instead of normal execution.
		 * @returns A promise that settles after the request and loading-state cleanup.
		 */
		const handleQuery = async (preview = false): Promise<void> => {
			if (!selectedOption || !isEditorValid) return;
			const requestSnapshot = createPreparedRequest(preparedRequest);
			setLoading(preview ? 2 : 1);
			Info.query_external(
				selectedOption.filename,
				requestSnapshot.plugin_params,
				preview,
				requestSnapshot.q,
				requestSnapshot.q_options,
				preview
					? undefined
					: {
							onIngestionStarted: () => {
								spawnBanner(
									<SelectFiles.Banner
										onClose={() =>
											spawnBanner(
												<QueryExternal.Banner
													preselectedOption={selectedOption}
													initialRequest={requestSnapshot}
													{...props}
												/>,
											)
										}
									/>,
								);
							},
						},
			)
				.then((result) => {
					if (result) {
						spawnBanner(
							<Preview.Banner
								total={result.total_hits || result.docs.length}
								fixed
								values={result.docs}
								back={() =>
									spawnBanner(
										<QueryExternal.Banner
											preselectedOption={selectedOption}
											initialRequest={requestSnapshot}
											{...props}
										/>,
									)
								}
							/>,
						);
						return;
					}

					toast.success(t("queryExternal.executed"), {
						description: t("queryExternal.selectNewSource"),
						icon: <Icon name="Check" />,
						richColors: true,
					});
				})
				.catch(() => {
					toast.error(t("queryExternal.failed"), {
						description: t("queryExternal.noMatches"),
						icon: <Icon name="X" />,
						richColors: true,
					});
				})
				.finally(() => setLoading(0));
		};

		/**
		 * Selects a backend query plugin and resets editor-owned request state.
		 * @param name Filename of the selected plugin.
		 * @returns Nothing.
		 */
		const handlePluginSelect = (name: string): void => {
			const option = options?.find((candidate) => candidate.filename === name);
			if (!option) return;
			selectedPluginFilenameRef.current = option.filename;
			setPreparedRequest(createPreparedRequest());
			setEditorValidation(null);
			setSelectedOption(option);
		};

		const submissionDisabled = isSubmissionDisabled(selectedOption, isEditorValid);

		const done = (
			<Button
				icon="Check"
				loading={loading === 1}
				disabled={submissionDisabled}
				onClick={() => handleQuery()}
				variant="glass"
			/>
		);

		const optionButton = (
			<Button
				icon="PreviewEye"
				variant="tertiary"
				loading={loading === 2}
				disabled={submissionDisabled}
				onClick={() => handleQuery(true)}
			/>
		);

		return (
			<UIBanner
				title={t("queryExternal.title")}
				done={done}
				option={optionButton}
				{...props}
			>
				<PluginSelection
					options={options}
					selectedOption={selectedOption}
					onSelect={handlePluginSelect}
				/>
				{selectedOption && !extensionsReady && <Skeleton />}
				{selectedOption && extensionsReady && selectedPluginEditor && (
					<Extension.Component
						key={editorKey}
						name={selectedPluginEditor.filename}
						props={{
							backendPlugin: selectedOption,
							value: preparedRequest,
							onChange: handleEditorChange,
							onValidityChange: handleEditorValidityChange,
						}}
					/>
				)}
				{selectedOption && extensionsReady && !selectedPluginEditor && (
					<DefaultRequestEditor
						key={editorKey}
						backendPlugin={selectedOption}
						value={preparedRequest}
						onChange={handleEditorChange}
						onValidityChange={handleEditorValidityChange}
					/>
				)}
			</UIBanner>
		);
	};
}

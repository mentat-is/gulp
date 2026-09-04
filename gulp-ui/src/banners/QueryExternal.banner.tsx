import type { GulpDataset } from "@/class/Info";
import { Application } from "@/context/Application.context";
import { Banner as UIBanner } from "@/ui/Banner";
import { Select } from "@/ui/Select";
import { Icon } from "@/ui/Icon";
import { useEffect, useState } from "react";
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
		setParams: React.Dispatch<
			React.SetStateAction<CustomParameters.ValueMap>
		>;
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

	export namespace Banner {
		export interface Props extends UIBanner.Props {
			preselectedOption?: GulpDataset.PluginList.Interface;
			preparams?: CustomParameters.ValueMap;
			preQ?: any;
			preQOptions?: Record<string, any>;
		}
	}

	/**
	 * Renders the external query workflow with typed plugin parameters.
	 * @param props.preselectedOption Optional plugin restored by back navigation.
	 * @param props.preparams Optional restored custom parameter values.
	 * @param props.preQ Optional restored query payload.
	 * @param props.preQOptions Optional restored query options.
	 * @returns The external query banner.
	 */
	export const Banner = ({
		preselectedOption,
		preparams,
		preQ,
		preQOptions,
		...props
	}: QueryExternal.Banner.Props) => {
		const { Info, spawnBanner, app } = Application.use();
		const { t } = Locale.use();
		const [options, setOptions] = useState<
			GulpDataset.PluginList.Interface[] | null
		>(app.target.plugins.filter((i) => i.type.includes("external")));
		const [selectedOption, setSelectedOption] = useState(
			preselectedOption ?? null,
		);
		const [loading, setLoading] = useState<number>(0);
		const [params, setParams] = useState<CustomParameters.ValueMap>(
			preparams ?? {},
		);
		const [areCustomParametersValid, setAreCustomParametersValid] =
			useState(true);
		const [q, setQ] = useState<string>(restoredQueryText(preQ));
		const [qOptions, setQOptions] = useState(preQOptions ?? {});
		const [qText, setQText] = useState(restoredQueryText(preQ));
		const [qOptionsText, setQOptionsText] = useState(
			preQOptions && Object.keys(preQOptions).length > 0
				? JSON.stringify(preQOptions, null, 2)
				: "",
		);
		const [showAdvanced, setShowAdvanced] = useState(false);
		const [pluginParams, setPluginParams] = useState<Record<string, any>>({
			custom_parameters: preparams ?? {},
		});

		useEffect(() => {
			setPluginParams((prev) => ({
				...prev,
				custom_parameters: params,
			}));
		}, [params]);

		useEffect(() => {
			setOptions(app.target.plugins.filter((i) => i.type.includes("external")));
		}, [app.target.plugins]);

		/**
		 * Executes or previews the current external query when its parameters are valid.
		 * @param preview Whether to request preview results instead of normal execution.
		 * @returns A promise that settles after the request and loading-state cleanup.
		 */
		const handleQuery = async (preview = false): Promise<void> => {
			if (!selectedOption || !areCustomParametersValid) return;
			setLoading(preview ? 2 : 1);
			Info.query_external(
				selectedOption.filename,
				pluginParams,
				preview,
				q,
				qOptions,
				preview
					? undefined
					: {
							onIngestionStarted: () => {
								spawnBanner(
									<SelectFiles.Banner
										onClose={() =>
											spawnBanner(
												<QueryExternal.Banner
													preparams={params}
													preselectedOption={selectedOption}
													preQ={q}
													preQOptions={qOptions}
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
											preparams={params}
											preselectedOption={selectedOption}
											preQ={q}
											preQOptions={qOptions}
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

		const updatePluginParams = (next: Record<string, any>) => {
			setPluginParams(next);

			if (
				next.custom_parameters &&
				typeof next.custom_parameters === "object"
			) {
				setParams(next.custom_parameters);
			}
		};

		/**
		 * Selects a query plugin and clears values owned by the previous plugin.
		 * @param name Filename of the selected plugin.
		 * @returns Nothing.
		 */
		const handlePluginSelect = (name: string): void => {
			const option = options?.find((candidate) => candidate.filename === name);
			if (!option) return;
			setParams({});
			setAreCustomParametersValid(true);
			setSelectedOption(option);
		};

		const submissionDisabled = isSubmissionDisabled(
			selectedOption,
			areCustomParametersValid,
		);

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
				<DynamicRequestBuilder
					selectedOption={selectedOption}
					params={params}
					setParams={setParams}
					onValidityChange={setAreCustomParametersValid}
				/>
				<AdvancedPluginParams
					plugin={selectedOption}
					pluginParams={pluginParams}
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
									onChange={(e) => {
										setQText(e.target.value);
									}}
									onBlur={(e) => {
										const value = e.currentTarget.value.trim();
										if (!value) {
											setQ("");
											setQText("");
											return;
										}
										setQ(value);
										setQText(value);
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
									onChange={(e) => {
										setQOptionsText(e.target.value);
									}}
									onBlur={(e) => {
										const value = e.currentTarget.value.trim();
										try {
											if (value) {
												const parsed = JSON.parse(value);
												setQOptions(parsed);
												setQOptionsText(JSON.stringify(parsed, null, 2));
											} else {
												setQOptions({});
												setQOptionsText("");
											}
										} catch {
											setQOptionsText(
												Object.keys(qOptions).length > 0
													? JSON.stringify(qOptions, null, 2)
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
			</UIBanner>
		);
	};
}

import { GulpDataset } from "@/class/Info";
import { Application } from "@/context/Application.context";
import { Banner as UIBanner } from "@/ui/Banner";
import { Select } from "@/ui/Select";
import { Switch } from "@/ui/Switch";
import { Icon } from "@/ui/Icon";
import { ChangeEvent, Dispatch, SetStateAction, useEffect, useMemo, useState } from "react";
import s from "./styles/QueryExternalBanner.module.css";
import { Label } from "@/ui/Label";
import { Input } from "@/ui/Input";
import { cn } from "@/ui/utils";
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

	export namespace Banner {
		export interface Props extends UIBanner.Props {
			preselectedOption?: GulpDataset.PluginList.Interface;
			preparams?: Record<string, any>;
			preQ?: any;
			preQOptions?: Record<string, any>;
		}
	}

	export const Banner = ({
		preselectedOption,
		preparams,
		preQ,
		preQOptions,
		...props
	}: QueryExternal.Banner.Props) => {
		const { Info, spawnBanner, destroyBanner, app } = Application.use();
		const { t } = Locale.use();
		const [options, setOptions] = useState<
			GulpDataset.PluginList.Interface[] | null
		>(app.target.plugins.filter((i) => i.type.includes("external")));
		const [selectedOption, setSelectedOption] = useState(
			preselectedOption ?? null,
		);
		const [loading, setLoading] = useState<number>(0);
		const [params, setParams] = useState(preparams ?? {});
		const [q, setQ] = useState<string>(
			preQ ? JSON.stringify(preQ, null, 2) : "",
		);
		const [qOptions, setQOptions] = useState(preQOptions ?? {});
		const [qText, setQText] = useState(
			preQ ? JSON.stringify(preQ, null, 2) : "",
		);
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

		const handleQuery = async (preview = false) => {
			if (!selectedOption) return;
			setLoading(preview ? 2 : 1);
			Info.query_external(
				selectedOption.filename,
				pluginParams,
				preview,
				q,
				qOptions,
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

					spawnBanner(<SelectFiles.Banner />);
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

		useEffect(() => {
			if (!selectedOption) {
				return setParams({});
			}
			const params: Record<string, any> = {};
			selectedOption.custom_parameters.forEach((param) => {
				params[param.name] = preparams
					? preparams[param.name]
					: param.default_value;
			});

			setParams(params);
		}, [selectedOption]);

		const done = (
			<Button
				icon="Check"
				loading={loading === 1}
				onClick={() => handleQuery()}
				variant="glass"
			/>
		);

		const optionButton = (
			<Button
				icon="PreviewEye"
				variant="tertiary"
				loading={loading === 2}
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
					onSelect={(name) =>
						setSelectedOption(options?.find((op) => op.filename === name)!)
					}
				/>
				<DynamicRequestBuilder
					selectedOption={selectedOption}
					params={params}
					setParams={setParams}
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

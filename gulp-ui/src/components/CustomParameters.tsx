import type { SetState } from "@/class/API";
import type { GulpDataset } from "@/class/Info";
import { Locale } from "@/locales";
import { Input } from "@/ui/Input";
import { Label } from "@/ui/Label";
import { Select } from "@/ui/Select";
import { Stack } from "@/ui/Stack";
import { Switch } from "@/ui/Switch";
import { Textarea } from "@/ui/Textarea";
import { type ChangeEvent, useCallback, useEffect, useRef, useState } from "react";
import s from "./styles/CustomParameters.module.css";

export namespace CustomParameters {
	export type Location = "query" | "header" | "body";

	export type Type = "int" | "float" | "str" | "bool" | "dict" | "list";

	export type JsonPrimitive = string | number | boolean | null;

	export type JsonValue =
		| JsonPrimitive
		| JsonValue[]
		| { [key: string]: JsonValue };

	export type ValueMap = Record<string, JsonValue>;

	export interface Interface {
		name: string;
		type: Type;
		desc?: string | null;
		location?: Location | null;
		required: boolean;
		default_value?: JsonValue;
		value?: JsonValue;
		is_secret?: boolean;
		example?: JsonValue;
		values?: JsonPrimitive[];
	}

	export namespace Editor {
		export interface Props {
			plugin?: GulpDataset.PluginList.Interface;
			parameters?: readonly CustomParameters.Interface[];
			customParameters: CustomParameters.ValueMap;
			setCustomParameters: SetState<CustomParameters.ValueMap>;
			onValidityChange?: (isValid: boolean) => void;
		}
	}

	interface DictionaryValidationResult {
		isValid: boolean;
		value: JsonValue;
	}

	interface DictionaryInputProps {
		parameter: Interface;
		value: JsonValue;
		onValueChange: (value: JsonValue) => void;
		onValidityChange: (name: string, isValid: boolean) => void;
	}

	interface SecretInputProps {
		parameter: Interface;
		value: JsonValue;
		onValueChange: (value: JsonValue) => void;
	}

	const emptyParameters: readonly Interface[] = [];

	/**
	 * Reports whether a parameter value is present, preserving valid falsy values.
	 * @param values Current custom parameter values.
	 * @param name Parameter name to inspect.
	 * @returns True when the record owns the parameter key.
	 */
	function hasParameterValue(values: ValueMap, name: string): boolean {
		return Object.prototype.hasOwnProperty.call(values, name);
	}

	/**
	 * Reports whether at least one declared parameter is absent from a value map.
	 * @param parameters Parameter definitions that must be represented.
	 * @param values Current controlled custom parameter values.
	 * @returns True when initialization still has work to do.
	 */
	function hasMissingParameterValues(
		parameters: readonly Interface[],
		values: ValueMap,
	): boolean {
		return parameters.some(
			(parameter) => !hasParameterValue(values, parameter.name),
		);
	}

	/**
	 * Resolves the initial value declared by a stored configuration or plugin definition.
	 * @param parameter Parameter definition to resolve.
	 * @returns The configured value, default value, or null when neither is present.
	 */
	function getDeclaredValue(parameter: Interface): JsonValue {
		if (Object.prototype.hasOwnProperty.call(parameter, "value")) {
			return parameter.value ?? null;
		}
		return parameter.default_value ?? null;
	}

	/**
	 * Resolves a current value without replacing an explicitly stored falsy value.
	 * @param parameter Parameter definition to resolve.
	 * @param values Current custom parameter values.
	 * @returns The controlled value or the definition fallback when absent.
	 */
	function getCurrentValue(parameter: Interface, values: ValueMap): JsonValue {
		return hasParameterValue(values, parameter.name)
			? values[parameter.name]
			: getDeclaredValue(parameter);
	}

	/**
	 * Converts a JSON value into text accepted by text-based controls.
	 * @param value JSON value to display.
	 * @returns A readable input value.
	 */
	function formatTextValue(value: JsonValue): string {
		if (value === null) return "";
		if (typeof value === "string") return value;
		if (typeof value === "number" || typeof value === "boolean") {
			return String(value);
		}
		return JSON.stringify(value);
	}

	/**
	 * Converts a JSON list into the comma-separated representation used by the editor.
	 * @param value Current parameter value.
	 * @returns Comma-separated text, or a scalar fallback for legacy values.
	 */
	function formatListValue(value: JsonValue): string {
		if (!Array.isArray(value)) return formatTextValue(value);
		return value.map((item) => formatTextValue(item)).join(", ");
	}

	/**
	 * Converts a dictionary value into editable JSON text.
	 * @param value Current parameter value.
	 * @returns Pretty-printed JSON or an empty draft for null.
	 */
	function formatDictionaryValue(value: JsonValue): string {
		if (value === null) return "";
		if (typeof value === "string") return value;
		return JSON.stringify(value, null, 2);
	}

	/**
	 * Parses a dictionary draft while rejecting arrays and scalar JSON values.
	 * @param draft Text entered in the dictionary control.
	 * @returns Validation state and the parsed JSON value when valid.
	 */
	function validateDictionaryDraft(draft: string): DictionaryValidationResult {
		if (draft.trim() === "") {
			return { isValid: true, value: null };
		}

		try {
			const parsed: unknown = JSON.parse(draft);
			if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
				return { isValid: false, value: null };
			}
			return { isValid: true, value: parsed as JsonValue };
		} catch {
			return { isValid: false, value: null };
		}
	}

	/**
	 * Serializes an enum value into a stable, type-preserving Select value.
	 * @param value Enum value declared by the plugin.
	 * @returns JSON-encoded select value.
	 */
	function encodeSelectValue(value: JsonPrimitive): string {
		return JSON.stringify(value);
	}

	/**
	 * Decodes a Select value back to the JSON primitive declared by the plugin.
	 * @param value JSON-encoded select value.
	 * @returns Decoded primitive value.
	 */
	function decodeSelectValue(value: string): JsonPrimitive {
		const parsed: unknown = JSON.parse(value);
		if (
			parsed === null ||
			typeof parsed === "string" ||
			typeof parsed === "number" ||
			typeof parsed === "boolean"
		) {
			return parsed;
		}
		return value;
	}

	/**
	 * Resolves the controlled Select value when the current parameter is scalar.
	 * @param value Current parameter value.
	 * @returns JSON-encoded primitive or an empty selection.
	 */
	function getSelectedValue(value: JsonValue): string {
		if (
			value === null ||
			typeof value === "string" ||
			typeof value === "number" ||
			typeof value === "boolean"
		) {
			return encodeSelectValue(value);
		}
		return "";
	}

	/**
	 * Renders optional descriptive text supplied by a plugin parameter.
	 * @param props.parameter Parameter whose description is rendered.
	 * @returns Description text or null when none is available.
	 */
	function Description({ parameter }: { parameter: Interface }) {
		if (!parameter.desc) return null;
		return <span className={s.description}>{parameter.desc}</span>;
	}

	/**
	 * Renders a secret parameter with an explicit visibility toggle.
	 * @param props.parameter Secret parameter definition.
	 * @param props.value Current masked or user-entered value.
	 * @param props.onValueChange Callback receiving the edited value.
	 * @returns A password input that can temporarily reveal its displayed value.
	 */
	function SecretInput({
		parameter,
		value,
		onValueChange,
	}: SecretInputProps) {
		const { t } = Locale.use();
		const [showSecret, setShowSecret] = useState(false);

		return (
			<Stack className={s.field} dir="column" gap={6} ai="stretch">
				<Input
					name={parameter.name}
					type={showSecret ? "text" : "password"}
					onChange={(event) => onValueChange(event.target.value)}
					value={formatTextValue(value)}
					variant="highlighted"
					label={parameter.name}
					placeholder={t("queryExternal.stringPlaceholder")}
					icon="Status"
					endIcon={showSecret ? "EyeOff" : "Eye"}
					endIconTitle={
						showSecret ? t("auth.hidePassword") : t("auth.showPassword")
					}
					onEndIconClick={() => setShowSecret((current) => !current)}
					required={parameter.required}
				/>
				<Description parameter={parameter} />
			</Stack>
		);
	}

	/**
	 * Renders and validates an object-valued parameter without publishing invalid drafts.
	 * @param props.parameter Dictionary parameter definition.
	 * @param props.value Last valid JSON value.
	 * @param props.onValueChange Callback for valid parsed values.
	 * @param props.onValidityChange Callback for draft validity changes.
	 * @returns The dictionary editor field.
	 */
	function DictionaryInput({
		parameter,
		value,
		onValueChange,
		onValidityChange,
	}: DictionaryInputProps) {
		const { t } = Locale.use();
		const externalDraft = formatDictionaryValue(value);
		const [draft, setDraft] = useState(externalDraft);
		const [isValid, setIsValid] = useState(
			validateDictionaryDraft(externalDraft).isValid,
		);
		const lastCommittedDraft = useRef(externalDraft);

		useEffect(() => {
			if (externalDraft === lastCommittedDraft.current) return;
			lastCommittedDraft.current = externalDraft;
			setDraft(externalDraft);
			setIsValid(validateDictionaryDraft(externalDraft).isValid);
		}, [externalDraft]);

		useEffect(() => {
			onValidityChange(parameter.name, isValid);
		}, [isValid, onValidityChange, parameter.name]);

		/**
		 * Updates the draft and publishes it only after it becomes a JSON object.
		 * @param event Textarea change event.
		 * @returns Nothing.
		 */
		const handleChange = (event: ChangeEvent<HTMLTextAreaElement>): void => {
			const nextDraft = event.target.value;
			const validation = validateDictionaryDraft(nextDraft);
			setDraft(nextDraft);
			setIsValid(validation.isValid);
			if (!validation.isValid) return;

			lastCommittedDraft.current = formatDictionaryValue(validation.value);
			onValueChange(validation.value);
		};

		return (
			<Stack className={s.field} dir="column" gap={6} ai="stretch">
				<Label
					value={parameter.name}
					className={parameter.required ? s.required : undefined}
				/>
				<Textarea
					name={parameter.name}
					value={draft}
					onChange={handleChange}
					placeholder={
						parameter.example === undefined
							? "{}"
							: formatDictionaryValue(parameter.example)
					}
					className={s.dictionary}
					error={!isValid}
					aria-invalid={!isValid}
				/>
				{!isValid && <span className={s.invalid}>{t("filterFile.invalidJson")}</span>}
				<Description parameter={parameter} />
			</Stack>
		);
	}

	/**
	 * Renders typed controls for plugin custom parameters.
	 * @param props.plugin Optional plugin supplying custom parameter definitions.
	 * @param props.parameters Optional explicit definitions, which take precedence over the plugin.
	 * @param props.customParameters Current values indexed by parameter name.
	 * @param props.setCustomParameters State setter receiving valid typed values.
	 * @param props.onValidityChange Optional aggregate dictionary-validity callback.
	 * @returns The custom parameter editor, or null when no definitions are available.
	 */
	export function Editor({
		plugin,
		parameters,
		customParameters,
		setCustomParameters,
		onValidityChange,
	}: CustomParameters.Editor.Props) {
		const { t } = Locale.use();
		const definitions = parameters ?? plugin?.custom_parameters ?? emptyParameters;
		const setCustomParametersRef = useRef(setCustomParameters);
		const [dictionaryValidity, setDictionaryValidity] = useState<
			Record<string, boolean>
		>({});

		useEffect(() => {
			setCustomParametersRef.current = setCustomParameters;
		}, [setCustomParameters]);

		useEffect(() => {
			if (!hasMissingParameterValues(definitions, customParameters)) return;
			setCustomParametersRef.current((currentValues) => {
				let changed = false;
				const initializedValues = { ...currentValues };
				definitions.forEach((parameter) => {
					if (hasParameterValue(currentValues, parameter.name)) return;
					initializedValues[parameter.name] = getDeclaredValue(parameter);
					changed = true;
				});
				return changed ? initializedValues : currentValues;
			});
		}, [customParameters, definitions]);

		useEffect(() => {
			const dictionaryNames = new Set(
				definitions
					.filter((parameter) => parameter.type === "dict" && !parameter.is_secret)
					.map((parameter) => parameter.name),
			);
			setDictionaryValidity((currentValidity) => {
				const activeEntries = Object.entries(currentValidity).filter(([name]) =>
					dictionaryNames.has(name),
				);
				if (activeEntries.length === Object.keys(currentValidity).length) {
					return currentValidity;
				}
				return Object.fromEntries(activeEntries);
			});
		}, [definitions]);

		const isValid = definitions.every((parameter) => {
			if (parameter.type !== "dict" || parameter.is_secret) return true;
			const externalValueIsValid = validateDictionaryDraft(
				formatDictionaryValue(getCurrentValue(parameter, customParameters)),
			).isValid;
			return dictionaryValidity[parameter.name] !== false && externalValueIsValid;
		});
		useEffect(() => {
			onValidityChange?.(isValid);
		}, [isValid, onValidityChange]);

		/**
		 * Publishes one typed parameter value without changing sibling fields.
		 * @param name Parameter name to update.
		 * @param value Parsed JSON value.
		 * @returns Nothing.
		 */
		const updateValue = useCallback(
			(name: string, value: JsonValue): void => {
				setCustomParameters((currentValues) => ({
					...currentValues,
					[name]: value,
				}));
			},
			[setCustomParameters],
		);

		/**
		 * Records validity for a dictionary draft by parameter name.
		 * @param name Dictionary parameter name.
		 * @param nextIsValid Whether the current draft is a JSON object.
		 * @returns Nothing.
		 */
		const updateDictionaryValidity = useCallback(
			(name: string, nextIsValid: boolean): void => {
				setDictionaryValidity((currentValidity) => {
					if (currentValidity[name] === nextIsValid) return currentValidity;
					return { ...currentValidity, [name]: nextIsValid };
				});
			},
			[],
		);

		if (definitions.length === 0) return null;

		return (
			<Stack className={s.wrapper} dir="column" ai="stretch" gap={16}>
				{definitions.map((parameter) => {
					const value = getCurrentValue(parameter, customParameters);

					if (parameter.is_secret) {
						return (
							<SecretInput
								key={parameter.name}
								parameter={parameter}
								value={value}
								onValueChange={(nextValue) =>
									updateValue(parameter.name, nextValue)
								}
							/>
						);
					}

					if (parameter.values && parameter.values.length > 0) {
						return (
							<Stack key={parameter.name} className={s.field} dir="column" gap={6} ai="stretch">
								<Label
									value={parameter.name}
									className={parameter.required ? s.required : undefined}
								/>
								<Select.Root
									value={getSelectedValue(value)}
									onValueChange={(selectedValue) =>
										updateValue(parameter.name, decodeSelectValue(selectedValue))
									}
								>
									<Select.Trigger data-no-icon>
										<Select.Value placeholder={t("common.choosePlaceholder")} />
									</Select.Trigger>
									<Select.Content>
										{parameter.values.map((option) => (
											<Select.Item key={encodeSelectValue(option)} value={encodeSelectValue(option)}>
												{option === null ? t("common.none") : formatTextValue(option)}
											</Select.Item>
										))}
									</Select.Content>
								</Select.Root>
								<Description parameter={parameter} />
							</Stack>
						);
					}

					if (parameter.type === "bool") {
						return (
							<Stack key={parameter.name} className={s.field} dir="column" gap={6} ai="flex-start">
								<Label
									value={parameter.name}
									className={parameter.required ? s.required : undefined}
								/>
								<Switch
									name={parameter.name}
									onCheckedChange={(checked) => updateValue(parameter.name, checked)}
									checked={value === true}
								/>
								<Description parameter={parameter} />
							</Stack>
						);
					}

					if (parameter.type === "dict") {
						return (
							<DictionaryInput
								key={parameter.name}
								parameter={parameter}
								value={value}
								onValueChange={(nextValue) => updateValue(parameter.name, nextValue)}
								onValidityChange={updateDictionaryValidity}
							/>
						);
					}

					const isNumber = parameter.type === "int" || parameter.type === "float";
					const inputValue =
						parameter.type === "list" ? formatListValue(value) : formatTextValue(value);
					const placeholder =
						parameter.type === "list"
							? t("queryExternal.listPlaceholder")
							: isNumber
								? t("queryExternal.numberPlaceholder")
								: t("queryExternal.stringPlaceholder");

					return (
						<Stack key={parameter.name} className={s.field} dir="column" gap={6} ai="stretch">
							<Input
								name={parameter.name}
								type={isNumber ? "number" : "text"}
								step={parameter.type === "float" ? "any" : undefined}
								onChange={(event) => {
									const rawValue = event.target.value;
									if (parameter.type === "list") {
										const listValue = rawValue.trim()
											? rawValue.split(",").map((item) => item.trim())
											: [];
										updateValue(parameter.name, listValue);
										return;
									}
									if (isNumber) {
										if (rawValue === "") {
											updateValue(parameter.name, null);
											return;
										}
										const numericValue = Number(rawValue);
										if (Number.isFinite(numericValue)) {
											updateValue(parameter.name, numericValue);
										}
										return;
									}
									updateValue(parameter.name, rawValue);
								}}
								value={inputValue}
								variant="highlighted"
								label={parameter.name}
								placeholder={placeholder}
								icon="Status"
								required={parameter.required}
							/>
							<Description parameter={parameter} />
						</Stack>
					);
				})}
			</Stack>
		);
	}
}

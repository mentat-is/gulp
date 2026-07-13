import { ChangeEvent, useCallback, useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { Application } from "@/context/Application.context";
import { GulpDataset } from "@/class/Info";
import { Glyph } from "@/entities/Glyph";
import { Badge } from "@/ui/Badge";
import { Banner as UIBanner } from "@/ui/Banner";
import { Button } from "@/ui/Button";
import { DeleteConfirmationBanner } from "@/ui/DeleteConfirmationBanner";
import { Dialog } from "@/ui/Dialog";
import { Icon } from "@/ui/Icon";
import { Input } from "@/ui/Input";
import { Label } from "@/ui/Label";
import { Stack } from "@/ui/Stack";
import { Switch } from "@/ui/Switch";
import { Textarea } from "@/ui/Textarea";
import { Locale } from "@/locales";
import detailStyles from "./styles/DisplayOperationDetailDialog.module.css";
import s from "./styles/PluginConfigurationDetailDialog.module.css";

type JsonValue = GulpDataset.SharedObject.JsonValue;
type JsonObject = GulpDataset.SharedObject.JsonObject;
type JsonPath = Array<string | number>;
type PluginConfigurationObject = GulpDataset.SharedObject.Type<JsonObject>;

export interface DisplayPluginConfigurationDetailDialogProps {
	/** Shared object identifier selected from the plugin configuration table. */
	objId: string;
	/** Name shown while the full shared object payload is loading. */
	fallbackName?: string;
	/** Callback function triggered when the dialog should close. */
	onClose: () => void;
	/** Callback function triggered after the configuration is updated. */
	onUpdated: () => void | Promise<void>;
}

/**
 * Checks whether a value is a non-array object compatible with JSON editing.
 *
 * @param value - Value read from the shared object payload.
 * @returns True when the value is a plain object.
 */
function isJsonObject(value: unknown): value is JsonObject {
	return (
		typeof value === "object" &&
		value !== null &&
		!Array.isArray(value) &&
		Object.prototype.toString.call(value) === "[object Object]"
	);
}

/**
 * Converts a value into a JSON object for the locked configuration editor.
 *
 * @param value - Raw shared object `obj` payload.
 * @returns The JSON object when valid, otherwise an empty object.
 */
function asJsonObject(value: unknown): JsonObject {
	return isJsonObject(value) ? value : {};
}

/**
 * Stringifies JSON values while keeping output stable for dirty-state checks.
 *
 * @param value - JSON value to stringify.
 * @returns A JSON string representation.
 */
function stringifyJsonValue(value: JsonValue): string {
	return JSON.stringify(value);
}

/**
 * Returns true when a string is large enough to benefit from a multiline editor.
 *
 * @param value - String value from a configuration field.
 * @returns True when a textarea should be used.
 */
function shouldUseTextarea(value: string): boolean {
	return value.length > 80 || value.includes("\n");
}

/**
 * Converts display or form tags into API-safe tag values.
 *
 * @param tagsText - Comma-separated tag text entered by the user.
 * @returns Trimmed tag values with empty entries removed.
 */
function parseTags(tagsText: string): string[] {
	return tagsText
		.split(",")
		.map((tag) => tag.trim())
		.filter((tag) => tag.length > 0);
}

/**
 * Formats primitive JSON values for read-only detail display.
 *
 * @param value - JSON value shown in the detail drawer.
 * @returns Compact display text.
 */
function formatJsonPreviewValue(value: JsonValue): string {
	if (value === null) {
		return "null";
	}

	if (typeof value === "string") {
		return value || "-";
	}

	return String(value);
}

/**
 * Replaces one JSON value at the provided fixed path without changing the JSON shape.
 *
 * @param root - Root JSON value to clone.
 * @param path - Existing path to the value being edited.
 * @param nextValue - Replacement primitive value.
 * @returns A cloned JSON value with the replacement applied.
 */
function replaceJsonValueAtPath(
	root: JsonValue,
	path: JsonPath,
	nextValue: JsonValue,
): JsonValue {
	if (path.length === 0) {
		return nextValue;
	}

	const [segment, ...rest] = path;

	if (Array.isArray(root)) {
		return root.map((entry, index) =>
			index === segment ? replaceJsonValueAtPath(entry, rest, nextValue) : entry,
		);
	}

	if (isJsonObject(root) && typeof segment === "string") {
		return Object.fromEntries(
			Object.entries(root).map(([key, entry]) => [
				key,
				key === segment
					? replaceJsonValueAtPath(entry, rest, nextValue)
					: entry,
			]),
		);
	}

	return root;
}

/**
 * Resolves a user-facing label for object keys and array indexes.
 *
 * @param segment - Object key or array index.
 * @returns A compact editor label.
 */
function getPathSegmentLabel(segment: string | number): string {
	return typeof segment === "number" ? `[${segment}]` : segment;
}

namespace LockedJsonEditor {
	export interface Props {
		/** Label shown for the current object key or array index. */
		label: string;
		/** JSON value rendered at this editor node. */
		value: JsonValue;
		/** Fixed path to this value in the configuration object. */
		path: JsonPath;
		/** Callback used to update a primitive value at a fixed path. */
		onValueChange: (path: JsonPath, value: JsonValue) => void;
	}
}

/**
 * Renders a recursive value-only JSON editor that preserves keys and array shape.
 *
 * @param props - Label, JSON value, path, and update callback for this node.
 * @returns A recursive editor section or primitive field.
 */
function LockedJsonEditor({
	label,
	value,
	path,
	onValueChange,
}: LockedJsonEditor.Props) {
	const { t } = Locale.use();

	if (Array.isArray(value)) {
		return (
			<div className={s.editorGroup}>
				<div className={s.editorGroupHeader}>{label}</div>
				<div className={s.editorChildren}>
					{value.map((entry, index) => (
						<LockedJsonEditor
							key={`${path.join(".")}.${index}`}
							label={getPathSegmentLabel(index)}
							value={entry}
							path={[...path, index]}
							onValueChange={onValueChange}
						/>
					))}
				</div>
			</div>
		);
	}

	if (isJsonObject(value)) {
		return (
			<div className={s.editorGroup}>
				<div className={s.editorGroupHeader}>{label}</div>
				<div className={s.editorChildren}>
					{Object.entries(value).map(([key, entry]) => (
						<LockedJsonEditor
							key={`${path.join(".")}.${key}`}
							label={key}
							value={entry}
							path={[...path, key]}
							onValueChange={onValueChange}
						/>
					))}
				</div>
			</div>
		);
	}

	if (typeof value === "string") {
		return (
			<div className={s.editorField}>
				<span className={s.editorLabel}>{label}</span>
				{shouldUseTextarea(value) ? (
					<Textarea
						className={s.editorTextarea}
						value={value}
						onChange={(event) => onValueChange(path, event.currentTarget.value)}
					/>
				) : (
					<Input
						value={value}
						onChange={(event) => onValueChange(path, event.currentTarget.value)}
					/>
				)}
			</div>
		);
	}

	if (typeof value === "number") {
		return (
			<div className={s.editorField}>
				<span className={s.editorLabel}>{label}</span>
				<Input
					type="number"
					value={String(value)}
					onChange={(event) => {
						const nextValue = Number(event.currentTarget.value);
						if (Number.isFinite(nextValue)) {
							onValueChange(path, nextValue);
						}
					}}
				/>
			</div>
		);
	}

	if (typeof value === "boolean") {
		return (
			<div className={s.editorField}>
				<span className={s.editorLabel}>{label}</span>
				<Switch
					checked={value}
					onCheckedChange={(checked) => onValueChange(path, checked)}
				/>
			</div>
		);
	}

	if (value === null) {
		return (
			<div className={s.editorField}>
				<span className={s.editorLabel}>{label}</span>
				<Input
					value="null"
					disabled
				/>
			</div>
		);
	}

	return (
		<div className={s.readOnlyValue}>
			<span className={s.editorLabel}>{label}</span>
			<span>{t("home.pluginConfigurations.readOnlyValue")}</span>
		</div>
	);
}

namespace ReadOnlyJsonValue {
	export interface Props {
		/** Label shown for the current configuration key or index. */
		label: string;
		/** JSON value to render without edit controls. */
		value: JsonValue;
	}
}

/**
 * Renders a recursive read-only JSON value tree for the detail drawer.
 *
 * @param props - Label and JSON value for the current display node.
 * @returns A read-only configuration display node.
 */
function ReadOnlyJsonValue({ label, value }: ReadOnlyJsonValue.Props) {
	if (Array.isArray(value)) {
		return (
			<div className={s.editorGroup}>
				<div className={s.editorGroupHeader}>{label}</div>
				<div className={s.editorChildren}>
					{value.map((entry, index) => (
						<ReadOnlyJsonValue
							key={`${label}.${index}`}
							label={getPathSegmentLabel(index)}
							value={entry}
						/>
					))}
				</div>
			</div>
		);
	}

	if (isJsonObject(value)) {
		return (
			<div className={s.editorGroup}>
				<div className={s.editorGroupHeader}>{label}</div>
				<div className={s.editorChildren}>
					{Object.entries(value).map(([key, entry]) => (
						<ReadOnlyJsonValue
							key={`${label}.${key}`}
							label={key}
							value={entry}
						/>
					))}
				</div>
			</div>
		);
	}

	return (
		<div className={s.previewField}>
			<span className={s.editorLabel}>{label}</span>
			<span className={s.previewValue}>{formatJsonPreviewValue(value)}</span>
		</div>
	);
}

/**
 * Renders the root configuration object without adding a duplicate title.
 *
 * @param props - Root configuration object to display.
 * @returns A list of read-only configuration fields.
 */
function ReadOnlyConfiguration({ value }: { value: JsonObject }) {
	return (
		<div className={s.editorChildrenRoot}>
			{Object.entries(value).map(([key, entry]) => (
				<ReadOnlyJsonValue
					key={key}
					label={key}
					value={entry}
				/>
			))}
		</div>
	);
}

namespace PluginConfigurationMetadataBanner {
	export interface Props extends UIBanner.Props {
		/** Shared object payload edited by the banner. */
		configuration: PluginConfigurationObject;
		/** Callback function triggered after metadata is saved. */
		onSaved: () => void | Promise<void>;
	}
}

/**
 * Banner used to edit shared-object metadata fields.
 *
 * @param props - Shared object payload, save callback, and banner props.
 * @returns A banner with name, icon, description, and tags controls.
 */
function PluginConfigurationMetadataBanner({
	configuration,
	onSaved,
	...props
}: PluginConfigurationMetadataBanner.Props) {
	const { Info, destroyBanner } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState<boolean>(false);
	const [name, setName] = useState<string>(configuration.name ?? "");
	const [description, setDescription] = useState<string>(
		configuration.description ?? "",
	);
	const [tagsText, setTagsText] = useState<string>(
		(configuration.tags ?? []).join(", "),
	);
	const [icon, setIcon] = useState<Glyph.Id | null>(
		(configuration.glyph_id as Glyph.Id | undefined) ??
			Glyph.getIdByName("AcronymJson"),
	);

	/**
	 * Stores the shared-object name field.
	 *
	 * @param event - Name input change event.
	 */
	const handleNameChange = (event: ChangeEvent<HTMLInputElement>) => {
		setName(event.currentTarget.value);
	};

	/**
	 * Stores the shared-object description field.
	 *
	 * @param event - Description textarea change event.
	 */
	const handleDescriptionChange = (
		event: ChangeEvent<HTMLTextAreaElement>,
	) => {
		setDescription(event.currentTarget.value);
	};

	/**
	 * Stores the comma-separated tags field.
	 *
	 * @param event - Tags input change event.
	 */
	const handleTagsChange = (event: ChangeEvent<HTMLInputElement>) => {
		setTagsText(event.currentTarget.value);
	};

	/**
	 * Persists metadata fields through the shared object update API.
	 *
	 * @returns A promise that settles after the update callback completes.
	 */
	const handleSave = async () => {
		setLoading(true);
		try {
			const updated = await Info.shared_object_update(
				configuration.obj_id ?? configuration.id ?? "",
				{
					name: name.trim(),
					glyph_id: icon,
					description,
					tags: parseTags(tagsText),
				},
			);
			if (updated) {
				toast.success(t("home.pluginConfigurations.updated"), {
					icon: <Icon name="Check" />,
					richColors: true,
				});
				await onSaved();
				destroyBanner();
			} else {
				toast.error(t("home.pluginConfigurations.updateFailed"), {
					icon: <Icon name="Stop" />,
					richColors: true,
				});
			}
		} finally {
			setLoading(false);
		}
	};

	const Done = () => (
		<Button
			variant="glass"
			icon="Check"
			loading={loading}
			disabled={name.trim().length === 0 || !icon}
			onClick={handleSave}
		/>
	);

	return (
		<UIBanner
			title={`${t("common.edit")} ${configuration.name}`}
			done={<Done />}
			{...props}
		>
			<Input
				icon="TextTitle"
				placeholder={t("common.name")}
				variant="highlighted"
				value={name}
				onChange={handleNameChange}
			/>
			<Glyph.Chooser
				icon={icon}
				setIcon={setIcon}
				label={t("operationEdit.icon")}
			/>
			<Stack
				dir="column"
				gap={6}
				ai="flex-start"
				data-input
				className={s.bannerField}
			>
				<Label
					htmlFor="plugin-configuration-description"
					value={t("common.description")}
				/>
				<Textarea
					id="plugin-configuration-description"
					className={s.bannerTextarea}
					value={description}
					onChange={handleDescriptionChange}
					placeholder={t("common.description")}
				/>
			</Stack>
			<Input
				icon="Tags"
				placeholder={t("common.tags")}
				variant="highlighted"
				value={tagsText}
				onChange={handleTagsChange}
			/>
		</UIBanner>
	);
}

namespace PluginConfigurationObjectBanner {
	export interface Props extends UIBanner.Props {
		/** Shared object payload edited by the banner. */
		configuration: PluginConfigurationObject;
		/** Callback function triggered after JSON configuration is saved. */
		onSaved: () => void | Promise<void>;
	}
}

/**
 * Banner used to edit the value-only JSON configuration object.
 *
 * @param props - Shared object payload, save callback, and banner props.
 * @returns A banner containing the locked recursive JSON editor.
 */
function PluginConfigurationObjectBanner({
	configuration,
	onSaved,
	...props
}: PluginConfigurationObjectBanner.Props) {
	const { Info, destroyBanner } = Application.use();
	const { t } = Locale.use();
	const initialConfiguration = asJsonObject(configuration.obj);
	const [loading, setLoading] = useState<boolean>(false);
	const [editedConfiguration, setEditedConfiguration] =
		useState<JsonObject>(initialConfiguration);

	const hasChanges = useMemo(
		() =>
			stringifyJsonValue(editedConfiguration) !==
			stringifyJsonValue(initialConfiguration),
		[editedConfiguration, initialConfiguration],
	);

	/**
	 * Updates one primitive value inside the locked JSON object.
	 *
	 * @param path - Existing path to the value being edited.
	 * @param value - Replacement JSON value.
	 */
	const handleValueChange = useCallback((path: JsonPath, value: JsonValue) => {
		setEditedConfiguration((current) =>
			replaceJsonValueAtPath(current, path, value) as JsonObject,
		);
	}, []);

	/**
	 * Persists the edited configuration object through the shared object update API.
	 *
	 * @returns A promise that settles after the update callback completes.
	 */
	const handleSave = async () => {
		setLoading(true);
		try {
			const updated = await Info.shared_object_update(
				configuration.obj_id ?? configuration.id ?? "",
				{ obj: editedConfiguration },
			);
			if (updated) {
				toast.success(t("home.pluginConfigurations.updated"), {
					icon: <Icon name="Check" />,
					richColors: true,
				});
				await onSaved();
				destroyBanner();
			} else {
				toast.error(t("home.pluginConfigurations.updateFailed"), {
					icon: <Icon name="Stop" />,
					richColors: true,
				});
			}
		} finally {
			setLoading(false);
		}
	};

	const Done = () => (
		<Button
			variant="glass"
			icon="Check"
			loading={loading}
			disabled={!hasChanges}
			onClick={handleSave}
		/>
	);

	return (
		<UIBanner
			title={`${t("common.edit")} ${t("common.configuration")}`}
			done={<Done />}
			{...props}
		>
			<div className={s.bannerEditor}>
				<LockedJsonEditor
					label={t("common.configuration")}
					value={editedConfiguration}
					path={[]}
					onValueChange={handleValueChange}
				/>
			</div>
		</UIBanner>
	);
}

/**
 * Dialog component displaying one plugin configuration shared object.
 *
 * @param props - Shared object identifier, fallback display data, close callback, and update callback.
 * @returns A docked detail dialog with read-only metadata and configuration sections.
 */
export function DisplayPluginConfigurationDetailDialog({
	objId,
	fallbackName,
	onClose,
	onUpdated,
}: DisplayPluginConfigurationDetailDialogProps) {
	const { Info, spawnBanner } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState<boolean>(true);
	const [details, setDetails] = useState<PluginConfigurationObject | null>(null);

	/**
	 * Loads the latest plugin configuration payload from the backend.
	 *
	 * @returns A promise that settles after the detail request completes.
	 */
	const loadDetails = useCallback(async () => {
		setLoading(true);
		try {
			const response = await Info.shared_object_get_by_id<JsonObject>(objId);
			setDetails(response);
		} finally {
			setLoading(false);
		}
	}, [Info, objId]);

	useEffect(() => {
		loadDetails();
	}, [loadDetails]);

	/**
	 * Refreshes this drawer and the backing table after a banner save.
	 *
	 * @returns A promise that settles after both refreshes complete.
	 */
	const refreshAfterSave = useCallback(async () => {
		await loadDetails();
		await onUpdated();
	}, [loadDetails, onUpdated]);

	const displayName = details?.name ?? fallbackName ?? objId;
	const configuration = asJsonObject(details?.obj);
	const tags = details?.tags ?? [];
	const iconName = (
		details?.glyph_id ? Glyph.List.get(details.glyph_id as Glyph.Id) : null
	) ?? "AcronymJson";

	/**
	 * Opens the shared-object metadata edit banner.
	 */
	const handleEditMetadata = () => {
		if (!details) return;
		spawnBanner(
			<PluginConfigurationMetadataBanner
				configuration={details}
				onSaved={refreshAfterSave}
			/>,
		);
	};

	/**
	 * Opens the JSON configuration edit banner.
	 */
	const handleEditConfiguration = () => {
		if (!details) return;
		spawnBanner(
			<PluginConfigurationObjectBanner
				configuration={details}
				onSaved={refreshAfterSave}
			/>,
		);
	};

	/**
	 * Deletes the displayed plugin configuration and refreshes the backing table.
	 *
	 * @returns A promise resolving to false when deletion fails.
	 */
	const handleDelete = async (): Promise<boolean | void> => {
		const deleted = await Info.shared_object_delete(objId);
		if (deleted) {
			toast.success(t("home.pluginConfigurations.deleted", { objId }), {
				icon: <Icon name="Check" />,
				richColors: true,
			});
			await onUpdated();
			onClose();
			return;
		}

		toast.error(t("home.pluginConfigurations.deleteFailed", { objId }), {
			icon: <Icon name="Stop" />,
			richColors: true,
		});
		return false;
	};

	/**
	 * Opens the shared confirmation banner for deleting the displayed plugin configuration.
	 */
	const openDeleteBanner = () => {
		spawnBanner(
			<DeleteConfirmationBanner onConfirm={handleDelete}>
				<p>
					{t("common.delete")}: <code>{displayName}</code>
				</p>
			</DeleteConfirmationBanner>,
		);
	};

	return (
		<Dialog
			callback={onClose}
			loading={loading}
		>
			<div className={detailStyles.header}>
				<div className={detailStyles.titleContainer}>
					<div className={detailStyles.iconWrapper}>
						<Icon
							name={iconName}
							size={18}
						/>
					</div>
					<h2 className={detailStyles.title}>{displayName}</h2>
				</div>
				<div className={detailStyles.buttonGroup}>
					<Button
						variant="secondary"
						icon="PencilEdit"
						title={t("common.edit")}
						onClick={handleEditMetadata}
					/>
					<Button
						variant="secondary"
						icon="X"
						title={t("common.closeDialog")}
						onClick={onClose}
					/>
				</div>
			</div>

			<Stack
				dir="column"
				gap={20}
				className={detailStyles.scrollable}
			>
				<div className={detailStyles.section}>
					<div className={detailStyles.detailsList}>
						<div className={detailStyles.detailItem}>
							<span className={detailStyles.detailLabel}>{t("common.id")}:</span>
							<span className={detailStyles.detailValue}>{objId}</span>
						</div>
						<div className={detailStyles.detailItem}>
							<span className={detailStyles.detailLabel}>
								{t("common.name")}:
							</span>
							<span className={detailStyles.detailValue}>{displayName}</span>
						</div>
						<div className={detailStyles.detailItem}>
							<span className={detailStyles.detailLabel}>
								{t("common.description")}:
							</span>
							<span className={detailStyles.detailValue}>
								{details?.description || "-"}
							</span>
						</div>
						<div className={detailStyles.detailItem}>
							<span className={detailStyles.detailLabel}>
								{t("common.icon")}:
							</span>
							<span className={detailStyles.detailValue}>{iconName}</span>
						</div>
						<div className={detailStyles.detailItemColumn}>
							<span className={detailStyles.detailLabelBlock}>
								{t("common.tags")}
							</span>
							{tags.length > 0 ? (
								<div className={detailStyles.badgeList}>
									{tags.map((tag) => (
										<Badge
											key={tag}
											variant="blue-subtle"
											size="sm"
										>
											{tag}
										</Badge>
									))}
								</div>
							) : (
								<span className={detailStyles.noData}>-</span>
							)}
						</div>
					</div>
				</div>

				<div className={detailStyles.section}>
					<Stack
						ai="center"
						jc="space-between"
						className={detailStyles.sectionTitle}
					>
						<span>{t("common.configuration")}</span>
						<Button
							icon="PenLine"
							variant="secondary"
							onClick={handleEditConfiguration}
						/>
					</Stack>
					<ReadOnlyConfiguration value={configuration} />
				</div>
			</Stack>
			<Stack
				jc="flex-end"
				noShrink
				style={{ marginTop: "auto" }}
			>
				<Button
					variant="tertiary"
					icon="Trash2"
					style={{ color: "var(--red-800)" }}
					onClick={openDeleteBanner}
				>
					{t("common.delete")}
				</Button>
			</Stack>
		</Dialog>
	);
}

import { useCallback, useEffect, useMemo, useState } from "react";
import { Application } from "@/context/Application.context";
import { Permissions } from "@/banners/Permissions.banner";
import { Group } from "@/entities/Group";
import { Glyph } from "@/entities/Glyph";
import { Badge } from "@/ui/Badge";
import { Button } from "@/ui/Button";
import { DeleteConfirmationBanner } from "@/ui/DeleteConfirmationBanner";
import { Dialog } from "@/ui/Dialog";
import { Icon } from "@/ui/Icon";
import { Stack } from "@/ui/Stack";
import { formatTimestampToReadableString } from "@/ui/utils";
import { Locale } from "@/locales";
import { toast } from "sonner";
import s from "./styles/DisplayOperationDetailDialog.module.css";

type GroupRecord = Group.Type & Record<string, unknown>;

interface DetailRow {
	label: string;
	value: string;
}

export interface DisplayGroupDetailDialogProps {
	/** Initial group row selected from the groups table. */
	group: Group.Type;
	/** Callback function triggered when the dialog should close. */
	onClose: () => void;
}

const HiddenDetailFields = new Set([
	"type",
	"user",
	"users",
	"granted_user_ids",
	"granted_user_group_ids",
]);

const PreferredDetailFields = [
	"permission",
	"id",
	"user_id",
	"name",
	"time_created",
	"time_updated",
	"description",
	"tags",
];

/**
 * Resolves a stable string identifier from a group payload.
 *
 * @param group - Group entity returned by the list or detail API.
 * @returns The group ID, or an empty string when unavailable.
 */
function getGroupId(group: Group.Type): string {
	return typeof group.id === "string" ? group.id : "";
}

/**
 * Formats timestamp-like API values for display.
 *
 * @param value - Timestamp value returned by the API.
 * @returns A readable timestamp string, or a fallback dash.
 */
function formatTimestampValue(value: unknown): string {
	if (typeof value !== "number" || Number.isNaN(value)) {
		return "-";
	}

	const isSeconds = String(value).length <= 10;
	const milliseconds = isSeconds ? value * 1000 : value;
	return formatTimestampToReadableString(milliseconds);
}

/**
 * Converts API values into compact detail-row text.
 *
 * @param value - Raw field value returned by the API.
 * @returns A string representation suitable for a detail row.
 */
function formatDetailValue(value: unknown): string {
	if (value === null || value === undefined || value === "") {
		return "-";
	}

	if (typeof value === "bigint") {
		return value.toString();
	}

	if (Array.isArray(value)) {
		return value.length > 0
			? value
					.map((entry) =>
						typeof entry === "string" ? entry : JSON.stringify(entry),
					)
					.join(", ")
			: "-";
	}

	if (typeof value === "object") {
		return JSON.stringify(value, (_, entry) =>
			typeof entry === "bigint" ? entry.toString() : entry,
		);
	}

	return String(value);
}

/**
 * Resolves a localized label for a group detail field.
 *
 * @param field - Raw API field name.
 * @param t - Locale translation function.
 * @returns A localized label for known group fields.
 */
function getDetailFieldLabel(
	field: string,
	t: ReturnType<typeof Locale.use>["t"],
): string {
	const labels: Record<string, string> = {
		permission: t("common.permissions"),
		id: t("common.id"),
		user_id: t("common.owner"),
		name: t("common.name"),
		time_created: t("common.created"),
		time_updated: t("common.updated"),
		description: t("common.description"),
		tags: t("common.tags"),
		glyph_id: t("common.icon"),
	};

	return labels[field] ?? field;
}

/**
 * Builds ordered, localized detail rows for the group dialog.
 *
 * @param group - Detail payload returned by the group API.
 * @param t - Locale translation function.
 * @returns Localized label/value rows for visible group fields.
 */
function buildGroupDetailRows(
	group: GroupRecord,
	t: ReturnType<typeof Locale.use>["t"],
): DetailRow[] {
	const orderedFields = [
		...PreferredDetailFields,
		...Object.keys(group).filter(
			(field) => !PreferredDetailFields.includes(field),
		),
	];
	const visibleFields = orderedFields.filter(
		(field, index) =>
			orderedFields.indexOf(field) === index && !HiddenDetailFields.has(field),
	);

	return visibleFields.map((field) => ({
		label: getDetailFieldLabel(field, t),
		value: field.toLowerCase().includes("time")
			? formatTimestampValue(group[field])
			: formatDetailValue(group[field]),
	}));
}

/**
 * Dialog component displaying group details and group user membership controls.
 *
 * @param props - Group payload and close callback.
 * @returns A docked dialog with localized group details.
 */
export function DisplayGroupDetailDialog({
	group,
	onClose,
}: DisplayGroupDetailDialogProps) {
	const { spawnBanner } = Application.use();
	const { t } = Locale.use();
	const groupId = getGroupId(group);
	const [loading, setLoading] = useState<boolean>(true);
	const [details, setDetails] = useState<GroupRecord>(group as GroupRecord);

	/**
	 * Loads the latest group payload and keeps fallback data visible on failure.
	 *
	 * @returns A promise that settles after the detail request completes.
	 */
	const loadDetails = useCallback(async () => {
		if (!groupId) {
			setLoading(false);
			return;
		}

		setLoading(true);
		try {
			const response = await Permissions.Users.Groups.getById(groupId);
			if (response) {
				setDetails(response as GroupRecord);
			}
		} finally {
			setLoading(false);
		}
	}, [groupId]);

	useEffect(() => {
		loadDetails();
	}, [loadDetails]);

	const userIds = Permissions.Users.Groups.getUserIds(details);
	const detailRows = useMemo(
		() => buildGroupDetailRows(details, t),
		[details, t],
	);
	const displayName = details.name ?? group.name ?? groupId;
	const glyphName = (
		(details.glyph_id ? Glyph.List.get(details.glyph_id as Glyph.Id) : null) ??
		"Users"
	) as Icon.Name;

	/**
	 * Opens the group edit banner and reloads details after a successful update.
	 */
	const handleEdit = () => {
		spawnBanner(
			<Permissions.Users.Groups.FormBanner
				group={details}
				onClose={loadDetails}
			/>,
		);
	};

	/**
	 * Opens the group membership manager and reloads details when it closes.
	 */
	const handleEditUsers = () => {
		spawnBanner(
			<Permissions.Users.Groups.ManageUsersBanner
				groupId={groupId}
				userIds={userIds}
				onClose={loadDetails}
			/>,
		);
	};

	/**
	 * Deletes the displayed group, notifies Home listeners, and closes the dialog.
	 *
	 * @returns A promise resolving to false when deletion fails.
	 */
	const handleDelete = async (): Promise<boolean | void> => {
		const deleted = await Permissions.Users.Groups.deleteById(groupId);
		if (deleted) {
			toast.success(t("home.groups.deleted", { groupId }), {
				icon: <Icon name="Check" />,
				richColors: true,
			});
			Permissions.notifyGroupListChanged();
			onClose();
			return;
		}

		return false;
	};

	/**
	 * Opens the shared confirmation banner for deleting the displayed group.
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
			<div className={s.header}>
				<div className={s.titleContainer}>
					<div className={s.iconWrapper}>
						<Icon
							name={glyphName}
							size={18}
						/>
					</div>
					<h2 className={s.title}>{displayName}</h2>
				</div>
				<div className={s.buttonGroup}>
					<Button
						variant="secondary"
						icon="PencilEdit"
						title={t("common.edit")}
						onClick={handleEdit}
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
				className={s.scrollable}
			>
				<div className={s.section}>
					<div className={s.detailsList}>
						{detailRows.map((row) => (
							<div
								key={row.label}
								className={s.detailItem}
							>
								<span className={s.detailLabel}>{row.label}:</span>
								<span className={s.detailValue}>{row.value}</span>
							</div>
						))}
					</div>
				</div>

				<div className={s.section}>
					<Stack
						ai="center"
						jc="space-between"
						className={s.sectionTitle}
					>
						<span>{t("common.users")}</span>
						<Button
							icon="PenLine"
							variant="secondary"
							onClick={handleEditUsers}
						/>
					</Stack>
					<div className={s.detailsList}>
						<div className={s.detailItemColumn}>
							{userIds.length > 0 ? (
								<div className={s.badgeList}>
									{userIds.map((userId) => (
										<Badge
											key={userId}
											variant="gray-subtle"
											size="sm"
										>
											{userId}
										</Badge>
									))}
								</div>
							) : (
								<span className={s.noData}>{t("operationDetails.noUsers")}</span>
							)}
						</div>
					</div>
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

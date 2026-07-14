import { useCallback, useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { Application } from "@/context/Application.context";
import { Permissions } from "@/banners/Permissions.banner";
import { Glyph } from "@/entities/Glyph";
import { Group } from "@/entities/Group";
import { User } from "@/entities/User";
import { Badge } from "@/ui/Badge";
import { Button } from "@/ui/Button";
import { DeleteConfirmationBanner } from "@/ui/DeleteConfirmationBanner";
import { Dialog } from "@/ui/Dialog";
import { Icon } from "@/ui/Icon";
import { Stack } from "@/ui/Stack";
import { formatTimestampToReadableString } from "@/ui/utils";
import { Locale } from "@/locales";
import s from "./styles/DisplayOperationDetailDialog.module.css";

type UserRecord = User.Type & Record<string, unknown>;

interface DetailRow {
	label: string;
	value: string;
}

export interface DisplayUserDetailDialogProps {
	/** Initial user row selected from the users table. */
	user: User.Type;
	/** Callback function triggered when the dialog should close. */
	onClose: () => void;
}

const HiddenDetailFields = new Set([
	"token",
	"password",
	"pwd_hash",
	"psw_hash",
	"user_data",
	"type",
	"groups",
]);

const PreferredDetailFields = [
	"permission",
	"id",
	"name",
	"time_last_login",
	"time_expire",
	"glyph_id",
];

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
 * Resolves display labels for user detail fields.
 *
 * @param field - Raw API field name.
 * @param t - Locale translation function.
 * @returns A localized label for known user fields.
 */
function getDetailFieldLabel(
	field: string,
	t: ReturnType<typeof Locale.use>["t"],
): string {
	const labels: Record<string, string> = {
		permission: t("common.permissions"),
		id: t("common.id"),
		name: t("common.name"),
		time_last_login: t("userDetails.lastLogin"),
		time_expire: t("userDetails.expires"),
		glyph_id: t("common.icon"),
	};

	return labels[field] ?? field;
}

/**
 * Builds ordered, localized detail rows for the user dialog.
 *
 * @param user - Detail payload returned by the user API.
 * @param t - Locale translation function.
 * @returns Localized label/value rows for visible user fields.
 */
function buildUserDetailRows(
	user: UserRecord,
	t: ReturnType<typeof Locale.use>["t"],
): DetailRow[] {
	const orderedFields = [
		...PreferredDetailFields,
		...Object.keys(user).filter((field) => !PreferredDetailFields.includes(field)),
	];
	const visibleFields = orderedFields.filter(
		(field, index) =>
			orderedFields.indexOf(field) === index && !HiddenDetailFields.has(field),
	);

	return visibleFields.map((field) => ({
		label: getDetailFieldLabel(field, t),
		value: field.toLowerCase().includes("time")
			? formatTimestampValue(user[field])
			: formatDetailValue(user[field]),
	}));
}

/**
 * Resolves group display names from the user detail payload.
 *
 * @param groups - Groups array returned by the user API.
 * @returns A list of group names or IDs.
 */
function getGroupLabels(groups: User.Type["groups"] | undefined): string[] {
	if (!groups) {
		return [];
	}

	return groups
		.map((group: Group.Type) => {
			const name = typeof group.name === "string" ? group.name : "";
			const id = typeof group.id === "string" ? group.id : "";
			return name || id;
		})
		.filter((label) => label.length > 0);
}

/**
 * Dialog component displaying user details and group membership.
 *
 * @param props - User payload and close callback.
 * @returns A docked dialog with localized user details.
 */
export function DisplayUserDetailDialog({
	user,
	onClose,
}: DisplayUserDetailDialogProps) {
	const { Info, app, spawnBanner } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState<boolean>(true);
	const [details, setDetails] = useState<UserRecord>(user as UserRecord);

	/**
	 * Loads the latest user payload and keeps fallback data visible on failure.
	 *
	 * @returns A promise that settles after the detail request completes.
	 */
	const loadDetails = useCallback(async () => {
		setLoading(true);
		try {
			const response = await Info.user_get_by_id(user.id);
			if (response) {
				setDetails(response as UserRecord);
			}
		} finally {
			setLoading(false);
		}
	}, [Info, user.id]);

	useEffect(() => {
		loadDetails();
	}, [loadDetails]);

	useEffect(() => {
		/**
		 * Refreshes the open user dialog when the edit banner saves changes.
		 */
		const handleUserListChanged = () => {
			loadDetails();
		};

		window.addEventListener(
			Permissions.UserListChangedEvent,
			handleUserListChanged,
		);
		return () => {
			window.removeEventListener(
				Permissions.UserListChangedEvent,
				handleUserListChanged,
			);
		};
	}, [loadDetails]);

	const detailRows = useMemo(
		() => buildUserDetailRows(details, t),
		[details, t],
	);
	const groupLabels = getGroupLabels(details.groups);
	const displayName = details.name || details.id || user.id;
	const glyphName = (
		(details.glyph_id ? Glyph.List.get(details.glyph_id as Glyph.Id) : null) ??
		"User"
	) as Icon.Name;

	/**
	 * Opens the user edit banner.
	 */
	const handleEdit = () => {
		spawnBanner(<Permissions.Users.Edit.Banner user={details} />);
	};

	/**
	 * Deletes the displayed user, notifies Home listeners, and closes the dialog.
	 *
	 * @returns A promise resolving to false when the current user cannot be deleted.
	 */
	const handleDelete = async (): Promise<boolean | void> => {
		const userId = details.id;
		if (userId === app.general.user?.id) {
			toast.error(t("home.users.cannotDeleteCurrentUser"), {
				icon: <Icon name="Stop" />,
				richColors: true,
			});
			return false;
		}

		const deleteResult = await Info.user_delete(userId);
		if (deleteResult.success) {
			toast.success(t("home.users.deleted", { userId }), {
				icon: <Icon name="Check" />,
				richColors: true,
			});
			Permissions.notifyUserListChanged();
			onClose();
			return;
		}

		return false;
	};

	/**
	 * Opens the shared confirmation banner for deleting the displayed user.
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
					<div className={s.sectionTitle}>{t("common.groups")}</div>
					<div className={s.detailsList}>
						<div className={s.detailItemColumn}>
							{groupLabels.length > 0 ? (
								<div className={s.badgeList}>
									{groupLabels.map((groupLabel) => (
										<Badge
											key={groupLabel}
											variant="purple-subtle"
											size="sm"
										>
											{groupLabel}
										</Badge>
									))}
								</div>
							) : (
								<span className={s.noData}>{t("operationDetails.noGroups")}</span>
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

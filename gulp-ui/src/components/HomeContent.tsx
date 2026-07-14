import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { toast } from "sonner";
import { Icon } from "@/ui/Icon";
import { Application } from "@/context/Application.context";
import { DisplayGroupDetailDialog } from "@/dialogs/GroupsDetail.dialog";
import { DisplayOperationDetailDialog } from "@/dialogs/OperationDetail.dialog";
import { DisplayPluginConfigurationDetailDialog } from "@/dialogs/PluginConfigurationDetail.dialog";
import { DisplayUserDetailDialog } from "@/dialogs/UserDetail.dialog";
import { GulpDataset } from "@/class/Info";
import { Operation } from "@/entities/Operation";
import { User } from "@/entities/User";
import { Group } from "@/entities/Group";
import { Glyph } from "@/entities/Glyph";
import { Permissions } from "@/banners/Permissions.banner";
import { Table } from "@/components/Table";
import { Button } from "@/ui/Button";
import { DeleteConfirmationBanner } from "@/ui/DeleteConfirmationBanner";
import { Stack } from "@/ui/Stack";
import { Locale } from "@/locales";
import s from "./styles/HomeContent.module.css";

type EntityRecord = Record<string, unknown> & { id?: string; name?: string };
type PluginConfigurationObject =
	GulpDataset.SharedObject.Type<GulpDataset.SharedObject.JsonObject>;
type PluginConfigurationRow = PluginConfigurationObject & {
	configurationPreview: string;
};

const DeleteButtonStyle = { color: "var(--red-800)" };

interface ApiListResult<T> {
	/** Items accepted from the API response. */
	items: T[];
	/** Whether the API response could not be used as a list. */
	failed: boolean;
}

export namespace HomeContent {
	export type Section =
		| "operations"
		| "users"
		| "groups"
		| "pluginConfigurations";

	export interface OperationsListProps {
		/** Whether the application is still loading the initial operation list. */
		loading: boolean;
	}
}

/**
 * Resolves a stable string identifier from a table row or entity object.
 *
 * @param entity - Entity-like object returned from the API or Table component.
 * @returns The entity ID as a string, or an empty string when not available.
 */
function getEntityId(entity: EntityRecord): string {
	return typeof entity.id === "string" ? entity.id : "";
}

/**
 * Resolves the backend identifier used by shared object APIs.
 *
 * @param sharedObject - Shared object returned by the list or detail API.
 * @returns The object identifier, preferring obj_id over id.
 */
function getSharedObjectId(sharedObject: {
	obj_id?: string;
	id?: string;
}): string {
	if (typeof sharedObject.obj_id === "string" && sharedObject.obj_id.length > 0) {
		return sharedObject.obj_id;
	}

	return typeof sharedObject.id === "string" ? sharedObject.id : "";
}

/**
 * Converts any API field value into a compact string suitable for detail rows.
 *
 * @param value - Raw field value from an API entity.
 * @returns A display-safe string representation.
 */
function formatDetailValue(value: unknown): string {
	if (value === null || value === undefined || value === "") {
		return "-";
	}

	if (typeof value === "bigint") {
		return value.toString();
	}

	if (Array.isArray(value) || typeof value === "object") {
		return JSON.stringify(value, (_, entry) =>
			typeof entry === "bigint" ? entry.toString() : entry,
		);
	}

	return String(value);
}

/**
 * Extracts a readable backend error message from an API error payload.
 *
 * @param response - Raw API response returned to a Home list loader.
 * @returns Backend error name/message text when the response exposes it.
 */
function getApiErrorDescription(response: unknown): string | undefined {
	if (typeof response !== "object" || response === null) {
		return undefined;
	}

	const error = (response as { __error?: { name?: unknown; msg?: unknown } })
		.__error;
	if (typeof error !== "object" || error === null) {
		return undefined;
	}

	const parts = [error.name, error.msg].filter(
		(part): part is string => typeof part === "string" && part.length > 0,
	);
	return parts.length > 0 ? parts.join(": ") : undefined;
}

/**
 * Normalizes a Home list API response before storing it in component state.
 *
 * @param response - Raw API response that should contain a list.
 * @param errorMessage - Localized message shown when the response is not a list.
 * @returns A typed list result and a failure marker for selection cleanup.
 */
function normalizeApiListResponse<T>(
	response: unknown,
	errorMessage: string,
): ApiListResult<T> {
	if (Array.isArray(response)) {
		return { items: response, failed: false };
	}

	toast.error(errorMessage, {
		description: getApiErrorDescription(response),
		icon: <Icon name="Stop" />,
		richColors: true,
	});

	return { items: [], failed: true };
}

interface EntityNameCellProps {
	/** Icon displayed before the entity name. */
	iconName: Icon.Name;
	/** Raw name value rendered for the entity. */
	name: unknown;
}

/**
 * Renders a table name cell with its entity icon directly before the label.
 *
 * @param props - Icon name and raw entity name value.
 * @returns A compact, left-aligned name cell for Home tables.
 */
function EntityNameCell({ iconName, name }: EntityNameCellProps) {
	const displayName = formatDetailValue(name);

	return (
		<div className={s.entityNameCell}>
			<div className={s.entityIcon}>
				<Icon name={iconName} />
			</div>
			<span
				className={s.entityNameText}
				title={displayName}
			>
				{displayName}
			</span>
		</div>
	);
}

/**
 * Resolves the icon used for a user table row.
 *
 * @param user - User row rendered by the table.
 * @returns The configured user glyph name, or the User icon fallback.
 */
function getUserIconName(user: User.Type): Icon.Name {
	return Glyph.List.get(user.glyph_id) ?? "User";
}

/**
 * Resolves the icon used for a group table row.
 *
 * @param group - Group row rendered by the table.
 * @returns The configured group glyph name, or the Users icon fallback.
 */
function getGroupIconName(group: Group.Type): Icon.Name {
	return (
		(group.glyph_id ? Glyph.List.get(group.glyph_id as Glyph.Id) : null) ??
		"Users"
	);
}

/**
 * Resolves the icon used for a plugin configuration shared object.
 *
 * @param configuration - Plugin configuration row rendered by the table.
 * @returns The shared object's glyph name, or the JSON icon fallback.
 */
function getPluginConfigurationIconName(
	configuration: PluginConfigurationObject,
): Icon.Name {
	return (
		configuration.glyph_id
			? Glyph.List.get(configuration.glyph_id as Glyph.Id)
			: null
	) ?? "AcronymJson";
}

/**
 * Renders the shared loading or empty state used by all Home list sections.
 *
 * @param text - Message shown in the center of the result panel.
 * @returns A result container with centered state text.
 */
function ResultState({ text }: { text: string }) {
	return (
		<div className={s.result}>
			<div className={s.resultScroll}>
				<div className={s.emptyState}>{text}</div>
			</div>
		</div>
	);
}

/**
 * Renders the header create action for the active Home section.
 *
 * @param props - Active section controlling which create action is displayed.
 * @returns A section-specific create button, or null when the section has none.
 */
export function HeaderAction({ section }: { section: HomeContent.Section }) {
	const { spawnBanner } = Application.use();
	const { t } = Locale.use();

	if (section === "users") {
		return (
			<Button
				variant="glass"
				icon="UserPlus"
				onClick={() => spawnBanner(<Permissions.Users.Create.Banner />)}
			>
				{t("permissions.createUser")}
			</Button>
		);
	}

	if (section === "groups") {
		return (
			<Button
				variant="glass"
				icon="Users"
				onClick={() => spawnBanner(<Permissions.Users.Groups.FormBanner />)}
			>
				{t("permissions.createGroup")}
			</Button>
		);
	}

	if (section === "operations") {
		return (
			<Button
				variant="glass"
				icon="Plus"
				onClick={() => spawnBanner(<Operation.CreateOrUpdate.Banner />)}
			>
				{t("common.createNew")}
			</Button>
		);
	}

	return null;
}

/**
 * Renders the Home content back action that returns routed sub-pages to the
 * root Home content.
 *
 * @returns A localized back button that navigates to the Home route.
 */
export function BackButton() {
	const { spawnDialog } = Application.use();
	const { t } = Locale.use();
	const navigate = useNavigate();

	/**
	 * Closes any open Home detail dialog and redirects the user to root Home content.
	 */
	const handleBackToHome = useCallback(() => {
		spawnDialog(null);
		navigate("/");
	}, [navigate, spawnDialog]);

	return (
		<Button
			variant="tertiary"
			icon="ArrowLeft"
			title={t("common.back")}
			onClick={handleBackToHome}
		>
			{t("common.back")}
		</Button>
	);
}

/**
 * Renders the operation list previously owned by Home.page.tsx.
 *
 * @param props - Loading state supplied by the Home page initializer.
 * @returns The operation table, loading state, or empty state.
 */
export function OperationsList({ loading }: HomeContent.OperationsListProps) {
	const { Info, app, spawnBanner, spawnDialog } = Application.use();
	const { t } = Locale.use();
	const navigate = useNavigate();
	const [selectedIds, setSelectedIds] = useState<Set<Operation.Id>>(new Set());

	/**
	 * Selects an operation in global state and navigates to the operation view.
	 *
	 * @param operation - The operation to open.
	 */
	const handleOpenOperation = useCallback(
		(operation: Operation.Type) => {
			Info.operations_select(operation.id);
			navigate(`/operations/${operation.id}`);
		},
		[Info, navigate],
	);

	/**
	 * Opens the operation detail drawer without changing the route.
	 *
	 * @param operation - The operation to display.
	 */
	const handleDisplayOperationDetails = useCallback(
		(operation: Operation.Type) => {
			spawnDialog(
				<DisplayOperationDetailDialog
					operationId={operation.id}
					fallbackName={operation.name}
					fallbackGlyphId={operation.glyph_id}
					onClose={() => spawnDialog(null)}
				/>,
			);
		},
		[spawnDialog],
	);

	/**
	 * Toggles the selection state of a specific operation by its ID.
	 *
	 * @param id - The ID of the operation to toggle.
	 */
	const toggleSelection = useCallback((id: Operation.Id) => {
		setSelectedIds((prev) => {
			const next = new Set(prev);
			if (next.has(id)) {
				next.delete(id);
			} else {
				next.add(id);
			}
			return next;
		});
	}, []);

	/**
	 * Selects or deselects all operations in the table.
	 *
	 * @param checked - True to select all operations, false to deselect all.
	 */
	const handleSelectAll = useCallback(
		(checked: boolean) => {
			if (checked) {
				setSelectedIds(new Set(app.target.operations.map((op) => op.id)));
			} else {
				setSelectedIds(new Set());
			}
		},
		[app.target.operations],
	);

	/**
	 * Deletes the selected operations after the shared confirmation banner approves it.
	 *
	 * @returns A promise resolving to false when no operation was deleted.
	 */
	const confirmBulkDelete = useCallback(async (): Promise<boolean | void> => {
		const operationsToDelete = [...selectedIds]
			.map((id) => app.target.operations.find((operation) => operation.id === id))
			.filter((operation): operation is Operation.Type => Boolean(operation));
		const deletedIds = await Info.deleteOperation(
			operationsToDelete,
			() => undefined,
		);
		if (deletedIds.length === 0) {
			return false;
		}
		setSelectedIds((prev) => {
			const next = new Set(prev);
			deletedIds.forEach((id) => next.delete(id));
			return next;
		});
	}, [Info, app.target.operations, selectedIds]);

	/**
	 * Opens the shared confirmation banner for selected operation deletion.
	 */
	const openBulkDeleteBanner = useCallback(() => {
		if (selectedIds.size === 0) return;

		spawnBanner(
			<DeleteConfirmationBanner onConfirm={confirmBulkDelete}>
				<p>{t("home.operations.deleteSelected", { count: selectedIds.size })}</p>
			</DeleteConfirmationBanner>,
			"table",
		);
	}, [confirmBulkDelete, selectedIds.size, spawnBanner, t]);

	const selectedIndices = useMemo(() => {
		const indices = new Set<number>();
		app.target.operations.forEach((operation, index) => {
			if (selectedIds.has(operation.id)) {
				indices.add(index);
			}
		});
		return indices;
	}, [app.target.operations, selectedIds]);

	const actions = useMemo<Table.Action<Operation.Type>[]>(
		() => [
			{
				icon: "ArrowRight",
				label: t("common.goTo"),
				variant: "secondary",
				onClick: handleOpenOperation,
			},
		],
		[handleOpenOperation, t],
	);

	if (loading) {
		return <ResultState text={t("common.loading")} />;
	}

	if (app.target.operations.length === 0) {
		return <ResultState text={t("home.operations.empty")} />;
	}

	return (
		<>
			<Table<Operation.Type>
				className={s.result}
				values={app.target.operations}
				selectable={true}
				selectedrows={selectedIndices}
				onrowselect={(index) => {
					const operation = app.target.operations[index];
					if (operation) {
						toggleSelection(operation.id);
					}
				}}
				onSelectAll={handleSelectAll}
				onRowClick={handleDisplayOperationDetails}
				includeIndex={false}
				persistId="home-operations-table"
				actions={actions}
				columns={[
					{
						key: "name",
						label: t("common.name"),
						width: "auto",
						render: (value, row) => (
							<EntityNameCell
								iconName={Operation.Entity.icon(row)}
								name={value}
							/>
						),
					},
				]}
			/>
			<div className={s.footer}>
				<Stack jc="flex-end">
					<Button
						variant="glass"
						disabled={selectedIds.size === 0}
						onClick={openBulkDeleteBanner}
						icon="Trash2"
						style={DeleteButtonStyle}
					>
						{t("home.operations.deleteSelected", { count: selectedIds.size })}
					</Button>
				</Stack>
			</div>
		</>
	);
}

/**
 * Renders the user management list inside the Home main content panel.
 *
 * @returns The users table, loading state, or empty state.
 */
export function UsersList() {
	const { Info, app, spawnBanner, spawnDialog } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState<boolean>(true);
	const [users, setUsers] = useState<User.Type[]>([]);
	const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

	/**
	 * Reloads the user list from the API and ignores non-list error payloads.
	 *
	 * @returns A promise that settles after users are refreshed.
	 */
	const reloadUsers = useCallback(async () => {
		setLoading(true);
		try {
			const response: unknown = await Info.user_list({
				suppressMissingPermissionRedirect: true,
				showErrorToast: false,
			});
			const result = normalizeApiListResponse<User.Type>(
				response,
				t("permissions.requestDeclined"),
			);
			setUsers(result.items);
			if (result.failed) {
				setSelectedIds(new Set());
			}
		} catch {
			toast.error(t("permissions.requestDeclined"), {
				icon: <Icon name="Stop" />,
				richColors: true,
			});
			setUsers([]);
			setSelectedIds(new Set());
		} finally {
			setLoading(false);
		}
	}, [Info, t]);

	useEffect(() => {
		reloadUsers();
	}, [reloadUsers]);

	useEffect(() => {
		/**
		 * Refreshes the user table after create/edit banners complete.
		 */
		const handleUserListChanged = () => {
			reloadUsers();
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
	}, [reloadUsers]);

	/**
	 * Deletes a user and refreshes the list when the request succeeds.
	 *
	 * @param userId - Identifier of the user to delete.
	 * @returns A promise resolving to true when deletion succeeds.
	 */
	const deleteUser = useCallback(
		async (userId: string): Promise<boolean> => {
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
				setSelectedIds((prev) => {
					const next = new Set(prev);
					next.delete(userId);
					return next;
				});
				await reloadUsers();
			}
			return deleteResult.success;
		},
		[Info, app.general.user?.id, reloadUsers, t],
	);

	/**
	 * Deletes all selected users sequentially after confirmation.
	 *
	 * @returns A promise resolving to false when no selected user was deleted.
	 */
	const confirmBulkDelete = useCallback(async (): Promise<boolean | void> => {
		let deletedCount = 0;
		for (const userId of selectedIds) {
			const deleted = await deleteUser(userId);
			if (deleted) {
				deletedCount += 1;
			}
		}
		return deletedCount > 0 ? undefined : false;
	}, [deleteUser, selectedIds]);

	/**
	 * Opens the shared confirmation banner for selected user deletion.
	 */
	const openBulkDeleteBanner = useCallback(() => {
		if (selectedIds.size === 0) return;

		spawnBanner(
			<DeleteConfirmationBanner onConfirm={confirmBulkDelete}>
				<p>{t("home.users.deleteSelected", { count: selectedIds.size })}</p>
			</DeleteConfirmationBanner>,
			"table",
		);
	}, [confirmBulkDelete, selectedIds.size, spawnBanner, t]);

	/**
	 * Opens the user detail drawer and loads the full user payload by ID.
	 *
	 * @param user - User row selected from the table.
	 */
	const openUserDetails = useCallback(
		(user: User.Type) => {
			spawnDialog(
				<DisplayUserDetailDialog
					user={user}
					onClose={() => spawnDialog(null)}
				/>,
			);
		},
		[spawnDialog],
	);

	const selectedIndices = useMemo(() => {
		const indices = new Set<number>();
		users.forEach((user, index) => {
			if (selectedIds.has(user.id)) {
				indices.add(index);
			}
		});
		return indices;
	}, [selectedIds, users]);

	if (loading) {
		return <ResultState text={t("home.users.loading")} />;
	}

	if (users.length === 0) {
		return <ResultState text={t("home.users.empty")} />;
	}

	return (
		<>
			<Table<User.Type>
				className={s.result}
				values={users}
				selectable={true}
				selectedrows={selectedIndices}
				onrowselect={(index) => {
					const user = users[index];
					if (!user) return;
					setSelectedIds((prev) => {
						const next = new Set(prev);
						if (next.has(user.id)) {
							next.delete(user.id);
						} else {
							next.add(user.id);
						}
						return next;
					});
				}}
				onSelectAll={(checked) => {
					setSelectedIds(
						checked ? new Set(users.map((user) => user.id)) : new Set(),
					);
				}}
				onRowClick={openUserDetails}
				includeIndex={false}
				persistId="home-users-table"
				columns={[
					{ key: "id", label: t("common.id"), width: "auto" },
					{
						key: "name",
						label: t("common.name"),
						width: "auto",
						render: (value, row) => (
							<EntityNameCell
								iconName={getUserIconName(row)}
								name={value}
							/>
						),
					},
					{
						key: "permission",
						label: t("permissions.roles"),
						width: 180,
						render: (value) =>
							Array.isArray(value)
								? value.join(", ")
								: formatDetailValue(value),
					},
				]}
			/>
			<div className={s.footer}>
				<Stack jc="flex-end">
					<Button
						variant="glass"
						disabled={selectedIds.size === 0}
						onClick={openBulkDeleteBanner}
						icon="Trash2"
						style={DeleteButtonStyle}
					>
						{t("home.users.deleteSelected", { count: selectedIds.size })}
					</Button>
				</Stack>
			</div>
		</>
	);
}

/**
 * Renders the group management list inside the Home main content panel.
 *
 * @returns The groups table, loading state, or empty state.
 */
export function GroupsList() {
	const { spawnBanner, spawnDialog } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState<boolean>(true);
	const [groups, setGroups] = useState<Group.Type[]>([]);
	const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

	/**
	 * Reloads the group list from the API and ignores non-list error payloads.
	 *
	 * @returns A promise that settles after groups are refreshed.
	 */
	const reloadGroups = useCallback(async () => {
		setLoading(true);
		try {
			const response: unknown = await Permissions.Users.Groups.list({
				suppressMissingPermissionRedirect: true,
				showErrorToast: false,
			});
			const result = normalizeApiListResponse<Group.Type>(
				response,
				t("permissions.requestDeclined"),
			);
			setGroups(result.items);
			if (result.failed) {
				setSelectedIds(new Set());
			}
		} catch {
			toast.error(t("permissions.requestDeclined"), {
				icon: <Icon name="Stop" />,
				richColors: true,
			});
			setGroups([]);
			setSelectedIds(new Set());
		} finally {
			setLoading(false);
		}
	}, [t]);

	useEffect(() => {
		reloadGroups();
	}, [reloadGroups]);

	useEffect(() => {
		/**
		 * Refreshes the group table after create/edit/membership banners complete.
		 */
		const handleGroupListChanged = () => {
			reloadGroups();
		};

		window.addEventListener(
			Permissions.GroupListChangedEvent,
			handleGroupListChanged,
		);
		return () => {
			window.removeEventListener(
				Permissions.GroupListChangedEvent,
				handleGroupListChanged,
			);
		};
	}, [reloadGroups]);

	/**
	 * Deletes a group and refreshes the list when the request succeeds.
	 *
	 * @param groupId - Identifier of the group to delete.
	 * @returns A promise resolving to true when deletion succeeds.
	 */
	const deleteGroup = useCallback(
		async (groupId: string): Promise<boolean> => {
			const deleted = await Permissions.Users.Groups.deleteById(groupId);
			if (deleted) {
				toast.success(t("home.groups.deleted", { groupId }), {
					icon: <Icon name="Check" />,
					richColors: true,
				});
				setSelectedIds((prev) => {
					const next = new Set(prev);
					next.delete(groupId);
					return next;
				});
				await reloadGroups();
			}
			return deleted;
		},
		[reloadGroups, t],
	);

	/**
	 * Deletes all selected groups sequentially after confirmation.
	 *
	 * @returns A promise resolving to false when no selected group was deleted.
	 */
	const confirmBulkDelete = useCallback(async (): Promise<boolean | void> => {
		let deletedCount = 0;
		for (const groupId of selectedIds) {
			const deleted = await deleteGroup(groupId);
			if (deleted) {
				deletedCount += 1;
			}
		}
		return deletedCount > 0 ? undefined : false;
	}, [deleteGroup, selectedIds]);

	/**
	 * Opens the shared confirmation banner for selected group deletion.
	 */
	const openBulkDeleteBanner = useCallback(() => {
		if (selectedIds.size === 0) return;

		spawnBanner(
			<DeleteConfirmationBanner onConfirm={confirmBulkDelete}>
				<p>{t("home.groups.deleteSelected", { count: selectedIds.size })}</p>
			</DeleteConfirmationBanner>,
			"table",
		);
	}, [confirmBulkDelete, selectedIds.size, spawnBanner, t]);

	/**
	 * Opens the group detail drawer and loads the full group payload by ID.
	 *
	 * @param group - Group row selected from the table.
	 */
	const openGroupDetails = useCallback(
		(group: Group.Type) => {
			const entity = group as EntityRecord;
			const groupId = getEntityId(entity);
			if (!groupId) return;

			spawnDialog(
				<DisplayGroupDetailDialog
					group={group}
					onClose={() => spawnDialog(null)}
				/>,
			);
		},
		[spawnDialog],
	);

	const selectedIndices = useMemo(() => {
		const indices = new Set<number>();
		groups.forEach((group, index) => {
			const groupId = getEntityId(group as EntityRecord);
			if (selectedIds.has(groupId)) {
				indices.add(index);
			}
		});
		return indices;
	}, [groups, selectedIds]);

	if (loading) {
		return <ResultState text={t("home.groups.loading")} />;
	}

	if (groups.length === 0) {
		return <ResultState text={t("home.groups.empty")} />;
	}

	return (
		<>
			<Table<Group.Type>
				className={s.result}
				values={groups}
				selectable={true}
				selectedrows={selectedIndices}
				onrowselect={(index) => {
					const group = groups[index];
					if (!group) return;

					const groupId = getEntityId(group as EntityRecord);
					if (!groupId) return;

					setSelectedIds((prev) => {
						const next = new Set(prev);
						if (next.has(groupId)) {
							next.delete(groupId);
						} else {
							next.add(groupId);
						}
						return next;
					});
				}}
				onSelectAll={(checked) => {
					const ids = groups
						.map((group) => getEntityId(group as EntityRecord))
						.filter((groupId) => groupId.length > 0);
					setSelectedIds(checked ? new Set(ids) : new Set());
				}}
				onRowClick={openGroupDetails}
				includeIndex={false}
				persistId="home-groups-table"
				columns={[
					{ key: "id", label: t("common.id"), width: "auto" },
					{
						key: "name",
						label: t("common.name"),
						width: "auto",
						render: (value, row) => (
							<EntityNameCell
								iconName={getGroupIconName(row)}
								name={value}
							/>
						),
					},
				]}
			/>
			<div className={s.footer}>
				<Stack jc="flex-end">
					<Button
						variant="glass"
						disabled={selectedIds.size === 0}
						onClick={openBulkDeleteBanner}
						icon="Trash2"
						style={DeleteButtonStyle}
					>
						{t("home.groups.deleteSelected", { count: selectedIds.size })}
					</Button>
				</Stack>
			</div>
		</>
	);
}

/**
 * Builds table rows for plugin configuration shared objects.
 *
 * @param sharedObjects - Shared objects returned by the backend.
 * @returns Rows with a compact configuration preview column.
 */
function buildPluginConfigurationRows(
	sharedObjects: PluginConfigurationObject[],
): PluginConfigurationRow[] {
	return sharedObjects.map((sharedObject) => ({
		...sharedObject,
		configurationPreview: formatDetailValue(sharedObject.obj),
	}));
}

/**
 * Renders the plugin configuration shared-object list inside the Home panel.
 *
 * @returns The plugin configuration table, loading state, or empty state.
 */
export function PluginConfigurationsList() {
	const { Info, spawnBanner, spawnDialog } = Application.use();
	const { t } = Locale.use();
	const [loading, setLoading] = useState<boolean>(true);
	const [configurations, setConfigurations] = useState<PluginConfigurationRow[]>(
		[],
	);
	const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

	/**
	 * Reloads plugin configuration shared objects and ignores non-list error payloads.
	 *
	 * @returns A promise that settles after configurations are refreshed.
	 */
	const reloadConfigurations = useCallback(async () => {
		setLoading(true);
		try {
			const response: unknown =
				await Info.shared_object_list<GulpDataset.SharedObject.JsonObject>(
					{
						type: "shared_object",
						obj_type: "plugin_configuration",
					},
					{
						showErrorToast: false,
					},
				);
			const result = normalizeApiListResponse<PluginConfigurationObject>(
				response,
				t("permissions.requestDeclined"),
			);
			setConfigurations(buildPluginConfigurationRows(result.items));
			if (result.failed) {
				setSelectedIds(new Set());
			}
		} catch {
			toast.error(t("permissions.requestDeclined"), {
				icon: <Icon name="Stop" />,
				richColors: true,
			});
			setConfigurations([]);
			setSelectedIds(new Set());
		} finally {
			setLoading(false);
		}
	}, [Info, t]);

	useEffect(() => {
		reloadConfigurations();
	}, [reloadConfigurations]);

	/**
	 * Deletes one plugin configuration and updates table selection state.
	 *
	 * @param objId - Shared object identifier to delete.
	 * @returns A promise resolving to true when deletion succeeds.
	 */
	const deleteConfiguration = useCallback(
		async (objId: string): Promise<boolean> => {
			const deleted = await Info.shared_object_delete(objId);
			if (deleted) {
				toast.success(t("home.pluginConfigurations.deleted", { objId }), {
					icon: <Icon name="Check" />,
					richColors: true,
				});
				setSelectedIds((prev) => {
					const next = new Set(prev);
					next.delete(objId);
					return next;
				});
			} else {
				toast.error(t("home.pluginConfigurations.deleteFailed", { objId }), {
					icon: <Icon name="Stop" />,
					richColors: true,
				});
			}
			return deleted;
		},
		[Info, t],
	);

	/**
	 * Deletes all selected plugin configurations after confirmation.
	 *
	 * @returns A promise resolving to false when no selected configuration was deleted.
	 */
	const confirmBulkDelete = useCallback(async (): Promise<boolean | void> => {
		let deletedCount = 0;
		for (const objId of selectedIds) {
			const deleted = await deleteConfiguration(objId);
			if (deleted) {
				deletedCount += 1;
			}
		}
		await reloadConfigurations();
		return deletedCount > 0 ? undefined : false;
	}, [deleteConfiguration, reloadConfigurations, selectedIds]);

	/**
	 * Opens the shared confirmation banner for selected plugin configuration deletion.
	 */
	const openBulkDeleteBanner = useCallback(() => {
		if (selectedIds.size === 0) return;

		spawnBanner(
			<DeleteConfirmationBanner onConfirm={confirmBulkDelete}>
				<p>
					{t("home.pluginConfigurations.deleteSelected", {
						count: selectedIds.size,
					})}
				</p>
			</DeleteConfirmationBanner>,
			"table",
		);
	}, [confirmBulkDelete, selectedIds.size, spawnBanner, t]);

	/**
	 * Opens the plugin configuration detail drawer for the selected row.
	 *
	 * @param configuration - Plugin configuration row selected from the table.
	 */
	const openConfigurationDetails = useCallback(
		(configuration: PluginConfigurationRow) => {
			const objId = getSharedObjectId(configuration);
			if (!objId) return;

			spawnDialog(
				<DisplayPluginConfigurationDetailDialog
					objId={objId}
					fallbackName={configuration.name}
					onClose={() => spawnDialog(null)}
					onUpdated={reloadConfigurations}
				/>,
			);
		},
		[reloadConfigurations, spawnDialog],
	);

	const selectedIndices = useMemo(() => {
		const indices = new Set<number>();
		configurations.forEach((configuration, index) => {
			const objId = getSharedObjectId(configuration);
			if (selectedIds.has(objId)) {
				indices.add(index);
			}
		});
		return indices;
	}, [configurations, selectedIds]);

	if (loading) {
		return <ResultState text={t("home.pluginConfigurations.loading")} />;
	}

	if (configurations.length === 0) {
		return <ResultState text={t("home.pluginConfigurations.empty")} />;
	}

	return (
		<>
			<Table<PluginConfigurationRow>
				className={s.result}
				values={configurations}
				selectable={true}
				selectedrows={selectedIndices}
				onrowselect={(index) => {
					const configuration = configurations[index];
					if (!configuration) return;

					const objId = getSharedObjectId(configuration);
					if (!objId) return;

					setSelectedIds((prev) => {
						const next = new Set(prev);
						if (next.has(objId)) {
							next.delete(objId);
						} else {
							next.add(objId);
						}
						return next;
					});
				}}
				onSelectAll={(checked) => {
					const ids = configurations
						.map((configuration) => getSharedObjectId(configuration))
						.filter((objId) => objId.length > 0);
					setSelectedIds(checked ? new Set(ids) : new Set());
				}}
				onRowClick={openConfigurationDetails}
				includeIndex={false}
				persistId="home-plugin-configurations-table"
				columns={[
					{
						key: "name",
						label: t("common.name"),
						width: 220,
						render: (value, row) => (
							<EntityNameCell
								iconName={getPluginConfigurationIconName(row)}
								name={value}
							/>
						),
					},
					{
						key: "configurationPreview",
						label: t("common.configuration"),
						width: "auto",
					},
				]}
			/>
			<div className={s.footer}>
				<Stack jc="flex-end">
					<Button
						variant="glass"
						disabled={selectedIds.size === 0}
						onClick={openBulkDeleteBanner}
						icon="Trash2"
						style={DeleteButtonStyle}
					>
						{t("home.pluginConfigurations.deleteSelected", {
							count: selectedIds.size,
						})}
					</Button>
				</Stack>
			</div>
		</>
	);
}

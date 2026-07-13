import { Application } from "@/context/Application.context";
import { Banner as UIBanner } from "@/ui/Banner";
import { Table } from "@/components/Table";
import { ChangeEvent, useCallback, useEffect, useState } from "react";
import { Icon } from "@/ui/Icon";
import { toast } from "sonner";
import type { ResponseBase, ResponseHandler, SetState } from "@/class/API";
import { Button } from "@/ui/Button";
import { Input } from "@/ui/Input";
import { Skeleton } from "@/ui/Skeleton";
import { Stack } from "@/ui/Stack";
import { Select } from "@/ui/Select";
import { Textarea } from "@/ui/Textarea";
import { User } from "@/entities/User";
import { Group } from "@/entities/Group";
import { Glyph } from "@/entities/Glyph";
import { Locale } from "@/locales";
import { translate } from "@/locales/core";
import s from "@/banners/styles/PermissaionsBanner.module.css";

export namespace Permissions {
	type MutationResponse = ResponseHandler<ResponseBase<unknown>> | undefined;

	export type Role = "admin" | "read" | "edit" | "ingest" | "delete";
	export const PermissionSelectRoles: Role[] = [
		"read",
		"ingest",
		"edit",
		"delete",
		"admin",
	];
	export const UserListChangedEvent = "gulp:user-list-changed";
	export const GroupListChangedEvent = "gulp:group-list-changed";

	/**
	 * Notifies listeners that the user list has changed.
	 */
	export function notifyUserListChanged() {
		window.dispatchEvent(new Event(UserListChangedEvent));
	}

	/**
	 * Notifies listeners that the user group list has changed.
	 */
	export function notifyGroupListChanged() {
		window.dispatchEvent(new Event(GroupListChangedEvent));
	}

	/**
	 * Resolves the default glyph used for group forms.
	 *
	 * @returns A glyph ID for the Users icon, or the first available glyph.
	 */
	function getDefaultGroupGlyphId(): Glyph.Id {
		return (
			Glyph.getIdByName("Users") ||
			(Array.from(Glyph.List.keys()).find(Boolean) as Glyph.Id)
		);
	}

	export namespace Users {
		export const UsernameRule = /^[a-zA-Z0-9_.@-]{4,16}$/;
		export const PasswordRule =
			/^(?=.*[A-Z])(?=.*[a-z])(?=.*[0-9])(?=.*[!@#$%^&*()_+\-])[A-Za-z0-9!@#$%^&*()_+\-]{8,64}$/;

		export namespace RolesSelect {
			export interface Props {
				value: Permissions.Role[];
				onChange: (roles: Permissions.Role[]) => void;
			}
		}

		/**
		 * Renders a multi-select control for the supported permission roles.
		 *
		 * @param props - Current selected roles and update callback.
		 * @returns A multi-select trigger with selectable permission options.
		 */
		export function RolesSelect({ value, onChange }: RolesSelect.Props) {
			const { t } = Locale.use();

			/**
			 * Orders the selected roles using the UI display order.
			 *
			 * @param selectedRoles - Raw selected role values from the select component.
			 * @returns The selected roles normalized to known permission values.
			 */
			const normalizeSelectedRoles = (
				selectedRoles: string[],
			): Permissions.Role[] =>
				PermissionSelectRoles.filter((role) => selectedRoles.includes(role));

			return (
				<Select.Multi.Root
					value={value}
					onValueChange={(selectedRoles) =>
						onChange(normalizeSelectedRoles(selectedRoles))
					}
				>
					<Select.Trigger>
						<Icon
							name="Gavel"
							color="currentColor"
						/>
						<span>
							{value.length > 0
								? value
										.map((role) =>
											t(
												role === "delete"
													? "common.delete"
													: role === "edit"
														? "common.edit"
														: `permissions.role.${role}`,
											),
										)
										.join(", ")
								: t("common.choosePlaceholder")}
						</span>
					</Select.Trigger>
					<Select.Content>
						{PermissionSelectRoles.map((role) => (
							<Select.Item
								key={role}
								value={role}
							>
								{t(
									role === "delete"
										? "common.delete"
										: role === "edit"
											? "common.edit"
											: `permissions.role.${role}`,
								)}
							</Select.Item>
						))}
					</Select.Content>
				</Select.Multi.Root>
			);
		}

		export namespace Create {
			export const Banner = () => {
				const { destroyBanner } = Application.use();
				const { t } = Locale.use();
				const [loading, setLoading] = useState<boolean>(false);
				const [icon, setIcon] = useState<Glyph.Id | null>(null);
				const [id, setId] = useState<string>("");
				const [isIdValid, setIsIdValid] = useState<boolean>(true);
				const [password, setPassword] = useState<string>("");
				const [isPasswordValid, setIsPasswordValid] = useState<boolean>(true);
				const [permissions, setPermissions] = useState<Permissions.Role[]>([
					"read",
				]);

				const inputConstructor =
					(
						set: SetState<string>,
						setValid: SetState<boolean>,
						regexp: RegExp,
					) =>
					(event: ChangeEvent<HTMLInputElement>) => {
						const { value } = event.target;
						setValid(value.length > 3 ? true : regexp.test(value));
						set(value);
					};

				/**
				 * Stores the permissions selected from the multi-select control.
				 *
				 * @param selectedRoles - Roles selected by the user.
				 */
				const handlePermissionsChange = (selectedRoles: Permissions.Role[]) => {
					setPermissions(selectedRoles);
				};

				/**
				 * Creates a user using the selected permission list and reports API failures.
				 */
				const submit = async () => {
					const response: MutationResponse = await api<undefined>(
						"/user_create",
						{
							method: "POST",
							query: {
								user_id: id,
								password,
								glyph_id:
									icon ||
									(Array.from(Glyph.List.entries()).find(
										(i) => i[1] === "User",
									)?.[0] as string),
							},
							setLoading,
							body: {
								permission: permissions,
							},
							raw: true,
							errorToastTitle: t("permissions.userAddFailed"),
							suppressMissingPermissionRedirect: true,
						},
					);

					if (response?.status === "success") {
						notifyUserListChanged();
						destroyBanner();
						return;
					}

					return;
				};

				const Done = () => (
					<Button
						icon="Check"
						loading={loading}
						onClick={submit}
						disabled={
							id.length < 3 ||
							!isIdValid ||
							password.length < 5 ||
							!isPasswordValid ||
							permissions.length === 0 ||
							!icon
						}
						variant="glass"
					/>
				);

				return (
					<UIBanner
						title={t("permissions.createUser")}
						done={<Done />}
					>
						<Input
							icon="User"
							placeholder={t("permissions.userIdPlaceholder")}
							variant="highlighted"
							value={id}
							valid={isIdValid}
							onChange={inputConstructor(
								setId,
								setIsIdValid,
								new RegExp(Permissions.Users.UsernameRule),
							)}
						/>
						<Input
							icon="Key"
							placeholder={t("auth.password")}
							variant="highlighted"
							value={password}
							valid={isPasswordValid}
							onChange={inputConstructor(
								setPassword,
								setIsPasswordValid,
								new RegExp(Permissions.Users.PasswordRule),
							)}
						/>
						<Users.RolesSelect
							value={permissions}
							onChange={handlePermissionsChange}
						/>
						<Glyph.Chooser
							icon={icon}
							setIcon={setIcon}
						/>
					</UIBanner>
				);
			};
		}
		export namespace Groups {
			export const NameRule = /^[a-zA-Z0-9_.@-]{2,64}$/;

			export interface SaveOptions {
				name: string;
				glyphId: Glyph.Id;
				permissions: Permissions.Role[];
				description: string;
				setLoading?: SetState<boolean>;
			}

			export interface UpdateOptions extends SaveOptions {
				groupId: string;
			}

			export interface MembershipOptions {
				groupId: string;
				userId: string;
			}

			/**
			 * Extracts user IDs from the group detail payload.
			 *
			 * @param group - Group detail payload returned by the API.
			 * @returns A normalized list of user IDs assigned to the group.
			 */
			export function getUserIds(
				group: Group.Type | null | undefined,
			): string[] {
				const members = group?.users ?? group?.user ?? [];
				return members
					.map((member) => {
						if (typeof member === "string") {
							return member;
						}

						return member.user_id ?? member.id ?? "";
					})
					.filter((userId) => userId.length > 0);
			}

			/**
			 * Fetches all user groups available to the current session.
			 *
			 * @param options - Request behavior flags for permission-denied responses.
			 * @returns A promise resolving to the user group list returned by the API.
			 */
			export function list(
				options: {
					suppressMissingPermissionRedirect?: boolean;
					showErrorToast?: boolean;
				} = {},
			): Promise<Group.Type[]> {
				return api<Group.Type[]>("/user_group_list", {
					method: "POST",
					suppressMissingPermissionRedirect:
						options.suppressMissingPermissionRedirect,
					showErrorToast: options.showErrorToast,
				});
			}

			/**
			 * Fetches detailed information about a specific user group by ID.
			 *
			 * @param groupId - Unique identifier of the group to fetch.
			 * @returns A promise resolving to the detailed group payload.
			 */
			export function getById(groupId: string): Promise<Group.Type> {
				return api<Group.Type>("/user_group_get_by_id", {
					method: "GET",
					query: { group_id: groupId },
				});
			}

			/**
			 * Deletes a user group by ID.
			 *
			 * @param groupId - Unique identifier of the group to delete.
			 * @returns A promise resolving to true when the API confirms deletion.
			 */
			export async function deleteById(groupId: string): Promise<boolean> {
				const response = await api<undefined>("/user_group_delete", {
					method: "DELETE",
					query: { group_id: groupId },
					raw: true,
					errorToastTitle: translate("home.groups.deleteFailed", { groupId }),
				});

				return response?.status === "success";
			}

			/**
			 * Creates a new user group with permissions and description metadata.
			 *
			 * @param options - Group name, glyph, permissions, and description payload.
			 * @returns A promise resolving to true only when the API confirms creation.
			 */
			export async function create(options: SaveOptions): Promise<boolean> {
				const response: MutationResponse = await api<undefined>(
					"/user_group_create",
					{
						method: "POST",
						query: {
							name: options.name,
							glyph_id: options.glyphId,
						},
						setLoading: options.setLoading,
						raw: true,
						body: {
							permission: options.permissions,
							description: options.description,
						},
						suppressMissingPermissionRedirect: true,
						errorToastTitle: translate("permissions.groupAddFailed"),
					},
				);

				return response?.status === "success";
			}

			/**
			 * Updates an existing user group with permissions and description metadata.
			 *
			 * @param options - Target group ID, glyph, permissions, and description payload.
			 * @returns A promise resolving to true only when the API confirms update.
			 */
			export async function update(options: UpdateOptions): Promise<boolean> {
				const response: MutationResponse = await api<undefined>(
					"/user_group_update",
					{
						method: "PATCH",
						query: {
							group_id: options.groupId,
							glyph_id: options.glyphId,
						},
						setLoading: options.setLoading,
						raw: true,
						body: {
							permission: options.permissions,
							description: options.description,
						},
						suppressMissingPermissionRedirect: true,
						errorToastTitle: translate("permissions.requestDeclined"),
					},
				);

				return response?.status === "success";
			}

			/**
			 * Adds a user to a group.
			 *
			 * @param options - Target group ID and user ID to add.
			 * @returns A promise resolving when the API request completes.
			 */
			export async function addUser(
				options: MembershipOptions,
			): Promise<boolean> {
				const response = await api<undefined>("/user_group_add_user", {
					method: "PATCH",
					query: {
						group_id: options.groupId,
						user_id: options.userId,
					},
					raw: true,
					errorToastTitle: translate("permissions.userAddFailed"),
					toast: {
						onSuccess: () =>
							toast.success(translate("permissions.userAdded"), {
								icon: <Icon name="Check" />,
								richColors: true,
							}),
					},
				});

				return response?.status === "success";
			}

			/**
			 * Removes a user from a group.
			 *
			 * @param options - Target group ID and user ID to remove.
			 * @returns A promise resolving when the API request completes.
			 */
			export async function removeUser(
				options: MembershipOptions,
			): Promise<boolean> {
				const response = await api<undefined>("/user_group_remove_user", {
					method: "PATCH",
					query: {
						group_id: options.groupId,
						user_id: options.userId,
					},
					raw: true,
					errorToastTitle: translate("permissions.userRemoveFailed"),
					toast: {
						onSuccess: () =>
							toast.success(translate("permissions.userRemoved"), {
								icon: <Icon name="Check" />,
								richColors: true,
							}),
					},
				});

				return response?.status === "success";
			}

			export namespace FormBanner {
				export interface Props extends UIBanner.Props {
					group?: Group.Type;
				}
			}

			/**
			 * Renders the create/edit form for user groups.
			 *
			 * @param props - Optional group payload and banner props.
			 * @returns A banner with group metadata and permission controls.
			 */
			export function FormBanner({ group, ...props }: FormBanner.Props) {
				const { destroyBanner } = Application.use();
				const { t } = Locale.use();
				const groupId = typeof group?.id === "string" ? group.id : "";
				const [loading, setLoading] = useState<boolean>(false);
				const [name, setName] = useState<string>(group?.name ?? groupId);
				const [isNameValid, setIsNameValid] = useState<boolean>(true);
				const [description, setDescription] = useState<string>(
					group?.description ?? "",
				);
				const [permissions, setPermissions] = useState<Permissions.Role[]>(
					(group?.permission as Permissions.Role[] | undefined) ?? ["read"],
				);
				const [icon, setIcon] = useState<Glyph.Id | null>(
					(group?.glyph_id as Glyph.Id | undefined) ?? getDefaultGroupGlyphId(),
				);

				/**
				 * Stores the group name and validates it against the API-safe pattern.
				 *
				 * @param event - Name input change event.
				 */
				const handleNameChange = (event: ChangeEvent<HTMLInputElement>) => {
					const { value } = event.target;
					setIsNameValid(value.length === 0 || Groups.NameRule.test(value));
					setName(value);
				};

				/**
				 * Stores the current description textarea value.
				 *
				 * @param event - Description textarea change event.
				 */
				const handleDescriptionChange = (
					event: ChangeEvent<HTMLTextAreaElement>,
				) => {
					setDescription(event.target.value);
				};

				/**
				 * Stores the permissions selected from the multi-select control.
				 *
				 * @param selectedRoles - Roles selected by the user.
				 */
				const handlePermissionsChange = (selectedRoles: Permissions.Role[]) => {
					setPermissions(selectedRoles);
				};

				/**
				 * Creates or updates a user group using the current form values.
				 */
				const submit = () => {
					const glyphId = icon ?? getDefaultGroupGlyphId();
					const onSuccess = () => {
						notifyGroupListChanged();
						destroyBanner();
						props.onClose?.();
					};

					if (groupId) {
						Groups.update({
							groupId,
							name,
							glyphId,
							permissions,
							description,
							setLoading,
						}).then((saved) => {
							if (saved) onSuccess();
						});
						return;
					}

					Groups.create({
						name,
						glyphId,
						permissions,
						description,
						setLoading,
					}).then((saved) => {
						if (saved) onSuccess();
					});
				};

				const Done = () => (
					<Button
						icon="Check"
						loading={loading}
						onClick={submit}
						disabled={
							name.length < 2 ||
							!isNameValid ||
							permissions.length === 0 ||
							!icon
						}
						variant="glass"
					/>
				);

				return (
					<UIBanner
						title={
							groupId
								? t("permissions.editGroup", { name: name || groupId })
								: t("permissions.createGroup")
						}
						done={<Done />}
						{...props}
					>
						<Input
							icon="Users"
							placeholder={t("common.name")}
							variant="highlighted"
							value={name}
							valid={isNameValid}
							disabled={!!groupId}
							onChange={handleNameChange}
						/>
						<Textarea
							className={s.textarea}
							placeholder={t("common.description")}
							value={description}
							onChange={handleDescriptionChange}
						/>
						<Users.RolesSelect
							value={permissions}
							onChange={handlePermissionsChange}
						/>
						<Glyph.Chooser
							icon={icon}
							setIcon={setIcon}
						/>
					</UIBanner>
				);
			}

			export namespace ManageUsersBanner {
				export interface Props extends UIBanner.Props {
					groupId: string;
					userIds: string[];
				}
			}

			/**
			 * Renders the group membership manager using selectable user rows.
			 *
			 * @param props - Target group ID, selected user IDs, and banner props.
			 * @returns A banner for adding/removing users from a group.
			 */
			export function ManageUsersBanner({
				groupId,
				userIds,
				onClose,
				...props
			}: ManageUsersBanner.Props) {
				const { t } = Locale.use();
				const [users, setUsers] = useState<User.Type[]>([]);
				const [loading, setLoading] = useState<boolean>(true);
				const [selectedUsers, setSelectedUsers] = useState<Set<string>>(
					new Set(userIds),
				);

				/**
				 * Reloads the complete user list used for membership selection.
				 */
				const reload = useCallback(() => {
					setLoading(true);
					api<User.Type[]>("/user_list", {}, (res) => {
						setUsers(res || []);
						setLoading(false);
					});
				}, []);

				useEffect(() => {
					reload();
				}, [reload]);

				/**
				 * Adds or removes a user from the group based on checkbox state.
				 *
				 * @param index - Row index selected in the users table.
				 * @param selected - Whether the row is now selected.
				 */
				const handleUserSelect = (index: number, selected: boolean) => {
					const user = users[index];
					if (!user) return;

					const action = selected ? Groups.addUser : Groups.removeUser;
					action({ groupId, userId: user.id }).then((saved) => {
						if (!saved) return;
						setSelectedUsers((prev) => {
							const next = new Set(prev);
							if (selected) {
								next.add(user.id);
							} else {
								next.delete(user.id);
							}
							return next;
						});
						notifyGroupListChanged();
					});
				};

				const selectedUserIndices = new Set<number>();
				users.forEach((user, index) => {
					if (selectedUsers.has(user.id)) {
						selectedUserIndices.add(index);
					}
				});

				return (
					<UIBanner
						title={t("permissions.manageUsers")}
						loading={loading}
						onClose={onClose}
						{...props}
					>
						<Stack
							dir="column"
							ai="stretch"
							className={s.tableScroll}
						>
							<Stack
								dir="column"
								ai="stretch"
								gap={8}
								className={s.tableSection}
							>
								<div className={s.sectionLabel}>{t("common.users")}</div>
								{users.length > 0 ? (
									<Table
										values={users}
										selectable={true}
										selectedrows={selectedUserIndices}
										onrowselect={handleUserSelect}
										columns={["id", "name", "permission"]}
										notshow={["glyph_id", "password"]}
										includeIndex={false}
									/>
								) : (
									<Skeleton
										width="full"
										className={s.tableSkeleton}
									/>
								)}
							</Stack>
						</Stack>
					</UIBanner>
				);
			}
		}
		export namespace Edit {
			export namespace Banner {
				export interface Props extends UIBanner.Props {
					user: User.Type;
				}
			}
			export function Banner({ user, ...props }: Banner.Props) {
				const { destroyBanner } = Application.use();
				const { t } = Locale.use();
				const [loading, setLoading] = useState<boolean>(false);
				const [icon, setIcon] = useState<Glyph.Id | null>(user.glyph_id);
				const [id, setId] = useState<string>(user.id);
				const [isIdValid, setIsIdValid] = useState<boolean>(true);
				const [password, setPassword] = useState<string>("");
				const [showPassword, setShowPassword] = useState(false);
				const [isPasswordValid, setIsPasswordValid] = useState<boolean>(true);
				const [permissions, setPermissions] = useState<Permissions.Role[]>(
					user.permission ?? ["read"],
				);

				const inputConstructor =
					(
						set: SetState<string>,
						setValid: SetState<boolean>,
						regexp: RegExp,
					) =>
					(event: ChangeEvent<HTMLInputElement>) => {
						const { value } = event.target;
						setValid(value.length > 3 ? true : regexp.test(value));
						set(value);
					};

				/**
				 * Stores the permissions selected from the multi-select control.
				 *
				 * @param selectedRoles - Roles selected by the user.
				 */
				const handlePermissionsChange = (selectedRoles: Permissions.Role[]) => {
					setPermissions(selectedRoles);
				};

				/**
				 * Keeps the password field optional while still validating non-empty values.
				 *
				 * @param event - Password input change event.
				 */
				const handleOptionalPasswordChange = (
					event: ChangeEvent<HTMLInputElement>,
				) => {
					const { value } = event.target;
					setPassword(value);
					setIsPasswordValid(
						value.length === 0 ||
							new RegExp(Permissions.Users.PasswordRule).test(value),
					);
				};

				/**
				 * Updates a user using the selected permission list and reports API failures.
				 */
				const submit = async () => {
					const query: Record<string, string> = {
						user_id: id,
						glyph_id:
							icon ||
							(Array.from(Glyph.List.entries()).find(
								(i) => i[1] === "User",
							)?.[0] as string),
					};

					if (password.length > 0) {
						query.password = password;
					}

					const response: MutationResponse = await api<undefined>(
						"/user_update",
						{
							method: "PATCH",
							query,
							setLoading,
							body: {
								permission: permissions,
							},
							raw: true,
							errorToastTitle: t("permissions.requestDeclined"),
							suppressMissingPermissionRedirect: true,
						},
					);

					if (response?.status === "success") {
						notifyUserListChanged();
						destroyBanner();
						return;
					}

					return;
				};

				const Done = () => (
					<Button
						icon="Check"
						loading={loading}
						onClick={submit}
						disabled={
							id.length < 3 ||
							!isIdValid ||
							!isPasswordValid ||
							permissions.length === 0 ||
							!icon
						}
						variant="glass"
					/>
				);

				return (
					<UIBanner
						title={t("permissions.editUser", { name: user.name || user.id })}
						done={<Done />}
						{...props}
					>
						<Input
							icon="User"
							placeholder={t("permissions.userIdPlaceholder")}
							variant="highlighted"
							value={id}
							valid={isIdValid}
							onChange={inputConstructor(
								setId,
								setIsIdValid,
								new RegExp(Permissions.Users.UsernameRule),
							)}
						/>
						<Input
							icon="Key"
							placeholder={t("auth.password")}
							variant="highlighted"
							type={showPassword ? "text" : "password"}
							value={password}
							endIcon={showPassword ? "EyeOff" : "Eye"}
							endIconTitle={
								showPassword ? t("auth.hidePassword") : t("auth.showPassword")
							}
							onEndIconClick={() => setShowPassword((current) => !current)}
							valid={isPasswordValid}
							onChange={handleOptionalPasswordChange}
						/>
						<Users.RolesSelect
							value={permissions}
							onChange={handlePermissionsChange}
						/>
						<Glyph.Chooser
							icon={icon}
							setIcon={setIcon}
						/>
					</UIBanner>
				);
			}
		}
	}
}

export namespace OperationPermissions {
	export namespace Banner {
		export interface Props extends UIBanner.Props {
			operationId: string;
			granted_user_ids: string[];
			granted_user_group_ids: string[];
			onSuccess?: () => void;
		}
	}

	export const Banner = ({
		operationId,
		granted_user_ids,
		granted_user_group_ids,
		onSuccess,
		...props
	}: Banner.Props) => {
		const { Info, destroyBanner } = Application.use();
		const { t } = Locale.use();
		const [users, setUsers] = useState<User.Type[]>([]);
		const [groups, setGroups] = useState<Group.Type[]>([]);
		const [loading, setLoading] = useState<boolean>(true);

		// Local sets for selection UI
		const [selectedUsers, setSelectedUsers] = useState<Set<string>>(
			new Set(granted_user_ids),
		);
		const [selectedGroups, setSelectedGroups] = useState<Set<string>>(
			new Set(granted_user_group_ids),
		);

		const reload = useCallback(() => {
			let usersLoaded = false;
			let groupsLoaded = false;

			const checkDone = () => {
				if (usersLoaded && groupsLoaded) {
					setLoading(false);
				}
			};

			api<User.Type[]>("/user_list", {}, (res) => {
				setUsers(res || []);
				usersLoaded = true;
				checkDone();
			});

			Permissions.Users.Groups.list().then((res) => {
				setGroups(res || []);
				groupsLoaded = true;
				checkDone();
			});
		}, []);

		useEffect(() => {
			reload();
		}, [reload]);

		const handleUserSelect = (index: number, selected: boolean) => {
			const user = users[index];
			if (!user) return;

			if (selected) {
				Info.add_granted_user("operation", operationId, user.id).then(() =>
					setSelectedUsers((prev) => new Set(prev).add(user.id)),
				);
			} else {
				Info.remove_granted_user("operation", operationId, user.id).then(() =>
					setSelectedUsers((prev) => {
						const next = new Set(prev);
						next.delete(user.id);
						return next;
					}),
				);
			}
		};

		const handleGroupSelect = (index: number, selected: boolean) => {
			const group = groups[index];
			if (!group) return;

			if (selected) {
				Info.add_granted_group("operation", operationId, group.id).then(() =>
					setSelectedGroups((prev) => new Set(prev).add(group.id)),
				);
			} else {
				Info.remove_granted_group("operation", operationId, group.id).then(() =>
					setSelectedGroups((prev) => {
						const next = new Set(prev);
						next.delete(group.id);
						return next;
					}),
				);
			}
		};

		// Convert sets to indices for Table
		const selectedUserIndices = new Set<number>();
		users.forEach((u, i) => {
			if (selectedUsers.has(u.id)) selectedUserIndices.add(i);
		});

		const selectedGroupIndices = new Set<number>();
		groups.forEach((g, i) => {
			if (selectedGroups.has(g.id)) selectedGroupIndices.add(i);
		});

		return (
			<UIBanner
				title={t("permissions.operationPermissions")}
				loading={loading}
				onClose={onSuccess}
				{...props}
			>
				<Stack
					dir="column"
					ai="stretch"
					className={s.tableScroll}
				>
					{/* Users Section */}
					<Stack
						dir="column"
						ai="stretch"
						gap={8}
					>
						<div className={s.sectionLabel}>{t("common.users")}</div>
						{users.length > 0 ? (
							<Table
								values={users}
								selectable={true}
								selectedrows={selectedUserIndices}
								onrowselect={handleUserSelect}
								columns={["id", "name", "permission"]}
								notshow={["glyph_id", "password"]}
								includeIndex={false}
							/>
						) : (
							<Skeleton
								width="full"
								className={s.tableSkeleton}
							/>
						)}
					</Stack>

					{/* Groups Section */}
					<Stack
						dir="column"
						ai="stretch"
						gap={8}
					>
						<div className={s.sectionLabel}>{t("common.groups")}</div>
						{groups.length > 0 ? (
							<Table
								values={groups}
								selectable={true}
								selectedrows={selectedGroupIndices}
								onrowselect={handleGroupSelect}
								columns={["id", "name"]}
								notshow={["glyph_id"]}
								includeIndex={false}
							/>
						) : (
							<Skeleton
								width="full"
								className={s.tableSkeleton}
							/>
						)}
					</Stack>
				</Stack>
			</UIBanner>
		);
	};
}

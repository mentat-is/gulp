import { Application } from "@/context/Application.context";
import { Banner as UIBanner } from "@/ui/Banner";
import {
	ChangeEvent,
	useCallback,
	useEffect,
	useMemo,
	useState,
} from "react";
import s from "./styles/Session.module.css";
import { toast } from "sonner";
import { Default } from "@/dto/Dataset";
import {
	ColorPicker,
	ColorPickerPopover,
	ColorPickerTrigger,
} from "@/ui/Color";
import { Input } from "@/ui/Input";
import { Logger } from "@/dto/Logger.class";
import { Icon } from "@/ui/Icon";
import { Stack } from "@/ui/Stack";
import { Button } from "@/ui/Button";
import { Glyph } from "@/entities/Glyph";
import { Operation } from "@/entities/Operation";
import { Label } from "@/ui/Label";
import { Select } from "@/ui/Select";
import { cn } from "@/ui/utils";
import { useNavigate } from "react-router-dom";
import { Locale } from "@/locales";
import type { StoredWorkspaceSession } from "@/class/WorkspaceSessionRepository";

export namespace Session {
	export namespace Save {
		export namespace Banner {
			export interface Props extends UIBanner.Props {
				/** Logs out after a successful save when used by the logout flow. */
				logoutAfterSave?: boolean;
				/** Notifies the operation selector after a non-logout save. */
				onSaved?: () => void;
			}
		}
		export function Banner({
			logoutAfterSave = true,
			onSaved,
			...props
		}: Session.Save.Banner.Props) {
			const { Info, app, spawnBanner } = Application.use();
			const defaultSessionName = `${app.general.user?.name || "session"}.session`;
			const [name, setName] = useState<string>(defaultSessionName);
			const [color, setColor] = useState<string>(Default.Color.OPERATION);
			const [icon, setIcon] = useState<Glyph.Id | null>(() =>
				Glyph.getIdByName(Default.Icon.SESSION),
			);
			const { t } = Locale.use();
			const [loading, setLoading] = useState<boolean>(false);
			const navigate = useNavigate();

			/**
			 * Copies the edited session name into local form state.
			 * @param event Name input change event.
			 * @returns Nothing.
			 */
			const changenameHandler = (event: ChangeEvent<HTMLInputElement>) => {
				const { value } = event.target;

				setName(value);
			};

			/**
			 * Persists a named session and completes the invoking flow.
			 * @returns Promise settled after the save attempt.
			 */
			const saveSession = async () => {
				const operation = Operation.Entity.selected(app);
				if (!operation) {
					return;
				}

				if (!icon) {
					toast.error(t("session.nameNeedsIcon"), {
						richColors: true,
					});
					return;
				}

				setLoading(true);
				const sessionName = name.trim() || defaultSessionName;
				const session = await Info.session_create({
					name: sessionName,
					color,
					icon: Glyph.List.get(icon)!,
				});
				setLoading(false);
				if (session) {
					Logger.log(
						t("session.saved", { name: sessionName }),
						Session.Save.Banner,
						{
							richColors: true,
							icon: <Icon name="Check" />,
						},
					);
					if (logoutAfterSave) {
						setTimeout(() => {
							logout();
						}, 250);
					} else {
						if (onSaved) onSaved();
						else spawnBanner(null);
					}
				}
			};

			/**
			 * Logs out the user by clearing the banner, navigating to the /login view
			 * (without appending any redirect query parameters), and calling the backend
			 * logout API to clear session credentials.
			 *
			 * @returns {Promise<void>} Resolves when the logout sequence completes.
			 */
			const logout = async () => {
				setLoading(true);
				try {
					spawnBanner(null);
					await Info.logout();
					navigate("/login", { replace: true });
				} catch (error) {
					Logger.error(error, "Session.Save.Banner.logout");
				} finally {
					setLoading(false);
				}
			};

			return (
				<UIBanner
					title={t("session.saveTitle")}
					{...props}
				>
					<Stack
						className={s.param}
						dir="column"
						ai="stretch"
						gap={12}
					>
						<Input
							icon="TextTitle"
							label={t("session.nameLabel")}
							placeholder={t("session.namePlaceholder")}
							variant="highlighted"
							value={name}
							onChange={changenameHandler}
						/>
						<Stack>
							<ColorPicker
								style={{ flex: 1 }}
								color={color}
								setColor={setColor}
							>
								<ColorPickerTrigger />
								<ColorPickerPopover />
							</ColorPicker>
							<Glyph.Chooser
								style={{ flex: 1 }}
								icon={icon}
								setIcon={setIcon}
							/>
						</Stack>
					</Stack>
					<Stack className={s.buttons}>
						<Button
							loading={loading}
							onClick={saveSession}
							variant="glass"
							disabled={!icon}
							icon="Check"
						>
							{t("session.saveCurrent")}
						</Button>
						{logoutAfterSave ? (
							<Button
								variant="destructive"
								icon="LogOut"
								onClick={logout}
							>
								{t("session.dontSave")}
							</Button>
						) : (
							<Button
								variant="tertiary"
								icon="X"
								onClick={() => spawnBanner(null)}
							>
								{t("common.cancel")}
							</Button>
						)}
					</Stack>
				</UIBanner>
			);
		}
	}

	export namespace Delete {
		export namespace Banner {
			export interface Props extends UIBanner.Props {
				onClose?: () => void;
			}
		}

		export function Banner({ onClose, ...props }: Session.Delete.Banner.Props) {
			const { Info, app, destroyBanner } = Application.use();
			const { t } = Locale.use();
			const [sessions, setSessions] = useState<StoredWorkspaceSession[]>([]);
			const [isDataLoading, setIsDataLoading] = useState<boolean>(true);
			const [isDataDeleting, setIsDataDeleting] = useState<boolean>(false);
			const [isRenaming, setIsRenaming] = useState<boolean>(false);
			const [selected, setSelected] = useState<Set<string>>(new Set());
			const [renameName, setRenameName] = useState("");
			const operationId = Operation.Entity.selected(app)?.id;

			/**
			 * Reloads selected-operation sessions from the repository cache.
			 * @returns Promise settled after local state is refreshed.
			 */
			const reload = useCallback(async () => {
				setIsDataLoading(true);
				setSessions(await Info.session_list(operationId));
				setSelected(() => new Set());
				setIsDataLoading(false);
			}, [Info, operationId]);

			useEffect(() => {
				void reload();
				if (!operationId) return;
				return Info.session_subscribe(operationId, () => {
					void Info.session_list(operationId).then(setSessions);
				});
			}, [Info, operationId, reload]);

			const selectedSession = useMemo(() => {
				if (selected.size !== 1) return null;
				const sessionId = selected.values().next().value;
				return sessions.find((session) => session.id === sessionId) ?? null;
			}, [selected, sessions]);

			useEffect(() => {
				setRenameName(
					selectedSession?.kind === "named" ? selectedSession.name : "",
				);
			}, [selectedSession]);

			/**
			 * Deletes every selected stable ID with one operation-bucket write.
			 * @returns Promise settled after deletion and local refresh.
			 */
			const deleteSessionButtonClickHandler = async () => {
				setIsDataDeleting(true);
				const deleted = await Info.sessions_delete([...selected.values()]);
				if (deleted) {
					Logger.log(
						t("session.deleted", { count: selected.size }),
						"Session.Delete.Banner.deleteSessionButtonClickHandler",
						{
							richColors: true,
							icon: <Icon name="Check" />,
						},
					);
				}
				setIsDataDeleting(false);
				if (deleted) {
					await reload();
					onClose?.();
					destroyBanner();
				}
			};

			/**
			 * Renames the single selected named session.
			 * @returns Promise settling after persistence and local refresh.
			 */
			const renameSelectedSession = async () => {
				if (!selectedSession || selectedSession.kind !== "named") return;
				setIsRenaming(true);
				const renamed = await Info.session_rename(
					selectedSession.id,
					renameName,
				);
				setIsRenaming(false);
				if (renamed) await reload();
			};

			/**
			 * Explicitly refreshes user_data for cross-device changes.
			 * @returns Promise settling after the refreshed sessions are displayed.
			 */
			const refreshFromBackend = async () => {
				setIsDataLoading(true);
				setSessions(await Info.session_refresh());
				setSelected(new Set());
				setIsDataLoading(false);
			};

			const DeleteButton = useMemo(
				() => (
					<Button
						onClick={deleteSessionButtonClickHandler}
						loading={isDataDeleting || isDataLoading}
						disabled={!selected.size}
						icon="Trash"
						variant="glass"
					/>
				),
				[
					selected,
					isDataDeleting,
					isDataLoading,
					deleteSessionButtonClickHandler,
				],
			);

			return (
				<UIBanner
					title={t("session.manage")}
					done={DeleteButton}
					option={
						<Button
							icon="RefreshClockwise"
							variant="tertiary"
							loading={isDataLoading}
							onClick={refreshFromBackend}
						>
							{t("common.refresh")}
						</Button>
					}
					{...props}
				>
					<Stack
						dir="column"
						gap={6}
						ai="flex-start"
						data-input
						className={cn(
							s.operation,
							!!app.general.user &&
								operationId &&
								sessions.length &&
								s.visible,
						)}
					>
						<Label value={t("session.label")} />
						<Stack style={{ width: "100%" }}>
							<Select.Multi.Root
								value={[...selected.values()]}
								onValueChange={(names) =>
									setSelected(() => new Set<string>(names))
								}
							>
								<Select.Trigger>
									<Select.Multi.Value
										icon="Status"
										placeholder={t("session.selectToDelete")}
										text={(len) =>
											typeof len === "number" ? t("session.selectedCount", { count: len }) : len
										}
									/>
								</Select.Trigger>
								<Select.Content>
									{sessions.map((session) => (
										<Select.Item
											key={session.id}
											value={session.id}
											style={{ color: session.color }}
										>
											<Select.Icon name={session.icon} />
											{session.name}
										</Select.Item>
									))}
								</Select.Content>
							</Select.Multi.Root>
						</Stack>
						{selectedSession?.kind === "named" ? (
							<Stack style={{ width: "100%" }}>
								<Input
									value={renameName}
									label={t("session.nameLabel")}
									valid={renameName.trim().length > 0}
									onChange={(event) => setRenameName(event.target.value)}
								/>
								<Button
									icon="PencilEdit"
									variant="tertiary"
									loading={isRenaming}
									disabled={
										!renameName.trim() ||
										renameName.trim() === selectedSession.name
									}
									onClick={renameSelectedSession}
								>
									{t("common.rename")}
								</Button>
							</Stack>
						) : null}
					</Stack>
				</UIBanner>
			);
		}
	}
}

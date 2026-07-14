import { Application } from "@/context/Application.context";
import { Banner as UIBanner } from "@/ui/Banner";
import { ChangeEvent, useEffect, useMemo, useState } from "react";
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
import { Internal } from "@/entities/addon/Internal";
import { Label } from "@/ui/Label";
import { Select } from "@/ui/Select";
import { cn } from "@/ui/utils";
import { useNavigate } from "react-router-dom";
import { Locale } from "@/locales";

export namespace Session {
	export namespace Save {
		export namespace Banner {
			export type Props = UIBanner.Props;
		}
		export function Banner({ ...props }: Session.Save.Banner.Props) {
			const [name, setName] = useState<string>("");
			const [color, setColor] = useState<string>(Default.Color.OPERATION);
			const [icon, setIcon] = useState<Glyph.Id | null>(null);
			const { Info, app, spawnBanner } = Application.use();
			const { t } = Locale.use();
			const [loading, setLoading] = useState<boolean>(false);
			const navigate = useNavigate();

			const changenameHandler = (event: ChangeEvent<HTMLInputElement>) => {
				const { value } = event.target;

				setName(value);
			};

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
				const session = await Info.session_create({
					name,
					color,
					icon: Glyph.List.get(icon)!,
				});
				setLoading(false);
				if (session) {
					Logger.log(
						t("session.saved", { name }),
						Session.Save.Banner,
						{
							richColors: true,
							icon: <Icon name="Check" />,
						},
					);
					setTimeout(() => {
						logout();
					}, 250);
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
							valid={name.length > 0}
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
							disabled={!name.length || !icon}
							icon="Check"
						>
							{t("session.saveCurrent")}
						</Button>
						<Button
							variant="destructive"
							icon="LogOut"
							onClick={logout}
						>
							{t("session.dontSave")}
						</Button>
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
			const [sessions, setSessions] = useState<Internal.Session.Data[]>([]);
			const [isDataLoading, setIsDataLoading] = useState<boolean>(true);
			const [isDataDeleating, setIsDataDeleating] = useState<boolean>(false);
			const [selected, setSelected] = useState<Set<string>>(new Set());

			const reload = async () => {
				setIsDataLoading(true);
				await Info.session_list().then(setSessions);
				setSelected(() => new Set());
				setIsDataLoading(false);
			};

			useEffect(() => {
				reload();
			}, []);

			const deleteSessionButtonClickHandler = async () => {
				setIsDataDeleating(true);
				await Info.sessions_delete([...selected.values()]).then(() => {
					Logger.log(
						t("session.deleted", { count: selected.size }),
						"Session.Delete.Banner.deleteSessionButtonClickHandler",
						{
							richColors: true,
							icon: <Icon name="Check" />,
						},
					);
				});
				setIsDataDeleating(false);
				await reload();
				onClose?.();
				destroyBanner();
			};

			const DeleteButton = useMemo(
				() => (
					<Button
						onClick={deleteSessionButtonClickHandler}
						loading={isDataDeleating || isDataLoading}
						disabled={!selected.size}
						icon="Trash"
						variant="glass"
					/>
				),
				[
					selected,
					isDataDeleating,
					isDataLoading,
					deleteSessionButtonClickHandler,
				],
			);

			return (
				<UIBanner
					title={t("session.deleteTitle")}
					done={DeleteButton}
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
								Operation.Entity.selected(app) &&
								sessions.filter(
									(session) =>
										session.selected.operations &&
										session.selected.operations ===
											Operation.Entity.selected(app)?.id,
								).length &&
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
											key={session.name}
											value={session.name}
											style={{ color: session.color }}
										>
											<Select.Icon name={session.icon} />
											{session.name}
										</Select.Item>
									))}
								</Select.Content>
							</Select.Multi.Root>
						</Stack>
					</Stack>
				</UIBanner>
			);
		}
	}
}

import { useEffect, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { Button as UIButton } from "@/ui/Button";
import { Application } from "@/context/Application.context";
import { Input } from "@/ui/Input";
import { toast } from "sonner";
import { GulpDataset, Pattern } from "@/class/Info";
import { Icon } from "@/ui/Icon";
import { capitalize, cn } from "@/ui/utils";
import s from "./styles/AuthPage.module.css";
import { Banner as UIBanner } from "@/ui/Banner";
import { Stack } from "@/ui/Stack";
import { User } from "@/entities/User";
import { Internal } from "@/entities/addon/Internal";
import { Shimmer } from "@/ui/Shimmer";
import { Locale } from "@/locales";

import { Logo } from "@/components/Logo";

export namespace Auth {
	export namespace Page {
		export interface Props {}
	}

	export function Page(_: Auth.Page.Props) {
		const navigate = useNavigate();
		const [searchParams] = useSearchParams();
		const redirectPath = searchParams.get("redirect");

		const { Info, app } = Application.use();
		const { t } = Locale.use();
		const [server, setServer] = useState<string>(Info.app.general.server);
		const [id, setId] = useState("admin" as User.Id);
		const [password, setPassword] = useState<string>("");
		const [showPassword, setShowPassword] = useState(false);
		const [loading, setLoading] = useState<boolean>(false);
		const [methods, setMethods] =
			useState<GulpDataset.GetAvailableLoginApi.Response>([]);
		useEffect(() => {
			Internal.Settings.server = server;
			api<GulpDataset.GetAvailableLoginApi.Response>(
				"/get_available_login_api",
				{
					authRedirect: false,
					showErrorToast: false,
					toast: {
						onError: (payload) =>
							payload.status === "error"
								? toast.warning(t("auth.pluginNotConfigured"), {
										description: t("auth.pluginConfigHint"),
										icon: <Icon name="Warning" />,
										richColors: true,
									})
								: undefined,
					},
				},
				setMethods,
			);
		}, []);

		const handleServerBlur = () => {
			if (!server) return;
			const removeOverload = (str: string): string =>
				str.endsWith("/") ? removeOverload(str.slice(0, -1)) : str;

			const validatedServer = Pattern.Server.test(server)
				? removeOverload(server)
				: server;

			Internal.Settings.server = validatedServer;
			window.dispatchEvent(new CustomEvent("gulp-server-changed"));

			setMethods([]);
			api<GulpDataset.GetAvailableLoginApi.Response>(
				"/get_available_login_api",
				{
					authRedirect: false,
					showErrorToast: false,
					toast: {
						onError: (payload) =>
							payload.status === "error"
								? toast.warning(t("auth.pluginNotConfigured"), {
										description: t("auth.pluginConfigHint"),
										icon: <Icon name="Warning" />,
										richColors: true,
									})
								: undefined,
					},
				},
				(data) => {
					if (Array.isArray(data)) {
						setMethods(data);
					} else {
						setMethods([]);
					}
				},
			);
		};

		useEffect(() => {
			const savedToken = localStorage.getItem("__token");
			const savedUserId = localStorage.getItem("__user_id");

			if (
				savedToken &&
				savedToken !== "-" &&
				savedUserId &&
				!app.general.user
			) {
				const autoLogin = async () => {
					try {
						const userProfile = await Info.user_get_by_id(savedUserId);
						if (userProfile) {
							Info.setInfoByKey(userProfile, "general", "user");
						}
					} catch (e) {
						// API error handler in API.tsx handles redirection on 401
					}
				};
				autoLogin();
				return;
			}

			if (app.general.user) {
				/**
				 * Initializes shared plugin/glyph state then redirects.
				 * If a redirect path was encoded in the URL query (e.g. returning from
				 * a protected route), honour it — otherwise send the user to the home page.
				 */
				const initializeAndRedirect = async () => {
					if (app.target.operations.length === 0) {
						await Info.plugin_list();
						await Info.glyphs_reload();
						await Info.sync();
					}

					if (redirectPath) {
						navigate(decodeURIComponent(redirectPath));
					} else {
						navigate("/");
					}
				};

				initializeAndRedirect();
			}
		}, [app.general.user, redirectPath, navigate]);

		const login = async () => {
			const removeOverload = (str: string): string =>
				str.endsWith("/") ? removeOverload(str.slice(0, -1)) : str;

			const validate = (str: string): string | void =>
				!Pattern.Server.test(str)
					? (() => {
							toast(t("auth.incorrectServerUrl"), {
								icon: <Icon name="Warning" />,
							});
						})()
					: removeOverload(str);

			const validatedServer = validate(server);

			if (!validatedServer) return;

			Internal.Settings.server = validatedServer;

			// Wrap into loading state
			setLoading(true);
			const user = await Info.login({ id, password });
			setLoading(false);

			// Redirect is handled by the initializeAndRedirect effect above
			// which fires when app.general.user becomes truthy.
		};

		/**
		 * Renders the login submit button.
		 * Post-login navigation is handled by the initializeAndRedirect effect;
		 * no additional buttons are needed here.
		 */
		const NextButton = () => (
			<Stack
				jc="flex-end"
				dir="row"
				gap={6}
				style={{ marginLeft: "auto" }}
			>
				<UIButton
					icon="LogIn"
					disabled={!!app.general.user || !id || !password}
					variant="glass"
					revert
					loading={loading}
					tabIndex={4}
					onClick={login}
				>
					{t("auth.login")}
				</UIButton>
			</Stack>
		);

		useEffect(() => {
			const query = new URLSearchParams(window.location.search);
			const token = query.get("token");

			if (!token) return;

			history.replaceState(null, "", window.location.origin);

			setLoading(true);
		}, []);

		const [customLoading, setCustomLoading] = useState<string | null>(null);

		const customLoginConstructor = (url: string) => () => {
			const x = new URLSearchParams();
			x.append("client", window.location.origin);
			x.append("ws_id", Info.app.general.ws_id);
			setCustomLoading(url);
			window.location.replace(`${Internal.Settings.server}${url}?${x}`);
		};

		const LoginMethods = () => {
			if (
				methods.length === 0 ||
				(methods.length === 1 && methods[0].name === "gulp")
			) {
				return null;
			}
			handleServerBlur();

			function LoginMethod({ name, icon }: LoginMethod.Props) {
				const method = methods.find((method) => method.name === name);
				if (!method) {
					return null;
				}

				return (
					<UIButton
						onClick={customLoginConstructor(method.login.url)}
						loading={customLoading === method.login.url}
						style={{ flex: 1 }}
						variant="glass"
						icon={icon}
					>
						{t("auth.loginWith", { provider: capitalize(name) })}
					</UIButton>
				);
			}

			return (
				<Stack>
					<LoginMethod
						name="microsoft"
						icon="LogoMicrosoft"
					/>
					<LoginMethod
						name="google"
						icon="LogoGoogle"
					/>
				</Stack>
			);
		};

		return (
			<Stack
				className={s.wrapper}
				dir="column"
				ai="center"
				jc="center"
			>
				<Logo />
				<Shimmer
					duration={2}
					className={s.title}
					as="p"
					color="var(--gray-800)"
				>
					{t("auth.title")}
				</Shimmer>
				<Input
					variant="highlighted"
					icon="Link"
					label={t("auth.serverAddress")}
					placeholder="http://localhost:8080"
					value={server}
					disabled={!!app.general.user}
					tabIndex={1}
					onChange={(e) => setServer(e.currentTarget.value)}
					onKeyDown={(e) =>
						e.key === "Enter" && id && password && !loading && login()
					}
					onBlur={handleServerBlur}
				/>
				<Input
					variant="highlighted"
					label={t("auth.username")}
					icon="User"
					placeholder={t("auth.usernamePlaceholder")}
					value={id}
					disabled={!!app.general.user}
					tabIndex={2}
					onChange={(e) => setId(e.currentTarget.value as typeof id)}
					onKeyDown={(e) =>
						e.key === "Enter" && id && password && !loading && login()
					}
				/>
				<Input
					variant="highlighted"
					icon="KeyRound"
					label={t("auth.password")}
					placeholder={t("auth.passwordPlaceholder")}
					type={showPassword ? "text" : "password"}
					value={password}
					endIcon={showPassword ? "EyeOff" : "Eye"}
					endIconTitle={
						showPassword ? t("auth.hidePassword") : t("auth.showPassword")
					}
					onEndIconClick={() => setShowPassword((current) => !current)}
					disabled={!!app.general.user}
					tabIndex={3}
					onChange={(e) => setPassword(e.currentTarget.value)}
					onKeyDown={(e) =>
						e.key === "Enter" && id && password && !loading && login()
					}
				/>
				<LoginMethods />

				<NextButton />
			</Stack>
		);
	}

	export namespace Banner {
		export interface Props extends UIBanner.Props {}
	}

	export function Banner({ className, ...props }: Banner.Props) {
		const { Info, app } = Application.use();
		const { t } = Locale.use();
		const [server, setServer] = useState<string>(Info.app.general.server);
		const [id, setId] = useState("admin" as User.Id);
		const [password, setPassword] = useState<string>("admin");
		const [showPassword, setShowPassword] = useState(false);
		const [loading, setLoading] = useState<boolean>(false);
		const [methods, setMethods] =
			useState<GulpDataset.GetAvailableLoginApi.Response>([]);

		useEffect(() => {
			Internal.Settings.server = server;
			api<GulpDataset.GetAvailableLoginApi.Response>(
				"/get_available_login_api",
				{
					authRedirect: false,
					showErrorToast: false,
					toast: {
						onError: (payload) =>
							payload.status === "error"
								? toast.warning(t("auth.pluginNotConfigured"), {
										description: t("auth.pluginConfigHint"),
										icon: <Icon name="Warning" />,
										richColors: true,
									})
								: undefined,
					},
				},
				setMethods,
			);
		}, []);

		const handleServerBlur = () => {
			if (!server) return;
			const removeOverload = (str: string): string =>
				str.endsWith("/") ? removeOverload(str.slice(0, -1)) : str;

			const validatedServer = Pattern.Server.test(server)
				? removeOverload(server)
				: server;

			Internal.Settings.server = validatedServer;
			window.dispatchEvent(new CustomEvent("gulp-server-changed"));

			setMethods([]);
			api<GulpDataset.GetAvailableLoginApi.Response>(
				"/get_available_login_api",
				{
					authRedirect: false,
					showErrorToast: false,
					toast: {
						onError: (payload) =>
							payload.status === "error"
								? toast.warning(t("auth.pluginNotConfigured"), {
										description: t("auth.pluginConfigHint"),
										icon: <Icon name="Warning" />,
										richColors: true,
									})
								: undefined,
					},
				},
				(data) => {
					if (Array.isArray(data)) {
						setMethods(data);
					} else {
						setMethods([]);
					}
				},
			);
		};

		const login = async () => {
			const removeOverload = (str: string): string =>
				str.endsWith("/") ? removeOverload(str.slice(0, -1)) : str;

			const validate = (str: string): string | void =>
				!Pattern.Server.test(str)
					? (() => {
							toast(t("auth.incorrectServerUrl"), {
								icon: <Icon name="Warning" />,
							});
						})()
					: removeOverload(str);

			const validatedServer = validate(server);

			if (!validatedServer) return;

			Internal.Settings.server = validatedServer;

			// Wrap into loading state
			setLoading(true);
			await Info.login({ id, password });
			setLoading(false);
		};

		const NextButton = () => (
			<UIButton
				icon="LogIn"
				disabled={!id || !password}
				variant="glass"
				loading={loading}
				tabIndex={4}
				onClick={login}
			/>
		);

		const [customLoading, setCustomLoading] = useState<string | null>(null);

		const customLoginConstructor = (url: string) => () => {
			const x = new URLSearchParams();
			x.append("client", window.location.origin);
			x.append("ws_id", Info.app.general.ws_id);
			setCustomLoading(url);
			window.location.replace(`${Internal.Settings.server}${url}?${x}`);
		};

		const LoginMethods = () => {
			if (
				methods.length === 0 ||
				(methods.length === 1 && methods[0].name === "gulp")
			) {
				return null;
			}

			function LoginMethod({ name, icon }: LoginMethod.Props) {
				const method = methods.find((method) => method.name === name);
				if (!method) {
					return null;
				}

				return (
					<UIButton
						onClick={customLoginConstructor(method.login.url)}
						loading={customLoading === method.login.url}
						style={{ flex: 1 }}
						variant="glass"
						icon={icon}
					>
						{t("auth.loginWith", { provider: capitalize(name) })}
					</UIButton>
				);
			}

			return (
				<Stack>
					<LoginMethod
						name="microsoft"
						icon="LogoMicrosoft"
					/>
					<LoginMethod
						name="google"
						icon="LogoGoogle"
					/>
				</Stack>
			);
		};

		return (
			<UIBanner
				done={<NextButton />}
				className={cn(className, s.banner)}
				{...props}
			>
				<Stack
					className={s.wrapper}
					dir="column"
					ai="center"
					jc="center"
				>
					<Shimmer
						duration={2}
						className={s.title}
						as="p"
						color="var(--gray-800)"
					>
						{t("auth.title")}
					</Shimmer>
					<Input
						variant="highlighted"
						icon="Link"
						label={t("auth.serverAddress")}
						placeholder="http://localhost:8080"
						value={server}
						disabled={!!app.general.user}
						tabIndex={1}
						onChange={(e) => setServer(e.currentTarget.value)}
						onKeyDown={(e) =>
							e.key === "Enter" && id && password && !loading && login()
						}
						onBlur={handleServerBlur}
					/>
					<Input
						variant="highlighted"
						label={t("auth.username")}
						icon="User"
						placeholder={t("auth.usernamePlaceholder")}
						value={id}
						disabled={!!app.general.user}
						tabIndex={2}
						onChange={(e) => setId(e.currentTarget.value as typeof id)}
						onKeyDown={(e) =>
							e.key === "Enter" && id && password && !loading && login()
						}
					/>
					<Input
						variant="highlighted"
						icon="KeyRound"
						label={t("auth.password")}
						placeholder={t("auth.passwordPlaceholder")}
						type={showPassword ? "text" : "password"}
						value={password}
						endIcon={showPassword ? "EyeOff" : "Eye"}
						endIconTitle={
							showPassword ? t("auth.hidePassword") : t("auth.showPassword")
						}
						onEndIconClick={() => setShowPassword((current) => !current)}
						disabled={!!app.general.user}
						tabIndex={3}
						onChange={(e) => setPassword(e.currentTarget.value)}
						onKeyDown={(e) =>
							e.key === "Enter" && id && password && !loading && login()
						}
					/>
					<LoginMethods />
				</Stack>
			</UIBanner>
		);
	}
}

namespace LoginMethod {
	export interface Props {
		name: string;
		icon: Icon.Name;
	}
}

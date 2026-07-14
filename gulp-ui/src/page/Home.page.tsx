import { useCallback, useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { Application } from "@/context/Application.context";
import { Menu, MenuItem } from "@/components/menu";
import { Session } from "@/banners/Session.banner";
import { Settings } from "@/banners/Settings.banner";
import s from "./styles/Home.module.css";
import { Stack } from "@/ui/Stack";
import { Resizer } from "@/ui/Resizer";
import { Locale } from "@/locales";
import {
	BackButton,
	GroupsList,
	HeaderAction,
	HomeContent,
	OperationsList,
	PluginConfigurationsList,
	UsersList,
} from "@/components/HomeContent";

export namespace Home {
	export namespace Page {
		export interface Props {
			/** Section rendered by the Home shell for the current route. */
			section?: HomeContent.Section;
		}
	}

	/**
	 * Root home page — displayed immediately after a successful login.
	 *
	 * Responsibilities:
	 * 1. Fetch plugin list, glyphs, and the operation list via Info.sync().
	 * 2. Render a 3-panel skeleton layout:
	 *    - Left: navigation menu for Operations, Users, and Groups.
	 *    - Main: active content table selected by the current Home route.
	 *    - Right: docked detail drawer when a row detail view is open.
	 */

	export function Page({ section = "operations" }: Home.Page.Props) {
		const { Info, app, spawnBanner, spawnDialog, dialog, banner } =
			Application.use();
		const { t } = Locale.use();
		const navigate = useNavigate();
		const location = useLocation();
		const [loading, setLoading] = useState(true);
		const isAdmin = Array.isArray(app.general.user?.permission)
			? app.general.user.permission.includes("admin")
			: false;

		/**
		 * On mount, initializes shared application state, mirroring OperationView's exact
		 * pattern: attempt auto-login from localStorage first (handles hard reload while
		 * the SmartSocket token hasn't been restored yet), then fetch plugins, glyphs,
		 * and the operations list.
		 */
		useEffect(() => {
			/**
			 * Initializes Home page data and restores the current user when needed.
			 *
			 * @returns A promise that resolves after all initial Home data has loaded.
			 */
			const initialize = async () => {
				// Replicate OperationView's auto-login guard: if a saved token/userId
				// exists but app.general.user hasn't been restored yet, re-hydrate the
				// user profile. This covers the hard-reload case where the SmartSocket
				// session hasn't reconnected before React mounts.
				if (app.target.operations.length === 0) {
					const savedUserId = localStorage.getItem("__user_id");
					if (savedUserId && !app.general.user) {
						try {
							const userProfile = await Info.user_get_by_id(savedUserId);
							if (userProfile) {
								Info.setInfoByKey(userProfile, "general", "user");
							}
						} catch {
							// API error handler in API.tsx handles redirection to /login on 401
							return;
						}
					}

					await Info.plugin_list();
					await Info.glyphs_reload();
					await Info.sync();
				}

				setLoading(false);
			};

			initialize();
		}, []);

		/**
		 * Navigates to the route that renders the requested Home section.
		 *
		 * @param nextSection - The section to render inside the main content area.
		 */
		const handleSectionChange = useCallback(
			(nextSection: HomeContent.Section) => {
				spawnDialog(null);
				const routes: Record<HomeContent.Section, string> = {
					operations: "/",
					users: "/users",
					groups: "/groups",
					pluginConfigurations: "/plugin-configurations",
				};
				navigate(routes[nextSection]);
			},
			[navigate, spawnDialog],
		);

		/**
		 * Resolves the title shown above the active Home content table.
		 *
		 * @returns A bracketed page title matching the existing Home style.
		 */
		const pageTitle = useMemo(() => {
			const titles: Record<HomeContent.Section, string> = {
				operations: t("home.operations.title"),
				users: `[ ${t("common.users")} ]`,
				groups: `[ ${t("common.groups")} ]`,
				pluginConfigurations: t("home.pluginConfigurations.title"),
			};
			return titles[section];
		}, [section, t]);

		/**
		 * Whether the current Home shell route should expose a shortcut back to
		 * the root Home content.
		 */
		const shouldShowBackButton = location.pathname !== "/";

		/**
		 * Resolves the active Home section content without remounting the list on
		 * unrelated parent re-renders such as banner or dialog state changes.
		 */
		const mainContent =
			section === "users" ? (
				<UsersList />
			) : section === "groups" ? (
				<GroupsList />
			) : section === "pluginConfigurations" ? (
				<PluginConfigurationsList />
			) : (
				<OperationsList loading={loading} />
			);

		/**
		 * Top area menu items for the Home page.
		 * Navigation only switches the Home main content panel.
		 */
		const menuTopItems = useMemo<MenuItem[]>(() => {
			const items: MenuItem[] = [
				{
					label: t("common.operations"),
					icon: "BookPlus",
					category: t("common.operations"),
					action: () => handleSectionChange("operations"),
				},
				{
					label: t("common.users"),
					icon: "User",
					category: t("common.administration"),
					action: () => handleSectionChange("users"),
				},
				{
					label: t("common.groups"),
					icon: "Users",
					category: t("common.administration"),
					action: () => handleSectionChange("groups"),
				},
			];

			if (isAdmin) {
				items.push({
					label: t("home.pluginConfigurations.menu"),
					icon: "AcronymJson",
					category: t("common.administration"),
					action: () => handleSectionChange("pluginConfigurations"),
				});
			}

			return items;
		}, [handleSectionChange, isAdmin, t]);

		/**
		 * Bottom area menu items for the Home page.
		 * Includes settings and the "LogOut" button that triggers the session
		 * save/logout flow.
		 */
		const menuBottomItems = useMemo<MenuItem[]>(
			() => [
				{
					label: t("settings.title"),
					icon: "Settings",
					category: t("common.account"),
					action: () =>
						spawnBanner(
							<Settings.Banner
								visibility={{
									timestamps: false,
									scroll: false,
									realtime: false,
								}}
							/>,
						),
				},
				{
					label: t("common.logout"),
					icon: "LogOut",
					category: t("common.account"),
					action: () => spawnBanner(<Session.Save.Banner />),
				},
			],
			[spawnBanner, t],
		);

		return (
			<div className={s.wrapper}>
				{/* Left navigation — data-driven Menu component */}
				<Menu
					topItems={menuTopItems}
					bottomItems={menuBottomItems}
				/>

				{/* Main content: active Home section */}
				<main className={s.main}>
					<div className={s.pageHeader}>
						<div className={s.pageTitleGroup}>
							{shouldShowBackButton ? <BackButton /> : null}
							<p className={s.pageTitle}>{pageTitle}</p>
						</div>
						<div className={s.pageActions}>
							<HeaderAction section={section} />
						</div>
					</div>
					{mainContent}
				</main>

				{/* Right drawer placeholder — reserved for the upcoming detail panel */}
				{dialog ? (
					<Stack
						className={s.dialog}
						style={{ width: app.timeline.dialogSize }}
						pos="relative"
					>
						<Resizer
							init={app.timeline.dialogSize}
							set={Info.setDialogSize}
						/>
						{dialog}
					</Stack>
				) : null}

				{banner?.target === "table" ? banner.node : null}
			</div>
		);
	}
}

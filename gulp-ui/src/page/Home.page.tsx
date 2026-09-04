import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Navigate, useLocation, useNavigate } from "react-router-dom";
import { Application } from "@/context/Application.context";
import { Extension } from "@/context/Extension.context";
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
	UsersList,
} from "@/components/HomeContent";

const EmptyPermissions: readonly string[] = [];

export namespace Home {
	export namespace Page {
		export interface Props {
			/** Built-in section rendered by the Home shell for the current route. */
			section?: HomeContent.Section;
			/** Whether the current pathname must resolve through a Home extension target. */
			extensionRoute?: boolean;
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

	export function Page({
		section = "operations",
		extensionRoute = false,
	}: Home.Page.Props) {
		const { Info, app, spawnBanner, spawnDialog, dialog, banner } =
			Application.use();
		const { extensions, ready: extensionsReady } = Extension.use();
		const { t } = Locale.use();
		const navigate = useNavigate();
		const location = useLocation();
		const dialogRef = useRef<HTMLDivElement>(null);
		const [loading, setLoading] = useState(true);
		const userPermissions = app.general.user?.permission ?? EmptyPermissions;
		const accessibleHomeSectionMounts = useMemo(
			() =>
				Extension.getAccessibleHomeSectionMounts(
					extensions,
					userPermissions,
				),
			[extensions, userPermissions],
		);
		const activeHomeSectionMount = useMemo(() => {
			if (!extensionRoute) return undefined;
			return Extension.resolveHomeSectionRoute(
				accessibleHomeSectionMounts,
				location.pathname,
			);
		}, [accessibleHomeSectionMounts, extensionRoute, location.pathname]);

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
				};
				navigate(routes[nextSection]);
			},
			[navigate, spawnDialog],
		);

		/**
		 * Opens a manifest-provided Home extension route and clears the current drawer.
		 *
		 * @param route - Validated route owned by a Home extension target.
		 */
		const handleHomeSectionChange = useCallback(
			(route: string) => {
				spawnDialog(null);
				navigate(route);
			},
			[navigate, spawnDialog],
		);

		/**
		 * Resolves the title shown above the active Home content table.
		 *
		 * @returns A bracketed page title matching the existing Home style.
		 */
		const pageTitle = useMemo(() => {
			if (activeHomeSectionMount) {
				return t(activeHomeSectionMount.target.titleKey);
			}

			const titles: Record<HomeContent.Section, string> = {
				operations: t("home.operations.title"),
				users: `[ ${t("common.users")} ]`,
				groups: `[ ${t("common.groups")} ]`,
			};
			return titles[section];
		}, [activeHomeSectionMount, section, t]);

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
			activeHomeSectionMount ? (
				<Extension.Component
					name={activeHomeSectionMount.extension.filename}
					props={{
						slot: Extension.Slot.HomeSection,
						target: activeHomeSectionMount.target,
					}}
				/>
			) : section === "users" ? (
				<UsersList />
			) : section === "groups" ? (
				<GroupsList />
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
					icon: "ActivitySquare",
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

			for (const mount of accessibleHomeSectionMounts) {
				items.push({
					label: t(mount.target.labelKey),
					icon: mount.target.icon,
					category: t(mount.target.group ?? "common.plugins"),
					action: () => handleHomeSectionChange(mount.target.route),
				});
			}

			return items;
		}, [
			accessibleHomeSectionMounts,
			handleHomeSectionChange,
			handleSectionChange,
			t,
		]);

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

		if (extensionRoute && (!extensionsReady || loading)) {
			return null;
		}

		if (extensionRoute && !activeHomeSectionMount) {
			return (
				<Navigate
					to="/"
					replace
				/>
			);
		}

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
							{activeHomeSectionMount ? null : (
								<HeaderAction section={section} />
							)}
						</div>
					</div>
					{mainContent}
				</main>

				{/* Right drawer placeholder — reserved for the upcoming detail panel */}
				{dialog ? (
					<Stack
						ref={dialogRef}
						className={s.dialog}
						style={{ width: app.timeline.dialogSize }}
						pos="relative"
						flex="0 1 auto"
					>
						<Resizer
							init={app.timeline.dialogSize}
							set={Info.setDialogSize}
							target={dialogRef}
						/>
						{dialog}
					</Stack>
				) : null}

				{banner?.target === "table" ? banner.node : null}
			</div>
		);
	}
}

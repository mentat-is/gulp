import React, {
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import { useParams, useNavigate } from "react-router-dom";
import { cn } from "@/ui/utils";
import { Application } from "../context/Application.context";
import { Preloader } from "../components/Preloader";
import s from "../App.module.css";
import { Stack } from "@/ui/Stack";
import { Menu, MenuItem, PluginNode } from "../components/menu";
import { Timeline } from "../app/body/Timeline";
import { Resizer } from "../ui/Resizer";
import { Hint } from "../dialogs/Hint.dialog";
import type { WindowBridge } from "../lib/WindowBridge";
import { Logger } from "../dto/Logger.class";
import { Source } from "../entities/Source";
import { SelectFiles } from "../banners/SelectFiles.banner";
import { UploadBanner } from "../banners/Upload.banner";
import { FilterFileBanner } from "../banners/FilterFile.banner";
import { QueryExternal } from "../banners/QueryExternal.banner";
import { BridgeManager } from "../banners/BridgeManager.banner";
import { Enrichment } from "../banners/Enrichment.banner";
import { Sigma } from "../banners/Sigma";
import { Requests } from "../banners/Requests.banner";
import { OperationPermissions } from "../banners/Permissions.banner";
import { Operation } from "../entities/Operation";
import { Settings } from "../banners/Settings.banner";
import { Session } from "../banners/Session.banner";
import { Extension } from "../context/Extension.context";
import { Doc } from "../entities/Doc";
import { DisplayEventDialog } from "../dialogs/Event.dialog";
import {
	DisplayGroupDialog,
	type DisplayGroupKind,
} from "../dialogs/Group.dialog";
import { Locale } from "@/locales";
import { DetachedWindow } from "@/context/DetachedWindow.context";

function createDetachedDialogDescriptor(
	dialog: React.ReactNode,
): WindowBridge.DetachedDialogDescriptor | null {
	if (!React.isValidElement(dialog)) return null;
	if (dialog.type === DisplayEventDialog) {
		const { event } = dialog.props as { event?: Doc.Type };
		return event ? { kind: "event", event } : null;
	}
	if (dialog.type === DisplayGroupDialog) {
		const {
			events,
			groupKind = "events",
		} = dialog.props as {
			events?: Doc.Type[];
			groupKind?: DisplayGroupKind;
		};
		return events
			? {
					kind: "group",
					events,
					groupKind,
				}
			: null;
	}
	return null;
}

export function OperationView() {
	const { operation_id } = useParams<{ operation_id: string }>();
	const navigate = useNavigate();
	const { app, Info, spawnBanner } = Application.use();
	const [initialized, setInitialized] = useState(false);

	// Sync route param with global selection state when operation is swapped in UI
	const selectedOpId = Operation.Entity.selected(app)?.id;

	useEffect(() => {
		if (!operation_id) return;
		const currentOperationId = operation_id as Operation.Id;
		return () => {
			void Info.cancelOperationQueries(currentOperationId);
		};
	}, [Info, operation_id]);

	useEffect(() => {
		if (selectedOpId && selectedOpId !== operation_id) {
			navigate(`/operations/${selectedOpId}`);
		}
	}, [selectedOpId, operation_id, navigate]);

	useEffect(() => {
		if (!operation_id) return;

		/**
		 * Internal async function to check for active sessions and configure selection banner.
		 */
		const initializeOperation = async () => {
			// 0. If operations are not loaded yet (e.g. page refreshed), fetch them from backend
			if (app.target.operations.length === 0) {
				const savedUserId = localStorage.getItem("__user_id");
				if (savedUserId && !app.general.user) {
					try {
						const userProfile = await Info.user_get_by_id(savedUserId);
						if (userProfile) {
							Info.setInfoByKey(userProfile, "general", "user");
						}
					} catch (e) {
						// API error handler in API.tsx handles redirection to /login on 401
						return;
					}
				}

				await Info.plugin_list();
				await Info.glyphs_reload();
				await Info.sync();
			}

			// 1. Ensure the operation is selected in the global state
			const currentOp = Operation.Entity.selected(app);
			const isNewOperation = !currentOp || currentOp.id !== operation_id;
			if (isNewOperation) {
				Info.operations_select(operation_id as Operation.Id);
			}

			// 2. Check if a session was manually loaded or timeline was skipped
			const hasSelectedFiles = Source.Entity.selected(app).length > 0;
			if ((!isNewOperation && hasSelectedFiles) || app.general.skippedAuth) {
				if (app.general.skippedAuth) {
					Info.setInfoByKey(false, "general", "skippedAuth");
				}
				setInitialized(true);
				return;
			}

			// 3. Present the SelectFiles banner to let user pick files/contexts or load a session
			spawnBanner(<SelectFiles.Banner fixed />);
			setInitialized(true);
		};

		initializeOperation();
	}, [operation_id]);

	if (!initialized) {
		return null;
	}

	return <OperationTimeline />;
}

export function OperationTimeline() {
	const {
		Info,
		app,
		dialog,
		dialogsDocked,
		hintOpen,
		setHintOpen,
		setDialogsDocked,
		spawnBanner,
		toggleHintOpen,
	} = Application.use();
	const { t } = Locale.use();
	const { extensions } = Extension.use();
	const detachedWindows = DetachedWindow.use();
	const dialogRef = useRef<HTMLDivElement>(null);

	const [isPreloaded, setIsPreloaded] = useState(false);
	const detachedDialogDescriptor = useMemo(
		() => createDetachedDialogDescriptor(dialog),
		[dialog],
	);

	useEffect(() => {
		if (isPreloaded) return;

		setTimeout(() => {
			setIsPreloaded(true);
		}, 500);
	}, [isPreloaded]);

	useEffect(() => {
		const root = document.getElementById("root");
		if (!root) {
			return Logger.error("ROOT_NOT_FOUND");
		}

		root.classList[app.hidden.toasts ? "add" : "remove"]("hidden_toats");
	}, [app.hidden.toasts]);

	useEffect(() => {
		if (dialogsDocked || !dialog) {
			detachedWindows.closeDialogWindow();
			return;
		}

		if (!detachedDialogDescriptor) {
			// Custom React dialogs cannot be serialized into an independent
			// browser context, so keep them safely docked.
			setDialogsDocked(true);
			return;
		}

		const width = Math.max(app.timeline.dialogSize, 480);
		if (
			!detachedWindows.openDialogWindow(
				detachedDialogDescriptor,
				`width=${width},height=${Math.max(window.innerHeight - 160, 640)},left=${Math.max(window.innerWidth - width - 48, 48)},top=60`,
			)
		) {
			setDialogsDocked(true);
		}
	}, [
		app.timeline.dialogSize,
		detachedDialogDescriptor,
		detachedWindows,
		dialog,
		dialogsDocked,
		setDialogsDocked,
	]);

	/**
	 * Builds the extension plugin nodes for the Menu's plugin section.
	 * Iterates the `extensions` registry directly (already consumed at the top
	 * level via `Extension.use()`) to avoid calling `Extension.Components` — a
	 * React component that uses `useContext` internally — inside `useMemo`,
	 * which would violate the Rules of Hooks.
	 */
	const pluginNodes = useMemo<PluginNode[]>(
		() =>
			Extension.getBySlot(extensions, Extension.Slot.OperationMenu).map(
				(ext) => {
					return {
						node: (
							<Extension.Component
								key={ext.filename}
								name={ext.filename}
							/>
						),
						title: ext.display_name || ext.filename,
					};
				},
			),
		[extensions],
	);

	/**
	 * Opens the permissions banner for the currently selected operation.
	 *
	 * @returns A promise that resolves after operation details are loaded and the banner is spawned.
	 */
	const openSelectedOperationPermissionsBanner =
		useCallback(async (): Promise<void> => {
			const selectedOperation = app.target.operations.find(
				(operation) => operation.selected,
			);
			if (!selectedOperation) {
				Logger.error(
					"No operation selected",
					"OperationTimeline.openSelectedOperationPermissionsBanner",
				);
				return;
			}

			try {
				const details = await Info.operation_get_by_id(selectedOperation.id);
				spawnBanner(
					<OperationPermissions.Banner
						operationId={selectedOperation.id}
						granted_user_ids={details.granted_user_ids ?? []}
						granted_user_group_ids={details.granted_user_group_ids ?? []}
					/>,
				);
			} catch (error) {
				Logger.error(
					t("operationDetails.loadFailed"),
					"OperationTimeline.openSelectedOperationPermissionsBanner",
					{
						description:
							error instanceof Error ? error.message : String(error),
					},
				);
			}
		}, [Info, app.target.operations, spawnBanner, t]);

	/**
	 * Top area menu items for the MainDashboard, grouped by category.
	 * Categories map to the original section headers: "Sources/filter", "External", "Plugins".
	 */
	const menuTopItems = useMemo<MenuItem[]>(
		() => [
			{
				label: t("operationView.menu.openNotesWindow"),
				icon: "FileText",
				category: t("operationView.menu.notesSources"),
				action: () => detachedWindows.openNotesWindow(),
			},
			{
				label: t("operationView.menu.openTableWindow"),
				icon: "Table",
				category: t("operationView.menu.notesSources"),
				action: () => detachedWindows.openTableWindow(),
			},
			{
				label: t("operationView.menu.openDashboardWindow"),
				icon: "AreaChart",
				category: t("operationView.menu.notesSources"),
				action: () => detachedWindows.openDashboardWindow(),
			},
			{
				label: t("operationView.menu.selectFilesAndContexts"),
				icon: "FileStack",
				category: t("operationView.menu.sourcesFilter"),
				action: () => spawnBanner(<SelectFiles.Banner />),
			},
			{
				label: t("operationView.menu.uploadFiles"),
				icon: "Upload",
				category: t("operationView.menu.sourcesFilter"),
				action: () => spawnBanner(<UploadBanner />),
			},
			{
				label: t("operationView.menu.applyFilters"),
				icon: "Filter",
				category: t("operationView.menu.sourcesFilter"),
				action: () => spawnBanner(<FilterFileBanner sources={[]} />),
			},
			{
				label: t("operationView.menu.queryExternalSource"),
				icon: "ServerCrash",
				category: t("operationView.menu.external"),
				action: () => spawnBanner(<QueryExternal.Banner />),
			},
			{
				label: t("operationView.menu.bridgeManager"),
				icon: "Network",
				category: t("operationView.menu.external"),
				action: () => spawnBanner(<BridgeManager.Banner />),
			},
			{
				label: t("operationView.menu.dataEnrichment"),
				icon: "PrismColor",
				category: t("operationView.menu.external"),
				action: () => spawnBanner(<Enrichment.Banner />),
			},
			{
				label: t("operationView.menu.uploadSigmaRule"),
				icon: "Sigma",
				category: t("common.plugins"),
				action: () => spawnBanner(<Sigma.Banner sources={[]} />),
			},
		],
		[spawnBanner, detachedWindows, t],
	);

	/**
	 * Bottom area menu items for the MainDashboard.
	 * These are pinned at the bottom of the side navigation panel.
	 */
	const menuBottomItems = useMemo<MenuItem[]>(
		() => [
			
			{
				label: t("operationView.menu.managePermissions"),
				icon: "LucideUserRoundPlus",
				category: t("operationView.menu.configuration"),
				action: () => void openSelectedOperationPermissionsBanner(),
			},
			{
				label: t("operationView.menu.backToOperations"),
				icon: "ActivitySquare",
				category: t("operationView.menu.configuration"),
				action: () => spawnBanner(<Operation.Select.Banner />),
			},
			{
				label: t("settings.title"),
				icon: "Settings",
				category: t("operationView.menu.configuration"),
				action: () => spawnBanner(<Settings.Banner />),
			},
			{
				label: hintOpen
					? t("operationView.menu.hideUsageInstructions")
					: t("operationView.menu.showUsageInstructions"),
				icon: "Info",
				category: t("operationView.menu.configuration"),
				action: toggleHintOpen,
			},
			{
				label: t("operationView.menu.requests"),
				icon: "Activity",
				category: t("operationView.menu.configuration"),
				action: () => spawnBanner(<Requests.Banner />),
			},
			{
				label: t("common.logout"),
				icon: "LogOut",
				category: t("operationView.menu.configuration"),
				action: () => spawnBanner(<Session.Save.Banner />),
			},
		],
		[spawnBanner, hintOpen, openSelectedOperationPermissionsBanner, toggleHintOpen, t],
	);

	if (!isPreloaded) {
		return <Preloader />;
	}

	return (
		<Stack
			gap={12}
			className={s.window}
			ai="stretch"
		>
			<Menu
				topItems={menuTopItems}
				bottomItems={menuBottomItems}
				pluginNodes={pluginNodes}
			/>
			<Stack
				className={s.workspace}
				flex
				ai="stretch"
				gap={12}
			>
				<Timeline stableCanvasWidth />
				{dialogsDocked && dialog ? (
					<Stack
						ref={dialogRef}
						className={cn(s.dialog)}
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
			</Stack>
			{hintOpen ? <Hint.Dialog onClose={() => setHintOpen(false)} /> : null}
		</Stack>
	);
}

// Simple toast mock/fallback if needed, though toast should be imported from sonner or global
const toast = (msg: string) => {
	import("sonner").then(({ toast: t }) => t(msg));
};

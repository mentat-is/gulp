import React, {
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import ReactDOM from "react-dom/client";
import { useParams, useNavigate } from "react-router-dom";
import { cn } from "@/ui/utils";
import { useTheme } from "next-themes";
import { Application } from "../context/Application.context";
import { Preloader } from "../components/Preloader";
import s from "../App.module.css";
import { Stack } from "@/ui/Stack";
import { Menu, MenuItem, PluginNode } from "../components/menu";
import { Timeline } from "../app/body/Timeline";
import { Resizer } from "../ui/Resizer";
import { Hint } from "../dialogs/Hint.dialog";
import { DetachedAppProvider } from "../context/DetachedApp.provider";
import { DataStore } from "../store/DataStore";
import { WindowBridge } from "../lib/WindowBridge";
import { RenderEngine } from "../class/RenderEngine";
import { Color } from "../entities/Color";
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
import { Note } from "../entities/Note";
import { Doc } from "../entities/Doc";
import { NotePoint } from "../ui/Note";
import { DisplayEventDialog } from "../dialogs/Event.dialog";
import { Locale } from "@/locales";
import { DetachedWindow } from "@/context/DetachedWindow.context";
import { applyThemeToWindow } from "@/utils";

/**
 * FetchEventBannerMain — fetches a note-linked event from the server and opens its dialog
 * in the main tab. Triggered when the detached NotesWindow sends TARGET_NOTE for an event
 * that is not currently loaded in the main tab's timeline.
 *
 * @param docId - The document ID of the event to fetch.
 * @param operationId - The operation ID under which the event was ingested.
 */
function FetchEventBannerMain({
	docId,
	operationId,
}: {
	docId: Doc.Id;
	operationId: Operation.Id;
}) {
	// Construct a minimal Note.Type shell so we can reuse the existing FetchEventBanner component.
	const shell = {
		doc: { _id: docId },
		operation_id: operationId,
		name: docId,
	} as unknown as Note.Type;

	return <NotePoint.FetchEventBanner note={shell} />;
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
	const { theme } = useTheme();
	const {
		Info,
		app,
		dialog,
		dialogsDocked,
		hintOpen,
		setHintOpen,
		setDialogsDocked,
		spawnBanner,
		spawnDialog,
		toggleHintOpen,
	} = Application.use();
	const { t } = Locale.use();
	const { extensions } = Extension.use();
	const detachedWindows = DetachedWindow.use();
	const detachedDialogAppRef = useRef(app);
	detachedDialogAppRef.current = app;

	/**
	 * Applies the main document stylesheets and theme variables to a detached dialog window.
	 *
	 * @param targetWindow - Detached browser window that should mirror the main application theme.
	 * @returns Nothing.
	 */
	const copyStylesToWindow = useCallback(
		(targetWindow: Window) => {
			const targetDoc = targetWindow.document;
			if (!applyThemeToWindow(document, targetDoc, theme) || !targetDoc.head) {
				return;
			}

			Array.from(document.styleSheets).forEach((styleSheet: CSSStyleSheet) => {
				try {
					if (styleSheet.href) {
						const link = document.createElement("link");
						link.rel = "stylesheet";
						link.href = styleSheet.href;
						targetDoc.head.appendChild(link);
					} else if (styleSheet.cssRules) {
						const style = document.createElement("style");
						Array.from(styleSheet.cssRules).forEach((rule) => {
							style.appendChild(document.createTextNode(rule.cssText));
						});
						targetDoc.head.appendChild(style);
					}
				} catch (error) {
					console.warn("error copying style", error);
				}
			});
		},
		[theme],
	);

	// Auto-open or focus the Table view window when app.general.tableViewSource is set
	useEffect(() => {
		if (app.general.tableViewSource) {
			detachedWindows.openTableWindow(app.general.tableViewSource.id);
			Info.setInfoByKey(null, "general", "tableViewSource");
		}
	}, [app.general.tableViewSource, detachedWindows, Info]);

	const [isPreloaded, setIsPreloaded] = useState(false);
	const dialogWindowRef = useRef<Window | null>(null);
	const dialogRootRef = useRef<ReactDOM.Root | null>(null);
	const dialogBridgeIdRef = useRef<string | null>(null);

	const unmountDialogWindow = useCallback(() => {
		const root = dialogRootRef.current;
		const win = dialogWindowRef.current;

		dialogRootRef.current = null;
		dialogBridgeIdRef.current = null;
		dialogWindowRef.current = null;

		if (root) {
			setTimeout(() => {
				root.unmount();
			}, 0);
		}

		if (win && !win.closed) {
			win.close();
		}
	}, []);

	/**
	 * Renders or updates the detached event-dialog root without subscribing it to
	 * unrelated main application changes such as canvas zoom updates.
	 *
	 * @param targetWindow - Detached browser window that owns the dialog root.
	 * @returns Nothing.
	 */
	const renderDetachedDialog = useCallback(
		(targetWindow: Window) => {
			const container =
				targetWindow.document.getElementById("detached-root") ??
				targetWindow.document.createElement("div");
			if (!container.id) {
				container.id = "detached-root";
				targetWindow.document.body.innerHTML = "";
				targetWindow.document.body.appendChild(container);
			}

			if (!dialogRootRef.current) {
				dialogRootRef.current = ReactDOM.createRoot(container);
			}

			if (!dialogBridgeIdRef.current) {
				dialogBridgeIdRef.current = WindowBridge.generateId();
			}

			dialogRootRef.current.render(
				<DetachedAppProvider
					initialApp={detachedDialogAppRef.current}
					initialNotes={[...DataStore.notes]}
					bridgeId={dialogBridgeIdRef.current}
					detachedDocument={targetWindow.document}
					mainSpawnBanner={spawnBanner}
				>
					<DetachedDialogWindowContent dialog={dialog} />
				</DetachedAppProvider>,
			);
		},
		[dialog, spawnBanner],
	);

	useEffect(() => {
		if (isPreloaded) return;

		setTimeout(() => {
			setIsPreloaded(true);
		}, 500);
	}, [isPreloaded]);

	useEffect(() => {
		if (theme) {
			Color.Themer.setTheme();
		}
	}, [theme]);

	useEffect(() => {
		if (dialogWindowRef.current && !dialogWindowRef.current.closed) {
			applyThemeToWindow(document, dialogWindowRef.current.document, theme);
		}
	}, [applyThemeToWindow, theme]);

	useEffect(() => {
		const root = document.getElementById("root");
		if (!root) {
			return Logger.error("ROOT_NOT_FOUND");
		}

		root.classList[app.hidden.toasts ? "add" : "remove"]("hidden_toats");
	}, [app.hidden.toasts]);

	useEffect(() => {
		return () => {
			unmountDialogWindow();
		};
	}, [unmountDialogWindow]);

	useEffect(() => {
		if (dialogsDocked || !dialog) {
			if (dialogWindowRef.current) {
				unmountDialogWindow();
			}
			return;
		}

		let nextWindow = dialogWindowRef.current;
		if (!nextWindow || nextWindow.closed) {
			nextWindow = window.open(
				"",
				"GulpDialogWindow",
				`width=${Math.max(app.timeline.dialogSize, 480)},height=${Math.max(window.innerHeight - 160, 640)},left=${Math.max(window.innerWidth - Math.max(app.timeline.dialogSize, 480) - 48, 48)},top=60`,
			);
			if (!nextWindow) {
				setDialogsDocked(true);
				return;
			}

			nextWindow.document.title = t("operationView.eventDetailsTitle");
			copyStylesToWindow(nextWindow);
			dialogWindowRef.current = nextWindow;
			nextWindow.addEventListener(
				"beforeunload",
				() => {
					const root = dialogRootRef.current;
					dialogRootRef.current = null;
					dialogBridgeIdRef.current = null;
					dialogWindowRef.current = null;
					if (root) {
						setTimeout(() => {
							root.unmount();
						}, 0);
					}
				},
				{ once: true },
			);
		}

		renderDetachedDialog(nextWindow);
	}, [
		app.timeline.dialogSize,
		copyStylesToWindow,
		dialogsDocked,
		renderDetachedDialog,
		setDialogsDocked,
		unmountDialogWindow,
	]);

	// Listen for redock requests sent from the detached dialog window
	useEffect(() => {
		const listenId = WindowBridge.generateId();
		const bridge = WindowBridge.create(listenId, (message) => {
			if (message.type === WindowBridge.MessageType.DOCK_DIALOG) {
				setDialogsDocked(true);
			}
		});
		return () => bridge.destroy();
	}, [setDialogsDocked]);

	// When dock state changes the canvas layout shifts; trigger a canvas redraw after layout settles
	useEffect(() => {
		const id = requestAnimationFrame(() => {
			DataStore.markDirty();
		});
		return () => cancelAnimationFrame(id);
	}, [dialogsDocked]);

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
				action: () => spawnBanner(<SelectFiles.Banner showSession={false} />),
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
				label: t("operationView.menu.requests"),
				icon: "Activity",
				category: t("operationView.menu.configuration"),
				action: () => spawnBanner(<Requests.Banner />),
			},
			{
				label: t("operationView.menu.managePermissions"),
				icon: "LucideUserRoundPlus",
				category: t("operationView.menu.configuration"),
				action: () => void openSelectedOperationPermissionsBanner(),
			},
			{
				label: t("operationView.menu.backToOperations"),
				icon: "Undo2",
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
			<Timeline />
			{hintOpen ? <Hint.Dialog onClose={() => setHintOpen(false)} /> : null}
			{dialogsDocked && dialog ? (
				<Stack
					className={cn(s.dialog)}
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
		</Stack>
	);
}

function DetachedDialogWindowContent({ dialog }: { dialog: React.ReactNode }) {
	const { dialog: detachedDialog, spawnDialog } = Application.use();

	useEffect(() => {
		spawnDialog(dialog);
	}, [dialog, spawnDialog]);

	return detachedDialog ?? null;
}

// Simple toast mock/fallback if needed, though toast should be imported from sonner or global
const toast = (msg: string) => {
	import("sonner").then(({ toast: t }) => t(msg));
};

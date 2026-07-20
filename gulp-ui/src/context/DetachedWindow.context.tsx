import React, {
	createContext,
	ReactNode,
	useCallback,
	useContext,
	useEffect,
	useMemo,
	useRef,
	useState,
	useSyncExternalStore,
} from "react";
import ReactDOM from "react-dom/client";
import { useLocation } from "react-router-dom";
import { useTheme } from "next-themes";
import { Application } from "@/context/Application.context";
import { DetachedAppProvider } from "@/context/DetachedApp.provider";
import { WindowBridge } from "@/lib/WindowBridge";
import { DataStore } from "@/store/DataStore";
import { AIAssistantWindow } from "@/components/AIAssistantWindow";
import { RenderEngine } from "@/class/RenderEngine";
import { App } from "@/entities/App";
import { Source } from "@/entities/Source";
import { Operation } from "@/entities/Operation";
import { Doc } from "@/entities/Doc";
import { Note } from "@/entities/Note";
import { NotePoint } from "@/ui/Note";
import { DisplayEventDialog } from "@/dialogs/Event.dialog";
import {
	applyThemeToWindow,
	getCanvasRenderedSourceIds,
	getPersistedDetachedWindowRecord,
	persistDetachedWindow,
	readPersistedDetachedWindows,
	removePersistedDetachedWindow,
} from "@/utils";

type DetachedWindowKind = "notes" | "table" | "dashboard" | "assistant";

interface AssistantWindowTarget {
	key: string;
	title: string;
	pluginFilename?: string;
}

interface DetachedWindowRecord {
	key: string;
	kind: DetachedWindowKind;
	window: Window;
	root?: ReactDOM.Root;
	bridgeId: string;
}

interface DetachedWindowContextValue {
	status: WindowBridge.MainContextStatus;
	contextVersion: number;
	openNotesWindow: () => void;
	openTableWindow: (sourceId?: Source.Id) => void;
	openDashboardWindow: () => void;
	openAssistantWindow: (target: AssistantWindowTarget) => void;
}

/**
 * FetchEventBannerMain fetches a note-linked event in the main tab when a detached
 * window targets an event that is not currently loaded in the timeline.
 *
 * @param props.docId - Event document ID to fetch.
 * @param props.operationId - Operation owning the event.
 * @returns Banner content that reuses the existing note fetch flow.
 */
function FetchEventBannerMain({
	docId,
	operationId,
}: {
	docId: Doc.Id;
	operationId: Operation.Id;
}) {
	const shell = {
		doc: { _id: docId },
		operation_id: operationId,
		name: docId,
	} as unknown as Note.Type;

	return <NotePoint.FetchEventBanner note={shell} />;
}

/**
 * Creates a stable browser window name for a detached assistant target.
 *
 * @param targetKey - Assistant target key from the built-in or extension registry.
 * @returns Sanitized browser window name.
 */
function createAssistantWindowName(targetKey: string): string {
	return `GulpAIAssistant-${targetKey.replace(/[^a-zA-Z0-9_-]/g, "_")}`;
}

/**
 * Builds the URL for an independently booted detached route.
 *
 * @param kind - Detached route kind.
 * @param bridgeId - Bridge identifier assigned to the detached window.
 * @param sourceId - Optional source preselected by the table view.
 * @returns Absolute URL used for window.open.
 */
function createDetachedRouteUrl(
	kind: Exclude<DetachedWindowKind, "assistant">,
	bridgeId: string,
	sourceId?: Source.Id,
): string {
	const url = new URL(`/detached/${kind}`, window.location.origin);
	url.searchParams.set("bridgeId", bridgeId);
	if (sourceId) {
		url.searchParams.set("sourceId", sourceId);
	}
	return url.toString();
}

/**
 * Extracts the operation ID from an operation workspace route.
 *
 * @param pathname - Current browser location pathname.
 * @returns Operation ID from the route, or null outside operation routes.
 */
function getRouteOperationId(pathname: string): Operation.Id | null {
	const match = pathname.match(/^\/operations\/([^/]+)/);
	return match ? decodeURIComponent(match[1]) as Operation.Id : null;
}

/**
 * Reads whether the browser still has a persisted auth token.
 *
 * @returns True when a non-placeholder token is present.
 */
function hasStoredToken(): boolean {
	const token = localStorage.getItem("__token");
	return !!token && token !== "-";
}

interface MainContextStatusInput {
	app: App.Type;
	authLost: boolean;
	routeOperationId: Operation.Id | null;
	selectedOperationId: Operation.Id | null | undefined;
	selectedSourceIds: Source.Id[];
}

/**
 * Resolves the lifecycle state that detached windows should render from the
 * current main-tab authentication and operation selection.
 *
 * @param input - Current app, auth, route, operation, and selected source data.
 * @returns Detached lifecycle status to broadcast.
 */
function resolveMainContextStatus(
	input: MainContextStatusInput,
): WindowBridge.MainContextStatus {
	const authenticated =
		!!input.app.general.user ||
		hasStoredToken() ||
		input.app.general.skippedAuth;

	if (input.authLost || !authenticated) return "auth_lost";
	if (!input.routeOperationId) return "idle";
	if (!input.selectedOperationId || input.selectedSourceIds.length === 0) {
		return "initializing";
	}

	return "active";
}

/**
 * Compares two note snapshots by item identity and order.
 *
 * @param previous - Previously broadcast notes snapshot.
 * @param next - Next candidate notes snapshot.
 * @returns True when both snapshots contain the same note object references in the same order.
 */
function areNoteSnapshotsEqual(
	previous: Note.Type[] | null,
	next: Note.Type[],
): boolean {
	if (!previous || previous.length !== next.length) return false;

	return previous.every((note, index) => note === next[index]);
}

/**
 * Builds the stable source-row signature sent to detached windows.
 * @param app Current main-tab app snapshot.
 * @param operationId Optional operation scope.
 * @returns Pipe-delimited rendered source row IDs.
 */
function getRenderedSourceSignature(
	app: App.Type,
	operationId?: Operation.Id | null,
): string {
	return getCanvasRenderedSourceIds(app, operationId).join("|");
}

/**
 * DetachedWindow.Provider owns detached browser windows independently of route
 * components so they can idle and resume while the main tab navigates.
 *
 * @param props.children - Routed application content rendered under the coordinator.
 * @returns Provider wrapping the routed application.
 */
function _({ children }: { children: ReactNode }) {
	const {
		Info,
		app,
		spawnBanner,
		spawnDialog,
		setDialogsDocked,
	} = Application.use();
	const location = useLocation();
	const { theme } = useTheme();
	const [windows, setWindows] = useState<Record<string, DetachedWindowRecord>>(
		{},
	);
	const windowsRef = useRef<Record<string, DetachedWindowRecord>>({});
	const recoveredPersistedWindowsRef = useRef(false);
	const bridgeIdRef = useRef(WindowBridge.generateId());
	const bridgeRef = useRef<WindowBridge.Bridge | null>(null);
	const [authLost, setAuthLost] = useState(false);
	const authLostRef = useRef(false);
	const routeOperationId = getRouteOperationId(location.pathname);
	const selectedOperationId =
		Operation.Entity.selected(app)?.id ?? routeOperationId;
	const selectedSourceIds = useMemo(
		() =>
			app.target.files
				.filter((source) =>
					source.selected &&
					(!routeOperationId || source.operation_id === routeOperationId),
				)
				.map((source) => source.id),
		[app.target.files, routeOperationId],
	);
	const selectedSourceIdsSignature = selectedSourceIds.join(",");

	const appRef = useRef(app);
	appRef.current = app;
	const routeOperationIdRef = useRef(routeOperationId);
	routeOperationIdRef.current = routeOperationId;
	authLostRef.current = authLost;
	const infoRef = useRef(Info);
	infoRef.current = Info;
	const dataStoreVersion = useSyncExternalStore(
		DataStore.subscribeOverlays,
		DataStore.getOverlaysSnapshot,
		DataStore.getOverlaysSnapshot,
	);
	const spawnDialogRef = useRef(spawnDialog);
	spawnDialogRef.current = spawnDialog;
	const spawnBannerRef = useRef(spawnBanner);
	spawnBannerRef.current = spawnBanner;

	const status = useMemo<WindowBridge.MainContextStatus>(() => {
		return resolveMainContextStatus({
			app,
			authLost,
			routeOperationId,
			selectedOperationId,
			selectedSourceIds,
		});
	}, [
		app.general.skippedAuth,
		app.general.user,
		authLost,
		routeOperationId,
		selectedOperationId,
		selectedSourceIdsSignature,
	]);

	const contextVersion = useMemo(
		() =>
			[
				status,
				selectedOperationId ?? "",
				selectedSourceIdsSignature,
				routeOperationId ?? "",
			].join("|"),
		[routeOperationId, selectedOperationId, selectedSourceIdsSignature, status],
	);

	const contextVersionNumber = useMemo(() => {
		let hash = 0;
		for (let index = 0; index < contextVersion.length; index++) {
			hash = (hash * 31 + contextVersion.charCodeAt(index)) >>> 0;
		}
		return hash;
	}, [contextVersion]);

	/**
	 * Applies current theme variables and stylesheets to a detached document.
	 *
	 * @param targetWindow - Detached browser window to style.
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

	/**
	 * Broadcasts the current lifecycle status and, when active, the current app snapshot.
	 *
	 * @returns Nothing.
	 */
	const replayDetachedState = useCallback(() => {
		const currentApp = appRef.current;
		const currentRouteOperationId = routeOperationIdRef.current;
		const currentSelectedOperationId =
			Operation.Entity.selected(currentApp)?.id ?? currentRouteOperationId;
		const currentSelectedSourceIds = currentApp.target.files
			.filter((source) =>
				source.selected &&
				(!currentRouteOperationId ||
					source.operation_id === currentRouteOperationId),
			)
			.map((source) => source.id);
		const currentStatus = resolveMainContextStatus({
			app: currentApp,
			authLost: authLostRef.current,
			routeOperationId: currentRouteOperationId,
			selectedOperationId: currentSelectedOperationId,
			selectedSourceIds: currentSelectedSourceIds,
		});

		if (bridgeRef.current) {
			WindowBridge.sendMainContext(
				bridgeRef.current,
				currentApp,
				currentStatus,
				currentSelectedOperationId,
			);
			return;
		}

		WindowBridge.broadcastMainContext(
			currentApp,
			currentStatus,
			currentSelectedOperationId,
		);
	}, []);
	const replayDetachedStateRef = useRef(replayDetachedState);
	const previousDetachedNotesRef = useRef<Note.Type[] | null>(null);
	const renderedSourceSignatureRef = useRef(
		getRenderedSourceSignature(app, selectedOperationId ?? routeOperationId),
	);
	const replayFrameRef = useRef<number | null>(null);
	const replayNotesFrameRef = useRef<number | null>(null);

	useEffect(() => {
		replayDetachedStateRef.current = replayDetachedState;
	}, [replayDetachedState]);

	/**
	 * Broadcasts the current selected-source notes when their DataStore snapshot changes.
	 *
	 * @returns Nothing.
	 */
	const replayDetachedNotes = useCallback(() => {
		const currentApp = appRef.current;
		const currentOperationId =
			Operation.Entity.selected(currentApp)?.id ?? routeOperationIdRef.current;
		const payload = WindowBridge.createNotesChangedPayload(
			currentApp,
			currentOperationId,
		);

		if (areNoteSnapshotsEqual(previousDetachedNotesRef.current, payload.notes)) {
			return;
		}

		previousDetachedNotesRef.current = payload.notes;

		if (bridgeRef.current) {
			bridgeRef.current.send(WindowBridge.MessageType.NOTES_CHANGED, payload);
			return;
		}

		WindowBridge.broadcastNotesSnapshot(currentApp, currentOperationId);
	}, []);
	const replayDetachedNotesRef = useRef(replayDetachedNotes);

	useEffect(() => {
		replayDetachedNotesRef.current = replayDetachedNotes;
	}, [replayDetachedNotes]);

	/**
	 * Coalesces detached context replays to one message burst per animation frame.
	 *
	 * @returns Nothing.
	 */
	const scheduleReplayDetachedState = useCallback(() => {
		if (replayFrameRef.current !== null) return;

		replayFrameRef.current = window.requestAnimationFrame(() => {
			replayFrameRef.current = null;
			replayDetachedStateRef.current();
		});
	}, []);

	/**
	 * Coalesces detached notes replays to one BroadcastChannel update per animation frame.
	 *
	 * @returns Nothing.
	 */
	const scheduleReplayDetachedNotes = useCallback(() => {
		if (replayNotesFrameRef.current !== null) return;

		replayNotesFrameRef.current = window.requestAnimationFrame(() => {
			replayNotesFrameRef.current = null;
			replayDetachedNotesRef.current();
		});
	}, []);

	useEffect(() => {
		return () => {
			if (replayFrameRef.current !== null) {
				window.cancelAnimationFrame(replayFrameRef.current);
			}
			if (replayNotesFrameRef.current !== null) {
				window.cancelAnimationFrame(replayNotesFrameRef.current);
			}
		};
	}, []);

	/**
	 * Sends only the selected timeline event to detached windows.
	 *
	 * @returns Nothing.
	 */
	const replaySelectedEvent = useCallback(() => {
		const event = appRef.current.timeline.target ?? null;

		if (bridgeRef.current) {
			WindowBridge.sendSelectedEvent(bridgeRef.current, event);
			return;
		}

		WindowBridge.broadcastSelectedEvent(event);
	}, []);

	/**
	 * Removes a detached window record after the external window closes.
	 *
	 * @param key - Detached window key.
	 * @returns Nothing.
	 */
	const forgetWindow = useCallback((key: string) => {
		const record = windowsRef.current[key];
		if (record?.root) {
			setTimeout(() => record.root?.unmount(), 0);
		}
		delete windowsRef.current[key];
		removePersistedDetachedWindow(key);
		setWindows({ ...windowsRef.current });
	}, []);

	/**
	 * Opens a detached browser route that owns its own React root and scheduler.
	 *
	 * @param key - Internal window key.
	 * @param kind - Detached route kind.
	 * @param windowName - Browser window name used for reuse.
	 * @param features - Browser window feature string.
	 * @param sourceId - Optional table source to preselect.
	 * @returns Opened or focused browser window.
	 */
	const openRoutedDetachedWindow = useCallback(
		(
			key: string,
			kind: Exclude<DetachedWindowKind, "assistant">,
			windowName: string,
			features: string,
			sourceId?: Source.Id,
		) => {
			const existing = windowsRef.current[key];
			if (existing && !existing.window.closed) {
				existing.window.focus();
				replayDetachedState();
				return existing.window;
			}

			const bridgeId = WindowBridge.generateId();
			const targetWindow = window.open(
				createDetachedRouteUrl(kind, bridgeId, sourceId),
				windowName,
				features,
			);
			if (!targetWindow) return null;

			const record: DetachedWindowRecord = {
				key,
				kind,
				window: targetWindow,
				bridgeId,
			};
			windowsRef.current[key] = record;
			persistDetachedWindow({ key, kind, windowName, features });
			setWindows({ ...windowsRef.current });
			targetWindow.addEventListener(
				"beforeunload",
				() => forgetWindow(key),
				{ once: true },
			);
			window.setTimeout(replayDetachedState, 250);
			window.setTimeout(replayDetachedState, 1000);
			return targetWindow;
		},
		[forgetWindow, replayDetachedState],
	);

	/**
	 * Opens a direct-root detached assistant window or focuses an existing one.
	 *
	 * @param key - Internal window key.
	 * @param kind - Detached window kind.
	 * @param windowName - Browser window name used for reuse.
	 * @param features - Browser window feature string.
	 * @param renderContent - Assistant content renderer for the detached root.
	 * @returns Opened or focused browser window.
	 */
	const openDetachedWindow = useCallback(
		(
			key: string,
			kind: DetachedWindowKind,
			windowName: string,
			features: string,
			renderContent: (targetWindow: Window) => ReactNode,
		) => {
			const existing = windowsRef.current[key];
			if (existing && !existing.window.closed) {
				existing.window.focus();
				replayDetachedState();
				return existing.window;
			}

			const targetWindow = window.open("", windowName, features);
			if (!targetWindow) return null;

			targetWindow.document.body.innerHTML = "";
			const container = document.createElement("div");
			targetWindow.document.body.appendChild(container);
			copyStylesToWindow(targetWindow);

			const bridgeId = WindowBridge.generateId();
			const root = ReactDOM.createRoot(container);
			root.render(
				<DetachedAppProvider
					initialApp={appRef.current}
					bridgeId={bridgeId}
					detachedDocument={targetWindow.document}
					mainSpawnBanner={spawnBannerRef.current}
				>
					{renderContent(targetWindow)}
				</DetachedAppProvider>,
			);

			const record: DetachedWindowRecord = {
				key,
				kind,
				window: targetWindow,
				root,
				bridgeId,
			};
			windowsRef.current[key] = record;
			if (kind !== "assistant") {
				persistDetachedWindow({ key, kind, windowName, features });
			}
			setWindows({ ...windowsRef.current });
			targetWindow.addEventListener(
				"beforeunload",
				() => forgetWindow(key),
				{ once: true },
			);
			setTimeout(replayDetachedState, 0);
			return targetWindow;
		},
		[copyStylesToWindow, forgetWindow, replayDetachedState],
	);

	/**
	 * Opens or focuses the notes detached window.
	 *
	 * @returns Nothing.
	 */
	const openNotesWindow = useCallback(() => {
		const record = getPersistedDetachedWindowRecord("notes");
		openRoutedDetachedWindow(
			record.key,
			record.kind,
			record.windowName,
			record.features,
		);
	}, [openRoutedDetachedWindow]);

	/**
	 * Opens or focuses the table detached window and optionally preselects a source.
	 *
	 * @param sourceId - Optional source to select after the table window is ready.
	 * @returns Nothing.
	 */
	const openTableWindow = useCallback(
		(sourceId?: Source.Id) => {
			const record = getPersistedDetachedWindowRecord("table");
			openRoutedDetachedWindow(
				record.key,
				record.kind,
				record.windowName,
				record.features,
				sourceId,
			);

			if (sourceId) {
				window.setTimeout(() => {
					bridgeRef.current?.send(WindowBridge.MessageType.TABLE_SELECT_SOURCE, {
						sourceId,
					});
				}, 500);
			}
		},
		[openRoutedDetachedWindow],
	);

	/**
	 * Opens or focuses the dashboard detached window.
	 *
	 * @returns Nothing.
	 */
	const openDashboardWindow = useCallback(() => {
		const record = getPersistedDetachedWindowRecord("dashboard");
		openRoutedDetachedWindow(
			record.key,
			record.kind,
			record.windowName,
			record.features,
		);
	}, [openRoutedDetachedWindow]);

	const openAssistantWindow = useCallback(
		(target: AssistantWindowTarget) => {
			openDetachedWindow(
				`assistant:${target.key}`,
				"assistant",
				createAssistantWindowName(target.key),
				"width=480,height=800,left=120,top=120",
				(targetWindow) => (
					<AIAssistantWindow
						title={target.title}
						pluginFilename={target.pluginFilename}
						onClose={() => targetWindow.close()}
					/>
				),
			);
		},
		[openDetachedWindow],
	);

	/**
	 * Reattaches named detached windows that survived a main-tab reload/login
	 * redirect but lost their React root and BroadcastChannel listener.
	 *
	 * @returns Nothing.
	 */
	const recoverPersistedDetachedWindows = useCallback(() => {
		const persistedWindows = readPersistedDetachedWindows();
		if (persistedWindows.length === 0) return;

		persistedWindows.forEach((record) => {
			if (windowsRef.current[record.key]) return;

			openRoutedDetachedWindow(
				record.key,
				record.kind,
				record.windowName,
				record.features,
			);
		});
	}, [openRoutedDetachedWindow]);

	useEffect(() => {
		if (status === "auth_lost") {
			recoveredPersistedWindowsRef.current = false;
			return;
		}

		if (recoveredPersistedWindowsRef.current) return;

		recoveredPersistedWindowsRef.current = true;
		recoverPersistedDetachedWindows();
	}, [recoverPersistedDetachedWindows, status]);

	useEffect(() => {
		const bridge = WindowBridge.create(bridgeIdRef.current, (message) => {
			switch (message.type) {
				case WindowBridge.MessageType.FLAGS_CHANGED:
					RenderEngine.clearAllCaches();
					DataStore.markDirty();
					infoRef.current.render();
					break;
				case WindowBridge.MessageType.TARGET_NOTE: {
					const { docId, operationId } =
						message.payload as WindowBridge.TargetNotePayload;
					const event = Doc.Entity.id(appRef.current, docId);
					if (event) {
						spawnDialogRef.current(<DisplayEventDialog event={event} />);
					} else {
						spawnBannerRef.current(
							<FetchEventBannerMain
								docId={docId}
								operationId={operationId}
							/>,
						);
					}
					break;
				}
				case WindowBridge.MessageType.EVENT_SELECTED: {
					const { event } = message.payload as WindowBridge.EventSelectedPayload;
					if (event?._id !== infoRef.current.app.timeline.target?._id) {
						infoRef.current.setTimelineTarget(event ?? null);
					}
					break;
				}
				case WindowBridge.MessageType.DOCK_DIALOG:
					setDialogsDocked(true);
					break;
				case WindowBridge.MessageType.DETACHED_READY: {
					replayDetachedStateRef.current();
					break;
				}
			}
		});
		bridgeRef.current = bridge;
		return () => {
			bridge.destroy();
			bridgeRef.current = null;
		};
	}, [setDialogsDocked]);

	useEffect(() => {
		scheduleReplayDetachedState();
		renderedSourceSignatureRef.current = getRenderedSourceSignature(
			app,
			selectedOperationId ?? routeOperationId,
		);
	}, [
		scheduleReplayDetachedState,
		app.target.files,
		app.target.operations,
		app.target.contexts,
		app.target.filters,
		app.timeline.filter,
		app.hidden,
		routeOperationId,
		selectedOperationId,
		selectedSourceIdsSignature,
		status,
	]);

	useEffect(() => {
		const handleDocumentStoreChange = (): void => {
			const currentApp = appRef.current;
			if (!currentApp.hidden.sourcesWithNoEvents) return;

			const currentOperationId =
				Operation.Entity.selected(currentApp)?.id ?? routeOperationIdRef.current;
			const signature = getRenderedSourceSignature(
				currentApp,
				currentOperationId,
			);
			if (signature === renderedSourceSignatureRef.current) return;

			renderedSourceSignatureRef.current = signature;
			scheduleReplayDetachedState();
		};

		return DataStore.subscribeDocuments(handleDocumentStoreChange);
	}, [scheduleReplayDetachedState]);

	useEffect(() => {
		scheduleReplayDetachedNotes();
	}, [dataStoreVersion, scheduleReplayDetachedNotes]);

	useEffect(() => {
		replaySelectedEvent();
	}, [app.timeline.target?._id, replaySelectedEvent]);

	useEffect(() => {
		bridgeRef.current?.send(WindowBridge.MessageType.THEME_CHANGE, {
			theme: theme ?? "dark",
		});
		Object.values(windowsRef.current).forEach((record) => {
			if (!record.window.closed) {
				requestAnimationFrame(() => {
					applyThemeToWindow(document, record.window.document, theme);
				});
			}
		});
	}, [theme, windows]);

	useEffect(() => {
		/**
		 * Marks detached windows as blocked until a fresh login succeeds.
		 *
		 * @returns Nothing.
		 */
		const handleAuthLost = () => {
			authLostRef.current = true;
			setAuthLost(true);
		};

		/**
		 * Clears a previous auth-lost state and replays the current context.
		 *
		 * @returns Nothing.
		 */
		const handleAuthRestored = () => {
			authLostRef.current = false;
			setAuthLost(false);
			window.setTimeout(() => replayDetachedStateRef.current(), 0);
		};

		window.addEventListener("gulp-auth-lost", handleAuthLost);
		window.addEventListener("gulp-auth-restored", handleAuthRestored);
		return () => {
			window.removeEventListener("gulp-auth-lost", handleAuthLost);
			window.removeEventListener("gulp-auth-restored", handleAuthRestored);
		};
	}, []);

	useEffect(() => {
		if (authLostRef.current && (app.general.user || hasStoredToken())) {
			authLostRef.current = false;
			setAuthLost(false);
		}
	}, [app.general.user]);

	useEffect(() => {
		/**
		 * Replays detached state after source/session selection commits in the main tab.
		 *
		 * @returns Nothing.
		 */
		const handleReplayRequest = () => {
			window.setTimeout(() => {
				replayDetachedStateRef.current();
			}, 0);
			window.setTimeout(() => {
				replayDetachedStateRef.current();
			}, 100);
		};

		window.addEventListener(WindowBridge.DETACHED_REPLAY_EVENT, handleReplayRequest);
		return () =>
			window.removeEventListener(
				WindowBridge.DETACHED_REPLAY_EVENT,
				handleReplayRequest,
			);
	}, []);

	useEffect(() => {
		const handleBeforeUnload = () => {
			WindowBridge.broadcastMainStatus("initializing", selectedOperationId);
		};
		window.addEventListener("beforeunload", handleBeforeUnload);
		return () => window.removeEventListener("beforeunload", handleBeforeUnload);
	}, [selectedOperationId]);

	useEffect(() => {
		if (app.general.tableViewSource) {
			openTableWindow(app.general.tableViewSource.id);
			Info.setInfoByKey(null, "general", "tableViewSource");
		}
	}, [Info, app.general.tableViewSource, openTableWindow]);

	const value = useMemo<DetachedWindowContextValue>(
		() => ({
			status,
			contextVersion: contextVersionNumber,
			openNotesWindow,
			openTableWindow,
			openDashboardWindow,
			openAssistantWindow,
		}),
		[
			contextVersionNumber,
			openAssistantWindow,
			openDashboardWindow,
			openNotesWindow,
			openTableWindow,
			status,
		],
	);

	return (
		<DetachedWindow.Context.Provider value={value}>
			{children}
		</DetachedWindow.Context.Provider>
	);
}

export namespace DetachedWindow {
	export const Context = createContext<DetachedWindowContextValue | null>(null);
	export const Provider = _;

	export function use(): DetachedWindowContextValue {
		const value = useContext(Context);
		if (!value) throw new Error("DetachedWindow.Context not found");
		return value;
	}
}

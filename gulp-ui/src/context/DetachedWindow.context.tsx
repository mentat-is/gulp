import React, {
	createContext,
	ReactNode,
	useCallback,
	useContext,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import { useLocation } from "react-router-dom";
import { useTheme } from "next-themes";
import { Application } from "@/context/Application.context";
import { WindowBridge } from "@/lib/WindowBridge";
import {
	DataStore,
	type DataStoreRenderChange,
	type NotesSyncChange,
} from "@/store/DataStore";
import { App } from "@/entities/App";
import { Source } from "@/entities/Source";
import { Operation } from "@/entities/Operation";
import { Doc } from "@/entities/Doc";
import { Note } from "@/entities/Note";
import { NotePoint } from "@/ui/Note";
import { DisplayEventDialog } from "@/dialogs/Event.dialog";
import { scrollStore } from "@/store/scroll.store";
import { FilterFileBanner } from "@/banners/FilterFile.banner";
import {
	getPersistedDetachedWindowRecord,
	persistDetachedWindow,
	readPersistedDetachedWindows,
	removePersistedDetachedWindow,
	resolveEventRenderColor,
} from "@/utils";
import {
	EVENT_MAIN_CACHE_SIZE,
	type EventNeighborWindow,
} from "@/event-store";

type DetachedWindowKind = WindowBridge.DetachedClientKind;
type RoutedDetachedWindowKind = DetachedWindowKind;
type SyncDomain =
	| "status"
	| "context"
	| "query"
	| "rendered"
	| "notes"
	| "event"
	| "assistant"
	| "dialog"
	| "theme";
type StateSyncDomain = WindowBridge.StateSyncDomain;

const DETACHED_CLIENT_LEASE_MSEC = 90_000;

interface AssistantWindowTarget {
	key: string;
	title: string;
	pluginFilename?: string;
}

interface DetachedWindowRecord {
	key: string;
	kind: DetachedWindowKind;
	window: Window;
	bridgeId: string;
}

interface DetachedClient {
	id: string;
	kind: WindowBridge.DetachedClientKind;
	bridge: WindowBridge.Bridge;
	lastSeenAt: number;
	tableSyncEnabled: boolean;
	notesRenderedSyncEnabled: boolean;
	assistantQuerySyncEnabled: boolean;
	signatures: Partial<Record<SyncDomain, string>>;
}

interface DetachedWindowContextValue {
	coordinatorId: string;
	openNotesWindow: () => void;
	openTableWindow: (sourceId?: Source.Id) => void;
	openDashboardWindow: () => void;
	openAssistantWindow: (target: AssistantWindowTarget) => void;
	openDialogWindow: (
		dialog: WindowBridge.DetachedDialogDescriptor,
		features: string,
	) => boolean;
	closeDialogWindow: () => void;
}

function isPersistedWindowKind(
	kind: DetachedWindowKind,
): kind is "notes" | "table" | "dashboard" {
	return kind === "notes" || kind === "table" || kind === "dashboard";
}

/**
 * Fetches a note-linked event in the main tab when it is absent from the
 * currently loaded timeline.
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

function createAssistantWindowName(
	targetKey: string,
	coordinatorId: string,
): string {
	const safeTarget = targetKey.replace(/[^a-zA-Z0-9_-]/g, "_");
	const safeCoordinator = coordinatorId.replace(/[^a-zA-Z0-9_-]/g, "_");
	return `GulpAIAssistant-${safeTarget}-${safeCoordinator}`;
}

function createDialogWindowName(coordinatorId: string): string {
	return `GulpDialogWindow-${coordinatorId.replace(
		/[^a-zA-Z0-9_-]/g,
		"_",
	)}`;
}

function createDetachedRouteUrl(
	kind: RoutedDetachedWindowKind,
	bridgeId: string,
	coordinatorId: string,
	sourceId?: Source.Id,
	assistantTarget?: AssistantWindowTarget,
): string {
	const url = new URL(`/detached/${kind}`, window.location.origin);
	url.searchParams.set("bridgeId", bridgeId);
	url.searchParams.set("coordinatorId", coordinatorId);
	if (sourceId) url.searchParams.set("sourceId", sourceId);
	if (assistantTarget) {
		url.searchParams.set("assistantTitle", assistantTarget.title);
		if (assistantTarget.pluginFilename) {
			url.searchParams.set(
				"assistantPlugin",
				assistantTarget.pluginFilename,
			);
		}
	}
	return url.toString();
}

function getRouteOperationId(pathname: string): Operation.Id | null {
	const match = pathname.match(/^\/operations\/([^/]+)/);
	return match ? decodeURIComponent(match[1]) as Operation.Id : null;
}

function hasStoredToken(): boolean {
	const token = localStorage.getItem("__token");
	return !!token && token !== "-";
}

interface MainContextStatusInput {
	app: App.Type;
	authLost: boolean;
	routeOperationId: Operation.Id | null;
	selectedOperationId: Operation.Id | null | undefined;
	hasSelectedSource: boolean;
}

function resolveMainContextStatus(
	input: MainContextStatusInput,
): WindowBridge.MainContextStatus {
	const authenticated =
		!!input.app.general.user ||
		hasStoredToken() ||
		input.app.general.skippedAuth;

	if (input.authLost || !authenticated) return "auth_lost";
	if (!input.routeOperationId) return "idle";
	if (!input.selectedOperationId || !input.hasSelectedSource) {
		return "initializing";
	}
	return "active";
}

/**
 * Serializes a bridge payload for semantic comparison while tolerating bigint
 * values in partially normalized application entities.
 */
function getPayloadSignature(value: unknown): string {
	return JSON.stringify(value, (_key, item) =>
		typeof item === "bigint" ? item.toString() : item,
	);
}

/**
 * Captures the active theme and every computed CSS custom property.
 */
function createThemeSnapshot(
	sourceDocument: Document,
	themeName: string | undefined,
): WindowBridge.ThemeChangePayload {
	const root = sourceDocument.documentElement;
	const view = sourceDocument.defaultView;
	const variables: Record<string, string> = {};
	let mode: "light" | "dark" =
		themeName?.includes("light") ? "light" : "dark";

	if (root && view) {
		const styles = view.getComputedStyle(root);
		for (let index = 0; index < styles.length; index++) {
			const key = styles[index];
			if (key.startsWith("--")) {
				variables[key] = styles.getPropertyValue(key);
			}
		}
		const declaredMode = styles.getPropertyValue("--theme-mode").trim();
		if (declaredMode === "light" || declaredMode === "dark") {
			mode = declaredMode;
		}
	}

	return {
		theme: themeName ?? "classic-dark",
		mode,
		cssVariables: variables,
	};
}

/**
 * Projects the centered event strip consumed by a detached single-event Dialog.
 * Complete source collections stay in the main realm; only the eleven visible
 * indicators and their compact presentation state cross the private channel.
 * @param app Current application state used to resolve render colors.
 * @param event Current event at the center of the detached navigation strip.
 * @param neighborWindow Bounded EventStore window, or null before hydration.
 * @returns Compact navigation state containing at most eleven event indicators.
 */
function createDialogNavigation(
	app: App.Type,
	event: Doc.Type,
	neighborWindow: EventNeighborWindow | null,
): WindowBridge.DetachedDialogNavigation {
	const sourceEvents = neighborWindow?.events ?? [event];
	const currentDescendingIndex = neighborWindow?.currentIndex ?? 0;

	const noteEventIds = new Set(
		DataStore.notes
			.map((note) => note.doc?._id)
			.filter((eventId): eventId is Doc.Id => Boolean(eventId)),
	);
	const linkedEventIds = new Set<Doc.Id>();
	DataStore.links.forEach((link) => {
		linkedEventIds.add(link.doc_id_from);
		link.doc_ids.forEach((eventId) => linkedEventIds.add(eventId));
	});

	const halfWindow = Math.floor(
		WindowBridge.DETACHED_DIALOG_NAVIGATION_WINDOW_SIZE / 2,
	);
	const items = Array.from(
		{ length: WindowBridge.DETACHED_DIALOG_NAVIGATION_WINDOW_SIZE },
		(_, index): WindowBridge.DetachedDialogNavigationItem | null => {
			const offset = index - halfWindow;
			const sourceEvent =
				currentDescendingIndex === -1
					? offset === 0
						? event
						: undefined
					: sourceEvents[currentDescendingIndex - offset];
			if (!sourceEvent) return null;
			const color = resolveEventRenderColor(app, sourceEvent);
			return {
				id: sourceEvent._id,
				sourceId: sourceEvent["gulp.source_id"],
				timestamp: sourceEvent.gulp_timestamp,
				eventCode: sourceEvent["gulp.event_code"],
				numberHash: sourceEvent.number_hash,
				...(color ? { color } : {}),
				hasNotes: noteEventIds.has(sourceEvent._id),
				hasLinks: linkedEventIds.has(sourceEvent._id),
			};
		},
	);

	return {
		currentEventId: event._id,
		items,
		previousEventId:
			currentDescendingIndex >= 0 &&
			currentDescendingIndex < sourceEvents.length - 1
				? sourceEvents[currentDescendingIndex + 1]._id
				: null,
		nextEventId:
			currentDescendingIndex > 0
				? sourceEvents[currentDescendingIndex - 1]._id
				: null,
	};
}

/**
 * Projects only the collaboration and presentation data needed by the active
 * detached Dialog. Related link events are included when they are already
 * indexed in the main tab; no timeline-wide event collection is cloned.
 * @param app Current application state used for presentation metadata.
 * @param dialog Dialog descriptor currently hosted by the detached window.
 * @param resolvedRelatedEvents Bounded link endpoints hydrated from EventStore.
 * @param neighborWindow Bounded neighbor window for a single-event dialog.
 * @returns Serializable detached dialog projection.
 */
function createDialogChangedPayload(
	app: App.Type,
	dialog: WindowBridge.DetachedDialogDescriptor | null,
	resolvedRelatedEvents: readonly Doc.Type[],
	neighborWindow: EventNeighborWindow | null,
): WindowBridge.DialogChangedPayload {
	if (!dialog) {
		return {
			dialog: null,
			events: [],
			notes: [],
			links: [],
			eventColors: {},
			navigation: null,
		};
	}

	const descriptorEvents =
		dialog.kind === "event" ? [dialog.event] : dialog.events;
	const descriptorEventIds = new Set(
		descriptorEvents.map((event) => event._id),
	);
	const links = DataStore.links.filter(
		(link) =>
			descriptorEventIds.has(link.doc_id_from) ||
			link.doc_ids.some((docId) => descriptorEventIds.has(docId)),
	);
	const relatedEventIds = new Set<Doc.Id>(descriptorEventIds);
	links.forEach((link) => {
		relatedEventIds.add(link.doc_id_from);
		link.doc_ids.forEach((docId) => relatedEventIds.add(docId));
	});

	const eventsById = new Map<Doc.Id, Doc.Type>();
	descriptorEvents.forEach((event) => eventsById.set(event._id, event));
	resolvedRelatedEvents.forEach((event) => {
		if (relatedEventIds.has(event._id)) eventsById.set(event._id, event);
	});
	const events = [...eventsById.values()];
	const canonicalDialog =
		WindowBridge.canonicalizeDetachedDialogDescriptor(dialog, eventsById);
	const transmittedEventIds = new Set(eventsById.keys());
	const notes = DataStore.notes.filter((note) =>
		transmittedEventIds.has(note.doc?._id),
	);
	const eventColors: Partial<Record<Doc.Id, string>> = {};
	events.forEach((event) => {
		const color = resolveEventRenderColor(app, event);
		if (color) eventColors[event._id] = color;
	});

	return {
		dialog: canonicalDialog,
		events,
		notes,
		links,
		eventColors,
		navigation:
			canonicalDialog.kind === "event"
				? createDialogNavigation(
						app,
						canonicalDialog.event,
						neighborWindow,
					)
				: null,
	};
}

/**
 * Collects document IDs referenced by the active detached dialog and its links.
 * @param dialog Dialog whose bounded related-event projection is being hydrated.
 * @returns Unique document IDs in stable descriptor/link order.
 */
function getDialogRelatedDocumentIds(
	dialog: WindowBridge.DetachedDialogDescriptor,
): Doc.Id[] {
	const descriptorEvents =
		dialog.kind === "event" ? [dialog.event] : dialog.events;
	const descriptorEventIds = new Set(
		descriptorEvents.map((event) => event._id),
	);
	const relatedEventIds = new Set<Doc.Id>(descriptorEventIds);
	for (const link of DataStore.links) {
		if (
			!descriptorEventIds.has(link.doc_id_from) &&
			!link.doc_ids.some((documentId) => descriptorEventIds.has(documentId))
		) {
			continue;
		}
		relatedEventIds.add(link.doc_id_from);
		for (const documentId of link.doc_ids) {
			relatedEventIds.add(documentId);
		}
	}
	return Array.from(relatedEventIds);
}

/**
 * Determines whether a render-store mutation can change the active detached
 * dialog projection. It uses the previous compact projection so unrelated
 * collaboration traffic never triggers a full notes/links scan.
 */
function isDialogRenderChangeRelevant(
	change: DataStoreRenderChange,
	dialog: WindowBridge.DetachedDialogDescriptor | null,
	projection: WindowBridge.DialogChangedPayload | null,
): boolean {
	if (!dialog) return false;
	if (!projection) return true;

	const descriptorEvents =
		dialog.kind === "event" ? [dialog.event] : dialog.events;
	const descriptorEventIds = new Set(
		descriptorEvents.map((event) => event._id),
	);
	const projectedEventIds = new Set(
		projection.events.map((event) => event._id),
	);
	projection.navigation?.items.forEach((item) => {
		if (item) projectedEventIds.add(item.id);
	});

	if (change.type === "source-events") {
		if (dialog.kind === "group") {
			return change.changedEvents.some((event) =>
				projectedEventIds.has(event._id),
			);
		}
		return change.sourceId === dialog.event["gulp.source_id"];
	}

	if (change.type === "supplemental-events") {
		if (dialog.kind === "group") {
			return (
				change.changedEvents.some((event) =>
					projectedEventIds.has(event._id),
				) ||
				change.removedEventIds.some((eventId) =>
					projectedEventIds.has(eventId),
				)
			);
		}
		return change.sourceId === dialog.event["gulp.source_id"];
	}

	if (change.type === "notes") {
		// Removed overlays no longer carry their event identity. Reproject the
		// compact strip so a deleted marker cannot remain visible.
		if (change.removedNoteIds.length > 0) return true;
		if (
			change.changedNotes.some((note) =>
				projectedEventIds.has(note.doc?._id),
			)
		) {
			return true;
		}
		if (change.mutation !== "replace") return false;
		if (projection.navigation) return true;
		const replacementIds = new Set(change.notes.map((note) => note.id));
		return projection.notes.some((note) => !replacementIds.has(note.id));
	}

	if (change.type === "links") {
		const projectedLinkIds = new Set(
			projection.links.map((link) => link.id),
		);
		if (change.removedLinkIds.length > 0) return true;
		if (
			change.changedLinks.some(
				(link) =>
					projectedLinkIds.has(link.id) ||
					descriptorEventIds.has(link.doc_id_from) ||
					projectedEventIds.has(link.doc_id_from) ||
					link.doc_ids.some((docId) =>
						descriptorEventIds.has(docId) ||
						projectedEventIds.has(docId),
					),
			)
		) {
			return true;
		}
		if (change.mutation !== "replace") return false;
		if (projection.navigation) return true;
		const replacementIds = new Set(change.links.map((link) => link.id));
		return projection.links.some((link) => !replacementIds.has(link.id));
	}

	return false;
}

/**
 * Builds the small source-settings signature that can affect dialog event
 * colors. Sample caches and unrelated source metadata are intentionally omitted.
 */
function getDialogSourcePresentationSignature(
	app: App.Type,
	projection: WindowBridge.DialogChangedPayload,
): string {
	const sourceIds = new Set(
		projection.events.map((event) => event["gulp.source_id"]),
	);
	const sources = app.target.files
		.filter((source) => sourceIds.has(source.id))
		.map((source) => ({
			id: source.id,
			color: source.color,
			renderColorPalette: source.settings.render_color_palette,
			colorOverride: source.settings.color_override,
			colorPalette: source.settings.color_palette,
		}))
		.sort((left, right) => left.id.localeCompare(right.id));
	return getPayloadSignature(sources);
}

/**
 * Signs a dialog payload without serializing descriptor documents twice. The
 * top-level event projection already carries every canonical document.
 */
function getDialogPayloadSignature(
	payload: WindowBridge.DialogChangedPayload,
): string {
	const descriptor =
		payload.dialog?.kind === "event"
			? { kind: "event", eventId: payload.dialog.event._id }
			: payload.dialog?.kind === "group"
				? {
						kind: "group",
						eventIds: payload.dialog.events.map((event) => event._id),
						groupKind: payload.dialog.groupKind,
					}
				: null;
	return getPayloadSignature({
		descriptor,
		events: payload.events,
		notes: payload.notes,
		links: payload.links,
		eventColors: payload.eventColors,
		navigation: payload.navigation,
	});
}

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
	const windowsRef = useRef<Record<string, DetachedWindowRecord>>({});
	const clientsRef = useRef<Map<string, DetachedClient>>(new Map());
	const pendingTableSourceRef = useRef<Map<string, Source.Id>>(new Map());
	const dialogDescriptorRef =
		useRef<WindowBridge.DetachedDialogDescriptor | null>(null);
	const dialogProjectionRef =
		useRef<WindowBridge.DialogChangedPayload | null>(null);
	const dialogRelatedEventsRef = useRef<readonly Doc.Type[]>([]);
	const dialogNeighborWindowRef = useRef<EventNeighborWindow | null>(null);
	const dialogHydrationSequenceRef = useRef(0);
	const dialogSourcePresentationSignatureRef = useRef("");
	const recoveredPersistedWindowsRef = useRef(false);
	const coordinatorIdRef = useRef(
		WindowBridge.getOrCreateCoordinatorId(),
	);
	const bridgeIdRef = useRef(WindowBridge.generateId());
	const bridgeRef = useRef<WindowBridge.Bridge | null>(null);
	const bridgeMessageHandlerRef = useRef<
		(message: WindowBridge.Message) => void
	>(() => {});
	const bridgeNavigateLookupSequenceRef = useRef(0);
	const bridgeTargetLookupSequenceRef = useRef(0);
	const [authLost, setAuthLost] = useState(false);
	const authLostRef = useRef(false);
	const routeOperationId = getRouteOperationId(location.pathname);
	const selectedOperationId =
		Operation.Entity.selected(app)?.id ?? routeOperationId;
	const hasSelectedSource = useMemo(
		() =>
			app.target.files.some(
				(source) =>
					source.selected &&
					(!routeOperationId ||
						source.operation_id === routeOperationId),
			),
		[app.target.files, routeOperationId],
	);
	const status = useMemo<WindowBridge.MainContextStatus>(
		() =>
			resolveMainContextStatus({
				app,
				authLost,
				routeOperationId,
				selectedOperationId,
				hasSelectedSource,
			}),
		[
			app.general.skippedAuth,
			app.general.user,
			authLost,
			routeOperationId,
			selectedOperationId,
			hasSelectedSource,
		],
	);

	const appRef = useRef(app);
	appRef.current = app;
	const routeOperationIdRef = useRef(routeOperationId);
	routeOperationIdRef.current = routeOperationId;
	const statusRef = useRef(status);
	statusRef.current = status;
	authLostRef.current = authLost;
	const infoRef = useRef(Info);
	infoRef.current = Info;
	const spawnDialogRef = useRef(spawnDialog);
	spawnDialogRef.current = spawnDialog;
	const spawnBannerRef = useRef(spawnBanner);
	spawnBannerRef.current = spawnBanner;
	const themeRef = useRef(theme);
	themeRef.current = theme;
	const themeSnapshotRef = useRef<WindowBridge.ThemeChangePayload | null>(
		null,
	);
	const getThemeSnapshot = useCallback(
		(): WindowBridge.ThemeChangePayload => {
			if (!themeSnapshotRef.current) {
				themeSnapshotRef.current = createThemeSnapshot(
					document,
					themeRef.current,
				);
			}
			return themeSnapshotRef.current;
		},
		[],
	);
	const pendingStateFlushRef = useRef<{
		id: number;
		kind: "idle" | "timeout";
	} | null>(null);
	const pendingStateDomainsRef = useRef<Set<StateSyncDomain>>(new Set());

	const pruneClients = useCallback((): void => {
		const cutoff = Date.now() - DETACHED_CLIENT_LEASE_MSEC;
		clientsRef.current.forEach((client, clientId) => {
			if (client.lastSeenAt >= cutoff) return;
			client.bridge.destroy();
			clientsRef.current.delete(clientId);
		});
	}, []);

	const getCurrentOperationId = useCallback((): Operation.Id | null => {
		return (
			Operation.Entity.selected(appRef.current)?.id ??
			routeOperationIdRef.current
		);
	}, []);

	/**
	 * Builds the current detached dialog payload from bounded hydrated references.
	 * @param currentApp Current application presentation state.
	 * @param dialog Dialog descriptor to project, defaulting to the active descriptor.
	 * @returns Serializable payload retained for incremental detached updates.
	 */
	const projectDialog = useCallback((
		currentApp: App.Type,
		dialog = dialogDescriptorRef.current,
	): WindowBridge.DialogChangedPayload => {
		const payload = createDialogChangedPayload(
			currentApp,
			dialog,
			dialogRelatedEventsRef.current,
			dialogNeighborWindowRef.current,
		);
		dialogProjectionRef.current = payload;
		dialogSourcePresentationSignatureRef.current =
			getDialogSourcePresentationSignature(currentApp, payload);
		return payload;
	}, []);

	const sendDomain = useCallback(
		<TType extends WindowBridge.MessageType>(
			client: DetachedClient,
			domain: SyncDomain,
			type: TType,
			payload: WindowBridge.MessagePayload[TType],
			signature = getPayloadSignature(payload),
		): void => {
			if (client.signatures[domain] === signature) return;
			client.signatures[domain] = signature;
			client.bridge.send(type, payload);
		},
		[],
	);

	const createBootstrap = useCallback(
		(
			client: DetachedClient,
		): WindowBridge.DetachedBootstrapPayload => {
			const currentApp = appRef.current;
			const currentStatus = statusRef.current;
			const operationId = getCurrentOperationId();
			const payload: WindowBridge.DetachedBootstrapPayload = {
				status: currentStatus,
				theme: getThemeSnapshot(),
			};

			if (currentStatus !== "active") return payload;
			const dialogPayload =
				client.kind === "dialog"
					? projectDialog(currentApp)
					: undefined;

			payload.context = WindowBridge.createContextSnapshot(
				currentApp,
				operationId,
				{
					operationOnly:
						client.kind === "assistant" &&
						!client.assistantQuerySyncEnabled,
					sourceIds: dialogPayload?.events.map(
						(event) => event["gulp.source_id"],
					),
					includeDialogUser: client.kind === "dialog",
					compactNotes: client.kind === "notes",
				},
			);

			if (
				(client.kind === "assistant" &&
					client.assistantQuerySyncEnabled) ||
				(client.kind === "table" && client.tableSyncEnabled)
			) {
				payload.queryContext = WindowBridge.createQueryContext(
					currentApp,
					operationId,
				);
			}

			if (client.kind === "notes") {
				payload.notes = WindowBridge.createNotesChangedPayload(
					currentApp,
					operationId,
				);
				if (client.notesRenderedSyncEnabled) {
					payload.renderedSourceIds =
						WindowBridge.createRenderedSourceIds(
							currentApp,
							operationId,
						);
				}
			}

			if (client.kind === "table" || client.kind === "notes") {
				payload.selectedEventId = currentApp.timeline.target?._id ?? null;
				if (client.kind === "table") {
					payload.tableSourceId = pendingTableSourceRef.current.get(client.id);
				}
			}

			if (client.kind === "assistant") {
				payload.wsId = currentApp.general.ws_id;
			}
			if (client.kind === "dialog") {
				payload.dialog = dialogPayload;
			}

			return payload;
		},
		[getCurrentOperationId, getThemeSnapshot, projectDialog],
	);

	const sendBootstrap = useCallback(
		(client: DetachedClient): void => {
			const payload = createBootstrap(client);
			client.bridge.send(
				WindowBridge.MessageType.DETACHED_BOOTSTRAP,
				payload,
			);
			client.signatures.status = getPayloadSignature(payload.status);
			client.signatures.theme = getPayloadSignature(payload.theme);
			if (payload.context) {
				client.signatures.context = getPayloadSignature(payload.context);
			}
			if (payload.queryContext) {
				client.signatures.query = getPayloadSignature(payload.queryContext);
			}
			if (payload.renderedSourceIds) {
				client.signatures.rendered =
					getPayloadSignature(payload.renderedSourceIds);
			}
			if (payload.notes) {
				client.signatures.notes = getPayloadSignature(payload.notes);
			}
			if (payload.wsId !== undefined) {
				client.signatures.assistant = getPayloadSignature(payload.wsId);
			}
			if (payload.dialog !== undefined) {
				client.signatures.dialog = getDialogPayloadSignature(payload.dialog);
			}
			client.signatures.event = getPayloadSignature(
				payload.selectedEventId ?? null,
			);
			pendingTableSourceRef.current.delete(client.id);
		},
		[createBootstrap],
	);

	const flushDetachedState = useCallback((
		domains: ReadonlySet<StateSyncDomain>,
	): void => {
		pruneClients();
		const clients = [...clientsRef.current.values()];
		if (clients.length === 0) return;

		const currentStatus = statusRef.current;
		const bootstrappedClients = new Set<string>();
		if (domains.has("status")) {
			const statusPayload: WindowBridge.MainContextStatusPayload = {
				status: currentStatus,
			};
			const statusSignature = getPayloadSignature(currentStatus);
			clients.forEach((client) => {
				if (client.signatures.status === statusSignature) return;
				if (currentStatus === "active") {
					sendBootstrap(client);
					bootstrappedClients.add(client.id);
					return;
				}
				sendDomain(
					client,
					"status",
					WindowBridge.MessageType.MAIN_CONTEXT_STATUS,
					statusPayload,
					statusSignature,
				);
			});
		}
		if (currentStatus !== "active") return;

		const incrementalClients = clients.filter(
			(client) => !bootstrappedClients.has(client.id),
		);
		if (incrementalClients.length === 0) return;

		const currentApp = appRef.current;
		const operationId = getCurrentOperationId();
		let projectedDialogPayload:
			| WindowBridge.DialogChangedPayload
			| undefined;
		const getProjectedDialogPayload =
			(): WindowBridge.DialogChangedPayload => {
				if (!projectedDialogPayload) {
					projectedDialogPayload = projectDialog(currentApp);
				}
				return projectedDialogPayload;
			};
		if (domains.has("context")) {
			const contextPayloads = new Map<
				string,
				{
					payload: WindowBridge.ContextSnapshotPayload;
					signature: string;
				}
			>();
			incrementalClients.forEach((client) => {
				const operationOnly =
					client.kind === "assistant" &&
					!client.assistantQuerySyncEnabled;
				const includeDialogUser = client.kind === "dialog";
				const compactNotes = client.kind === "notes";
				const projectionKey =
					`${operationOnly}:${includeDialogUser}:${compactNotes}`;
				let projection = contextPayloads.get(projectionKey);
				if (!projection) {
					const payload = WindowBridge.createContextSnapshot(
						currentApp,
						operationId,
						{
							operationOnly,
							sourceIds: includeDialogUser
								? (
										domains.has("dialog")
											? getProjectedDialogPayload()
											: dialogProjectionRef.current ??
												getProjectedDialogPayload()
									).events.map(
										(event) => event["gulp.source_id"],
									)
								: undefined,
							includeDialogUser,
							compactNotes,
						},
					);
					projection = {
						payload,
						signature: getPayloadSignature(payload),
					};
					contextPayloads.set(projectionKey, projection);
				}
				sendDomain(
					client,
					"context",
					WindowBridge.MessageType.CONTEXT_SNAPSHOT,
					projection.payload,
					projection.signature,
				);
			});
		}

		if (domains.has("query")) {
			const queryClients = incrementalClients.filter(
				(client) =>
					(client.kind === "assistant" &&
						client.assistantQuerySyncEnabled) ||
					(client.kind === "table" && client.tableSyncEnabled),
			);
			if (queryClients.length > 0) {
				const queryPayload = WindowBridge.createQueryContext(
					currentApp,
					operationId,
				);
				const signature = getPayloadSignature(queryPayload);
				queryClients.forEach((client) =>
					sendDomain(
						client,
						"query",
						WindowBridge.MessageType.QUERY_CONTEXT,
						queryPayload,
						signature,
					),
				);
			}
		}

		if (domains.has("rendered")) {
			const notesClients = incrementalClients.filter(
				(client) =>
					client.kind === "notes" &&
					client.notesRenderedSyncEnabled,
			);
			if (notesClients.length > 0) {
				const renderedSourceIds = WindowBridge.createRenderedSourceIds(
					currentApp,
					operationId,
				);
				const renderedPayload: WindowBridge.RenderedSourcesChangedPayload = {
					sourceIds: renderedSourceIds,
				};
				const signature = getPayloadSignature(renderedSourceIds);
				notesClients.forEach((client) =>
					sendDomain(
						client,
						"rendered",
						WindowBridge.MessageType.RENDERED_SOURCES_CHANGED,
						renderedPayload,
						signature,
					),
				);
			}
		}

		if (domains.has("event")) {
			const event = currentApp.timeline.target ?? null;
			incrementalClients.forEach((client) => {
				if (client.kind === "table" || client.kind === "notes") {
					const payload: WindowBridge.EventTargetChangedPayload = {
						docId: event?._id ?? null,
					};
					sendDomain(
						client,
						"event",
						WindowBridge.MessageType.EVENT_TARGET_CHANGED,
						payload,
						getPayloadSignature(payload.docId),
					);
				}
			});
		}

		if (domains.has("assistant")) {
			const assistantClients = incrementalClients.filter(
				(client) => client.kind === "assistant",
			);
			if (assistantClients.length > 0) {
				const payload: WindowBridge.AssistantConnectionChangedPayload = {
					wsId: currentApp.general.ws_id,
				};
				const signature = getPayloadSignature(payload.wsId);
				assistantClients.forEach((client) =>
					sendDomain(
						client,
						"assistant",
						WindowBridge.MessageType.ASSISTANT_CONNECTION_CHANGED,
						payload,
						signature,
					),
				);
			}
		}

		if (domains.has("dialog")) {
			const dialogClients = incrementalClients.filter(
				(client) => client.kind === "dialog",
			);
			if (dialogClients.length > 0) {
				const payload = getProjectedDialogPayload();
				const signature = getDialogPayloadSignature(payload);
				dialogClients.forEach((client) =>
					sendDomain(
						client,
						"dialog",
						WindowBridge.MessageType.DIALOG_CHANGED,
						payload,
						signature,
					),
				);
			}
		}
	}, [
		getCurrentOperationId,
		projectDialog,
		pruneClients,
		sendBootstrap,
		sendDomain,
	]);

	const flushDetachedStateRef = useRef(flushDetachedState);
	flushDetachedStateRef.current = flushDetachedState;

	const scheduleDetachedState = useCallback((
		domains: readonly StateSyncDomain[],
	): void => {
		if (clientsRef.current.size === 0) return;
		domains.forEach((domain) => {
			if (
				WindowBridge.hasStateDomainSubscriber(
					clientsRef.current.values(),
					domain,
				)
			) {
				pendingStateDomainsRef.current.add(domain);
			}
		});
		if (pendingStateDomainsRef.current.size === 0) return;
		if (pendingStateFlushRef.current !== null) return;
		const flush = (): void => {
			pendingStateFlushRef.current = null;
			const pendingDomains = new Set(pendingStateDomainsRef.current);
			pendingStateDomainsRef.current.clear();
			flushDetachedStateRef.current(pendingDomains);
		};
		if (typeof window.requestIdleCallback === "function") {
			pendingStateFlushRef.current = {
				id: window.requestIdleCallback(flush, { timeout: 100 }),
				kind: "idle",
			};
			return;
		}
		pendingStateFlushRef.current = {
			id: window.setTimeout(flush, 0),
			kind: "timeout",
		};
	}, []);

	/**
	 * Hydrates only the bounded EventStore records needed by a detached dialog.
	 * Stale reads are ignored across descriptor and operation changes.
	 * @param dialog Dialog descriptor to hydrate, defaulting to the active dialog.
	 * @returns Promise settled after the bounded projection has been refreshed.
	 */
	const hydrateDialogProjection = useCallback(async (
		dialog = dialogDescriptorRef.current,
	): Promise<void> => {
		const hydrationSequence = ++dialogHydrationSequenceRef.current;
		if (!dialog) {
			dialogRelatedEventsRef.current = [];
			dialogNeighborWindowRef.current = null;
			projectDialog(appRef.current, null);
			return;
		}

		const operationEpoch = DataStore.getRenderOperationEpoch();
		const descriptorEvents =
			dialog.kind === "event" ? [dialog.event] : dialog.events;
		const descriptorEventIds = new Set(
			descriptorEvents.map((event) => event._id),
		);
		const relatedLimit = Math.max(
			0,
			EVENT_MAIN_CACHE_SIZE - descriptorEventIds.size,
		);
		const relatedDocumentIds = getDialogRelatedDocumentIds(dialog)
			.filter((documentId) => !descriptorEventIds.has(documentId))
			.slice(0, relatedLimit);
		const halfWindow = Math.floor(
			WindowBridge.DETACHED_DIALOG_NAVIGATION_WINDOW_SIZE / 2,
		);

		try {
			const [relatedEvents, neighborWindow] = await Promise.all([
				DataStore.Events.getByIds(relatedDocumentIds),
				dialog.kind === "event"
					? DataStore.Events.getNeighbors(
							dialog.event["gulp.source_id"],
							dialog.event._id,
							halfWindow,
							halfWindow,
						)
					: Promise.resolve(null),
			]);
			if (
				hydrationSequence !== dialogHydrationSequenceRef.current ||
				dialogDescriptorRef.current !== dialog ||
				operationEpoch !== DataStore.getRenderOperationEpoch()
			) {
				return;
			}

			dialogRelatedEventsRef.current = relatedEvents;
			dialogNeighborWindowRef.current = neighborWindow;
			projectDialog(appRef.current, dialog);
			scheduleDetachedState(["context", "dialog"]);
		} catch (error) {
			console.warn("Unable to hydrate detached dialog events", error);
		}
	}, [projectDialog, scheduleDetachedState]);

	const unregisterClient = useCallback((clientId: string): void => {
		const client = clientsRef.current.get(clientId);
		client?.bridge.destroy();
		clientsRef.current.delete(clientId);
		pendingTableSourceRef.current.delete(clientId);
	}, []);

	const forgetWindow = useCallback(
		(key: string): void => {
			const record = windowsRef.current[key];
			if (record?.kind === "dialog") {
				setDialogsDocked(true);
			}
			if (record) unregisterClient(record.bridgeId);
			delete windowsRef.current[key];
			if (record && isPersistedWindowKind(record.kind)) {
				removePersistedDetachedWindow(
					key,
					coordinatorIdRef.current,
				);
			}
		},
		[setDialogsDocked, unregisterClient],
	);

	const verifyWindowClosed = useCallback(
		(key: string, expectedWindow: Window): void => {
			window.setTimeout(() => {
				const record = windowsRef.current[key];
				if (
					record?.window === expectedWindow &&
					expectedWindow.closed
				) {
					forgetWindow(key);
				}
			}, 250);
		},
		[forgetWindow],
	);

	const openRoutedDetachedWindow = useCallback(
		(
			key: string,
			kind: RoutedDetachedWindowKind,
			windowName: string,
			features: string,
			sourceId?: Source.Id,
			assistantTarget?: AssistantWindowTarget,
		) => {
			const existing = windowsRef.current[key];
			if (existing && !existing.window.closed) {
				existing.window.focus();
				if (sourceId) {
					const client = clientsRef.current.get(existing.bridgeId);
					if (client) {
						client.bridge.send(
							WindowBridge.MessageType.TABLE_SELECT_SOURCE,
							{ sourceId },
						);
					} else {
						pendingTableSourceRef.current.set(existing.bridgeId, sourceId);
					}
				}
				return existing.window;
			}

			const bridgeId = WindowBridge.generateId();
			const targetWindow = window.open(
				createDetachedRouteUrl(
					kind,
					bridgeId,
					coordinatorIdRef.current,
					sourceId,
					assistantTarget,
				),
				windowName,
				features,
			);
			if (!targetWindow) return null;

			windowsRef.current[key] = {
				key,
				kind,
				window: targetWindow,
				bridgeId,
			};
			if (isPersistedWindowKind(kind)) {
				persistDetachedWindow(
					{ key, kind, windowName, features },
					coordinatorIdRef.current,
				);
			}
			targetWindow.addEventListener(
				"beforeunload",
				() => verifyWindowClosed(key, targetWindow),
				{ once: true },
			);
			return targetWindow;
		},
		[verifyWindowClosed],
	);

	const openNotesWindow = useCallback(() => {
		const record = getPersistedDetachedWindowRecord(
			"notes",
			coordinatorIdRef.current,
		);
		openRoutedDetachedWindow(
			record.key,
			record.kind,
			record.windowName,
			record.features,
		);
	}, [openRoutedDetachedWindow]);

	const openTableWindow = useCallback(
		(sourceId?: Source.Id) => {
			const record = getPersistedDetachedWindowRecord(
				"table",
				coordinatorIdRef.current,
			);
			openRoutedDetachedWindow(
				record.key,
				record.kind,
				record.windowName,
				record.features,
				sourceId,
			);
		},
		[openRoutedDetachedWindow],
	);

	const openDashboardWindow = useCallback(() => {
		const record = getPersistedDetachedWindowRecord(
			"dashboard",
			coordinatorIdRef.current,
		);
		openRoutedDetachedWindow(
			record.key,
			record.kind,
			record.windowName,
			record.features,
		);
	}, [openRoutedDetachedWindow]);

	const openAssistantWindow = useCallback(
		(target: AssistantWindowTarget) => {
			openRoutedDetachedWindow(
				`assistant:${target.key}`,
				"assistant",
				createAssistantWindowName(
					target.key,
					coordinatorIdRef.current,
				),
				"width=480,height=800,left=120,top=120",
				undefined,
				target,
			);
		},
		[openRoutedDetachedWindow],
	);

	const openDialogWindow = useCallback(
		(
			nextDialog: WindowBridge.DetachedDialogDescriptor,
			features: string,
		): boolean => {
			pruneClients();
			dialogDescriptorRef.current = nextDialog;
			dialogRelatedEventsRef.current = [];
			dialogNeighborWindowRef.current = null;
			void hydrateDialogProjection(nextDialog);
			const key = "dialog";
			const existing = windowsRef.current[key];
			if (existing && !existing.window.closed) {
				const client = clientsRef.current.get(existing.bridgeId);
				if (client) {
					const payload = projectDialog(appRef.current, nextDialog);
					const contextPayload =
						WindowBridge.createContextSnapshot(
							appRef.current,
							getCurrentOperationId(),
							{
								sourceIds: payload.events.map(
									(event) => event["gulp.source_id"],
								),
								includeDialogUser: true,
							},
						);
					sendDomain(
						client,
						"context",
						WindowBridge.MessageType.CONTEXT_SNAPSHOT,
						contextPayload,
					);
					sendDomain(
						client,
						"dialog",
						WindowBridge.MessageType.DIALOG_CHANGED,
						payload,
						getDialogPayloadSignature(payload),
					);
				}
				return true;
			}
			return !!openRoutedDetachedWindow(
				key,
				"dialog",
				createDialogWindowName(coordinatorIdRef.current),
				features,
			);
		},
		[
			getCurrentOperationId,
			hydrateDialogProjection,
			projectDialog,
			openRoutedDetachedWindow,
			pruneClients,
			sendDomain,
		],
	);

	const closeDialogWindow = useCallback((): void => {
		dialogDescriptorRef.current = null;
		dialogProjectionRef.current = null;
		dialogRelatedEventsRef.current = [];
		dialogNeighborWindowRef.current = null;
		dialogHydrationSequenceRef.current++;
		dialogSourcePresentationSignatureRef.current = "";
		const record = windowsRef.current.dialog;
		if (!record) return;
		unregisterClient(record.bridgeId);
		delete windowsRef.current.dialog;
		if (!record.window.closed) record.window.close();
	}, [unregisterClient]);

	const recoverPersistedDetachedWindows = useCallback(() => {
		readPersistedDetachedWindows(
			coordinatorIdRef.current,
		).forEach((record) => {
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
		if (status !== "active") {
			if (status === "auth_lost") {
				recoveredPersistedWindowsRef.current = false;
			}
			return;
		}
		if (recoveredPersistedWindowsRef.current) return;
		recoveredPersistedWindowsRef.current = true;
		recoverPersistedDetachedWindows();
	}, [recoverPersistedDetachedWindows, status]);

	useEffect(() => {
		const handleBridgeMessage = (message: WindowBridge.Message): void => {
			switch (message.type) {
				case WindowBridge.MessageType.NAVIGATE_TO_EVENT: {
					const { docId, operationId } =
						message.payload as WindowBridge.NavigateToEventPayload;
					const lookupSequence = ++bridgeNavigateLookupSequenceRef.current;
					const operationEpoch = DataStore.getRenderOperationEpoch();
					void DataStore.Events.getById(docId)
						.then((event) => {
							if (
								lookupSequence !== bridgeNavigateLookupSequenceRef.current ||
								operationEpoch !== DataStore.getRenderOperationEpoch()
							) {
								return;
							}
							if (!event) {
								window.focus();
								spawnBannerRef.current(
									<FetchEventBannerMain
										docId={docId}
										operationId={operationId}
									/>,
								);
								return;
							}

							infoRef.current.setTimelineTarget(event);
							const focusCanvasOnEvent = (
								window as Window & {
									focusCanvasOnEvent?: (
										timestamp: number,
										onLeft?: boolean,
										sourceId?: Source.Id,
									) => void;
								}
							).focusCanvasOnEvent;
							focusCanvasOnEvent?.(
								event.gulp_timestamp,
								false,
								event["gulp.source_id"],
							);
							window.focus();
							spawnDialogRef.current(
								<DisplayEventDialog event={event} />,
							);
						})
						.catch(() => {
							if (
								lookupSequence !== bridgeNavigateLookupSequenceRef.current ||
								operationEpoch !== DataStore.getRenderOperationEpoch()
							) {
								return;
							}
							window.focus();
							spawnBannerRef.current(
								<FetchEventBannerMain
									docId={docId}
									operationId={operationId}
								/>,
							);
						});
					break;
				}
				case WindowBridge.MessageType.FOCUS_TIMELINE_REQUEST: {
					const { timestamp, onLeft, sourceId } =
						message.payload as WindowBridge.FocusTimelineRequestPayload;
					const focusCanvasOnEvent = (
						window as Window & {
							focusCanvasOnEvent?: (
								timestamp: number,
								onLeft?: boolean,
								sourceId?: Source.Id,
							) => void;
						}
					).focusCanvasOnEvent;
					focusCanvasOnEvent?.(timestamp, onLeft, sourceId);
					window.focus();
					break;
				}
				case WindowBridge.MessageType.TIMELINE_ZOOM_REQUEST: {
					const { min, max } =
						message.payload as WindowBridge.TimelineZoomRequestPayload;
					if (
						!Number.isFinite(min) ||
						!Number.isFinite(max) ||
						min >= max
					) {
						break;
					}
					infoRef.current.setTimelineFrame({ min, max });
					infoRef.current.setTimelineScale(1);
					scrollStore.setScrollX(0);
					window.focus();
					break;
				}
				case WindowBridge.MessageType.OPEN_FILTER_REQUEST: {
					const { sourceIds, query, name } =
						message.payload as WindowBridge.OpenFilterRequestPayload;
					const sources = sourceIds
						.map((sourceId) =>
							Source.Entity.id(appRef.current, sourceId),
						)
						.filter((source): source is Source.Type => Boolean(source));
					if (sources.length === 0) break;
					spawnBannerRef.current(
						<FilterFileBanner
							sources={sources}
							query={query}
							name={name}
						/>,
					);
					window.focus();
					break;
				}
				case WindowBridge.MessageType.AUTH_LOST:
					authLostRef.current = true;
					setAuthLost(true);
					window.dispatchEvent(new CustomEvent("gulp-auth-lost"));
					break;
				case WindowBridge.MessageType.EVENT_TARGET_REQUEST: {
					const { docId } =
						message.payload as WindowBridge.EventTargetRequestPayload;
					const lookupSequence = ++bridgeTargetLookupSequenceRef.current;
					if (!docId) {
						infoRef.current.setTimelineTarget(null);
						if (
							clientsRef.current.get(message.senderId)?.kind === "dialog"
						) {
							spawnDialogRef.current(null);
						}
						break;
					}
					const operationEpoch = DataStore.getRenderOperationEpoch();
					void DataStore.Events.getById(docId).then((event) => {
						if (
							!event ||
							lookupSequence !== bridgeTargetLookupSequenceRef.current ||
							operationEpoch !== DataStore.getRenderOperationEpoch()
						) {
							return;
						}
						if (event._id !== infoRef.current.app.timeline.target?._id) {
							infoRef.current.setTimelineTarget(event);
						}
						if (
							clientsRef.current.get(message.senderId)?.kind === "dialog"
						) {
							spawnDialogRef.current(
								<DisplayEventDialog event={event} />,
							);
						}
					}).catch(() => undefined);
					break;
				}
				case WindowBridge.MessageType.DOCK_DIALOG:
					setDialogsDocked(true);
					break;
				case WindowBridge.MessageType.DETACHED_READY: {
					const payload =
						message.payload as WindowBridge.DetachedReadyPayload;
					const existing = clientsRef.current.get(message.senderId);
					if (existing) {
						existing.lastSeenAt = Date.now();
						if (existing.kind === "dialog") {
							void hydrateDialogProjection();
						}
						const wasTableSyncEnabled = existing.tableSyncEnabled;
						existing.tableSyncEnabled =
							payload.tableSyncEnabled ?? existing.tableSyncEnabled;
						const wasNotesRenderedSyncEnabled =
							existing.notesRenderedSyncEnabled;
						existing.notesRenderedSyncEnabled =
							payload.notesRenderedSyncEnabled ??
							existing.notesRenderedSyncEnabled;
						const wasAssistantQuerySyncEnabled =
							existing.assistantQuerySyncEnabled;
						existing.assistantQuerySyncEnabled =
							payload.assistantQuerySyncEnabled ??
							existing.assistantQuerySyncEnabled;
						if (
							payload.needsBootstrap &&
							statusRef.current === "active"
						) {
							delete existing.signatures.status;
							scheduleDetachedState(["status"]);
							break;
						}
						if (
							existing.kind === "table" &&
							!wasTableSyncEnabled &&
							existing.tableSyncEnabled &&
							statusRef.current === "active"
						) {
							delete existing.signatures.query;
							scheduleDetachedState(["query"]);
						}
						if (
							existing.kind === "notes" &&
							!wasNotesRenderedSyncEnabled &&
							existing.notesRenderedSyncEnabled &&
							statusRef.current === "active"
						) {
							delete existing.signatures.rendered;
							scheduleDetachedState(["rendered"]);
						}
						if (
							existing.kind === "assistant" &&
							!wasAssistantQuerySyncEnabled &&
							existing.assistantQuerySyncEnabled &&
							statusRef.current === "active"
						) {
							delete existing.signatures.context;
							delete existing.signatures.query;
							scheduleDetachedState(["context", "query"]);
						}
						break;
					}
					const clientId = message.senderId;
					const client: DetachedClient = {
						id: clientId,
						kind: payload.kind,
						bridge: WindowBridge.createClient(
							clientId,
							bridgeIdRef.current,
							(privateMessage) => {
								if (privateMessage.senderId !== clientId) return;
								const registeredClient =
									clientsRef.current.get(clientId);
								if (!registeredClient) return;
								registeredClient.lastSeenAt = Date.now();
								if (
									privateMessage.type ===
									WindowBridge.MessageType.DETACHED_HEARTBEAT
								) {
									return;
								}
								bridgeMessageHandlerRef.current(privateMessage);
							},
							coordinatorIdRef.current,
						),
						lastSeenAt: Date.now(),
						tableSyncEnabled: payload.tableSyncEnabled ?? false,
						notesRenderedSyncEnabled:
							payload.notesRenderedSyncEnabled ?? false,
						assistantQuerySyncEnabled:
							payload.assistantQuerySyncEnabled ?? false,
						signatures: {},
					};
					clientsRef.current.set(client.id, client);
					if (client.kind === "dialog") {
						void hydrateDialogProjection();
					}
					if (statusRef.current === "active") {
						scheduleDetachedState(["status"]);
					} else {
						sendBootstrap(client);
					}
					break;
				}
				case WindowBridge.MessageType.DETACHED_CLOSED: {
					const record = Object.values(windowsRef.current).find(
						(windowRecord) =>
							windowRecord.bridgeId === message.senderId,
					);
					if (record) {
						verifyWindowClosed(record.key, record.window);
					} else {
						unregisterClient(message.senderId);
					}
					break;
				}
				case WindowBridge.MessageType.TABLE_SYNC_CHANGED: {
					const client = clientsRef.current.get(message.senderId);
					if (!client || client.kind !== "table") break;
					client.lastSeenAt = Date.now();
					client.tableSyncEnabled = (
						message.payload as WindowBridge.TableSyncChangedPayload
					).enabled;
					delete client.signatures.query;
					if (client.tableSyncEnabled && statusRef.current === "active") {
						scheduleDetachedState(["query"]);
					}
					break;
				}
				case WindowBridge.MessageType.NOTES_RENDERED_SYNC_CHANGED: {
					const client = clientsRef.current.get(message.senderId);
					if (!client || client.kind !== "notes") break;
					client.lastSeenAt = Date.now();
					client.notesRenderedSyncEnabled = (
						message.payload as WindowBridge.NotesRenderedSyncChangedPayload
					).enabled;
					delete client.signatures.rendered;
					if (
						client.notesRenderedSyncEnabled &&
						statusRef.current === "active"
					) {
						scheduleDetachedState(["rendered"]);
					}
					break;
				}
			}
		};
		bridgeMessageHandlerRef.current = handleBridgeMessage;
		const bridge = WindowBridge.create(
			bridgeIdRef.current,
			handleBridgeMessage,
			coordinatorIdRef.current,
		);
		bridgeRef.current = bridge;
		return () => {
			bridge.destroy();
			if (bridgeMessageHandlerRef.current === handleBridgeMessage) {
				bridgeMessageHandlerRef.current = () => {};
			}
			bridgeRef.current = null;
			clientsRef.current.forEach((client) => client.bridge.destroy());
			clientsRef.current.clear();
		};
	}, [
		hydrateDialogProjection,
		scheduleDetachedState,
		sendBootstrap,
		setDialogsDocked,
		unregisterClient,
		verifyWindowClosed,
	]);

	useEffect(() => {
		scheduleDetachedState(["status"]);
	}, [scheduleDetachedState, status]);

	useEffect(() => {
		scheduleDetachedState(["context", "query", "rendered"]);
	}, [
		app.target.files,
		app.target.operations,
		app.target.contexts,
		app.general.user?.id,
		app.general.user?.user_data?.show_preview,
		routeOperationId,
		selectedOperationId,
		scheduleDetachedState,
	]);

	useEffect(() => {
		if (
			statusRef.current !== "active" ||
			!WindowBridge.hasStateDomainSubscriber(
				clientsRef.current.values(),
				"dialog",
			)
		) {
			return;
		}
		const projection = dialogProjectionRef.current;
		if (!projection) return;
		const signature = getDialogSourcePresentationSignature(
			appRef.current,
			projection,
		);
		if (signature === dialogSourcePresentationSignatureRef.current) return;
		dialogSourcePresentationSignatureRef.current = signature;
		scheduleDetachedState(["dialog"]);
	}, [app.target.files, scheduleDetachedState]);

	useEffect(() => {
		scheduleDetachedState(["query"]);
	}, [
		app.target.files,
		app.timeline.frame.min,
		app.timeline.frame.max,
		scheduleDetachedState,
	]);

	useEffect(() => {
		scheduleDetachedState(["rendered"]);
	}, [
		app.target.files,
		app.target.contexts,
		app.timeline.filter,
		app.hidden.sourcesWithNoEvents,
		scheduleDetachedState,
	]);

	useEffect(() => {
		scheduleDetachedState(["event"]);
	}, [
		app.timeline.target?._id,
		scheduleDetachedState,
	]);

	useEffect(() => {
		scheduleDetachedState(["assistant"]);
	}, [app.general.ws_id, scheduleDetachedState]);

	useEffect(() => {
		return DataStore.subscribeDocuments(() => {
			if (
				![...clientsRef.current.values()].some(
					(client) =>
						client.kind === "notes" &&
						client.notesRenderedSyncEnabled,
				)
			) {
				return;
			}
			scheduleDetachedState(["rendered"]);
		});
	}, [scheduleDetachedState]);

	useEffect(() => {
		return DataStore.subscribeRenderChanges((change) => {
			if (
				statusRef.current !== "active" ||
				![...clientsRef.current.values()].some(
					(client) => client.kind === "dialog",
				)
			) {
				return;
			}

			if (
				!isDialogRenderChangeRelevant(
					change,
					dialogDescriptorRef.current,
					dialogProjectionRef.current,
				)
			) {
				return;
			}

			if (change.type === "notes") {
				scheduleDetachedState(["dialog"]);
				return;
			}
			if (change.type === "links") {
				void hydrateDialogProjection();
				scheduleDetachedState(["context", "dialog"]);
				return;
			}
			if (change.type === "source-events") {
				void hydrateDialogProjection();
				scheduleDetachedState(["context", "dialog"]);
				return;
			}
			if (change.type === "supplemental-events") {
				void hydrateDialogProjection();
				scheduleDetachedState(["context", "dialog"]);
			}
		});
	}, [hydrateDialogProjection, scheduleDetachedState]);

	useEffect(() => {
		return DataStore.subscribeEventSourceSummaries((summary) => {
			if (
				summary.status === "building" ||
				statusRef.current !== "active" ||
				!dialogDescriptorRef.current ||
				![...clientsRef.current.values()].some(
					(client) => client.kind === "dialog",
				)
			) {
				return;
			}
			void hydrateDialogProjection();
		});
	}, [hydrateDialogProjection]);

	useEffect(() => {
		if (status !== "active") return;
		pruneClients();
		const activeStatusSignature = getPayloadSignature("active");
		const notesClients = [...clientsRef.current.values()].filter(
			(client) =>
				client.kind === "notes" &&
				client.signatures.status === activeStatusSignature,
		);
		if (notesClients.length === 0) return;
		const operationId = getCurrentOperationId();
		const payload = WindowBridge.createNotesChangedPayload(
			appRef.current,
			operationId,
		);
		const signature = getPayloadSignature(payload);
		notesClients.forEach((client) =>
			sendDomain(
				client,
				"notes",
				WindowBridge.MessageType.NOTES_CHANGED,
				payload,
				signature,
			),
		);
	}, [
		getCurrentOperationId,
		selectedOperationId,
		app.target.files,
		pruneClients,
		sendDomain,
		status,
	]);

	useEffect(() => {
		const handleNotesSyncChange = (change: NotesSyncChange): void => {
			if (statusRef.current !== "active") return;
			pruneClients();
			const operationId = getCurrentOperationId();
			if (change.operationId !== operationId) return;
			const activeStatusSignature = getPayloadSignature("active");
			const notesClients = [...clientsRef.current.values()].filter(
				(client) =>
					client.kind === "notes" &&
					client.signatures.status === activeStatusSignature,
			);
			if (notesClients.length === 0) return;

			if (change.mutation === "snapshot-ready") {
				const payload = WindowBridge.createNotesChangedPayload(
					appRef.current,
					operationId,
				);
				const signature = getPayloadSignature(payload);
				notesClients.forEach((client) =>
					sendDomain(
						client,
						"notes",
						WindowBridge.MessageType.NOTES_CHANGED,
						payload,
						signature,
					),
				);
				return;
			}

			let payload: WindowBridge.NotesDeltaPayload = change;
			if (change.mutation !== "delete") {
				const selectedSourceIds = new Set(
					appRef.current.target.files
						.filter(
							(source) =>
								source.selected &&
								(!operationId ||
									source.operation_id === operationId),
						)
						.map((source) => source.id),
				);
				const filteredPayload =
					WindowBridge.filterNotesDeltaForSelectedSources(
						change,
						selectedSourceIds,
					);
				if (!filteredPayload) return;
				payload = filteredPayload;
			}
			notesClients.forEach((client) =>
				client.bridge.send(WindowBridge.MessageType.NOTES_DELTA, payload),
			);
		};
		return DataStore.subscribeNotesSyncChanges(handleNotesSyncChange);
	}, [getCurrentOperationId, pruneClients, sendDomain]);

	useEffect(() => {
		themeSnapshotRef.current = null;
		if (clientsRef.current.size === 0) return;

		let timeoutId: number | null = null;
		const frameId = window.requestAnimationFrame(() => {
			timeoutId = window.setTimeout(() => {
				pruneClients();
				if (clientsRef.current.size === 0) return;
				const nextTheme = getThemeSnapshot();
				themeSnapshotRef.current = nextTheme;
				const signature = getPayloadSignature(nextTheme);
				clientsRef.current.forEach((client) =>
					sendDomain(
						client,
						"theme",
						WindowBridge.MessageType.THEME_CHANGE,
						nextTheme,
						signature,
					),
				);
			}, 0);
		});
		return () => {
			window.cancelAnimationFrame(frameId);
			if (timeoutId !== null) window.clearTimeout(timeoutId);
		};
	}, [getThemeSnapshot, pruneClients, sendDomain, theme]);

	useEffect(() => {
		const handleAuthLost = () => {
			authLostRef.current = true;
			setAuthLost(true);
		};
		const handleAuthRestored = () => {
			authLostRef.current = false;
			setAuthLost(false);
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
		WindowBridge.persistMainStatus(status, coordinatorIdRef.current);
	}, [status]);

	useEffect(() => {
		const handleBeforeUnload = () => {
			const payload: WindowBridge.MainContextStatusPayload = {
				status: "initializing",
				reconnectRequired: true,
			};
			clientsRef.current.forEach((client) =>
				client.bridge.send(
					WindowBridge.MessageType.MAIN_CONTEXT_STATUS,
					payload,
				),
			);
			WindowBridge.persistMainStatus(
				"initializing",
				coordinatorIdRef.current,
				true,
			);
		};
		window.addEventListener("beforeunload", handleBeforeUnload);
		return () => window.removeEventListener("beforeunload", handleBeforeUnload);
	}, []);

	useEffect(() => {
		return () => {
			const pendingFlush = pendingStateFlushRef.current;
			if (!pendingFlush) return;
			if (pendingFlush.kind === "idle") {
				window.cancelIdleCallback(pendingFlush.id);
			} else {
				window.clearTimeout(pendingFlush.id);
			}
		};
	}, []);

	useEffect(() => {
		if (!app.general.tableViewSource) return;
		openTableWindow(app.general.tableViewSource.id);
		Info.setInfoByKey(null, "general", "tableViewSource");
	}, [Info, app.general.tableViewSource, openTableWindow]);

	const value = useMemo<DetachedWindowContextValue>(
		() => ({
			coordinatorId: coordinatorIdRef.current,
			openNotesWindow,
			openTableWindow,
			openDashboardWindow,
			openAssistantWindow,
			openDialogWindow,
			closeDialogWindow,
		}),
		[
			closeDialogWindow,
			openAssistantWindow,
			openDashboardWindow,
			openDialogWindow,
			openNotesWindow,
			openTableWindow,
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

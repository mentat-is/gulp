/**
 * Lightweight Application context for independently rendered detached windows.
 *
 * Detached roots receive an atomic bootstrap and capability-scoped updates over
 * a private channel. Once registered, user actions return on that same private
 * channel; only registration/reconnect/close uses shared control.
 */
import React, {
	ReactNode,
	useCallback,
	useEffect,
	useMemo,
	useRef,
	useState,
} from "react";
import { Application } from "@/context/Application.context";
import { Info } from "@/class/Info";
import { App } from "@/entities/App";
import { WindowBridge } from "@/lib/WindowBridge";
import { DataStore } from "@/store/DataStore";
import { Note } from "@/entities/Note";
import { RenderEngine } from "@/class/RenderEngine";
import { Color } from "@/entities/Color";
import { Operation } from "@/entities/Operation";
import { Source } from "@/entities/Source";
import { createEmptyQueryCacheState } from "@/entities/QueryCache";
import type { Context } from "@/entities/Context";
import { Doc } from "@/entities/Doc";
import { User } from "@/entities/User";
import { Request } from "@/entities/Request";
import { Extension } from "@/context/Extension.context";
import { Locale } from "@/locales";
import { ownsIndependentDetachedDataStore } from "@/context/detachedDataStore";
import { applyDetachedThemeSnapshot } from "@/context/detachedTheme";
import { SmartSocket } from "@/class/SmartSocket";
import { Boundary } from "@/context/Boundary.context";
import { isEventStoreLodEnabled } from "@/event-store";
import { resolveEventStoreCacheNamespaceId } from "@/event-store/EventStore.namespace";
import s from "./styles/DetachedApp.module.css";

const ACTIVE_HEARTBEAT_MSEC = 30_000;
const RECONNECT_HEARTBEAT_MSEC = 1_000;
// Storage is recovery-only; let the normal coalesced private bootstrap arrive first.
const STORAGE_BOOTSTRAP_FALLBACK_MSEC = 250;
const EMPTY_EXTENSION_CONTEXT: Extension.Export = {
	extensions: {},
	ready: true,
};

type DetachedPayloadDomain =
	| "bootstrap"
	| "context"
	| "query"
	| "rendered"
	| "event"
	| "assistant"
	| "dialog"
	| "theme";

export namespace DetachedApp {
	export interface ProviderProps {
		/** Optional initial state for opener-rendered popup roots. */
		initialApp?: App.Type;
		/** Stable detached-client identifier used for its private data channel. */
		bridgeId: string;
		/** Main-tab namespace that owns this detached client. */
		coordinatorId: string;
		/** Capability advertised to the main coordinator. */
		kind: WindowBridge.DetachedClientKind;
		/** Document that owns the detached React root. */
		detachedDocument?: Document;
		/** Main-window banner dispatcher used by opener-rendered roots. */
		mainSpawnBanner?: (node: ReactNode, target?: string) => void;
		/** Whether local timeline selections should update the main tab. */
		syncTimelineTargetToMain?: boolean;
		/** Whether an assistant plugin needs live main query/frame state. */
		assistantQuerySyncEnabled?: boolean;
		/** Whether this detached surface consumes dynamically loaded UI extensions. */
		loadExtensions?: boolean;
		/** Optional filename allow-list for a targeted plugin surface. */
		extensionFilenames?: string[];
		/** Optional extension mount slots consumed by this surface. */
		extensionSlots?: Extension.Slot[];
		children: ReactNode;
	}
}

/**
 * Checks whether a detached source uses the compact Table View projection.
 * @param source Detached source payload to inspect.
 * @returns True for compact Table View source summaries.
 */
function isDetachedTableSource(
	source:
		| WindowBridge.DetachedSourceSummary
		| WindowBridge.DetachedNotesSourceSummary
		| WindowBridge.DetachedTableSourceSummary,
): source is WindowBridge.DetachedTableSourceSummary {
	return "projection" in source && source.projection === "table";
}

/**
 * Restores a serializable source summary to the Source shape used by the UI.
 * @param source Full or capability-scoped detached source payload.
 * @returns Hydrated source entity with required client-only defaults.
 */
function hydrateDetachedSource(
	source:
		| WindowBridge.DetachedSourceSummary
		| WindowBridge.DetachedNotesSourceSummary
		| WindowBridge.DetachedTableSourceSummary,
): Source.Type {
	if (isDetachedTableSource(source)) {
		const { projection: _projection, ...tableSource } = source;
		return {
			...tableSource,
			owner_user_id: "" as User.Id,
			granted_user_ids: [],
			granted_user_group_ids: [],
			time_created: 0,
			selected: true,
			appliedQuery: null,
			queryCache: createEmptyQueryCacheState(),
			nanotimestamp: {
				min: BigInt(source.nanotimestamp.min),
				max: BigInt(source.nanotimestamp.max),
			},
			settings: {
				render_color_palette: "plasma",
				render_engine: "default",
				timeline_label: "filename",
				...source.settings,
			},
			_sampleDataCached: {
				frequency_sample: source.settings.frequency_sample,
				min_timestamp: source.timestamp.min,
				max_timestampe: source.timestamp.max,
				sample_data: null,
			},
		};
	}

	if (!("nanotimestamp" in source)) {
		return {
			...source,
			plugin: "",
			owner_user_id: "" as User.Id,
			granted_user_ids: [],
			granted_user_group_ids: [],
			time_created: 0,
			time_updated: null,
			glyph_id: null as never,
			pinned: false,
			appliedQuery: null,
			queryCache: createEmptyQueryCacheState(),
			timestamp: { min: 0, max: 0 },
			nanotimestamp: { min: 0n, max: 0n },
			total: 0,
			settings: {
				offset: 0,
				frequency_sample: 0,
				...source.settings,
			} as Source.Type["settings"],
			_sampleDataCached: {
				frequency_sample: 0,
				min_timestamp: 0,
				max_timestampe: 0,
				sample_data: null,
			},
		};
	}
	return {
		...source,
		selected: true,
		appliedQuery: source.appliedQuery ?? null,
		queryCache: source.queryCache ?? createEmptyQueryCacheState(),
		nanotimestamp: {
			min: BigInt(source.nanotimestamp.min),
			max: BigInt(source.nanotimestamp.max),
		},
	};
}

/**
 * Restores a compact Notes context to the entity shape expected by app state.
 */
function hydrateDetachedContext(
	context: Context.Type | WindowBridge.DetachedNotesContextSummary,
): Context.Type {
	if ("owner_user_id" in context) return context;
	return {
		...context,
		owner_user_id: "" as User.Id,
		granted_user_ids: [],
		granted_user_group_ids: [],
		time_created: 0,
		time_updated: 0,
		glyph_id: null as never,
	};
}

/**
 * Creates a deterministic signature for structured bridge data.
 */
function serializePayload(value: unknown): string {
	return JSON.stringify(value, (_key, item) =>
		typeof item === "bigint" ? item.toString() : item,
	);
}

/**
 * Applies operation, context, and selected-source state without discarding an
 * opener-rendered dialog's event cache when the operation is unchanged.
 */
function applyContextSnapshot(
	app: App.Type,
	payload: WindowBridge.ContextSnapshotPayload,
): App.Type {
	const previousOperationId = Operation.Entity.selected(app)?.id;
	const nextOperationId = payload.operation?.id;
	const operationChanged = previousOperationId !== nextOperationId;
	const user =
		payload.user === undefined
			? app.general.user
			: payload.user === null
				? null
				: ({
						...(app.general.user?.id === payload.user.id
							? app.general.user
							: {}),
						id: payload.user.id,
						user_data: {
							...(app.general.user?.id === payload.user.id
								? app.general.user.user_data
								: {}),
							...(payload.user.showPreview === undefined
								? {}
								: { show_preview: payload.user.showPreview }),
						},
					} as User.Type);

	return {
		...app,
		general: {
			...app.general,
			user,
		},
		target: {
			...app.target,
			operations: payload.operation ? [payload.operation] : [],
			contexts: payload.contexts.map(hydrateDetachedContext),
			files: payload.sources.map(hydrateDetachedSource),
			events: operationChanged ? new Map() : app.target.events,
		},
		timeline: {
			...app.timeline,
			target: operationChanged ? null : app.timeline.target,
		},
	};
}

/**
 * Applies synced query state to an app snapshot.
 */
function applyQueryContext(
	app: App.Type,
	payload: WindowBridge.QueryContextPayload,
): App.Type {
	return {
		...app,
		target: {
			...app.target,
			files: app.target.files.map((source) => ({
				...source,
				appliedQuery:
					payload.filters[source.id] ?? source.appliedQuery ?? null,
			})),
		},
		timeline: {
			...app.timeline,
			frame: payload.frame,
		},
	};
}

export function DetachedAppProvider({
	initialApp = App.Base,
	bridgeId,
	coordinatorId,
	kind,
	detachedDocument = document,
	mainSpawnBanner = () => {},
	syncTimelineTargetToMain = false,
	assistantQuerySyncEnabled = false,
	loadExtensions = false,
	extensionFilenames,
	extensionSlots,
	children,
}: DetachedApp.ProviderProps) {
	const [app, setRawInfo] = useState<App.Type>(initialApp);
	const [banner, setBanner] = useState<{
		node: ReactNode;
		target: string;
	} | null>(null);
	const [dialog, setDialog] = useState<ReactNode>(null);
	const timeline = useRef<HTMLDivElement>(null as unknown as HTMLDivElement);
	const [highlightsOverlay, setHighlightsOverlay] = useState<ReactNode>(null);
	const [detachedStatus, setDetachedStatus] =
		useState<WindowBridge.MainContextStatus>("initializing");
	const detachedStatusRef = useRef(detachedStatus);
	detachedStatusRef.current = detachedStatus;
	const [detachedTableSourceCommand, setDetachedTableSourceCommand] = useState<{
		sourceId: Source.Id | null;
		revision: number;
	}>({ sourceId: null, revision: 0 });
	const [detachedSelectedEventId, setDetachedSelectedEventId] =
		useState<Doc.Id | null>(initialApp.timeline.target?._id ?? null);
	const [detachedDialog, setDetachedDialog] =
		useState<WindowBridge.DetachedDialogDescriptor | null>(null);
	const [detachedDialogEventColors, setDetachedDialogEventColors] =
		useState<Partial<Record<Doc.Id, string>>>({});
	const [detachedDialogNavigation, setDetachedDialogNavigation] =
		useState<WindowBridge.DetachedDialogNavigation | null>(null);
	const [detachedEventDetail, setDetachedEventDetail] =
		useState<Doc.Type | null>(null);
	const [awaitingBootstrap, setAwaitingBootstrap] = useState(true);
	const awaitingBootstrapRef = useRef(awaitingBootstrap);
	awaitingBootstrapRef.current = awaitingBootstrap;
	const selectedOperationIdRef = useRef<Operation.Id | null>(null);
	const initialCacheNamespaceId = resolveEventStoreCacheNamespaceId(
		initialApp.general.user?.id,
		initialApp.general.server,
		coordinatorId,
	);
	const [cacheNamespaceId, setCacheNamespaceId] = useState(
		initialCacheNamespaceId,
	);
	const selectedSourceIdsRef = useRef<Source.Id[]>([]);
	const controlBridgeRef = useRef<WindowBridge.Bridge | null>(null);
	const privateBridgeRef = useRef<WindowBridge.Bridge | null>(null);
	const tableSyncEnabledRef = useRef(false);
	const notesRenderedSyncEnabledRef = useRef(false);
	const assistantConnectionRef = useRef<{
		mainWsId: string | null;
		detachedWsId: string;
	}>({
		mainWsId: null,
		detachedWsId: WindowBridge.generateId(),
	});
	const payloadSignaturesRef = useRef<
		Partial<Record<DetachedPayloadDomain, string>>
	>({});
	const appliedThemeVariablesRef = useRef<Set<string>>(new Set());
	const suppressOutboundEventIdRef = useRef<Doc.Id | null | undefined>(
		undefined,
	);
	const ownsIndependentDataStore = ownsIndependentDetachedDataStore(
		detachedDocument,
		window,
	);

	/**
	 * Accepts the main realm's authenticated cache namespace or resolves a safe
	 * reload fallback before any detached EventStore reader is opened.
	 * @param propagatedCacheNamespaceId Namespace carried by WindowBridge context.
	 * @returns Namespace selected for detached IndexedDB and socket persistence.
	 */
	const applyCacheNamespace = useCallback(
		(propagatedCacheNamespaceId: string | null | undefined): string => {
			const nextCacheNamespaceId =
				propagatedCacheNamespaceId ??
				resolveEventStoreCacheNamespaceId(
					null,
					initialApp.general.server,
					coordinatorId,
				);
			setCacheNamespaceId(nextCacheNamespaceId);
			return nextCacheNamespaceId;
		},
		[coordinatorId, initialApp.general.server],
	);

	/**
	 * The loading maps are kept outside React so concurrent Info updates cannot
	 * overwrite a request that is still running.
	 */
	const loadingsRef = useRef<{
		byRequestId: Map<Request.Id, Source.Id>;
		byFileId: Map<Source.Id, Request.Id>;
	}>({
		byRequestId: new Map(),
		byFileId: new Map(),
	});

	const setInfo = useCallback((action: React.SetStateAction<App.Type>) => {
		setRawInfo((previous) => {
			const next =
				typeof action === "function" ? action(previous) : action;
			return {
				...next,
				general: {
					...next.general,
					loadings: {
						byRequestId: new Map(loadingsRef.current.byRequestId),
						byFileId: new Map(loadingsRef.current.byFileId),
					},
				},
			};
		});
	}, []);

	const infoRef = useRef<Info | null>(null);
	if (!infoRef.current) {
		infoRef.current = new Info({ app, setInfo, timeline });
	} else {
		infoRef.current.app = app;
		infoRef.current.setInfo = setInfo;
	}
	const instance = infoRef.current;

	/**
	 * Maps the main connection identity to a socket ID owned by this detached
	 * browser realm. The backend permits only one socket per ID, so reusing the
	 * main tab's ID here would evict its socket. A changed main ID is treated as
	 * a new authenticated connection epoch and rotates the detached ID.
	 */
	const getAssistantSocketId = useCallback(
		(mainWsId: string): string => {
			if (kind !== "assistant") return mainWsId;

			if (assistantConnectionRef.current.mainWsId !== mainWsId) {
				assistantConnectionRef.current = {
					mainWsId,
					detachedWsId: WindowBridge.generateId(),
				};
			}
			return assistantConnectionRef.current.detachedWsId;
		},
		[kind],
	);

	if (!(instance as Info & { _detachedPatched?: boolean })._detachedPatched) {
		const originalSetLoading = instance.setLoading.bind(instance);
		instance.setLoading = (requestId: Request.Id, sourceId: Source.Id) => {
			loadingsRef.current.byRequestId.set(requestId, sourceId);
			loadingsRef.current.byFileId.set(sourceId, requestId);
			originalSetLoading(requestId, sourceId);
		};

		const originalDelLoading = instance.delLoading.bind(instance);
		instance.delLoading = (requestId: Request.Id) => {
			loadingsRef.current.byRequestId.delete(requestId);
			const sourceId = [...loadingsRef.current.byFileId.entries()].find(
				([, loadingRequestId]) => loadingRequestId === requestId,
			)?.[0];
			if (sourceId) loadingsRef.current.byFileId.delete(sourceId);
			originalDelLoading(requestId);
		};

		(instance as Info & { _detachedPatched?: boolean })._detachedPatched =
			true;
	}

	/**
	 * Applies theme identity, mode, and computed custom properties to the
	 * detached document. Returns false for an identical replay.
	 */
	const applyTheme = useCallback(
		(payload: WindowBridge.ThemeChangePayload): boolean => {
			const signature = serializePayload(payload);
			if (payloadSignaturesRef.current.theme === signature) return false;
			payloadSignaturesRef.current.theme = signature;

			appliedThemeVariablesRef.current = applyDetachedThemeSnapshot(
				detachedDocument,
				payload,
				appliedThemeVariablesRef.current,
			);
			Color.Themer.setTheme(payload.mode);
			return true;
		},
		[detachedDocument],
	);

	const applyNotesChanged = useCallback(
		(payload: WindowBridge.NotesChangedPayload): void => {
			if (kind !== "notes" || !ownsIndependentDataStore) return;
			DataStore.replaceNotes(payload.notes, "deferred");
			Note.Entity.invalidateCache();
			RenderEngine.reset("notes");
			const operationId =
				payload.operationId ?? selectedOperationIdRef.current;
			if (operationId) DataStore.publishNotesSnapshotReady(operationId);
		},
		[kind, ownsIndependentDataStore],
	);

	const applyNotesDelta = useCallback(
		(payload: WindowBridge.NotesDeltaPayload): void => {
			if (kind !== "notes" || !ownsIndependentDataStore) return;
			if (payload.operationId !== selectedOperationIdRef.current) return;

			if (payload.mutation === "delete") {
				const deletedNotes = DataStore.deleteNotes(
					payload.noteIds,
					"deferred",
				);
				const affectedSourceIds = new Set<Source.Id>();
				deletedNotes.forEach((note) => {
					const sourceId = Note.Entity.sourceId(note);
					if (sourceId) affectedSourceIds.add(sourceId);
				});
				Note.Entity.removeIndexedNotes(deletedNotes);
				affectedSourceIds.forEach((sourceId) =>
					RenderEngine.resetSourceNotes(sourceId),
				);
			} else {
				const selectedSourceIds = new Set(selectedSourceIdsRef.current);
				const selectedSourceNotes = payload.notes.filter((note) => {
					const sourceId = Note.Entity.sourceId(note);
					return !!sourceId && selectedSourceIds.has(sourceId);
				});
				instance.notes_upsert_from_collab(selectedSourceNotes);
			}

			DataStore.relayNotesRealtimeDelta(payload);
		},
		[instance, kind, ownsIndependentDataStore],
	);

	const applyDialogChanged = useCallback(
		(payload: WindowBridge.DialogChangedPayload): void => {
			if (kind !== "dialog") return;
			const signature = serializePayload(payload);
			if (payloadSignaturesRef.current.dialog === signature) return;
			payloadSignaturesRef.current.dialog = signature;

			if (ownsIndependentDataStore) {
				Doc.Entity.clearIndex();
				DataStore.events.clear();
				if (payload.events.length > 0) {
					Doc.Entity.add(instance.app, payload.events);
				}
				DataStore.replaceNotes(payload.notes, "deferred");
				DataStore.replaceLinks(payload.links, "deferred");
				Note.Entity.invalidateCache();
				RenderEngine.reset("notes");
				RenderEngine.reset("links");
			}

			const selectedEvent =
				payload.dialog?.kind === "event"
					? payload.dialog.event
					: null;
			const selectedEventId = selectedEvent?._id ?? null;
			setDetachedEventDetail((currentDetail) =>
				currentDetail?._id === selectedEventId ? currentDetail : null,
			);
			suppressOutboundEventIdRef.current = selectedEventId;
			setDetachedSelectedEventId(selectedEventId);
			setDetachedDialog(payload.dialog);
			setDetachedDialogEventColors(payload.eventColors);
			setDetachedDialogNavigation(payload.navigation);
			setDialog(null);
			setInfo((previous) => ({
				...previous,
				timeline: {
					...previous.timeline,
					target: selectedEvent,
					renderVersion: previous.timeline.renderVersion + 1,
				},
			}));
		},
		[instance, kind, ownsIndependentDataStore, setInfo],
	);

	/**
	 * Releases a detached realm's transient QueryWorker operation context.
	 * @param operationId Previous operation leaving this detached surface.
	 */
	const releaseDetachedOperation = useCallback(
		(operationId: Operation.Id | null): void => {
			if (!operationId) return;
			void SmartSocket.Class.instance
				?.releaseOperation(operationId)
				.catch(() => undefined);
		},
		[],
	);

	/**
	 * Applies an atomic first-state replay before exposing detached content.
	 */
	const applyBootstrap = useCallback(
		(payload: WindowBridge.DetachedBootstrapPayload): void => {
			const signature = serializePayload(payload);
			if (payloadSignaturesRef.current.bootstrap === signature) {
				awaitingBootstrapRef.current = false;
				detachedStatusRef.current = payload.status;
				setAwaitingBootstrap(false);
				setDetachedStatus(payload.status);
				return;
			}
			payloadSignaturesRef.current.bootstrap = signature;
			const themeChanged = applyTheme(payload.theme);

			if (payload.context) {
				const nextCacheNamespaceId = applyCacheNamespace(
					payload.context.cacheNamespaceId,
				);
				const operationId = payload.context.operation?.id ?? null;
				const previousOperationId = selectedOperationIdRef.current;
				if (previousOperationId !== operationId) {
					releaseDetachedOperation(previousOperationId);
				}
				selectedOperationIdRef.current = operationId;
				if (ownsIndependentDataStore && operationId) {
					if (DataStore.getRenderOperationId() !== operationId) {
						DataStore.resetRenderOperation(operationId);
					}
					if (isEventStoreLodEnabled()) {
						DataStore.openEventStoreNamespace({
							cacheNamespaceId: nextCacheNamespaceId,
							operationId,
						});
					}
				}
				selectedSourceIdsRef.current = payload.context.sources.map(
					(source) => source.id,
				);
				payloadSignaturesRef.current.context = serializePayload(
					payload.context,
				);
			}
			if (payload.queryContext) {
				payloadSignaturesRef.current.query = serializePayload(
					payload.queryContext,
				);
			}
			if (payload.renderedSourceIds) {
				payloadSignaturesRef.current.rendered = serializePayload(
					payload.renderedSourceIds,
				);
			}

			const selectedEventId = payload.selectedEventId;
			if (selectedEventId !== undefined) {
				payloadSignaturesRef.current.event =
					serializePayload(selectedEventId);
				suppressOutboundEventIdRef.current = selectedEventId;
				setDetachedSelectedEventId(selectedEventId);
			}
			if (payload.wsId !== undefined) {
				payloadSignaturesRef.current.assistant =
					serializePayload(payload.wsId);
			}
			setInfo((previous) => {
				let next = previous;
				if (payload.context) {
					next = applyContextSnapshot(next, payload.context);
				}
				if (payload.queryContext) {
					next = applyQueryContext(next, payload.queryContext);
				}
				if (payload.renderedSourceIds) {
					next = {
						...next,
						timeline: {
							...next.timeline,
							renderedSourceIds: payload.renderedSourceIds,
						},
					};
				}
				if (payload.wsId !== undefined) {
					next = {
						...next,
						general: {
							...next.general,
							ws_id: getAssistantSocketId(payload.wsId),
						},
					};
				}
				if (themeChanged) {
					next = {
						...next,
						timeline: {
							...next.timeline,
							renderVersion: next.timeline.renderVersion + 1,
						},
					};
				}
				return next;
			});

			if (payload.tableSourceId) {
				setDetachedTableSourceCommand((previous) => ({
					sourceId: payload.tableSourceId!,
					revision: previous.revision + 1,
				}));
			}
			if (payload.notes) applyNotesChanged(payload.notes);
			if (payload.dialog) applyDialogChanged(payload.dialog);
			awaitingBootstrapRef.current = false;
			detachedStatusRef.current = payload.status;
			setAwaitingBootstrap(false);
			setDetachedStatus(payload.status);
		},
		[
			applyCacheNamespace,
			applyDialogChanged,
			applyNotesChanged,
			applyTheme,
			getAssistantSocketId,
			coordinatorId,
			ownsIndependentDataStore,
			releaseDetachedOperation,
			setInfo,
		],
	);

	const applyContext = useCallback(
		(payload: WindowBridge.ContextSnapshotPayload): void => {
			const signature = serializePayload(payload);
			if (payloadSignaturesRef.current.context === signature) return;
			payloadSignaturesRef.current.context = signature;
			const nextCacheNamespaceId = applyCacheNamespace(
				payload.cacheNamespaceId,
			);
			const operationId = payload.operation?.id ?? null;
			const previousOperationId = selectedOperationIdRef.current;
			if (previousOperationId !== operationId) {
				releaseDetachedOperation(previousOperationId);
			}
			selectedOperationIdRef.current = operationId;
			if (ownsIndependentDataStore && operationId) {
				if (DataStore.getRenderOperationId() !== operationId) {
					DataStore.resetRenderOperation(operationId);
				}
				if (isEventStoreLodEnabled()) {
					DataStore.openEventStoreNamespace({
						cacheNamespaceId: nextCacheNamespaceId,
						operationId,
					});
				}
			}
			selectedSourceIdsRef.current = payload.sources.map(
				(source) => source.id,
			);
			setInfo((previous) => applyContextSnapshot(previous, payload));
		},
		[
			applyCacheNamespace,
			ownsIndependentDataStore,
			releaseDetachedOperation,
			setInfo,
		],
	);

	const applyQuery = useCallback(
		(payload: WindowBridge.QueryContextPayload): void => {
			const signature = serializePayload(payload);
			if (payloadSignaturesRef.current.query === signature) return;
			payloadSignaturesRef.current.query = signature;
			setInfo((previous) => applyQueryContext(previous, payload));
		},
		[setInfo],
	);

	const applyRenderedSources = useCallback(
		(payload: WindowBridge.RenderedSourcesChangedPayload): void => {
			const signature = serializePayload(payload.sourceIds);
			if (payloadSignaturesRef.current.rendered === signature) return;
			payloadSignaturesRef.current.rendered = signature;
			setInfo((previous) => ({
				...previous,
				timeline: {
					...previous.timeline,
					renderedSourceIds: payload.sourceIds,
				},
			}));
		},
		[setInfo],
	);

	// Open the private channel before registering on the control channel.
	useEffect(() => {
		const bridge = WindowBridge.createClient(bridgeId, bridgeId, (message) => {
			switch (message.type) {
				case WindowBridge.MessageType.DETACHED_BOOTSTRAP:
					applyBootstrap(
						message.payload as WindowBridge.DetachedBootstrapPayload,
					);
					break;
				case WindowBridge.MessageType.CONTEXT_SNAPSHOT:
					applyContext(
						message.payload as WindowBridge.ContextSnapshotPayload,
					);
					break;
				case WindowBridge.MessageType.QUERY_CONTEXT:
					applyQuery(message.payload as WindowBridge.QueryContextPayload);
					break;
				case WindowBridge.MessageType.RENDERED_SOURCES_CHANGED:
					applyRenderedSources(
						message.payload as WindowBridge.RenderedSourcesChangedPayload,
					);
					break;
				case WindowBridge.MessageType.EVENT_TARGET_CHANGED: {
					const { docId } =
						message.payload as WindowBridge.EventTargetChangedPayload;
					payloadSignaturesRef.current.event = serializePayload(docId);
					setDetachedSelectedEventId(docId);
					break;
				}
				case WindowBridge.MessageType.ASSISTANT_CONNECTION_CHANGED: {
					const { wsId } =
						message.payload as WindowBridge.AssistantConnectionChangedPayload;
					const signature = serializePayload(wsId);
					if (payloadSignaturesRef.current.assistant === signature) break;
					payloadSignaturesRef.current.assistant = signature;
					const detachedWsId = getAssistantSocketId(wsId);
					setInfo((previous) => ({
						...previous,
						general: {
							...previous.general,
							ws_id: detachedWsId,
						},
					}));
					break;
				}
				case WindowBridge.MessageType.DIALOG_CHANGED:
					applyDialogChanged(
						message.payload as WindowBridge.DialogChangedPayload,
					);
					break;
				case WindowBridge.MessageType.EVENT_DETAIL_CHANGED:
					if (kind === "dialog") {
						setDetachedEventDetail(
							(
								message.payload as WindowBridge.EventDetailChangedPayload
							).event,
						);
					}
					break;
				case WindowBridge.MessageType.THEME_CHANGE: {
					const themeChanged = applyTheme(
						message.payload as WindowBridge.ThemeChangePayload,
					);
					if (themeChanged) {
						setInfo((previous) => ({
							...previous,
							timeline: {
								...previous.timeline,
								renderVersion: previous.timeline.renderVersion + 1,
							},
						}));
					}
					break;
				}
				case WindowBridge.MessageType.NOTES_CHANGED:
					applyNotesChanged(
						message.payload as WindowBridge.NotesChangedPayload,
					);
					break;
				case WindowBridge.MessageType.NOTES_DELTA:
					applyNotesDelta(
						message.payload as WindowBridge.NotesDeltaPayload,
					);
					break;
				case WindowBridge.MessageType.MAIN_CONTEXT_STATUS: {
					const payload =
						message.payload as WindowBridge.MainContextStatusPayload;
					const reconnectRequired = payload.reconnectRequired === true;
					awaitingBootstrapRef.current = reconnectRequired;
					detachedStatusRef.current = payload.status;
					setAwaitingBootstrap(reconnectRequired);
					setDetachedStatus(payload.status);
					break;
				}
				case WindowBridge.MessageType.TABLE_SELECT_SOURCE:
					setDetachedTableSourceCommand((previous) => ({
						sourceId: (
							message.payload as WindowBridge.TableSelectSourcePayload
						).sourceId,
						revision: previous.revision + 1,
					}));
					break;
			}
		}, coordinatorId);
		privateBridgeRef.current = bridge;
		const unregisterPrivateActionSender =
			WindowBridge.registerPrivateActionSender(coordinatorId, bridge);

		return () => {
			unregisterPrivateActionSender();
			bridge.destroy();
			if (privateBridgeRef.current === bridge) {
				privateBridgeRef.current = null;
			}
		};
	}, [
		applyBootstrap,
		applyContext,
		applyDialogChanged,
		applyNotesChanged,
		applyNotesDelta,
		applyQuery,
		applyRenderedSources,
		applyTheme,
		bridgeId,
		coordinatorId,
		getAssistantSocketId,
		setInfo,
	]);

	const createReadyPayload = useCallback(
		(needsBootstrap: boolean): WindowBridge.DetachedReadyPayload => ({
			kind,
			needsBootstrap,
			...(kind === "table"
				? { tableSyncEnabled: tableSyncEnabledRef.current }
				: {}),
			...(kind === "notes"
				? {
						notesRenderedSyncEnabled:
							notesRenderedSyncEnabledRef.current,
					}
				: {}),
			...(kind === "assistant"
				? {
						assistantQuerySyncEnabled,
					}
				: {}),
		}),
		[
			assistantQuerySyncEnabled,
			kind,
		],
	);

	const sendReady = useCallback((needsBootstrap?: boolean): void => {
		const requestBootstrap =
			needsBootstrap ?? awaitingBootstrapRef.current;
		const bridge = requestBootstrap
			? controlBridgeRef.current
			: privateBridgeRef.current ?? controlBridgeRef.current;
		bridge?.send(
			WindowBridge.MessageType.DETACHED_READY,
			createReadyPayload(requestBootstrap),
		);
	}, [
		createReadyPayload,
	]);

	const sendHeartbeat = useCallback((): void => {
		privateBridgeRef.current?.send(
			WindowBridge.MessageType.DETACHED_HEARTBEAT,
			{},
		);
	}, []);

	useEffect(() => {
		const bridge = WindowBridge.create(bridgeId, () => {}, coordinatorId);
		controlBridgeRef.current = bridge;
		bridge.send(
			WindowBridge.MessageType.DETACHED_READY,
			createReadyPayload(true),
		);

		return () => {
			bridge.send(WindowBridge.MessageType.DETACHED_CLOSED, {});
			bridge.destroy();
			controlBridgeRef.current = null;
		};
	}, [
		assistantQuerySyncEnabled,
		bridgeId,
		coordinatorId,
		createReadyPayload,
		kind,
		setInfo,
	]);

	useEffect(() => {
		if (
			detachedStatus === "idle" ||
			detachedStatus === "auth_lost"
		) {
			return;
		}
		if (!awaitingBootstrap && detachedStatus !== "active") return;
		const interval = window.setInterval(
			awaitingBootstrap
				? () => sendReady(true)
				: sendHeartbeat,
			awaitingBootstrap
				? RECONNECT_HEARTBEAT_MSEC
				: ACTIVE_HEARTBEAT_MSEC,
		);
		return () => window.clearInterval(interval);
	}, [awaitingBootstrap, detachedStatus, sendHeartbeat, sendReady]);

	/**
	 * Owns a websocket only for routed assistant clients. Other detached kinds
	 * stay bridge-driven and therefore add no backend connections. Rotating the
	 * detached-local ID tears down the previous singleton before creating the
	 * replacement used by assistant API requests and stream listeners.
	 */
	useEffect(() => {
		if (
			kind !== "assistant" ||
			detachedStatus !== "active" ||
			assistantConnectionRef.current.mainWsId === null
		) {
			return;
		}

		const socket = new SmartSocket.Class(
			app.general.ws_id,
			app.general.user?.id,
			cacheNamespaceId,
		);
		return () => socket.destroy();
	}, [app.general.user?.id, app.general.ws_id, cacheNamespaceId, detachedStatus, kind]);

	useEffect(() => {
		let bootstrapFallbackTimeoutId: number | null = null;

		const cancelBootstrapFallback = (): void => {
			if (bootstrapFallbackTimeoutId === null) return;
			window.clearTimeout(bootstrapFallbackTimeoutId);
			bootstrapFallbackTimeoutId = null;
		};

		const applyStoredStatus = (requestActiveBootstrap: boolean): void => {
			const replay = WindowBridge.readStoredMainContext(coordinatorId);
			if (!replay) return;
			if (replay.status.status === "active") {
				if (requestActiveBootstrap) {
					if (
						detachedStatusRef.current === "active" &&
						!awaitingBootstrapRef.current
					) {
						return;
					}
					cancelBootstrapFallback();
					bootstrapFallbackTimeoutId = window.setTimeout(() => {
						bootstrapFallbackTimeoutId = null;
						if (
							detachedStatusRef.current === "active" &&
							!awaitingBootstrapRef.current
						) {
							return;
						}
						awaitingBootstrapRef.current = true;
						setAwaitingBootstrap(true);
						sendReady(true);
					}, STORAGE_BOOTSTRAP_FALLBACK_MSEC);
				}
				return;
			}
			cancelBootstrapFallback();
			const reconnectRequired =
				replay.status.reconnectRequired === true;
			awaitingBootstrapRef.current = reconnectRequired;
			detachedStatusRef.current = replay.status.status;
			setAwaitingBootstrap(reconnectRequired);
			setDetachedStatus(replay.status.status);
		};
		const handleStorage = (event: StorageEvent): void => {
			if (event.key === Doc.Entity.flag.KEY) {
				if (
					kind !== "table" &&
					kind !== "assistant" &&
					kind !== "dialog"
				) {
					return;
				}
				Doc.Entity.flag.invalidateCache();
				RenderEngine.reset("flags");
				setInfo((previous) => ({
					...previous,
					timeline: {
						...previous.timeline,
						renderVersion: previous.timeline.renderVersion + 1,
					},
				}));
				return;
			}
			if (
				event.key !==
				WindowBridge.getMainContextStorageKey(coordinatorId)
			) {
				return;
			}
			applyStoredStatus(true);
		};

		// The control-channel mount effect already sent the initial READY.
		applyStoredStatus(false);
		window.addEventListener("storage", handleStorage);
		return () => {
			cancelBootstrapFallback();
			window.removeEventListener("storage", handleStorage);
		};
	}, [coordinatorId, kind, sendReady, setInfo]);

	const destroyBanner = useCallback(() => {
		setBanner(null);
		detachedDocument.body.classList.remove("no-scroll");
	}, [detachedDocument]);

	const spawnBanner = useCallback(
		(node: ReactNode, target = "main") => {
			if (!node) {
				setBanner(null);
				detachedDocument.body.classList.remove("no-scroll");
				return;
			}
			if (target === "table" || target === "main") {
				setBanner({ node, target });
				detachedDocument.body.classList.add("no-scroll");
				return;
			}
			mainSpawnBanner(node, target);
		},
		[detachedDocument, mainSpawnBanner],
	);

	const spawnDialog = useCallback((nextDialog: ReactNode) => {
		setDialog(nextDialog);
	}, []);

	const setDockedState = useCallback(() => {
		(privateBridgeRef.current ?? controlBridgeRef.current)?.send(
			WindowBridge.MessageType.DOCK_DIALOG,
			{},
		);
	}, []);

	useEffect(() => {
		const detachedWindow = detachedDocument.defaultView as
			| (Window & {
					focusCanvasOnEvent?: (
						timestamp: number,
						onLeft?: boolean,
						sourceId?: Source.Id,
					) => void;
				})
			| null;
		if (!detachedWindow) return;
		const previousFocusCanvasOnEvent = detachedWindow.focusCanvasOnEvent;
		const forwardFocusCanvasOnEvent = (
			timestamp: number,
			onLeft = false,
			sourceId?: Source.Id,
		): void => {
			(privateBridgeRef.current ?? controlBridgeRef.current)?.send(
				WindowBridge.MessageType.FOCUS_TIMELINE_REQUEST,
				{ timestamp, onLeft, sourceId },
			);
		};
		detachedWindow.focusCanvasOnEvent = forwardFocusCanvasOnEvent;
		return () => {
			if (
				detachedWindow.focusCanvasOnEvent ===
				forwardFocusCanvasOnEvent
			) {
				detachedWindow.focusCanvasOnEvent =
					previousFocusCanvasOnEvent;
			}
		};
	}, [detachedDocument]);

	const setDetachedTableSync = useCallback(
		(enabled: boolean): void => {
			if (kind !== "table" || tableSyncEnabledRef.current === enabled) return;
			tableSyncEnabledRef.current = enabled;
			(privateBridgeRef.current ?? controlBridgeRef.current)?.send(
				WindowBridge.MessageType.TABLE_SYNC_CHANGED,
				{ enabled },
			);
		},
		[kind],
	);

	const setDetachedNotesRenderedSync = useCallback(
		(enabled: boolean): void => {
			if (
				kind !== "notes" ||
				notesRenderedSyncEnabledRef.current === enabled
			) {
				return;
			}
			notesRenderedSyncEnabledRef.current = enabled;
			if (enabled) {
				setInfo((previous) => ({
					...previous,
					timeline: {
						...previous.timeline,
						renderedSourceIds: [],
					},
				}));
			}
			(privateBridgeRef.current ?? controlBridgeRef.current)?.send(
				WindowBridge.MessageType.NOTES_RENDERED_SYNC_CHANGED,
				{ enabled },
			);
		},
		[kind, setInfo],
	);

	const selectDetachedDialogEvent = useCallback(
		(docId: Doc.Id): void => {
			if (kind !== "dialog") return;
			(privateBridgeRef.current ?? controlBridgeRef.current)?.send(
				WindowBridge.MessageType.EVENT_TARGET_REQUEST,
				{ docId },
			);
		},
		[kind],
	);

	/**
	 * Sends a serializable Event action to the owning main document.
	 * @param payload Event identity and workflow descriptor to execute.
	 * @returns True when this active detached Dialog accepted the request.
	 */
	const requestMainEventAction = useCallback(
		(payload: WindowBridge.OpenEventActionRequestPayload): boolean => {
			if (kind !== "dialog" || detachedStatusRef.current !== "active") {
				return false;
			}
			const bridge = privateBridgeRef.current ?? controlBridgeRef.current;
			if (!bridge) return false;
			bridge.send(WindowBridge.MessageType.OPEN_EVENT_ACTION_REQUEST, payload);
			return true;
		},
		[kind],
	);

	// Suppress re-sending an event that was just replayed by the coordinator.
	useEffect(() => {
		if (!syncTimelineTargetToMain || detachedStatus !== "active") return;
		const eventId = app.timeline.target?._id ?? null;
		if (suppressOutboundEventIdRef.current === eventId) {
			suppressOutboundEventIdRef.current = undefined;
			return;
		}
		(privateBridgeRef.current ?? controlBridgeRef.current)?.send(
			WindowBridge.MessageType.EVENT_TARGET_REQUEST,
			{
				docId: app.timeline.target?._id ?? null,
			},
		);
	}, [
		app.timeline.target?._id,
		detachedStatus,
		syncTimelineTargetToMain,
	]);

	const previousStatusRef = useRef(detachedStatus);
	useEffect(() => {
		const previousStatus = previousStatusRef.current;
		previousStatusRef.current = detachedStatus;
		if (previousStatus !== "active" || detachedStatus === "active") return;

		// The next active bootstrap must rehydrate context even when the main
		// snapshot is byte-for-byte identical to the pre-auth-loss snapshot.
		delete payloadSignaturesRef.current.bootstrap;
		delete payloadSignaturesRef.current.dialog;
		loadingsRef.current.byRequestId.clear();
		loadingsRef.current.byFileId.clear();
		releaseDetachedOperation(selectedOperationIdRef.current);
		selectedOperationIdRef.current = null;
		setBanner(null);
		setDialog(null);
		setDetachedDialog(null);
		setDetachedDialogEventColors({});
		setDetachedDialogNavigation(null);
		setDetachedEventDetail(null);
		if (ownsIndependentDataStore) {
			Doc.Entity.clearIndex();
			DataStore.resetRenderOperation(null);
		}
		if (kind === "dialog" && ownsIndependentDataStore) {
			DataStore.replaceNotes([], "deferred");
			DataStore.replaceLinks([], "deferred");
		}
		detachedDocument.body.classList.remove("no-scroll");
		setInfo((previous) => ({
			...previous,
			target: {
				...App.Base.target,
				operations: previous.target.operations.map((operation) => ({
					...operation,
					selected: false,
				})),
			},
			timeline: {
				...previous.timeline,
				target: null,
				renderVersion: previous.timeline.renderVersion + 1,
			},
			general: {
				...previous.general,
				loadings: {
					byRequestId: new Map(),
					byFileId: new Map(),
				},
			},
		}));
	}, [
		detachedDocument,
		detachedStatus,
		kind,
		ownsIndependentDataStore,
		releaseDetachedOperation,
		setInfo,
	]);

	const props = useMemo<Application.Context.Props>(
		() => ({
			spawnBanner,
			destroyBanner,
			banner,
			spawnDialog,
			dialog,
			app,
			setInfo,
			Info: instance,
			timeline,
			highlightsOverlay,
			setHighlightsOverlay,
			eventStoreFallbackRequest: null,
			dismissEventStoreFallbackRequest: () => {},
			canvasDocked: true,
			setCanvasDocked: setDockedState,
			dialogsDocked: true,
			setDialogsDocked: setDockedState,
			hintOpen: false,
			setHintOpen: () => {},
			toggleHintOpen: () => {},
			isDetachedWindow: true,
			currentDocument: detachedDocument,
			detachedWindowId: bridgeId,
			detachedCoordinatorId: coordinatorId,
			detachedClientKind: kind,
			detachedTableSourceId: detachedTableSourceCommand.sourceId,
			detachedTableSourceRevision: detachedTableSourceCommand.revision,
			detachedSelectedEventId,
			detachedDialog,
			detachedDialogEventColors,
			detachedDialogNavigation,
			detachedEventDetail,
			requestMainEventAction,
			selectDetachedDialogEvent,
			setDetachedTableSync,
			setDetachedNotesRenderedSync,
		}),
		[
			app,
			banner,
			bridgeId,
			coordinatorId,
			destroyBanner,
			detachedDocument,
			detachedSelectedEventId,
			detachedDialog,
			detachedDialogEventColors,
			detachedDialogNavigation,
			detachedEventDetail,
			detachedTableSourceCommand,
			dialog,
			highlightsOverlay,
			instance,
			kind,
			requestMainEventAction,
			setDetachedTableSync,
			setDetachedNotesRenderedSync,
			setDockedState,
			setInfo,
			selectDetachedDialogEvent,
			spawnBanner,
			spawnDialog,
		],
	);
	const content =
		detachedStatus === "active"
			? children
			: <DetachedIdleState status={detachedStatus} />;

	return (
		<Application.Context.Provider value={props}>
			<Locale.Provider>
				<Boundary.Provider>
					{loadExtensions && detachedStatus === "active" ? (
						<Extension.Provider
							filenames={extensionFilenames}
							slots={extensionSlots}
						>
							{content}
						</Extension.Provider>
					) : (
						<Extension.Context.Provider value={EMPTY_EXTENSION_CONTEXT}>
							{content}
						</Extension.Context.Provider>
					)}
				</Boundary.Provider>
			</Locale.Provider>
		</Application.Context.Provider>
	);
}

function DetachedIdleState({
	status,
}: {
	status: WindowBridge.MainContextStatus;
}) {
	const { t } = Locale.use();
	const titleKey =
		status === "auth_lost"
			? "detachedApp.authLost"
			: status === "initializing"
				? "detachedApp.initializing"
				: "detachedApp.idle";
	const descriptionKey =
		status === "auth_lost"
			? "detachedApp.authLostDescription"
			: status === "initializing"
				? "detachedApp.initializingDescription"
				: "detachedApp.idleDescription";

	return (
		<div className={s.idle}>
			<div className={s.panel}>
				<h2>{t(titleKey)}</h2>
				<p>{t(descriptionKey)}</p>
			</div>
		</div>
	);
}

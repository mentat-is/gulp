import {
	useState,
	createContext,
	useContext,
	ReactNode,
	useRef,
	useEffect,
	useMemo,
	useCallback,
} from "react";
import { Info } from "@/class/Info";
import "@/class/API";
import { DisplayEventDialog } from "@/dialogs/Event.dialog";
import { toast } from "sonner";
import { SetState } from "@/class/API";
import { SmartSocket } from "@/class/SmartSocket";
import { Note } from "@/entities/Note";
import { Link } from "@/entities/Link";
import { Source } from "@/entities/Source";
import { App } from "@/entities/App";
import { Context } from "@/entities/Context";
import { DataStore } from "@/store/DataStore";
import { RenderEngine } from "@/class/RenderEngine";
import { Operation } from "@/entities/Operation";
import { Doc } from "@/entities/Doc";
import { useLocation } from "react-router-dom";
import { translate } from "@/locales/core";
import {
	normalizeCollabDeleteIds,
	normalizeCollabDeleteOperationId,
	type CollabDeletePayload,
	type CollabDeleteOperationEnvelope,
} from "@/context/collabDelete";
import type { WindowBridge } from "@/lib/WindowBridge";
import {
	clearEventStoreFallbackRequest,
	getEventStoreFallbackRequest,
	subscribeEventStoreFallbackRequest,
	type EventStoreFallbackRequest,
} from "@/event-store/EventStoreFallback.request";

interface CollabPayloadObject {
	type?: string;
	id?: string;
	operation_id?: Operation.Id;
}

interface CollabDeleteMessage extends CollabDeleteOperationEnvelope {
	payload: CollabDeletePayload;
}

/**
 * Normalizes a websocket collab payload object into typed object entries.
 * @param payloadObject Single object or object array from the websocket payload.
 * @returns Payload objects that expose a collab type discriminator.
 */
function getCollabPayloadObjects(payloadObject: unknown): CollabPayloadObject[] {
	const objects = Array.isArray(payloadObject) ? payloadObject : [payloadObject];
	return objects.filter(
		(obj): obj is CollabPayloadObject =>
			typeof obj === "object" &&
			obj !== null &&
			typeof (obj as CollabPayloadObject).type === "string",
	);
}

/**
 * Checks whether a collaboration entity belongs to the selected operation.
 * @param app Current application state read at websocket delivery time.
 * @param object Collaboration entity carrying an operation identifier.
 * @returns True only when the entity belongs to the selected operation.
 */
function isSelectedOperationObject(
	app: App.Type,
	object: CollabPayloadObject,
): boolean {
	const selectedOperationId =
		DataStore.getRenderOperationId() ?? Operation.Entity.selected(app)?.id;
	return Boolean(
		selectedOperationId && object.operation_id === selectedOperationId,
	);
}

function _({ children }: { children: ReactNode }) {
	const [app, setInfo] = useState<App.Type>(App.Base);
	const [banner, setBanner] = useState<{
		node: ReactNode;
		target: string;
	} | null>(null);
	const [dialog, setDialog] = useState<ReactNode>(null);
	const [hintOpen, setHintOpen] = useState(false);
	const timeline = useRef<HTMLDivElement>(null as unknown as HTMLDivElement);
	const [highlightsOverlay, setHighlightsOverlay] =
		useState<React.ReactNode>(null);
	const [eventStoreFallbackRequest, setEventStoreFallbackRequest] =
		useState<EventStoreFallbackRequest | null>(getEventStoreFallbackRequest);

	const location = useLocation();

	useEffect(
		() => subscribeEventStoreFallbackRequest(setEventStoreFallbackRequest),
		[],
	);

	/**
	 * Resets the active dialog state to null when either the selected
	 * operation ID changes or the user navigates to a different route.
	 */
	useEffect(() => {
		setDialog(null);
	}, [Operation.Entity.selected(app)?.id, location.pathname]);

	useEffect(() => {
		/**
		 * Clears authenticated application and EventStore state after auth loss.
		 */
		const handleAuthLost = () => {
			SmartSocket.Class.instance?.destroy();
			DataStore.resetRenderOperation(null);
			setBanner(null);
			setDialog(null);
			setInfo((prev) => ({
				...App.Base,
				general: {
					...App.Base.general,
					server: prev.general.server,
					ws_id: prev.general.ws_id,
				},
			}));
		};

		window.addEventListener("gulp-auth-lost", handleAuthLost);
		return () => {
			window.removeEventListener("gulp-auth-lost", handleAuthLost);
		};
	}, []);

	/**
	 * STABLE INFO INSTANCE: Info is stored in a ref and updated in-place
	 * instead of being recreated on every render with `new Info(...)`.
	 *
	 * ARCHITECTURAL DECISION: Previously, `const instance = new Info(...)` ran on
	 * every render of the root Provider — every state change anywhere in the app
	 * created a new Info object. Since `instance` was in the WS useEffect deps,
	 * this also caused WS listeners to be re-registered on every render.
	 * Using useRef avoids this: the instance is created once, and only its
	 * mutable props (app, setInfo, scrollX, etc.) are updated each render.
	 */
	const infoRef = useRef<Info | null>(null);
	if (!infoRef.current) {
		infoRef.current = new Info({ app, setInfo, timeline });
	} else {
		infoRef.current.app = app;
		infoRef.current.setInfo = setInfo;
	}
	const instance = infoRef.current;

	/**
	 * STABLE REFS FOR WS CALLBACKS: These refs hold the latest `app` and `instance`
	 * without being useEffect dependencies. The WS callbacks read `.current` at call
	 * time, ensuring they always access fresh state while the effect itself only
	 * re-runs when the WebSocket connection changes (deps: [ws]).
	 *
	 * Without this pattern, `[ws, app, instance]` as deps caused the WS listeners
	 * to be unregistered and re-registered on every single state update in the entire app.
	 */
	const appRef = useRef(app);
	appRef.current = app;
	const instanceRef = useRef(instance);

	const ws = useMemo(() => {
		if (!app.general.user) {
			return;
		}

		return new SmartSocket.Class(app.general.ws_id, app.general.user.id);
	}, [app.general.server, app.general.ws_id, app.general.user?.id]);

	const activeUserAuthorityRef = useRef<string | null>(null);
	useEffect(() => {
		const userAuthority = app.general.user
			? `${app.general.server}:${app.general.user.id}`
			: null;
		const previousUserAuthority = activeUserAuthorityRef.current;
		activeUserAuthorityRef.current = userAuthority;
		if (
			previousUserAuthority !== null &&
			previousUserAuthority !== userAuthority
		) {
			DataStore.resetRenderOperation(null);
		}
	}, [app.general.server, app.general.user?.id]);

	useEffect(() => {
		if (!ws) return;

		/**
		 * Handles incoming collab create messages from WebSocket.
		 * This ensures the UI is reactive to new items regardless of who created them.
		 * @param message WebSocket message containing one or more collaboration entities.
		 * @returns Nothing.
		 */
		const collabCreateCallback = (
			message: SmartSocket.Message.Entity<unknown>,
		): void => {
			const objects = getCollabPayloadObjects(message.payload.obj);
			const notes = objects.filter(
				(obj) =>
					obj.type === "note" &&
					isSelectedOperationObject(appRef.current, obj),
			) as Note.Type[];
			if (notes.length > 0) {
				const storedNotes =
					instanceRef.current.notes_upsert_from_collab(notes);
				if (storedNotes.length > 0) {
					DataStore.publishNotesUpsertDelta(
						storedNotes[0].operation_id,
						"create",
						storedNotes,
					);
				}
			}

			objects.forEach((obj) => {
				if (!isSelectedOperationObject(appRef.current, obj)) return;
				if (obj.type === "context") {
					const context = obj as Context.Type;
					setInfo((prev) => {
						if (prev.target.contexts.find((c) => c.id === context.id)) return prev;
						return {
							...prev,
							target: {
								...prev.target,
								contexts: [...prev.target.contexts, context],
							},
						};
					});
				} else if (obj.type === "source") {
					const source = obj as Source.Type;
					setInfo((prev) => {
						if (prev.target.files.find((f) => f.id === source.id)) return prev;
						return {
							...prev,
							target: {
								...prev.target,
								files: [
									...prev.target.files,
									Source.Entity.normalize(prev, source),
								],
							},
						};
					});
				}
			});
		};

		/**
		 * Handles incoming collab update messages (note/link/highlight) from WebSocket.
		 * @param message WebSocket message containing a collaboration entity.
		 * @returns Nothing.
		 */
		const collabUpdateCallback = (
			message: SmartSocket.Message.Entity<CollabPayloadObject>,
		): void => {
			if (!isSelectedOperationObject(appRef.current, message.payload.obj)) {
				return;
			}
			switch (message.payload.obj.type) {
				case "note": {
					const storedNotes =
						instanceRef.current.notes_upsert_from_collab([
						message.payload.obj as Note.Type,
					]);
					if (storedNotes.length > 0) {
						DataStore.publishNotesUpsertDelta(
							storedNotes[0].operation_id,
							"update",
							storedNotes,
						);
					}
					return;
				}
				case "link": {
					const link = message.payload.obj as Link.Type;
					DataStore.upsertLink(link, "immediate");
					RenderEngine.clearAllCaches();
					instanceRef.current.render();
					return;
				}
				case "highlight":
					instanceRef.current.highlights_reload();
					return;
			}
		};

		/**
		 * Handles collab delete messages from WebSocket.
		 * @param message WebSocket message containing one or more deleted entity identifiers.
		 * @returns Nothing.
		 */
		const collabDeleteCallback = (message: CollabDeleteMessage): void => {
			const { type } = message.payload;
			const deletedIds = normalizeCollabDeleteIds(message.payload);
			const isBulkDelete = Array.isArray(message.payload.ids);
			if (deletedIds.length === 0) return;

			const operationId = normalizeCollabDeleteOperationId(message) as
				| Operation.Id
				| undefined;
			const selectedOperationId =
				DataStore.getRenderOperationId() ??
				Operation.Entity.selected(appRef.current)?.id;

			if (type === "note") {
				if (!operationId || operationId !== selectedOperationId) return;

				const noteIds = deletedIds as Note.Id[];
				const deletedNotes = DataStore.deleteNotes(noteIds, "deferred");
				const affectedSourceIds = new Set<Source.Id>();
				deletedNotes.forEach((deletedNote) => {
					const sourceId = Note.Entity.sourceId(deletedNote);
					if (sourceId) affectedSourceIds.add(sourceId);
				});
				Note.Entity.removeIndexedNotes(deletedNotes);
				affectedSourceIds.forEach((sourceId) =>
					RenderEngine.resetSourceNotes(sourceId),
				);
				DataStore.publishNotesDeleteDelta(operationId, noteIds);
				// Keep timeline work and user feedback coalesced to the websocket packet.
				instanceRef.current.render();
				if (isBulkDelete) {
					toast.success(
						translate("notes.deletedCount", { count: noteIds.length }),
						{ richColors: true },
					);
				}
				return;
			}

			if (type === "highlight") {
				instanceRef.current.highlights_reload();
				return;
			}

			if (type === "link") {
				const deletedLinks = DataStore.deleteLinks(
					deletedIds as Link.Id[],
					"immediate",
				);
				if (deletedLinks.length > 0) {
					RenderEngine.clearAllCaches();
					instanceRef.current.render();
				}
				return;
			}

			const deletedIdSet = new Set(deletedIds);
			const deletedSources = appRef.current.target.files.filter(
				(source) => deletedIdSet.has(source.id),
			);
			deletedSources.forEach((deletedSource) => {
				if (typeof ws.deleteSourceEventStore === "function") {
					void ws
						.deleteSourceEventStore(
							deletedSource.operation_id,
							deletedSource.id,
						)
						.catch((error: unknown) => {
							console.warn(
								`Unable to clear deleted source cache ${deletedSource.id}`,
								error,
							);
						});
				}
				Doc.Entity.delete(appRef.current, deletedSource);
				RenderEngine.resetSource(deletedSource.id);
			});

			setInfo((prev) => {
				// Context/Source deletion (Low frequency)
				if (prev.target.contexts.some((c) => deletedIdSet.has(c.id))) {
					return {
						...prev,
						target: {
							...prev.target,
							contexts: prev.target.contexts.filter(
								(c) => !deletedIdSet.has(c.id),
							),
						},
					};
				}
				if (prev.target.files.some((f) => deletedIdSet.has(f.id))) {
					return {
						...prev,
						target: {
							...prev.target,
							files: prev.target.files.filter(
								(f) => !deletedIdSet.has(f.id),
							),
						},
					};
				}
				return prev;
			});
		};

		/**
		 * Applies request_stats websocket messages to the shared request list.
		 * @param message Incoming websocket message that contains a request stats object.
		 * @returns Nothing.
		 */
		const requestStatsCallback = (message: any): void =>
			instanceRef.current.request_add(message.payload.obj);

		ws.on(
			SmartSocket.Message.Type.COLLAB_CREATE,
			collabCreateCallback,
		);
		ws.on(
			SmartSocket.Message.Type.COLLAB_UPDATE,
			collabUpdateCallback,
		);
		ws.on(
			SmartSocket.Message.Type.COLLAB_DELETE,
			collabDeleteCallback,
		);
		const statsCreateListenerId = ws.con(
			SmartSocket.Message.Type.STATS_CREATE,
			(m) => m.payload.obj?.type === "request_stats",
			requestStatsCallback,
		);
		const statsUpdateListenerId = ws.con(
			SmartSocket.Message.Type.STATS_UPDATE,
			(m) => m.payload.obj?.type === "request_stats",
			requestStatsCallback,
		);
		const unsubscribeSourceSummaries =
			ws.subscribeSourceSummaries((event) => {
				if (
					event.operationEpoch !== DataStore.getRenderOperationEpoch() ||
					DataStore.getRenderOperationId() !== event.operationId
				) {
					return;
				}
				instanceRef.current.handleEventSourceSummary(event);
			});
		const subscribeStreamFailures =
			ws.subscribeDocumentStreamFailures((event) => {
				instanceRef.current.handleEventDocumentStreamFailure(event);
			});
		const subscribeStreamStates =
			ws.subscribeDocumentStreamStates((change) => {
				instanceRef.current.handleEventDocumentStreamState(change);
			});

		return () => {
			unsubscribeSourceSummaries();
			subscribeStreamFailures();
			subscribeStreamStates();
			ws.off(
				SmartSocket.Message.Type.COLLAB_CREATE,
				collabCreateCallback,
			);
			ws.off(
				SmartSocket.Message.Type.COLLAB_UPDATE,
				collabUpdateCallback,
			);
			ws.off(
				SmartSocket.Message.Type.COLLAB_DELETE,
				collabDeleteCallback,
			);
			ws.coff(
				SmartSocket.Message.Type.STATS_CREATE,
				statsCreateListenerId,
			);
			ws.coff(
				SmartSocket.Message.Type.STATS_UPDATE,
				statsUpdateListenerId,
			);
			if (SmartSocket.Class.instance === ws && !appRef.current.general.user) {
				ws.destroy();
			}
		};
	}, [ws]);

	const spawnBanner = useCallback(
		(node: React.ReactNode, target: string = "main") => {
			if (!node) {
				setBanner(null);
				document.querySelector("body")?.classList.remove("no-scroll");
			} else {
				setBanner({ node, target });
				document.querySelector("body")?.classList.add("no-scroll");
			}
		},
		[],
	);

	// @ts-ignore
	window.spawnBanner = spawnBanner;

	const destroyBanner = useCallback(() => {
		setBanner(() => null);
		document.querySelector("body")?.classList.remove("no-scroll");
	}, []);

	const spawnDialog = useCallback((dialog: React.ReactNode) => {
		setDialog(dialog);
	}, []);

	const toggleHintOpen = useCallback(() => {
		setHintOpen((value) => !value);
	}, []);

	const [canvasDocked, setCanvasDocked] = useState(true);
	const [dialogsDocked, setDialogsDocked] = useState(true);

	const props = useMemo(
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
			eventStoreFallbackRequest,
			dismissEventStoreFallbackRequest: clearEventStoreFallbackRequest,
			canvasDocked,
			setCanvasDocked,
			dialogsDocked,
			setDialogsDocked,
			hintOpen,
			setHintOpen,
			toggleHintOpen,
			isDetachedWindow: false,
			currentDocument: globalThis.document,
			detachedWindowId: null,
			detachedCoordinatorId: null,
			detachedClientKind: null,
			detachedTableSourceId: null,
			detachedTableSourceRevision: 0,
			detachedSelectedEventId: app.timeline.target?._id ?? null,
			detachedDialog: null,
			detachedDialogEventColors: {},
			detachedDialogNavigation: null,
			selectDetachedDialogEvent: () => {},
			setDetachedTableSync: () => {},
			setDetachedNotesRenderedSync: () => {},
		}),
		[
			spawnBanner,
			destroyBanner,
			banner,
			spawnDialog,
			dialog,
			app,
			setInfo,
			instance,
			timeline,
			highlightsOverlay,
			setHighlightsOverlay,
			eventStoreFallbackRequest,
			canvasDocked,
			dialogsDocked,
			hintOpen,
			toggleHintOpen,
			app.timeline.target?._id,
		],
	);

	const handleKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
		if (!app.timeline.target || banner) return;

		const key = event.key.toLowerCase();

		// check for input etc...
		const target = event.target as HTMLElement;
		const tag = target.tagName.toLowerCase();
		if (
			["input", "textarea", "select"].includes(tag) ||
			target.isContentEditable
		)
			return;

		if (["d", "a", "arrowright", "arrowleft"].includes(key)) {
			const delta: -1 | 1 = key === "a" || key === "arrowleft" ? 1 : -1;
			void instance.moveTimelineTarget(delta).then((nextTarget) => {
				if (nextTarget) {
					spawnDialog(<DisplayEventDialog event={nextTarget} />);
				} else {
					toast(translate(delta > 0 ? "application.cannotOpenPreviousEvent" : "application.cannotOpenNextEvent"));
				}
			});
		} else if (["ф", "а"].includes(key)) {
			toast(translate("application.useEnglishScrollKeys"));
		} else if (key === "end") {
			event.preventDefault();
			void instance
				.getTimelineSourceEdge(
					app.timeline.target["gulp.source_id"],
					true,
				)
				.then((nextTarget) => {
					if (!nextTarget) return;
					instance.setTimelineTarget(nextTarget);
					spawnDialog(<DisplayEventDialog event={nextTarget} />);
				});
		} else if (key === "home") {
			event.preventDefault();
			void instance
				.getTimelineSourceEdge(
					app.timeline.target["gulp.source_id"],
					false,
				)
				.then((nextTarget) => {
					if (!nextTarget) return;
					instance.setTimelineTarget(nextTarget);
					spawnDialog(<DisplayEventDialog event={nextTarget} />);
				});
		}
	};

	useEffect(() => {
		window.addEventListener("keydown", handleKeyDown as any);

		return () => {
			window.removeEventListener("keydown", handleKeyDown as any);
		};
	}, [dialog, app.timeline.target, banner]);

	return (
		<Application.Context.Provider value={props}>
			{children}
		</Application.Context.Provider>
	);
}

export namespace Application {
	export namespace Context {
		export interface Props {
			spawnBanner: (banner: React.ReactNode, target?: string) => void;
			destroyBanner: () => void;
			banner: { node: React.ReactNode; target: string } | null;
			spawnDialog: (dialog: React.ReactNode) => void;
			dialog: React.ReactNode;
			app: App.Type;
			setInfo: (info: App.Type) => void;
			Info: Info;
			timeline: React.RefObject<HTMLDivElement>;
			highlightsOverlay: React.ReactNode;
			setHighlightsOverlay: SetState<React.ReactNode>;
			eventStoreFallbackRequest: EventStoreFallbackRequest | null;
			dismissEventStoreFallbackRequest: () => void;
			canvasDocked: boolean;
			setCanvasDocked: React.Dispatch<React.SetStateAction<boolean>>;
			dialogsDocked: boolean;
			setDialogsDocked: React.Dispatch<React.SetStateAction<boolean>>;
			hintOpen: boolean;
			setHintOpen: React.Dispatch<React.SetStateAction<boolean>>;
			toggleHintOpen: () => void;
			isDetachedWindow: boolean;
			currentDocument: Document;
			detachedWindowId: string | null;
			detachedCoordinatorId: string | null;
			detachedClientKind: WindowBridge.DetachedClientKind | null;
			detachedTableSourceId: Source.Id | null;
			detachedTableSourceRevision: number;
			detachedSelectedEventId: Doc.Id | null;
			detachedDialog: WindowBridge.DetachedDialogDescriptor | null;
			detachedDialogEventColors: Partial<Record<Doc.Id, string>>;
			detachedDialogNavigation: WindowBridge.DetachedDialogNavigation | null;
			selectDetachedDialogEvent: (docId: Doc.Id) => void;
			setDetachedTableSync: (enabled: boolean) => void;
			setDetachedNotesRenderedSync: (enabled: boolean) => void;
		}
	}

	export const Context = createContext<Application.Context.Props>(null!);

	export const use = (): Application.Context.Props =>
		useContext(Application.Context);

	export const Provider = _;
}

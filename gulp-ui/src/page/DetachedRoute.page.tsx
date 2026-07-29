import { useEffect, useMemo, useRef } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { DetachedAppProvider } from "@/context/DetachedApp.provider";
import { WindowBridge } from "@/lib/WindowBridge";
import { NotesWindow } from "@/components/NotesWindow";
import { TableViewWindow } from "@/components/TableViewWindow";
import { DashboardViewWindow } from "@/components/DashboardViewWindow";
import { AIAssistantWindow } from "@/components/AIAssistantWindow";
import { DisplayEventDialog } from "@/dialogs/Event.dialog";
import { DisplayGroupDialog } from "@/dialogs/Group.dialog";
import { Application } from "@/context/Application.context";
import { Extension } from "@/context/Extension.context";
import type { Source } from "@/entities/Source";

type DetachedRouteKind =
	| "notes"
	| "table"
	| "dashboard"
	| "assistant"
	| "dialog";

const DETACHED_ROUTE_TITLES: Record<DetachedRouteKind, string> = {
	notes: "Gulp Notes",
	table: "Gulp Table View",
	dashboard: "Gulp Dashboard",
	assistant: "Gulp AI Assistant",
	dialog: "Gulp Event Details",
};

/**
 * Parses a route param into a supported detached route kind.
 * @param value Raw route param value.
 * @returns Supported detached route kind, or null for unknown routes.
 */
function parseDetachedRouteKind(value: string | undefined): DetachedRouteKind | null {
	if (
		value === "notes" ||
		value === "table" ||
		value === "dashboard" ||
		value === "assistant" ||
		value === "dialog"
	) {
		return value;
	}

	return null;
}

/**
 * Reads the optional table source id from query params.
 * @param value Raw source id query param.
 * @returns Source id for table preselection, or undefined.
 */
function parseSourceId(value: string | null): Source.Id | undefined {
	return value ? value as Source.Id : undefined;
}

/**
 * Boots an independent detached React route backed by WindowBridge hydration.
 * @returns Detached route provider and selected detached app content.
 */
export function DetachedRouteShell() {
	const { kind } = useParams<{ kind?: string }>();
	const [searchParams] = useSearchParams();
	const routeKind = parseDetachedRouteKind(kind);
	const bridgeIdRef = useRef(
		searchParams.get("bridgeId") ?? WindowBridge.generateId(),
	);
	const coordinatorIdRef = useRef(
		searchParams.get("coordinatorId") ??
			WindowBridge.getOrCreateCoordinatorId(),
	);
	const sourceId = useMemo(
		() => parseSourceId(searchParams.get("sourceId")),
		[searchParams],
	);
	const assistantTitle =
		searchParams.get("assistantTitle") ?? DETACHED_ROUTE_TITLES.assistant;
	const assistantPluginFilename =
		searchParams.get("assistantPlugin") ?? undefined;

	useEffect(() => {
		if (!routeKind) return;
		document.title =
			routeKind === "assistant"
				? assistantTitle
				: DETACHED_ROUTE_TITLES[routeKind];
	}, [assistantTitle, routeKind]);

	if (!routeKind) {
		return null;
	}

	return (
		<DetachedAppProvider
			bridgeId={bridgeIdRef.current}
			coordinatorId={coordinatorIdRef.current}
			kind={routeKind}
			detachedDocument={document}
			syncTimelineTargetToMain={routeKind === "dialog"}
			assistantQuerySyncEnabled={
				routeKind === "assistant" && Boolean(assistantPluginFilename)
			}
			loadExtensions={
				routeKind === "dashboard" ||
				routeKind === "dialog" ||
				(routeKind === "assistant" && Boolean(assistantPluginFilename))
			}
			extensionFilenames={
				assistantPluginFilename
					? [assistantPluginFilename]
					: undefined
			}
			extensionSlots={
				routeKind === "dashboard"
					? [Extension.Slot.DashboardView]
					: routeKind === "dialog"
						? [
								Extension.Slot.SendData,
								Extension.Slot.EventActions,
								Extension.Slot.EventContextMenu,
							]
						: undefined
			}
		>
			<DetachedRouteContent
				kind={routeKind}
				initialSourceId={sourceId}
				assistantTitle={assistantTitle}
				assistantPluginFilename={assistantPluginFilename}
			/>
		</DetachedAppProvider>
	);
}

/**
 * Renders detached app content once DetachedAppProvider supplies context.
 * @param props.kind Detached route kind.
 * @param props.initialSourceId Optional table source preselection.
 * @returns Detached app window component.
 */
function DetachedRouteContent({
	kind,
	initialSourceId,
	assistantTitle,
	assistantPluginFilename,
}: {
	kind: DetachedRouteKind;
	initialSourceId?: Source.Id;
	assistantTitle: string;
	assistantPluginFilename?: string;
}) {
	if (kind === "notes") {
		return <NotesWindow onClose={() => window.close()} />;
	}

	if (kind === "table") {
		return (
			<TableViewWindow
				initialSourceId={initialSourceId}
				onClose={() => window.close()}
			/>
		);
	}

	if (kind === "dashboard") {
		return <DashboardViewWindow onClose={() => window.close()} />;
	}

	if (kind === "dialog") {
		return <DetachedDialogRouteContent />;
	}

	return (
		<AIAssistantWindow
			title={assistantTitle}
			pluginFilename={assistantPluginFilename}
			onClose={() => window.close()}
		/>
	);
}

function DetachedDialogRouteContent() {
	const { dialog, detachedDialog } = Application.use();
	const content =
		dialog ??
		(detachedDialog?.kind === "group"
			? (
					<DisplayGroupDialog
						events={detachedDialog.events}
						groupKind={detachedDialog.groupKind}
					/>
				)
			: detachedDialog
				? <DisplayEventDialog event={detachedDialog.event} />
				: null);
	if (!content) return null;

	return (
		<div
			data-detached-dialog-root
			style={{
				display: "flex",
				width: "100%",
				height: "100vh",
				minHeight: 0,
				overflow: "hidden",
			}}
		>
			{content}
		</div>
	);
}

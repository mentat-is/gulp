import { useEffect, useMemo, useRef } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { DetachedAppProvider } from "@/context/DetachedApp.provider";
import { WindowBridge } from "@/lib/WindowBridge";
import { NotesWindow } from "@/components/NotesWindow";
import { TableViewWindow } from "@/components/TableViewWindow";
import { DashboardViewWindow } from "@/components/DashboardViewWindow";
import { Source } from "@/entities/Source";
import {
	getPersistedDetachedWindowRecord,
	persistDetachedWindow,
	removePersistedDetachedWindow,
} from "@/utils";

type DetachedRouteKind = "notes" | "table" | "dashboard";

const DETACHED_ROUTE_TITLES: Record<DetachedRouteKind, string> = {
	notes: "Gulp Notes",
	table: "Gulp Table View",
	dashboard: "Gulp Dashboard",
};

/**
 * Parses a route param into a supported detached route kind.
 * @param value Raw route param value.
 * @returns Supported detached route kind, or null for unknown routes.
 */
function parseDetachedRouteKind(value: string | undefined): DetachedRouteKind | null {
	if (value === "notes" || value === "table" || value === "dashboard") {
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
	const sourceId = useMemo(
		() => parseSourceId(searchParams.get("sourceId")),
		[searchParams],
	);

	useEffect(() => {
		if (!routeKind) return;
		document.title = DETACHED_ROUTE_TITLES[routeKind];
	}, [routeKind]);

	useEffect(() => {
		if (!routeKind) return;

		const record = getPersistedDetachedWindowRecord(routeKind);

		/**
		 * Removes this detached window from recovery storage when the actual
		 * detached browser window is closed or navigated away.
		 *
		 * @returns Nothing.
		 */
		const removeDetachedRecoveryRecord = () => {
			removePersistedDetachedWindow(record.key);
		};

		persistDetachedWindow(record);
		window.addEventListener("pagehide", removeDetachedRecoveryRecord);
		window.addEventListener("beforeunload", removeDetachedRecoveryRecord);

		return () => {
			window.removeEventListener("pagehide", removeDetachedRecoveryRecord);
			window.removeEventListener("beforeunload", removeDetachedRecoveryRecord);
		};
	}, [routeKind]);

	if (!routeKind) {
		return null;
	}

	return (
		<DetachedAppProvider
			bridgeId={bridgeIdRef.current}
			detachedDocument={document}
		>
			<DetachedRouteContent
				kind={routeKind}
				initialSourceId={sourceId}
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
}: {
	kind: DetachedRouteKind;
	initialSourceId?: Source.Id;
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

	return <DashboardViewWindow onClose={() => window.close()} />;
}

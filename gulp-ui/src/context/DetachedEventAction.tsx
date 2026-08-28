import type { ReactNode } from "react";
import type { App } from "@/entities/App";
import type { Info } from "@/class/Info";
import { Doc } from "@/entities/Doc";
import { Note } from "@/entities/Note";
import { Link } from "@/entities/Link";
import { Source } from "@/entities/Source";
import { Filter } from "@/entities/Filter";
import { DataStore } from "@/store/DataStore";
import { WindowBridge } from "@/lib/WindowBridge";
import { Enrichment } from "@/banners/Enrichment.banner";
import {
	LinkFunctionality,
	NoteFunctionality,
} from "@/banners/Collab.functionality";
import { FilterFileBanner } from "@/banners/FilterFile.banner";
import { SendData } from "@/banners/SendData.banner";
import { LinkEventActionBanner } from "@/dialogs/Event.dialog";

interface MainEventActionOptions {
	app: App.Type;
	Info: Info;
	event: Doc.Type;
	action: WindowBridge.EventActionRequest;
	onEnrichment: (event: Doc.Type) => void;
}

/**
 * Checks whether a link can be managed from the requested Event dialog.
 * @param link Link resolved from the main collaboration store.
 * @param eventId Event that originated the action.
 * @returns True when the event is the link root or one of its destinations.
 */
function isLinkRelatedToEvent(link: Link.Type, eventId: Doc.Id): boolean {
	return link.doc_id_from === eventId || link.doc_ids.includes(eventId);
}

/**
 * Creates the main-document banner represented by a detached Event action.
 * Collaboration entities, sources, and query state are resolved from the main
 * stores so detached snapshots never become mutation authority.
 * @param options Main application state, current event, action, and result callback.
 * @returns Main-owned banner element, or null when the request is invalid or stale.
 */
export function createMainEventActionBanner({
	app,
	Info,
	event,
	action,
	onEnrichment,
}: MainEventActionOptions): ReactNode {
	switch (action.kind) {
		case "enrich":
			if (
				action.field &&
				(typeof action.field.key !== "string" ||
					typeof action.field.value !== "string")
			) {
				return null;
			}
			return (
				<Enrichment.Banner
					event={event}
					enrichmentField={action.field}
					onEnrichment={onEnrichment}
				/>
			);
		case "note-create":
			if (
				action.initialText !== undefined &&
				typeof action.initialText !== "string"
			) {
				return null;
			}
			return (
				<NoteFunctionality.Create.Banner
					event={event}
					note={
						action.initialText
							? ({ text: action.initialText } as Note.Type)
							: undefined
					}
				/>
			);
		case "note-edit":
		case "note-delete": {
			const note = DataStore.notes.find(
				(candidate) => candidate.id === action.noteId,
			);
			if (!note || note.doc._id !== event._id) return null;
			return action.kind === "note-edit" ? (
				<NoteFunctionality.Create.Banner event={event} note={note} />
			) : (
				<Note.Delete.Banner note={note} />
			);
		}
		case "link-create":
			return (
				<LinkFunctionality.Create.Banner
					event={event}
					initialDocIds={[event._id]}
				/>
			);
		case "link-connect":
			return <LinkFunctionality.Connect.Banner event={event} />;
		case "link-manage":
		case "link-edit":
		case "link-delete": {
			const link = DataStore.links.find(
				(candidate) => candidate.id === action.linkId,
			);
			if (!link || !isLinkRelatedToEvent(link, event._id)) return null;
			if (action.kind === "link-manage") {
				return <LinkEventActionBanner event={event} link={link} />;
			}
			if (action.kind === "link-edit") {
				return <LinkFunctionality.Create.Banner event={event} link={link} />;
			}
			return <Link.Delete.Banner link={link} />;
		}
		case "filter": {
			if (
				typeof action.field !== "string" ||
				action.field.trim() === "" ||
				typeof action.value !== "string"
			) {
				return null;
			}
			const source = Source.Entity.id(app, event["gulp.source_id"]);
			if (!source) return null;
			const currentQuery = Info.getQuery(source);
			const filter = Filter.Entity.createCondition(
				WindowBridge.generateId() as Filter.Id,
				{ field: action.field, value: action.value },
			);
			return (
				<FilterFileBanner
					sources={[source]}
					query={{
						...currentQuery,
						filters: [...currentQuery.filters, filter],
					}}
				/>
			);
		}
		case "send-data":
			return <SendData.Banner event={event} />;
		default:
			return null;
	}
}

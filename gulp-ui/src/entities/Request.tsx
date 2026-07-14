type UUID = string;
import type { Context } from "./Context";
import type { Source } from "./Source";
import { User } from "./User";

export namespace Request {
	export const name = "Request";
	const _ = Symbol(Request.name);
	export type Id = UUID & {
		readonly [_]: unique symbol;
	};

	export interface Type {
		id: Request.Id;
		completed: string;
		granted_user_group_ids: User.Id[];
		granted_user_ids: User.Id[];
		name: string;
		owner_user_id: User.Id;
		records_failed: number;
		records_ingested: number;
		records_processed: number;
		records_skipped: number;
		source_failed: number;
		source_processed: number;
		source_total: number;
		status: Request.Status;
		time_created: number;
		time_expire: number;
		time_finished: number;
		time_updated: number;
		type: "request_stats";
		req_type?: string;
		data?: Request.Data;
		errors?: Request.ErrorEntry[];
	}

	export type SourceLinkTuple = [
		Context.Id | string,
		string,
		Source.Id | string,
		string,
	];

	export interface Data {
		sources?: SourceLinkTuple[];
		[key: string]: unknown;
	}

	export interface SourceLink {
		contextId: Context.Id | string;
		contextName: string;
		sourceId: Source.Id | string;
		sourceName: string;
	}

	export type ErrorEntry =
		| string
		| {
				message?: string;
				msg?: string;
				error?: string;
				[key: string]: unknown;
		  };

	export interface RecordCounts {
		records_ingested: number;
		records_skipped: number;
		records_failed: number;
	}

	export enum Status {
		ONGOING = "ongoing",
		DONE = "done",
		FAILED = "failed",
		CANCELED = "canceled",
		PENDING = "pending",
	}

	export enum Prefix {
		INGESTION = "ingestion",
		QUERY = "query",
		ENRICHMENT = "enrichment",
		AI = "ai",
	}

	export class Entity {
		/**
		 * Extracts all linked context/source identifiers from a request payload.
		 * @param request Request object that may contain backend data.sources details.
		 * @returns Linked context/source details.
		 */
		public static sourceLinks = (
			request: Pick<Request.Type, "data">,
		): Request.SourceLink[] => {
			return Request.Entity.sourceLinkTuples(request).flatMap((sourceTuple) => {
				const [contextId, contextName, sourceId, sourceName] = sourceTuple;
				if (
					!Request.Entity.isRenderableValue(contextId) ||
					!Request.Entity.isRenderableValue(contextName) ||
					!Request.Entity.isRenderableValue(sourceId) ||
					!Request.Entity.isRenderableValue(sourceName)
				) {
					return [];
				}

				return [
					{
						contextId: String(contextId),
						contextName: String(contextName),
						sourceId: String(sourceId),
						sourceName: String(sourceName),
					},
				];
			});
		};

		/**
		 * Extracts the first linked context/source identifiers from a request payload.
		 * @param request Request object that may contain backend data.sources details.
		 * @returns First linked context/source details, or null when unavailable.
		 */
		public static sourceLink = (
			request: Pick<Request.Type, "data">,
		): Request.SourceLink | null => {
			return Request.Entity.sourceLinks(request)[0] ?? null;
		};

		/**
		 * Normalizes backend source tuple data from request.data.sources.
		 * @param request Request object that may contain source tuple data.
		 * @returns Source tuple list.
		 */
		private static sourceLinkTuples = (
			request: Pick<Request.Type, "data">,
		): unknown[][] => {
			const sources = request.data?.sources as unknown;
			if (!Array.isArray(sources)) {
				return [];
			}

			if (Request.Entity.isSourceTuple(sources)) {
				return [sources];
			}

			return sources.filter(Request.Entity.isSourceTuple);
		};

		/**
		 * Checks whether a backend value matches the expected source tuple layout.
		 * @param value Backend value to inspect.
		 * @returns True when the value looks like a source tuple.
		 */
		private static isSourceTuple = (value: unknown): value is unknown[] => {
			return Array.isArray(value) && value.length >= 4;
		};

		/**
		 * Extracts human-readable error messages from backend request errors.
		 * @param request Request object that may contain an errors array.
		 * @returns Non-empty error message strings.
		 */
		public static errorMessages = (
			request: Pick<Request.Type, "errors">,
		): string[] => {
			if (!Array.isArray(request.errors)) {
				return [];
			}

			return request.errors.flatMap((entry) => {
				const message = Request.Entity.errorEntryMessage(entry);
				return message ? [message] : [];
			});
		};

		/**
		 * Reads record counters from request stats or an ingest completion payload.
		 * @param source Object that may contain ingestion record counters.
		 * @returns Normalized numeric record counters.
		 */
		public static recordCounts = (source: unknown): Request.RecordCounts => {
			const recordSource =
				source && typeof source === "object"
					? Request.Entity.recordCountSource(source as Record<string, unknown>)
					: {};

			return {
				records_ingested: Request.Entity.readNumber(
					recordSource.records_ingested,
				),
				records_skipped: Request.Entity.readNumber(
					recordSource.records_skipped,
				),
				records_failed: Request.Entity.readNumber(recordSource.records_failed),
			};
		};

		/**
		 * Selects the object that contains record counters from a request or websocket payload.
		 * @param source Backend request object or completion payload.
		 * @returns Object that may contain record counters.
		 */
		private static recordCountSource = (
			source: Record<string, unknown>,
		): Partial<Request.RecordCounts> => {
			if (source.data && typeof source.data === "object") {
				return source.data as Partial<Request.RecordCounts>;
			}

			return source as Partial<Request.RecordCounts>;
		};

		/**
		 * Converts one backend error entry to display text.
		 * @param entry Backend error entry from request.errors.
		 * @returns The error message, or null when no message can be rendered.
		 */
		private static errorEntryMessage = (
			entry: Request.ErrorEntry,
		): string | null => {
			if (typeof entry === "string") {
				return entry.trim() || null;
			}

			const message = entry.message ?? entry.msg ?? entry.error;
			return typeof message === "string" && message.trim()
				? message.trim()
				: null;
		};

		/**
		 * Checks whether a backend value can be safely rendered as text.
		 * @param value Backend value to inspect.
		 * @returns True when the value is a string or number.
		 */
		private static isRenderableValue = (
			value: unknown,
		): value is string | number =>
			typeof value === "string" || typeof value === "number";

		/**
		 * Converts backend count values to numbers.
		 * @param value Backend count value.
		 * @returns Numeric value, or zero when absent/invalid.
		 */
		private static readNumber = (value: unknown): number => {
			if (typeof value === "number" && Number.isFinite(value)) {
				return value;
			}

			if (typeof value === "string" && value.trim()) {
				const parsed = Number(value);
				return Number.isFinite(parsed) ? parsed : 0;
			}

			return 0;
		};
	}
}

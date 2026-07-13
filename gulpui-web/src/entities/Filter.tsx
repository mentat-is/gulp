import { OpenSearchQueryBuilder } from "@/components/QueryBuilder";
import { Acceptable } from "@/dto/ElasticGetMapping.dto";
import { Query } from "./Query";
import { MinMax } from "@/class/Info";
import { Source } from "./Source";
import { App } from "./App";
import { translate } from "@/locales/core";
type UUID = string;
import { Internal } from "./addon/Internal";

export namespace Filter {
	export const name = "Filter";
	const _ = Symbol(Filter.name);
	export type Id = UUID & {
		readonly [_]: unique symbol;
	};

	export enum Operator {
		GREATER_OR_EQUAL = ">=",
		EQUAL = "==",
		LESS_OR_EQUAL = "<=",
		NOT_EQUAL = "!=",
		LESS_THAN = "<",
		GREATER_THAN = ">",
	}

	export type Options = Record<string, Acceptable>;

	export interface Type {
		id: Filter.Id;
		type: OpenSearchQueryBuilder.Condition;
		operator: OpenSearchQueryBuilder.Operator;
		field: string;
		value: any;
		enabled: boolean;
		case_insensitive?: boolean;
		min?: string;
		max?: string;
	}

	/**
	 * A grouped set of conditions that maps to a nested OpenSearch bool query —
	 * effectively a parenthesis around its children.
	 *
	 * - `operator` — where this group (the wrapping bool) is placed in the **parent's**
	 *   bool clause.
	 * - `children` — each child carries its own `operator`, which determines which
	 *   bucket (must/should/must_not/filter) it lands in inside this group's inner bool.
	 *   This means a single group can mix AND, OR, and NOT children freely.
	 */
	export interface Group {
		id: Filter.Id;
		type: "group";
		operator: OpenSearchQueryBuilder.Operator;
		children: Filter.Item[];
		enabled: boolean;
	}

	/** Union of a leaf condition and a nested group. Discriminated by `type`. */
	export type Item = Filter.Type | Filter.Group;

	export function isGroup(item: Filter.Item): item is Filter.Group {
		return (item as any).type === "group";
	}

	export class Entity {
		/**
		 * Builds a single OpenSearch clause for one `Filter.Item` (leaf or group).
		 *
		 * - Leaf: produces a match/wildcard/regexp/range clause as before.
		 * - Group: recurses over its children and wraps them in a nested bool whose
		 *   buckets are populated by each child's own `operator` — so a single group
		 *   can hold a mix of must / should / must_not / filter children. The group's
		 *   own `operator` is handled by the caller (placement in the parent bool).
		 *
		 * Returns `null` when the item is disabled or produces no clause.
		 */
		static buildItemClause = (
			item: Filter.Item,
			fieldTypeMap?: Record<string, string>,
		): Record<string, any> | null => {
			if (!item.enabled) return null;

			if (Filter.isGroup(item)) {
				const inner: Record<string, any[]> = {
					must: [],
					should: [],
					must_not: [],
					filter: [],
				};
				for (const child of item.children) {
					if (!child.enabled) continue;
					const clause = Filter.Entity.buildItemClause(child, fieldTypeMap);
					if (clause) inner[child.operator].push(clause);
				}
				Object.keys(inner).forEach((k) => {
					if (inner[k].length === 0) delete inner[k];
				});
				if (Object.keys(inner).length === 0) return null;
				return { bool: inner };
			}

			const { type, field, value, case_insensitive = false } = item;
			if (!field || !value) return null;

			const fieldType = fieldTypeMap?.[field];
			if (
				(type === "wildcard" || type === "regexp") &&
				(fieldType === "long" || fieldType === "integer")
			) {
				return { query_string: { query: `${field}:${value}` } };
			}

			switch (type) {
				case "regexp":
					return { regexp: { [field]: { value, flags: "ALL" } } };
				case "wildcard":
					return { wildcard: { [field]: { value, case_insensitive } } };
				case "range":
					return {
						range: {
							[field]: {
								gte: value.split(",")[0]?.trim() || 0,
								lte: value.split(",")[1]?.trim() || 0,
							},
						},
					};
				case "LTE":
					return { range: { [field]: { lte: Number(value) } } };
				case "GTE":
					return { range: { [field]: { gte: Number(value) } } };
				case "eq":
					return { term: { [field]: value } };
				default:
					return { match: { [field]: value } };
			}
		};

		static query = (q: Query.Type) => {
			// Manual mode: return raw JSON directly
			if (q.isManual && q.raw) return q.raw;

			const { filters, text_filter, source_config, fieldTypeMap } = q;
			const query: Record<string, any> = {
				bool: {
					must: [],
					should: [],
					must_not: [],
					filter: [],
				},
			};

			// Build source scoping clauses from structured source_config
			if (source_config) {
				query.bool.must.push({
					term: { "gulp.operation_id": source_config.operation_id },
				});
				query.bool.must.push({
					terms: { "gulp.source_id": source_config.source_ids },
				});
				query.bool.must.push({
					range: {
						"gulp.timestamp": {
							gte: source_config.range.min,
							lte: source_config.range.max,
						},
					},
				});
			}

			// Apply wildcard text filter on event.original (case-insensitive)
			if (text_filter) {
				query.bool.must.push({
					wildcard: {
						"event.original": {
							value: text_filter.trim(),
							case_insensitive: true,
						},
					},
				});
			}

			// Process each item — flat condition or nested group
			filters.forEach((item) => {
				if (!item.enabled) return;
				const clause = Filter.Entity.buildItemClause(item, fieldTypeMap);
				if (clause) query.bool[item.operator].push(clause);
			});

			// Clean up empty bool buckets to keep the query compact
			Object.keys(query.bool).forEach((key) => {
				if (query.bool[key].length === 0) {
					delete query.bool[key];
				}
			});

			return query;
		};

		/**
		 * Generates a human-readable description of a Query.Type for display purposes.
		 *
		 * Builds a multi-line label showing:
		 * - Text filter value (if set)
		 * - Source file names (resolved from app state when available, else IDs)
		 * - Each filter condition with field, type, and value
		 * - Manual mode indicator
		 *
		 * @param q - The query to describe.
		 * @param app - Optional app state for resolving source IDs to names.
		 * @returns A human-readable string label.
		 */
		public static describe = (q: Query.Type, app?: App.Type): string => {
			const lines: string[] = [];

			// Show text filter value
			if (q.text_filter) {
				lines.push(`Text: "${q.text_filter}"`);
			}

			// Show source names (resolved from app) or IDs as fallback
			if (q.source_config?.source_ids?.length) {
				const names = q.source_config.source_ids.map((id) => {
					if (app) {
						try {
							const file = Source.Entity.id(app, id as Source.Id);
							return file?.name || id;
						} catch {
							return id;
						}
					}
					return id;
				});
				lines.push(
					translate("filter.sourcesLine", { sources: names.join(", ") }),
				);
			}

			// Show each enabled filter condition/group
			if (q.filters.length > 0) {
				const describeItem = (item: Filter.Item, indent = ""): string => {
					if (!item.enabled) return "";
					if (Filter.isGroup(item)) {
						const childLines = item.children
							.map((c) => describeItem(c, indent + "  "))
							.filter(Boolean);
						const body = childLines.length
							? `\n${childLines.join("\n")}`
							: translate("filter.emptyGroup");
						return `${indent}${item.operator} ${translate("filter.groupLabel")}:${body}`;
					}
					return `${indent}${item.operator} ${item.type} ${item.field}: ${item.value}`;
				};
				q.filters.forEach((item) => {
					const desc = describeItem(item);
					if (desc) lines.push(desc);
				});
			}

			if (q.isManual) {
				lines.push(translate("filter.manualQuery"));
			}

			return lines.join("\n") || translate("filter.noFiltersApplied");
		};

		/**
		 * Builds a default Query.Type for a single source file.
		 *
		 * Creates a clean query scoped to the given file's operation, source ID,
		 * and timestamp range. Used as the initial/fallback query when no
		 * persisted filter exists.
		 *
		 * @param app - The application state containing source metadata.
		 * @param file - The source file (or its ID) to scope the query to.
		 * @param range - Optional custom time range override.
		 * @returns A default Query.Type with empty filters and source_config set.
		 */
		public static default = (
			app: App.Type,
			file: Source.Type | Source.Id,
			range?: MinMax,
		): Query.Type => {
			const id = typeof file === "object" ? file.id : file;
			const targetFile = Source.Entity.id(app, id);

			return {
				string: "",
				text_filter: "",
				source_config: {
					operation_id: targetFile.operation_id,
					source_ids: [targetFile.id],
					range: {
						min: (
							range?.min ??
							targetFile.nanotimestamp?.min ??
							Internal.Transformator.toNanos(targetFile.timestamp.min)
						).toString(),
						max: (
							range?.max ??
							targetFile.nanotimestamp?.max ??
							Internal.Transformator.toNanos(targetFile.timestamp.max)
						).toString(),
					},
				},
				filters: [],
			};
		};

		/**
		 * Wraps a Query.Type into the body format expected by the /query_raw API.
		 *
		 * @param query - The query to wrap.
		 * @returns An object with `q` and `q_options` (sort config).
		 */
		static body = (query: Query.Type) => {
			const body: Record<string, any> = {
				q: { query: Filter.Entity.query(query) },
				q_options: {
					sort: {
						"@timestamp": "desc",
					},
				},
			};

			return body;
		};
	}
}

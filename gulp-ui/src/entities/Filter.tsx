import type { Acceptable } from "@/dto/ElasticGetMapping.dto";
import type { Query } from "./Query";
import type { MinMax } from "@/class/Info";
import { Source } from "./Source";
import type { App } from "./App";
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
	/** Stable empty field catalog for editors without mapping metadata. */
	export const EMPTY_OPTIONS: Filter.Options = {};

	/** Structured query condition stored on a leaf filter. */
	export type Condition =
		| "regexp"
		| "wildcard"
		| "range"
		| "LTE"
		| "GTE"
		| "eq";

	/** OpenSearch bool bucket that receives a filter or group. */
	export type BoolOperator = "must" | "should" | "must_not" | "filter";

	export interface Type {
		id: Filter.Id;
		type: Filter.Condition;
		operator: Filter.BoolOperator;
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
		operator: Filter.BoolOperator;
		children: Filter.Item[];
		enabled: boolean;
	}

	/** Union of a leaf condition and a nested group. Discriminated by `type`. */
	export type Item = Filter.Type | Filter.Group;

	export function isGroup(item: Filter.Item): item is Filter.Group {
		return (item as any).type === "group";
	}

	export class Entity {
		private static readonly allConditions: readonly Filter.Condition[] = [
			"regexp",
			"wildcard",
			"range",
			"LTE",
			"GTE",
			"eq",
		];

		private static readonly numericFieldTypes = new Set<Acceptable>([
			"byte",
			"short",
			"integer",
			"long",
			"unsigned_long",
			"half_float",
			"float",
			"double",
			"scaled_float",
		]);

		/**
		 * Creates a structured leaf condition with the application-wide defaults.
		 * @param id Stable identifier assigned by the caller.
		 * @param overrides Optional values that replace the default filter fields.
		 * @returns A new condition that defaults to enabled equality in the `must` bucket.
		 */
		public static createCondition = (
			id: Filter.Id,
			overrides: Partial<Omit<Filter.Type, "id">> = {},
		): Filter.Type => ({
			id,
			type: "eq",
			field: "",
			case_insensitive: true,
			value: "",
			operator: "must",
			enabled: true,
			...overrides,
		});

		/**
		 * Returns the structured conditions supported by an OpenSearch field type.
		 * @param fieldType OpenSearch mapping type, or undefined when metadata is unavailable.
		 * @returns Conditions allowed for the field type; unknown types allow equality only.
		 */
		public static getAllowedConditions = (
			fieldType?: string,
		): readonly Filter.Condition[] => {
			const normalizedType = fieldType?.trim().toLowerCase();
			if (normalizedType === "keyword" || normalizedType === "wildcard") {
				return Filter.Entity.allConditions;
			}
			if (normalizedType === "text") {
				return ["eq", "wildcard"];
			}
			if (
				normalizedType === "date" ||
				normalizedType === "date_nanos" ||
				normalizedType === "ip" ||
				Filter.Entity.numericFieldTypes.has(normalizedType as Acceptable)
			) {
				return ["eq", "range", "LTE", "GTE"];
			}
			return ["eq"];
		};

		/**
		 * Checks whether a condition can be applied to an OpenSearch field type.
		 * @param condition Structured condition to validate.
		 * @param fieldType OpenSearch mapping type, or undefined when unknown.
		 * @returns True when the condition is valid for the field type.
		 */
		public static isConditionAllowed = (
			condition: Filter.Condition,
			fieldType?: string,
		): boolean =>
			Filter.Entity.getAllowedConditions(fieldType).includes(condition);

		/**
		 * Assigns a field to a filter and resets incompatible conditions to equality.
		 * @param filter Leaf filter being edited.
		 * @param field Selected or manually entered field name.
		 * @param options Available field-to-type metadata.
		 * @returns Updated filter with a condition valid for the selected field.
		 */
		public static assignField = (
			filter: Filter.Type,
			field: string,
			options: Filter.Options,
		): Filter.Type => ({
			...filter,
			field,
			type: Filter.Entity.isConditionAllowed(filter.type, options[field])
				? filter.type
				: "eq",
		});

		/**
		 * Recursively resets conditions that are incompatible with current field metadata.
		 * @param items Structured filters and nested groups to validate.
		 * @param options Available field-to-type metadata.
		 * @returns Original array when already valid, otherwise a normalized copy.
		 */
		public static normalizeConditions = (
			items: Filter.Item[],
			options: Filter.Options,
		): Filter.Item[] => {
			let changed = false;
			const normalized = items.map((item): Filter.Item => {
				if (Filter.isGroup(item)) {
					const children = Filter.Entity.normalizeConditions(item.children, options);
					if (children === item.children) return item;
					changed = true;
					return { ...item, children };
				}

				const next = Filter.Entity.assignField(item, item.field, options);
				if (next.type === item.type) return item;
				changed = true;
				return next;
			});

			return changed ? normalized : items;
		};

		/**
		 * Converts a range input into the JSON value expected for its field type.
		 * @param value User-provided range boundary.
		 * @param fieldType OpenSearch mapping type for the filtered field.
		 * @returns A finite number for numeric fields, a trimmed string for other
		 * fields, or null when a numeric boundary is invalid.
		 */
		private static toRangeBoundary = (
			value: unknown,
			fieldType?: string,
		): string | number | null => {
			const normalizedValue = String(value).trim();
			const normalizedType = fieldType?.trim().toLowerCase() as
				| Acceptable
				| undefined;
			if (normalizedType && Filter.Entity.numericFieldTypes.has(normalizedType)) {
				const numericValue = Number(normalizedValue);
				return Number.isFinite(numericValue) ? numericValue : null;
			}
			return normalizedValue;
		};

		/**
		 * Checks whether a top-level manual-query clause is one of the structured
		 * operation, source, or time constraints generated from source_config.
		 * @param clause Manual OpenSearch clause to inspect.
		 * @param sourceConfig Structured source scope represented by the clause.
		 * @returns True when the clause exactly duplicates the structured scope.
		 */
		private static isSourceScopeClause = (
			clause: Record<string, any>,
			sourceConfig: NonNullable<Query.Type["source_config"]>,
		): boolean => {
			if (
				String(clause.term?.["gulp.operation_id"]) ===
				String(sourceConfig.operation_id)
			) {
				return true;
			}

			const sourceIds = clause.terms?.["gulp.source_id"];
			if (Array.isArray(sourceIds)) {
				const actual = sourceIds.map(String).sort();
				const expected = sourceConfig.source_ids.map(String).sort();
				if (
					actual.length === expected.length &&
					actual.every((id, index) => id === expected[index])
				) {
					return true;
				}
			}

			const range = clause.range?.["gulp.timestamp"];
			return (
				range !== undefined &&
				String(range.gte) === String(sourceConfig.range.min) &&
				String(range.lte) === String(sourceConfig.range.max)
			);
		};

		/**
		 * Removes generated source-scope clauses from a manual query so the current
		 * structured scope can be applied exactly once. User clauses that differ
		 * from the previous generated values are preserved.
		 * @param raw Manual OpenSearch query JSON.
		 * @param sourceConfig Previous structured source scope represented in raw.
		 * @returns The manual filter without duplicate generated scope, or undefined
		 * when no manual clauses remain.
		 */
		public static withoutSourceScope = (
			raw: any,
			sourceConfig?: Query.Type["source_config"],
		): any => {
			if (
				!sourceConfig ||
				!raw ||
				typeof raw !== "object" ||
				!raw.bool ||
				!Array.isArray(raw.bool.must)
			) {
				return raw;
			}

			const must = raw.bool.must.filter(
				(clause: Record<string, any>) =>
					!Filter.Entity.isSourceScopeClause(clause, sourceConfig),
			);
			const bool = { ...raw.bool };
			if (must.length > 0) {
				bool.must = must;
			} else {
				delete bool.must;
			}

			if (Object.keys(bool).length === 0) return undefined;
			return { ...raw, bool };
		};

		/**
		 * Builds a single OpenSearch clause for one `Filter.Item` (leaf or group).
		 *
		 * - Leaf: validates its condition and produces a term/wildcard/regexp/range clause.
		 * - Group: recurses over its children and wraps them in a nested bool whose
		 *   buckets are populated by each child's own `operator` — so a single group
		 *   can hold a mix of must / should / must_not / filter children. The group's
		 *   own `operator` is handled by the caller (placement in the parent bool).
		 *
		 * @param item Leaf condition or group to translate.
		 * @param fieldTypeMap Available field-to-type metadata.
		 * @returns OpenSearch clause, or null when the item is disabled or empty.
		 */
		static buildItemClause = (
			item: Filter.Item,
			fieldTypeMap: Filter.Options = {},
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

			const { field, value, case_insensitive = false } = item;
			if (
				!field ||
				(typeof value === "string" && value.trim() === "") ||
				value === null ||
				value === undefined
			) {
				return null;
			}

			const fieldType = fieldTypeMap?.[field];
			const type = Filter.Entity.isConditionAllowed(item.type, fieldType)
				? item.type
				: "eq";

			switch (type) {
				case "regexp":
					return { regexp: { [field]: { value, flags: "ALL" } } };
				case "wildcard":
					return { wildcard: { [field]: { value, case_insensitive } } };
				case "range": {
					const [rawMin = "", rawMax = ""] = String(value).split(",", 2);
					const range: Record<string, string | number> = {};
					if (rawMin.trim()) {
						const min = Filter.Entity.toRangeBoundary(rawMin, fieldType);
						if (min === null) return null;
						range.gte = min;
					}
					if (rawMax.trim()) {
						const max = Filter.Entity.toRangeBoundary(rawMax, fieldType);
						if (max === null) return null;
						range.lte = max;
					}
					if (Object.keys(range).length === 0) return null;
					return {
						range: {
							[field]: range,
						},
					};
				}
				case "LTE": {
					const boundary = Filter.Entity.toRangeBoundary(value, fieldType);
					if (boundary === null) return null;
					return {
						range: {
							[field]: {
								lte: boundary,
							},
						},
					};
				}
				case "GTE": {
					const boundary = Filter.Entity.toRangeBoundary(value, fieldType);
					if (boundary === null) return null;
					return {
						range: {
							[field]: {
								gte: boundary,
							},
						},
					};
				}
				case "eq":
					return { term: { [field]: value } };
			}
		};

		/**
		 * Builds the OpenSearch query while keeping operation, source, and timeline
		 * constraints as the immutable base of structured and manual filters.
		 * @param q Structured or manual query definition.
		 * @returns OpenSearch query DSL object.
		 */
		static query = (q: Query.Type) => {
			const { filters, text_filter, source_config, fieldTypeMap } = q;
			if (q.isManual && q.raw && !source_config) return q.raw;

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

			if (q.isManual && q.raw) {
				const manualFilter = Filter.Entity.withoutSourceScope(
					q.raw,
					source_config,
				);
				if (manualFilter) query.bool.must.push(manualFilter);
			} else {
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
			}

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
			const rawRange = range
				? {
						min: range.min - targetFile.settings.offset,
						max: range.max - targetFile.settings.offset,
					}
				: undefined;

			return {
				string: "",
				text_filter: "",
				source_config: {
					operation_id: targetFile.operation_id,
					source_ids: [targetFile.id],
					range: {
						min: (
							rawRange
								? Internal.Transformator.toNanos(rawRange.min)
								: targetFile.nanotimestamp?.min ??
									Internal.Transformator.toNanos(targetFile.timestamp.min)
						).toString(),
						max: (
							rawRange
								? Internal.Transformator.toNanos(rawRange.max)
								: targetFile.nanotimestamp?.max ??
									Internal.Transformator.toNanos(targetFile.timestamp.max)
						).toString(),
					},
				},
				filters: [],
			};
		};

		/**
		 * Scopes an existing query to one source and optionally replaces its base
		 * range with an offset-adjusted rendered timeline frame.
		 * @param app Current application state.
		 * @param file Source receiving the dedicated query.
		 * @param query Existing query whose user filters must be preserved.
		 * @param range Optional rendered millisecond timeline frame.
		 * @returns Dedicated query with a canonical source_config and cleaned manual filter.
		 */
		public static scopeToSource = (
			app: App.Type,
			file: Source.Type,
			query: Query.Type,
			range?: MinMax,
		): Query.Type => {
			const defaultQuery = Filter.Entity.default(app, file, range);
			const defaultSourceConfig = defaultQuery.source_config!;

			return {
				...query,
				raw: query.isManual
					? Filter.Entity.withoutSourceScope(query.raw, query.source_config)
					: query.raw,
				source_config: {
					operation_id: file.operation_id,
					source_ids: [file.id],
					range:
						range !== undefined
							? defaultSourceConfig.range
							: query.source_config?.range ?? defaultSourceConfig.range,
				},
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

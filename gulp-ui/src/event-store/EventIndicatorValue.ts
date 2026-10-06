import type { Doc } from "@/entities/Doc";

export const DEFAULT_EVENT_INDICATOR_FIELD = "gulp.event_code";
export const EVENT_INDICATOR_LABEL_LENGTH = 4;
export const EVENT_INDICATOR_VALUE_VERSION = 1;

/** One minimal event indicator: a complete default code or compact custom label. */
export type EventIndicatorValue = number | string;

/** Physical interpretation of the single exact-page indicator column. */
export type EventIndicatorLayout = "event-code" | "label";

/** Equal-width exact-page indicator storage selected by the source color field. */
export type EventIndicatorValues = Float64Array | Uint16Array;

/** Compact label and tooltip derived from one minimal event. */
export interface EventIndicatorPresentation {
	label: string;
	tooltip: string;
}

/**
 * Returns whether a raw field value can provide a meaningful compact label.
 * @param value Raw color-field value.
 * @returns True for non-empty scalar values.
 */
function isLabelScalar(value: unknown): value is string | number | bigint | boolean {
	return (
		(typeof value === "string" && value.length > 0) ||
		typeof value === "number" ||
		typeof value === "bigint" ||
		typeof value === "boolean"
	);
}

/**
 * Copies at most four UTF-16 code units without retaining the complete source value.
 * NUL and unpaired surrogate units are replaced so zero remains available as padding.
 * @param value Scalar value selected by source color settings.
 * @param eventCode Event code used when the selected value is empty or unsupported.
 * @returns A non-empty bounded label suitable for the fixed-width page column.
 */
export function createEventIndicatorLabel(
	value: unknown,
	eventCode: number,
): string {
	const source = isLabelScalar(value) ? String(value) : String(eventCode);
	const prefix = source.slice(0, EVENT_INDICATOR_LABEL_LENGTH);
	let requiresSanitization = false;
	for (let index = 0; index < prefix.length; index++) {
		const unit = prefix.charCodeAt(index);
		if (
			unit === 0 ||
			(unit >= 0xdc00 && unit <= 0xdfff) ||
			(unit >= 0xd800 &&
				unit <= 0xdbff &&
				(Number.isNaN(prefix.charCodeAt(index + 1)) ||
					prefix.charCodeAt(index + 1) < 0xdc00 ||
					prefix.charCodeAt(index + 1) > 0xdfff))
		) {
			requiresSanitization = true;
			break;
		}
		if (unit >= 0xd800 && unit <= 0xdbff) index++;
	}
	if (!requiresSanitization) return prefix;

	const units: number[] = [];
	for (let index = 0; index < prefix.length; index++) {
		const unit = prefix.charCodeAt(index);
		const nextUnit = prefix.charCodeAt(index + 1);
		if (
			unit >= 0xd800 &&
			unit <= 0xdbff &&
			nextUnit >= 0xdc00 &&
			nextUnit <= 0xdfff
		) {
			units.push(unit, nextUnit);
			index++;
		} else if (
			unit === 0 ||
			(unit >= 0xd800 && unit <= 0xdfff)
		) {
			units.push(0xfffd);
		} else {
			units.push(unit);
		}
	}
	return String.fromCharCode(...units);
}

/**
 * Creates the single minimal indicator value from source normalization settings.
 * @param value Raw value used for the color hash.
 * @param eventCode Normalized complete event code used by the default field or fallback.
 * @param field Source field selected for color calculation.
 * @returns Complete numeric event code or bounded custom label.
 */
export function createEventIndicatorValue(
	value: unknown,
	eventCode: number,
	field: string,
): EventIndicatorValue {
	return field === DEFAULT_EVENT_INDICATOR_FIELD
		? eventCode
		: createEventIndicatorLabel(value, eventCode);
}

/**
 * Resolves the minimal indicator value, with backend event-code fallback for full docs.
 * @param event Minimal EventStore document or complete backend document.
 * @returns A finite event code or compact label.
 */
export function resolveEventIndicatorValue(
	event: Pick<Doc.Type, "indicatorValue" | "gulp.event_code">,
): EventIndicatorValue {
	if (
		typeof event.indicatorValue === "string" ||
		Number.isFinite(event.indicatorValue)
	) {
		return event.indicatorValue as EventIndicatorValue;
	}
	const eventCode = Number(event["gulp.event_code"] ?? 0);
	return Number.isFinite(eventCode) ? eventCode : 0;
}

/**
 * Maps a source color field to the expected exact-page value representation.
 * @param field Source field selected for color calculation.
 * @returns Numeric default-code or compact-label layout.
 */
export function getEventIndicatorFieldLayout(
	field: string,
): EventIndicatorLayout {
	return field === DEFAULT_EVENT_INDICATOR_FIELD ? "event-code" : "label";
}

/**
 * Returns the physical layout required by one semantic indicator value.
 * @param value Complete numeric event code or compact custom label.
 * @returns Corresponding exact-page layout.
 */
export function getEventIndicatorValueLayout(
	value: EventIndicatorValue,
): EventIndicatorLayout {
	return typeof value === "string" ? "label" : "event-code";
}

/**
 * Returns the physical layout of the single exact-page indicator column.
 * @param values Typed indicator column.
 * @returns Corresponding exact-page layout.
 */
export function getEventIndicatorColumnLayout(
	values: EventIndicatorValues,
): EventIndicatorLayout {
	return values instanceof Uint16Array ? "label" : "event-code";
}

/**
 * Writes one label into its fixed four-cell page slot in constant time.
 * @param column Page-level UTF-16 label column.
 * @param eventOffset Zero-based event offset.
 * @param label Already bounded compact label.
 */
export function writeEventIndicatorLabel(
	column: Uint16Array,
	eventOffset: number,
	label: string,
): void {
	const start = eventOffset * EVENT_INDICATOR_LABEL_LENGTH;
	const first = label.charCodeAt(0);
	const second = label.charCodeAt(1);
	const third = label.charCodeAt(2);
	const fourth = label.charCodeAt(3);
	column[start] = first === 0 ? 0xfffd : first;
	column[start + 1] = second === 0 ? 0xfffd : second;
	column[start + 2] = third === 0 ? 0xfffd : third;
	column[start + 3] = fourth === 0 ? 0xfffd : fourth;
}

/**
 * Reads one compact label from its fixed page slot.
 * @param column Page-level UTF-16 label column.
 * @param eventOffset Zero-based event offset.
 * @returns Stored label without zero padding.
 */
export function readEventIndicatorLabel(
	column: Uint16Array,
	eventOffset: number,
): string {
	const start = eventOffset * EVENT_INDICATOR_LABEL_LENGTH;
	const units: number[] = [];
	for (let index = 0; index < EVENT_INDICATOR_LABEL_LENGTH; index++) {
		const unit = column[start + index];
		if (unit === 0) break;
		units.push(unit);
	}
	return String.fromCharCode(...units);
}

/**
 * Resolves the visible label and safe tooltip for one minimal event.
 * @param event Minimal or complete event document.
 * @returns Compact custom presentation or legacy event-code presentation.
 */
export function getEventIndicatorPresentation(
	event: Pick<Doc.Type, "indicatorValue" | "gulp.event_code">,
): EventIndicatorPresentation {
	const indicatorValue = resolveEventIndicatorValue(event);
	if (typeof indicatorValue === "string") {
		const label = createEventIndicatorLabel(
			indicatorValue,
			Number(event["gulp.event_code"] ?? 0),
		);
		return {
			label,
			tooltip: label,
		};
	}
	const eventCode = String(indicatorValue);
	return {
		label: eventCode.slice(0, EVENT_INDICATOR_LABEL_LENGTH),
		tooltip: eventCode,
	};
}

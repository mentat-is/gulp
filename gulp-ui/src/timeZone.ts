export const DEFAULT_TIMESTAMP_TIME_ZONE = "UTC";

type IntlWithSupportedValues = typeof Intl & {
	supportedValuesOf?: (key: "timeZone") => string[];
};

interface TimestampParts {
	yyyy: string;
	MM: string;
	dd: string;
	HH: string;
	mm: string;
	ss: string;
	SSS: string;
}

const timestampFormatterCache = new Map<string, Intl.DateTimeFormat>();
const TIMESTAMP_FORMATTER_CACHE_LIMIT = 16;

/**
 * Returns a cached formatter for the selected IANA time zone.
 * @param timeZone IANA time zone used for calendar projection.
 * @returns Deterministic numeric Gregorian formatter.
 */
function getTimestampFormatter(timeZone: string): Intl.DateTimeFormat {
	const cachedFormatter = timestampFormatterCache.get(timeZone);
	if (cachedFormatter) return cachedFormatter;
	const formatter = new Intl.DateTimeFormat("en-US-u-ca-gregory-nu-latn", {
		timeZone,
		year: "numeric",
		month: "2-digit",
		day: "2-digit",
		hour: "2-digit",
		minute: "2-digit",
		second: "2-digit",
		hourCycle: "h23",
	});
	if (timestampFormatterCache.size >= TIMESTAMP_FORMATTER_CACHE_LIMIT) {
		const oldestTimeZone = timestampFormatterCache.keys().next().value;
		if (oldestTimeZone) timestampFormatterCache.delete(oldestTimeZone);
	}
	timestampFormatterCache.set(timeZone, formatter);
	return formatter;
}

/**
 * Checks whether the runtime recognizes an IANA time-zone identifier.
 * @param timeZone Candidate IANA time-zone identifier.
 * @returns True when Intl can format dates in the requested time zone.
 */
export function isSupportedTimestampTimeZone(timeZone: string): boolean {
	try {
		new Intl.DateTimeFormat("en-US", { timeZone }).format(0);
		return true;
	} catch {
		return false;
	}
}

/**
 * Returns the runtime-supported IANA time zones with UTC pinned first.
 * @returns Ordered, unique time-zone identifiers suitable for the settings selector.
 */
export function getSupportedTimestampTimeZones(): string[] {
	const supportedValues = (Intl as IntlWithSupportedValues).supportedValuesOf;
	const runtimeTimeZones = supportedValues?.call(Intl, "timeZone") ?? [];
	const browserTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;
	const browserTimeZones =
		browserTimeZone && isSupportedTimestampTimeZone(browserTimeZone)
			? [browserTimeZone]
			: [];
	const timeZones = [
		DEFAULT_TIMESTAMP_TIME_ZONE,
		...runtimeTimeZones,
		...browserTimeZones,
	];
	return Array.from(new Set(timeZones));
}

/**
 * Resolves numeric timestamp fields in a deterministic Gregorian representation.
 * @param date Valid date whose fields should be formatted.
 * @param timeZone IANA time zone used to project the instant.
 * @returns Token values used by Canvas and pointer timestamp layouts.
 */
function getTimestampParts(date: Date, timeZone: string): TimestampParts {
	const parts = getTimestampFormatter(timeZone).formatToParts(date);
	const valueByType = new Map(parts.map((part) => [part.type, part.value]));
	return {
		yyyy: valueByType.get("year") ?? "",
		MM: valueByType.get("month") ?? "",
		dd: valueByType.get("day") ?? "",
		HH: valueByType.get("hour") ?? "",
		mm: valueByType.get("minute") ?? "",
		ss: valueByType.get("second") ?? "",
		SSS: date.getUTCMilliseconds().toString().padStart(3, "0"),
	};
}

/**
 * Formats one absolute timestamp with the subset of date-fns tokens used by Canvas.
 * @param value Date-compatible event timestamp.
 * @param pattern Output pattern containing yyyy, MM, dd, HH, mm, ss, or SSS.
 * @param timeZone IANA time zone used for calendar projection.
 * @returns Formatted timestamp, or an empty string for invalid input or time zones.
 */
export function formatTimestampInTimeZone(
	value: Date | number | string,
	pattern: string,
	timeZone: string,
): string {
	const date = new Date(value);
	if (Number.isNaN(date.valueOf())) return "";

	try {
		const timestampParts = getTimestampParts(date, timeZone);
		return pattern.replace(
			/yyyy|SSS|MM|dd|HH|mm|ss/g,
			(token) => timestampParts[token as keyof TimestampParts],
		);
	} catch {
		return "";
	}
}

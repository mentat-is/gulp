import {
	DEFAULT_TIMESTAMP_TIME_ZONE,
	formatTimestampInTimeZone,
	getSupportedTimestampTimeZones,
	isSupportedTimestampTimeZone,
} from "@/timeZone";

describe("timestamp time zones", () => {
	const timestamp = "2025-03-26T09:24:39.186065+00:00";

	it("formats one @timestamp instant consistently in selected IANA zones", () => {
		expect(
			formatTimestampInTimeZone(
				timestamp,
				"yyyy.MM.dd HH:mm:ss SSS",
				"UTC",
			),
		).toBe("2025.03.26 09:24:39 186");
		expect(
			formatTimestampInTimeZone(
				timestamp,
				"yyyy.MM.dd HH:mm:ss SSS",
				"America/New_York",
			),
		).toBe("2025.03.26 05:24:39 186");
		expect(
			formatTimestampInTimeZone(
				timestamp,
				"yyyy.MM.dd HH:mm:ss SSS",
				"Asia/Tokyo",
			),
		).toBe("2025.03.26 18:24:39 186");
	});

	it("rejects invalid dates and time zones", () => {
		expect(formatTimestampInTimeZone("invalid", "yyyy", "UTC")).toBe("");
		expect(formatTimestampInTimeZone(timestamp, "yyyy", "Mars/Base")).toBe("");
		expect(isSupportedTimestampTimeZone("Mars/Base")).toBe(false);
	});

	it("pins UTC first in the supported selector values", () => {
		const timeZones = getSupportedTimestampTimeZones();
		expect(timeZones[0]).toBe(DEFAULT_TIMESTAMP_TIME_ZONE);
		expect(new Set(timeZones).size).toBe(timeZones.length);
		expect(timeZones.every(isSupportedTimestampTimeZone)).toBe(true);
	});
});

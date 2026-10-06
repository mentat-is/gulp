import type { Doc } from "@/entities/Doc";
import {
	createEventIndicatorLabel,
	createEventIndicatorValue,
	getEventIndicatorPresentation,
	readEventIndicatorLabel,
	writeEventIndicatorLabel,
} from "@/event-store/EventIndicatorValue";

describe("event indicator values", () => {
	it("bounds scalar values and sanitizes NUL and incomplete surrogate units", () => {
		expect(createEventIndicatorLabel("abcdef", 7)).toBe("abcd");
		expect(createEventIndicatorLabel(123456, 7)).toBe("1234");
		expect(createEventIndicatorLabel("A😀BC", 7)).toBe("A😀B");
		expect(createEventIndicatorLabel("A\0BC", 7)).toBe("A�BC");
		expect(createEventIndicatorLabel("abc😀", 7)).toBe("abc�");
	});

	it("falls back for empty, missing, and non-scalar values", () => {
		for (const value of ["", undefined, null, { value: "hidden" }, ["hidden"]]) {
			expect(createEventIndicatorLabel(value, 987654)).toBe("9876");
		}
	});

	it("uses one number-or-string value selected by the color field", () => {
		expect(createEventIndicatorValue("ignored", 987654, "gulp.event_code")).toBe(
			987654,
		);
		expect(createEventIndicatorValue("network", 987654, "category")).toBe(
			"netw",
		);
	});

	it("writes and overwrites exactly four UTF-16 cells", () => {
		const column = new Uint16Array(8);
		writeEventIndicatorLabel(column, 1, "A😀B");
		expect(readEventIndicatorLabel(column, 1)).toBe("A😀B");
		writeEventIndicatorLabel(column, 1, "xy");
		expect(readEventIndicatorLabel(column, 1)).toBe("xy");
		expect(column.byteLength).toBe(16);
	});

	it("uses compact custom tooltips while preserving full default event codes", () => {
		const defaultEvent = {
			indicatorValue: 123456,
		} as Pick<Doc.Type, "indicatorValue" | "gulp.event_code">;
		const customEvent = {
			indicatorValue: "cust",
		} as Pick<Doc.Type, "indicatorValue" | "gulp.event_code">;

		expect(getEventIndicatorPresentation(defaultEvent)).toEqual({
			label: "1234",
			tooltip: "123456",
		});
		expect(getEventIndicatorPresentation(customEvent)).toEqual({
			label: "cust",
			tooltip: "cust",
		});
		expect(
			getEventIndicatorPresentation({ "gulp.event_code": 987654 }),
		).toEqual({
			label: "9876",
			tooltip: "987654",
		});
	});
});

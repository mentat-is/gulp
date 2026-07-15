import {
	VIEWPORT_QUERY_SETTLE_DELAY_MS,
	ViewportQueryGate,
} from "@/class/ViewportQueryGate";

describe("ViewportQueryGate", () => {
	it("opens only after the viewport remains unchanged for the settle delay", () => {
		const gate = new ViewportQueryGate();
		gate.markViewportChanged(1_000);

		expect(gate.getRemainingDelay(1_000)).toBe(
			VIEWPORT_QUERY_SETTLE_DELAY_MS,
		);
		expect(gate.getRemainingDelay(1_179)).toBe(1);
		expect(gate.getRemainingDelay(1_180)).toBe(0);
	});

	it("restarts the delay when scrolling reaches another source row set", () => {
		const gate = new ViewportQueryGate(180);
		gate.markViewportChanged(1_000);
		gate.markViewportChanged(1_120);

		expect(gate.getRemainingDelay(1_180)).toBe(120);
		expect(gate.getRemainingDelay(1_300)).toBe(0);
	});
});

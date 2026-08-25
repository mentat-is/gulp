import { Algorhithm } from "@/class/TimelineAlgorithm";

describe("Algorhithm viewport centering", () => {
	it("centers against the visible viewport when the Canvas world is wider", () => {
		const algorithm = new Algorhithm({
			frame: { min: 0, max: 1_000 },
			scroll: { x: 0, y: 0 },
			width: 2_000,
			scale: 2,
			viewportWidth: 600,
		});

		expect(algorithm.center_scroll_from_timestamp(500)).toBe(700);
		expect(algorithm.abs_x_from_timestamp(500)).toBe(1_000);
	});

	it("preserves the legacy world-width fallback for non-clipped consumers", () => {
		const algorithm = new Algorhithm({
			frame: { min: 0, max: 1_000 },
			scroll: { x: 0, y: 0 },
			width: 2_000,
			scale: 2,
		});

		expect(algorithm.center_scroll_from_timestamp(500)).toBe(500);
	});
});

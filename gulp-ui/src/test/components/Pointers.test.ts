import {
	applyLocalPointerSnapshot,
	applyRemotePointerOrientations,
	applyRemotePointerScrollX,
	applyRemotePointerScrollY,
	resolveRemotePointerHorizontalPosition,
} from "@/components/PointerPosition.store";

describe("imperative local pointer updates", () => {
	it("updates position, orientation, and timestamp label on existing DOM nodes", () => {
		const pointerElement = document.createElement("div");
		const labelElement = document.createElement("p");

		applyLocalPointerSnapshot(
			pointerElement,
			labelElement,
			{ x: 12, y: 34, timestamp: 100, isRightSide: true },
			"You on 100ms",
			"right",
		);

		expect(pointerElement.style.transform).toBe(
			"translate3d(12px, 34px, 0)",
		);
		expect(pointerElement.classList.contains("right")).toBe(true);
		expect(labelElement.textContent).toBe("You on 100ms");

		applyLocalPointerSnapshot(
			pointerElement,
			labelElement,
			{ x: 20, y: 40, timestamp: 101, isRightSide: false },
			"You on 101ms",
			"right",
		);

		expect(pointerElement.style.transform).toBe(
			"translate3d(20px, 40px, 0)",
		);
		expect(pointerElement.classList.contains("right")).toBe(false);
		expect(labelElement.textContent).toBe("You on 101ms");
	});

	it("scrolls timestamp-derived X and Y through compositor layers", () => {
		const horizontalLayer = document.createElement("div");
		const verticalLayer = document.createElement("div");
		const getPixelPosition = (timestamp: number): number => timestamp / 2;

		expect(
			resolveRemotePointerHorizontalPosition(
				undefined,
				200,
				getPixelPosition,
				25,
			),
		).toEqual({
			positionX: 125,
			viewportX: 100,
			followsTimelineScroll: true,
		});

		applyRemotePointerScrollX(horizontalLayer, 25);
		applyRemotePointerScrollY(verticalLayer, 30);
		expect(
			horizontalLayer.style.getPropertyValue("--remote-pointer-scroll-x"),
		).toBe("-25px");
		expect(
			verticalLayer.style.getPropertyValue("--remote-pointer-scroll-y"),
		).toBe("-30px");
	});

	it("flips timestamp pointer direction when scroll crosses the midpoint", () => {
		const timelineLayer = document.createElement("div");
		const timestampPointer = document.createElement("div");
		timestampPointer.dataset.remotePointerDocumentX = "180";
		timelineLayer.append(timestampPointer);

		applyRemotePointerOrientations(timelineLayer, 50, 200, "right");
		expect(timestampPointer.classList.contains("right")).toBe(true);

		applyRemotePointerOrientations(timelineLayer, 100, 200, "right");
		expect(timestampPointer.classList.contains("right")).toBe(false);
	});

	it("treats explicit X zero as viewport-fixed and excludes it from orientation updates", () => {
		const explicitPosition = resolveRemotePointerHorizontalPosition(
			0,
			200,
			(timestamp) => timestamp / 2,
			25,
		);
		const timelineLayer = document.createElement("div");
		const explicitPointer = document.createElement("div");
		explicitPointer.classList.add("right");
		timelineLayer.append(explicitPointer);

		applyRemotePointerOrientations(timelineLayer, 100, 200, "right");

		expect(explicitPosition).toEqual({
			positionX: 0,
			viewportX: 0,
			followsTimelineScroll: false,
		});
		expect(explicitPointer.classList.contains("right")).toBe(true);
	});
});

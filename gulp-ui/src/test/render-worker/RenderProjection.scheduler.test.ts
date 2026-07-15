import { RenderProjectionScheduler } from "@/render-worker/RenderProjection.scheduler";
import type { Source } from "@/entities/Source";

describe("RenderProjectionScheduler", () => {
	it("runs a visible source before older background work", async () => {
		const scheduler = new RenderProjectionScheduler();
		const order: string[] = [];
		let releaseBlocker!: () => void;
		const blocker = new Promise<void>((resolve) => {
			releaseBlocker = resolve;
		});
		scheduler.setViewportSourceIds(["visible" as Source.Id]);
		scheduler.enqueue({
			sourceId: "active" as Source.Id,
			replacesSource: false,
			run: async () => blocker,
		});
		scheduler.enqueue({
			sourceId: "background" as Source.Id,
			replacesSource: false,
			run: async () => {
				order.push("background");
			},
		});
		scheduler.enqueue({
			sourceId: "visible" as Source.Id,
			replacesSource: false,
			run: async () => {
				order.push("visible");
			},
		});

		releaseBlocker();
		await new Promise((resolve) => setTimeout(resolve, 0));
		expect(order).toEqual(["visible", "background"]);
		scheduler.dispose();
	});

	it("drops queued deltas behind a newer source replacement", () => {
		const scheduler = new RenderProjectionScheduler();
		scheduler.enqueue({
			sourceId: "source" as Source.Id,
			replacesSource: false,
			run: async () => undefined,
		});
		scheduler.enqueue({
			sourceId: "source" as Source.Id,
			replacesSource: true,
			run: async () => undefined,
		});

		expect(scheduler.getMetrics().visibleQueueDepth).toBe(1);
		scheduler.dispose();
	});

	it("cancels an active stale delta before a replacement can publish", async () => {
		const scheduler = new RenderProjectionScheduler();
		const publishedRevisions: number[] = [];
		let releaseActiveJob!: () => void;
		const activeJobBlocker = new Promise<void>((resolve) => {
			releaseActiveJob = resolve;
		});
		const sourceId = "source" as Source.Id;

		scheduler.enqueue({
			sourceId,
			replacesSource: false,
			run: async (context) => {
				await activeJobBlocker;
				if (!context.isCancelled()) publishedRevisions.push(1);
			},
		});
		expect(scheduler.hasSourceWork(sourceId)).toBe(true);
		scheduler.enqueue({
			sourceId,
			replacesSource: true,
			run: async (context) => {
				if (!context.isCancelled()) publishedRevisions.push(2);
			},
		});

		releaseActiveJob();
		await new Promise((resolve) => setTimeout(resolve, 0));

		expect(publishedRevisions).toEqual([2]);
		expect(scheduler.hasSourceWork(sourceId)).toBe(false);
		scheduler.dispose();
	});

	it("retains visibility priority after collapsing queued source work", async () => {
		const scheduler = new RenderProjectionScheduler();
		const order: string[] = [];
		let releaseBlocker!: () => void;
		const blocker = new Promise<void>((resolve) => {
			releaseBlocker = resolve;
		});
		const visibleSourceId = "visible" as Source.Id;
		scheduler.setViewportSourceIds([visibleSourceId]);
		scheduler.enqueue({
			sourceId: "active" as Source.Id,
			replacesSource: false,
			run: async () => blocker,
		});
		scheduler.enqueue({
			sourceId: "background" as Source.Id,
			replacesSource: false,
			run: async () => {
				order.push("background");
			},
		});
		scheduler.enqueue({
			sourceId: visibleSourceId,
			replacesSource: false,
			run: async () => {
				order.push("stale-visible");
			},
		});
		scheduler.enqueue({
			sourceId: visibleSourceId,
			replacesSource: true,
			run: async () => {
				order.push("replacement-visible");
			},
		});

		releaseBlocker();
		await new Promise((resolve) => setTimeout(resolve, 0));

		expect(order).toEqual(["replacement-visible", "background"]);
		scheduler.dispose();
	});

	it("retains small delta bursts and collapses only at the bounded threshold", async () => {
		const scheduler = new RenderProjectionScheduler();
		const sourceId = "source" as Source.Id;
		const published: string[] = [];
		let releaseActive!: () => void;
		const activeBlocker = new Promise<void>((resolve) => {
			releaseActive = resolve;
		});
		scheduler.enqueue({
			sourceId,
			replacesSource: false,
			run: async (context) => {
				await activeBlocker;
				if (!context.isCancelled()) published.push("active-delta");
			},
		});
		expect(scheduler.getSourceWorkDepth(sourceId)).toBe(1);

		for (let index = 0; index < 3; index++) {
			scheduler.enqueue({
				sourceId,
				replacesSource: false,
				run: async () => {
					published.push(`queued-delta-${index}`);
				},
			});
			expect(scheduler.getSourceWorkDepth(sourceId)).toBe(index + 2);
		}
		expect(scheduler.getMetrics().visibleQueueDepth).toBe(3);

		if (scheduler.getSourceWorkDepth(sourceId) >= 4) {
			scheduler.enqueue({
				sourceId,
				replacesSource: true,
				run: async () => {
					published.push("stale-snapshot");
				},
			});
		}
		expect(scheduler.hasSourceReplacementWork(sourceId)).toBe(true);
		if (scheduler.hasSourceReplacementWork(sourceId)) {
			scheduler.enqueue({
				sourceId,
				replacesSource: true,
				run: async () => {
					published.push("latest-snapshot");
				},
			});
		}
		releaseActive();
		await new Promise((resolve) => setTimeout(resolve, 0));

		expect(published).toEqual(["latest-snapshot"]);
		scheduler.dispose();
	});
});

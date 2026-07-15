/**
 * Creates the production timeline render worker from its bundler-resolved module URL.
 * @returns Dedicated module worker that owns one transferred timeline canvas.
 */
export function createDefaultRenderWorker(): Worker {
	return new Worker(new URL("./Render.worker", import.meta.url), {
		type: "module",
		name: "timeline-renderer",
	});
}

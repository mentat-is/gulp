/** Creates the dedicated synthetic-socket worker without a main-thread fallback. */
export function createRendererQueryReplayWorker(): Worker {
	return new Worker(new URL("./RendererQueryReplay.worker", import.meta.url), {
		type: "module",
	});
}

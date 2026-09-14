/**
 * Creates one module worker for chunked file uploads.
 * @returns Browser worker ready to accept ingestion tasks.
 */
export function createIngestWorker(): Worker {
	return new Worker(new URL("./Ingest.worker", import.meta.url), {
		type: "module",
	});
}

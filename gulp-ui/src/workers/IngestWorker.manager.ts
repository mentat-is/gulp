import { Request } from '@/entities/Request';
import { createIngestWorker } from './IngestWorker.factory';

export const INGEST_WORKER_IDLE_TIMEOUT_MS = 5_000;

type IngestSettings = {
  plugin?: string;
  offset?: number;
  custom_parameters?: unknown;
  override_chunk_size?: unknown;
  override_allow_unmapped_fields?: unknown;
  method?: string;
  mapping?: string;
  additional_mapping_files?: unknown;
  mappings?: unknown;
  additional_mappings?: unknown;
  sigma_mappings?: unknown;
  store_file?: boolean;
};

export type IngestTask = {
  req_id: Request.Id;
  file: File;
  operation_id: string;
  context_name: string;
  ws_id: string;
  settings: IngestSettings;
  server: string;
  token: string;
  frame?: { min: number; max: number };
  onProgress?: (progress: number, bytes: number) => void;
  onDone?: () => void;
  onError?: (error: string) => void;
  /** Keeps the slot occupied until the corresponding ingestion is terminal. */
  releaseWhen?: Promise<void>;
};

export class IngestWorkerManager {
  private workers: Worker[] = [];
  private taskQueue: IngestTask[] = [];
  private activeTaskCount = 0;
  private readonly MAX_WORKERS = 5;
  private idleCleanupTimer: ReturnType<typeof setTimeout> | null = null;
  private terminalTaskReleases = new Map<string, () => void>();
  private taskHandlers = new Map<string, {
    onProgress?: (progress: number, bytes: number) => void,
    onDone?: () => void,
    onError?: (error: string) => void,
    releaseWhen?: Promise<void>,
    uploadComplete?: boolean
  }>();

  /**
   * Creates a bounded upload-worker pool.
   *
   * @param workerFactory Factory used to create browser workers or deterministic test doubles.
   * @param idleTimeoutMs Grace period before an empty worker pool is terminated.
   */
  constructor(
    private readonly workerFactory: () => Worker = createIngestWorker,
    private readonly idleTimeoutMs = INGEST_WORKER_IDLE_TIMEOUT_MS,
  ) {}

  /**
   * Creates and wires one upload worker.
   * @returns Worker registered with the manager message handler.
   */
  private createWorker(): Worker {
    const worker = this.workerFactory();
    worker.onmessage = (event) => {
      const { type, payload } = event.data;
      const handlers = this.taskHandlers.get(payload.req_id);

      if (type === 'PROGRESS' && handlers?.onProgress) {
        handlers.onProgress(payload.progress, payload.bytes);
      } else if (type === 'DONE') {
        if (handlers?.uploadComplete) return;
        if (handlers) handlers.uploadComplete = true;
        try {
          handlers?.onDone?.();
        } catch (error) {
          this.finishTask(worker, payload.req_id);
          throw error;
        }
        this.waitForTerminalRelease(worker, payload.req_id, handlers?.releaseWhen);
      } else if (type === 'ERROR') {
        try {
          handlers?.onError?.(payload.message);
        } finally {
          this.finishTask(worker, payload.req_id);
        }
      }
    };
    return worker;
  }

  /**
   * Queues one file upload and keeps the idle pool alive for the active batch.
   * @param task Upload task and lifecycle callbacks.
   * @returns Nothing.
   */
  public enqueue(task: IngestTask): void {
    this.clearIdleCleanupTimer();
    this.taskHandlers.set(task.req_id, {
      onProgress: task.onProgress,
      onDone: task.onDone,
      onError: task.onError,
      releaseWhen: task.releaseWhen
    });
    this.taskQueue.push(task);
    this.processQueue();
  }

  /**
   * Dispatches queued uploads without exceeding the worker ceiling.
   * @returns Nothing.
   */
  private processQueue(): void {
    while (this.activeTaskCount < this.MAX_WORKERS && this.taskQueue.length > 0) {
      const task = this.taskQueue.shift()!;
      this.runTask(task);
    }
  }

  /**
   * Starts a worker with a serialized ingestion task payload.
   *
   * @param task - Queued ingestion task to execute.
   * @returns Nothing.
   */
  private runTask(task: IngestTask): void {
    this.activeTaskCount++;
    const worker = this.workers.pop() || this.createWorker();

    worker.postMessage({
      type: 'START_INGEST',
      payload: {
        file: task.file,
        req_id: task.req_id,
        operation_id: task.operation_id,
        context_name: task.context_name,
        ws_id: task.ws_id,
        settings: JSON.parse(JSON.stringify(task.settings, (_, v) => typeof v === 'function' ? undefined : v)),
        server: task.server,
        token: task.token,
        frame: task.frame,
      }
    });
  }

  /**
   * Releases one completed slot and schedules idle worker disposal when appropriate.
   * @param worker Worker that completed its current upload.
   * @param req_id Completed request identifier.
   * @returns Nothing.
   */
  private finishTask(worker: Worker, req_id: string): void {
    this.activeTaskCount = Math.max(0, this.activeTaskCount - 1);
    this.terminalTaskReleases.delete(req_id);
    this.workers.push(worker);
    this.taskHandlers.delete(req_id);
    this.processQueue();
    this.scheduleIdleCleanup();
  }

  /**
   * Removes one queued upload or detaches callbacks from an active upload.
   * @param req_id Request identifier to detach.
   * @returns Nothing.
   */
  public abort(req_id: string): void {
    // If task is in queue, remove it
    const queueIndex = this.taskQueue.findIndex(t => t.req_id === req_id);
    if (queueIndex !== -1) {
      this.taskQueue.splice(queueIndex, 1);
      this.taskHandlers.delete(req_id);
      this.scheduleIdleCleanup();
      return;
    }

    const releaseTerminalTask = this.terminalTaskReleases.get(req_id);
    if (releaseTerminalTask) {
      releaseTerminalTask();
      return;
    }

    // If task is active, we currently don't have a clean way to "stop" the specific worker 
    // without terminating it, which is expensive. 
    // Better to just let the worker finish its current chunk and signal the worker to stop.
    // For now, we just delete the handlers so the main thread ignores future messages.
    this.taskHandlers.delete(req_id);
  }

  /**
   * Holds an uploaded file's worker slot until its backend ingestion is terminal.
   * @param worker Worker that completed the HTTP upload.
   * @param req_id Uploaded request identifier.
   * @param releaseWhen Optional ingestion lifecycle barrier.
   * @returns Nothing.
   */
  private waitForTerminalRelease(
    worker: Worker,
    req_id: string,
    releaseWhen?: Promise<void>,
  ): void {
    if (!releaseWhen) {
      this.finishTask(worker, req_id);
      return;
    }
    let released = false;
    const release = (): void => {
      if (released) return;
      released = true;
      this.finishTask(worker, req_id);
    };
    this.terminalTaskReleases.set(req_id, release);
    void releaseWhen.then(release, release);
  }

  /**
   * Cancels a pending idle-pool disposal when a new batch arrives.
   * @returns Nothing.
   */
  private clearIdleCleanupTimer(): void {
    if (this.idleCleanupTimer === null) return;
    clearTimeout(this.idleCleanupTimer);
    this.idleCleanupTimer = null;
  }

  /**
   * Terminates every pooled worker after the manager remains fully idle.
   * @returns Nothing.
   */
  private scheduleIdleCleanup(): void {
    if (
      this.idleCleanupTimer !== null ||
      this.activeTaskCount !== 0 ||
      this.taskQueue.length !== 0 ||
      this.workers.length === 0
    ) {
      return;
    }
    this.idleCleanupTimer = setTimeout(() => {
      this.idleCleanupTimer = null;
      if (this.activeTaskCount !== 0 || this.taskQueue.length !== 0) return;
      for (const worker of this.workers.splice(0)) {
        worker.onmessage = null;
        worker.terminate();
      }
    }, this.idleTimeoutMs);
  }
}

export const ingestWorkerManager = new IngestWorkerManager();

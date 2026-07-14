import { generateUUID } from '@/ui/utils';
import type {
	BinarySearchDescPayload,
	DataWorkerAction,
	DataWorkerPayloadMap,
	DataWorkerResponse,
	DataWorkerResultMap,
	NormalizeDashboardAggregationPayload,
	NormalizeDashboardAggregationResult,
	NormalizeQueryDocsPayload,
	NormalizeQueryDocsResult,
	TimestampedWorkerItem,
} from './DataWorker.types';

interface WorkerPromiseHandlers {
	resolve: (value: unknown) => void;
	reject: (reason?: unknown) => void;
}

let workerMap = new Map<string, WorkerPromiseHandlers>();

const isBrowser = typeof window !== 'undefined';
let workerInstance: Worker | null = null;

if (isBrowser) {
  workerInstance = new Worker(new URL('./Data.worker', import.meta.url), { type: 'module' });

  workerInstance.onmessage = (event: MessageEvent<DataWorkerResponse>) => {
    const { id, result, error } = event.data;

    const promiseHandlers = workerMap.get(id);
    if (promiseHandlers) {
      if (error) {
        promiseHandlers.reject(new Error(error));
      } else {
        promiseHandlers.resolve(result);
      }
      workerMap.delete(id);
    }
  };
}

export class DataWorker {
  /**
   * Sends a typed action to the shared data worker.
   * @param type Worker action to execute.
   * @param payload Payload matching the worker action.
   * @returns Promise resolving with the typed worker result.
   */
  static async execute<TAction extends DataWorkerAction>(
    type: TAction,
    payload: DataWorkerPayloadMap[TAction],
  ): Promise<DataWorkerResultMap[TAction]> {
    if (!workerInstance) {
      throw new Error('Worker not initialized in correct environment.');
    }

    const id = generateUUID() as string;
    
    return new Promise<DataWorkerResultMap[TAction]>((resolve, reject) => {
      workerMap.set(id, {
        resolve: (value: unknown) => resolve(value as DataWorkerResultMap[TAction]),
        reject,
      });
      workerInstance!.postMessage({
        type,
        payload,
        id 
      });
    });
  }

  /**
   * Performs a descending timestamp binary search in the worker.
   * @param items Timestamped items to search.
   * @param timestamp Timestamp boundary to locate.
   * @param findFirst Whether to find the first matching boundary.
   * @returns Matching index, or -1 when no item matches.
   */
  static binarySearchDesc(
    items: TimestampedWorkerItem[],
    timestamp: number,
    findFirst: boolean,
  ): Promise<number> {
    const payload: BinarySearchDescPayload = { items, timestamp, findFirst };
    return this.execute('BINARY_SEARCH_DESC', payload);
  }

  /**
   * Sorts timestamped events in descending order in the worker.
   * @param events Timestamped events to sort.
   * @returns Sorted event array.
   */
  static sortEventsAsync<TEvent extends TimestampedWorkerItem>(
    events: TEvent[],
  ): Promise<TEvent[]> {
    return this.execute('SORT_EVENTS', events) as Promise<TEvent[]>;
  }

  /**
   * Normalizes raw query documents into grouped, sorted document batches.
   * @param payload Raw documents plus source-specific normalization settings.
   * @returns Prepared batches grouped by source id.
   */
  static normalizeQueryDocs(
    payload: NormalizeQueryDocsPayload,
  ): Promise<NormalizeQueryDocsResult> {
    return this.execute('NORMALIZE_QUERY_DOCS', payload);
  }

  /**
   * Normalizes dashboard aggregation responses into render-ready chart or table data.
   * @param payload Aggregation response and target dashboard display mode.
   * @returns Processed dashboard aggregation result.
   */
  static normalizeDashboardAggregation(
    payload: NormalizeDashboardAggregationPayload,
  ): Promise<NormalizeDashboardAggregationResult> {
    return this.execute('NORMALIZE_DASHBOARD_AGGREGATION', payload);
  }
}

/// <reference lib="webworker" />

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

type IngestMessage = {
  type: 'START_INGEST';
  payload: {
    file: File;
    req_id: string;
    operation_id: string;
    context_name: string;
    ws_id: string;
    settings: IngestSettings;
    server: string;
    token: string;
    frame?: { min: number; max: number };
    chunkSize?: number;
  };
};

const DEFAULT_CHUNK_SIZE = 1024 * 1024 * 1;  // 256kb

self.onmessage = async (event: MessageEvent<IngestMessage>) => {
  const { type, payload } = event.data;

  if (type !== 'START_INGEST') return;

  const { file, req_id, operation_id, context_name, ws_id, settings, server, token, frame, chunkSize = DEFAULT_CHUNK_SIZE } = payload;

  try {
    const ingest = async (start = 0, current_req_id?: string): Promise<void> => {
      const end = Math.min(file.size, start + chunkSize);
      const formData = new FormData();

      const ingestPayload: Record<string, unknown> = {
        original_file_path: file.name,
        offset: settings.offset ?? 0,
      };

      const pluginParams: Record<string, unknown> = {};
      if (settings.custom_parameters) pluginParams.custom_parameters = settings.custom_parameters;
      if (settings.override_chunk_size) pluginParams.override_chunk_size = settings.override_chunk_size;
      if (settings.override_allow_unmapped_fields) pluginParams.override_allow_unmapped_fields = settings.override_allow_unmapped_fields;

      const mappingParameters: Record<string, unknown> = {};
      if (settings.method) mappingParameters.mapping_file = settings.method;
      if (settings.mapping) mappingParameters.mapping_id = settings.mapping;
      if (settings.additional_mapping_files) mappingParameters.additional_mapping_files = settings.additional_mapping_files;
      if (settings.mappings) mappingParameters.mappings = settings.mappings;
      if (settings.additional_mappings) mappingParameters.additional_mappings = settings.additional_mappings;
      if (settings.sigma_mappings) mappingParameters.sigma_mappings = settings.sigma_mappings;

      if (Object.keys(mappingParameters).length > 0) {
        pluginParams.mapping_parameters = mappingParameters;
      }

      if (settings.store_file !== undefined) pluginParams.store_file = settings.store_file;
      if (Object.keys(pluginParams).length > 0) ingestPayload.plugin_params = pluginParams;

      if (frame) {
        ingestPayload.flt = { int_filter: [frame.min, frame.max] };
      }

      formData.append("payload", new Blob([JSON.stringify(ingestPayload)], { type: "application/json" }));
      formData.append("f", file.slice(start, end), file.name);

      if (!settings.plugin) {
        throw new Error("Missing ingestion plugin");
      }

      const query = new URLSearchParams({
        plugin: settings.plugin.split(".")[0],
        operation_id,
        context_name,
        ws_id,
        req_id: current_req_id || req_id,
      });

      const response = await fetch(`${server}/ingest_file?${query}`, {
        method: "POST",
        body: formData,
        headers: {
          token,
          size: file.size.toString(),
          continue_offset: start.toString(),
        },
      });

      if (!response.ok) {
        throw new Error(`Ingestion failed with status ${response.status}`);
      }

      const data = await response.json();
      const nextOffset = data?.data?.continue_offset;

      // Report progress
      const progress = Math.round((end / file.size) * 100);
      self.postMessage({ type: 'PROGRESS', payload: { req_id, progress, bytes: end, total: file.size } });

      if (nextOffset !== undefined && nextOffset !== null && end < file.size) {
        return ingest(nextOffset, data.req_id || current_req_id || req_id);
      }
    };

    await ingest();
    self.postMessage({ type: 'DONE', payload: { req_id } });

  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    self.postMessage({ type: 'ERROR', payload: { req_id, message } });
  }
};

interface UploadFileSettingsValidation {
	plugin?: string;
	method?: string;
}

export const NO_METHOD_SELECTED = "__no_method_selected__";
export const NO_MAPPING_SELECTED = "__no_mapping_selected__";

/**
 * Converts the mapping-file selector's clear option into an unset method.
 *
 * @param selectedMethod Value emitted by the mapping-file selector.
 * @returns The selected mapping file, or undefined when selection was cleared.
 */
export const resolveOptionalMethod = (
	selectedMethod: string,
): string | undefined =>
	selectedMethod === NO_METHOD_SELECTED ? undefined : selectedMethod;

/**
 * Converts the mapping selector's clear option into an unset mapping.
 *
 * @param selectedMapping Value emitted by the mapping selector.
 * @returns The selected mapping name, or undefined when selection was cleared.
 */
export const resolveOptionalMapping = (
	selectedMapping: string,
): string | undefined =>
	selectedMapping === NO_MAPPING_SELECTED ? undefined : selectedMapping;

/**
 * Checks whether a file has the plugin configuration required for ingestion.
 * Mapping files are optional because ingestion plugins may support their own
 * default mapping behavior.
 *
 * @param settings Ingestion settings configured for one file.
 * @returns Whether the file can be submitted for ingestion.
 */
export const isUploadFileSettingsValid = (
	settings: UploadFileSettingsValidation,
): boolean => !!settings.plugin;

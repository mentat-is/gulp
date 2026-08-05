const FILE_SIGNATURES: Record<string, Uint8Array[]> = {
	"win_evtx.py": [new Uint8Array([0x45, 0x6c, 0x66, 0x46, 0x69, 0x6c, 0x65])],
	"systemd_journal.py": [
		new Uint8Array([0x4c, 0x50, 0x4b, 0x53, 0x48, 0x48, 0x52, 0x48]),
	],
	"sqlite.py": [
		new Uint8Array([
			0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6f, 0x72,
		]),
	],
	"pcap.py": [
		new Uint8Array([0x0a, 0x0d, 0x0d, 0x0a]),
		new Uint8Array([0xa1, 0xb2, 0xc3, 0xd4]),
		new Uint8Array([0xd4, 0xc3, 0xb2, 0xa1]),
		new Uint8Array([0xa1, 0xb2, 0x3c, 0x4d]),
		new Uint8Array([0x4d, 0x3c, 0xb2, 0xa1]),
	],
	"win_reg.py": [new Uint8Array([0x72, 0x65, 0x67, 0x66])],
	"win_pe.py": [new Uint8Array([0x4d, 0x5a])],
	"zip.py": [
		new Uint8Array([0x50, 0x4b, 0x05, 0x06]),
		new Uint8Array([0x50, 0x4b, 0x07, 0x08]),
		new Uint8Array([0x50, 0x4b, 0x03, 0x04]),
	],
	"eml.py": [
		new Uint8Array([0x52, 0x65, 0x63, 0x65, 0x69, 0x76, 0x65, 0x64, 0x3A])]
	};

const FILE_EXTENSION_PLUGINS: Record<string, string> = {
	csv: "csv.py",
	eml: "eml.py",
	evtx: "win_evtx.py",
	journal: "systemd_journal.py",
	json: "json.py",
	mbox: "mbox.py",
	pcap: "pcap.py",
	pcapng: "pcap.py",
	sqlite: "sqlite.py",
	sqlite3: "sqlite.py",
	zip: "zip.py",
};

export const MAX_FILE_SIGNATURE_LENGTH = Math.max(
	...Object.values(FILE_SIGNATURES).flatMap((signatures) =>
		signatures.map((signature) => signature.length),
	),
);

const pluginBasename = (plugin: string): string =>
	plugin.toLowerCase().replace(/\.pyc?$/, "");

const availablePlugin = (
	plugin: string,
	availablePlugins: string[],
): string | undefined => {
	const basename = pluginBasename(plugin);
	return availablePlugins.find(
		(candidate) => pluginBasename(candidate) === basename,
	);
};

const matchesSignature = (
	buffer: ArrayBuffer,
	signature: Uint8Array,
): boolean => {
	const bytes = new Uint8Array(buffer);
	return signature.every((value, index) => value === bytes[index]);
};

/**
 * Resolves an available ingestion plugin from a file's leading magic bytes.
 *
 * @param buffer Leading bytes read from the uploaded file.
 * @param availablePlugins Plugin identifiers exposed by the current backend.
 * @returns The backend's plugin identifier, or undefined when no signature matches.
 */
export const detectPluginBySignature = (
	buffer: ArrayBuffer,
	availablePlugins: string[],
): string | undefined => {
	for (const [plugin, signatures] of Object.entries(FILE_SIGNATURES)) {
		if (signatures.some((signature) => matchesSignature(buffer, signature))) {
			return availablePlugin(plugin, availablePlugins);
		}
	}

	return undefined;
};

/**
 * Resolves an available ingestion plugin from the explicit extension allowlist.
 *
 * @param filename Uploaded filename.
 * @param availablePlugins Plugin identifiers exposed by the current backend.
 * @returns The matched plugin identifier, or undefined for no/ambiguous matches.
 */
export const detectPluginByExtension = (
	filename: string,
	availablePlugins: string[],
): string | undefined => {
	const extensionSeparator = filename.lastIndexOf(".");
	if (extensionSeparator <= 0 || extensionSeparator === filename.length - 1) {
		return undefined;
	}

	const extension = filename.slice(extensionSeparator + 1).toLowerCase();
	const configuredPlugin = FILE_EXTENSION_PLUGINS[extension];
	return configuredPlugin
		? availablePlugin(configuredPlugin, availablePlugins)
		: undefined;
};

/**
 * Resolves an available plugin using content before the filename extension.
 *
 * @param buffer Leading bytes read from the uploaded file.
 * @param filename Uploaded filename.
 * @param availablePlugins Plugin identifiers exposed by the current backend.
 * @returns The matched plugin identifier, or undefined when detection is inconclusive.
 */
export const detectFilePlugin = (
	buffer: ArrayBuffer,
	filename: string,
	availablePlugins: string[],
): string | undefined =>
	detectPluginBySignature(buffer, availablePlugins) ??
	detectPluginByExtension(filename, availablePlugins);

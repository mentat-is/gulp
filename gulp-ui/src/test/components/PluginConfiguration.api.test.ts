import {
	deletePluginConfiguration,
	getPluginConfiguration,
	listPluginConfigurations,
	updatePluginConfiguration,
} from "@/plugins/plugin-configuration/PluginConfiguration.api";
import type {
	PluginConfigurationParameter,
	PluginConfigurationRecord,
} from "@/plugins/plugin-configuration/PluginConfiguration.model";

const parameters: PluginConfigurationParameter[] = [
	{
		name: "api_key",
		type: "str",
		location: "header",
		required: true,
		is_secret: true,
		value: "masked-value",
	},
];

const configurationRecord: PluginConfigurationRecord = {
	obj_id: "configuration-id",
	type: "shared_object",
	obj_type: "plugin_configuration",
	name: "Configuration",
	description: "Description",
	tags: ["tag"],
	obj: {
		plugin: "query_family",
		default: false,
		configuration: parameters,
	},
};

describe("PluginConfiguration dedicated API", () => {
	const previousApi = globalThis.api;
	let apiMock: JestMock<
		Promise<unknown>,
		[string, Record<string, unknown>]
	>;
	let queuedResponses: unknown[];

	beforeEach(() => {
		queuedResponses = [];
		apiMock = jest.fn<
			Promise<unknown>,
			[string, Record<string, unknown>]
		>(async (path: string, _options: Record<string, unknown>) => {
			if (queuedResponses.length > 0) {
				return queuedResponses.shift();
			}
			switch (path) {
				case "/plugin_configuration_list":
					return [configurationRecord];
				case "/plugin_configuration_get":
				case "/plugin_configuration_update":
					return configurationRecord;
				case "/plugin_configuration_delete":
					return { id: "configuration-id" };
				default:
					throw new Error(`Unexpected API path: ${path}`);
			}
		});
		globalThis.api = apiMock as unknown as typeof globalThis.api;
	});

	afterEach(() => {
		globalThis.api = previousApi;
	});

	it("uses only dedicated endpoints with exact query and body contracts", async () => {
		const listed = await listPluginConfigurations();
		const fetched = await getPluginConfiguration("configuration-id");
		const updated = await updatePluginConfiguration("configuration-id", {
				name: "Renamed configuration",
				configuration: parameters,
				default: true,
				glyph_id: null,
				description: null,
				tags: [],
			});
		const deleted = await deletePluginConfiguration("configuration-id");

		expect(listed).toEqual([configurationRecord]);
		expect(fetched).toEqual(configurationRecord);
		expect(updated).toEqual(configurationRecord);
		expect(deleted).toBe(true);
		expect(apiMock.mock.calls).toEqual([
			[
			"/plugin_configuration_list",
			{
				method: "POST",
				body: {},
				showErrorToast: false,
			},
			],
			[
			"/plugin_configuration_get",
			{
				method: "GET",
				query: { obj_id: "configuration-id" },
				showErrorToast: false,
			},
			],
			[
			"/plugin_configuration_update",
			{
				method: "PATCH",
				query: {
					obj_id: "configuration-id",
					name: "Renamed configuration",
				},
				body: {
					configuration: parameters,
					default: true,
					glyph_id: null,
					description: null,
					tags: [],
				},
				showErrorToast: false,
			},
			],
			[
			"/plugin_configuration_delete",
			{
				method: "DELETE",
				query: { obj_id: "configuration-id" },
				showErrorToast: false,
			},
			],
		]);
		expect(apiMock.mock.calls.map((call) => call[0])).toEqual([
			"/plugin_configuration_list",
			"/plugin_configuration_get",
			"/plugin_configuration_update",
			"/plugin_configuration_delete",
		]);
	});

	it("returns null or false for malformed dedicated API responses", async () => {
		queuedResponses = [
			[{ ...configurationRecord, obj: null }],
			{ ...configurationRecord, obj_type: "note" },
			{ ...configurationRecord, type: "event" },
			{ id: "different-id" },
		];

		expect(await listPluginConfigurations()).toBeNull();
		expect(await getPluginConfiguration("configuration-id")).toBeNull();
		expect(
			await updatePluginConfiguration("configuration-id", { default: true }),
		).toBeNull();
		expect(await deletePluginConfiguration("configuration-id")).toBe(false);
	});
});

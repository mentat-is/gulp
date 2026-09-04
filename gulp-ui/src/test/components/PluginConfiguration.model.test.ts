import {
	SECRET_SUMMARY_PLACEHOLDER,
	applyPluginConfigurationValues,
	buildPluginConfigurationRows,
	formatPluginConfigurationSummary,
	formatPluginConfigurationValue,
	parsePluginConfigurationList,
	sortPluginConfigurationsForDeletion,
	type PluginConfigurationParameter,
	type PluginConfigurationRecord,
} from "@/plugins/plugin-configuration/PluginConfiguration.model";

const configurationParameters: PluginConfigurationParameter[] = [
	{
		name: "api_key",
		type: "str",
		desc: "Authentication key",
		location: "header",
		required: true,
		default_value: null,
		is_secret: true,
		example: "secret-example",
		value: "server-secret-mask",
	},
	{
		name: "ratio",
		type: "float",
		desc: "Result ratio",
		location: "body",
		required: false,
		default_value: 1.5,
		values: [null, 0, 1.5, 2],
		value: 0,
	},
	{
		name: "enabled",
		type: "bool",
		location: "query",
		required: false,
		default_value: true,
		value: false,
	},
	{
		name: "labels",
		type: "list",
		location: "body",
		required: false,
		default_value: [],
		value: ["alpha", "beta"],
	},
	{
		name: "options",
		type: "dict",
		location: "body",
		required: false,
		default_value: {},
		value: { nested: true, attempts: 2 },
	},
];

const configurationRecord: PluginConfigurationRecord = {
	id: "configuration-id",
	type: "shared_object",
	obj_type: "plugin_configuration",
	name: "Default profile",
	description: "Profile used by tests",
	tags: ["test"],
	obj: {
		plugin: "query_family",
		default: true,
		configuration: configurationParameters,
	},
};

describe("PluginConfiguration model", () => {
	it("parses typed records including float, header/body, examples, and enum values", () => {
		const parsed = parsePluginConfigurationList([configurationRecord]);

		expect(parsed).toEqual([configurationRecord]);
		expect(parsed?.[0].obj.configuration[0]).toMatchObject({
			location: "header",
			is_secret: true,
			example: "secret-example",
		});
		expect(parsed?.[0].obj.configuration[1]).toMatchObject({
			type: "float",
			location: "body",
			values: [null, 0, 1.5, 2],
		});
	});

	it("rejects a list when one payload does not match the typed contract", () => {
		const malformedRecord = {
			...configurationRecord,
			obj: {
				...configurationRecord.obj,
				configuration: [
					...configurationParameters,
					{
						name: "unsupported",
						type: "binary",
						location: "cookie",
						required: true,
					},
				],
			},
		};

		expect(
			parsePluginConfigurationList([configurationRecord, malformedRecord]),
		).toBeNull();
		expect(parsePluginConfigurationList({ data: [configurationRecord] })).toBeNull();
	});

	it("formats compact values and replaces secrets with a fixed placeholder", () => {
		expect(formatPluginConfigurationValue(["alpha", "beta"])).toBe(
			"alpha, beta",
		);
		expect(
			formatPluginConfigurationValue({ nested: true, attempts: 2 }),
		).toBe('{"nested":true,"attempts":2}');
		expect(formatPluginConfigurationValue(null)).toBe("-");
		expect(formatPluginConfigurationValue("")).toBe("-");

		const summary = formatPluginConfigurationSummary(configurationParameters);
		expect(summary).toBe(
			`api_key=${SECRET_SUMMARY_PLACEHOLDER}; ratio=0; enabled=false; labels=alpha, beta; options={"nested":true,"attempts":2}`,
		);
		expect(summary.includes("server-secret-mask")).toBe(false);
	});

	it("builds separate plugin, default, and configuration table values", () => {
		const [row] = buildPluginConfigurationRows([configurationRecord]);

		expect(row.plugin).toBe("query_family");
		expect(row.default).toBe(true);
		expect(row.configurationPreview).toBe(
			`api_key=${SECRET_SUMMARY_PLACEHOLDER}; ratio=0; enabled=false; labels=alpha, beta; options={"nested":true,"attempts":2}`,
		);
	});

	it("applies edited null and falsy values without mutating parameter metadata", () => {
		const original = configurationParameters.map((parameter) => ({
			...parameter,
		}));
		const updated = applyPluginConfigurationValues(configurationParameters, {
			api_key: null,
			ratio: 0,
			enabled: false,
			labels: [],
			options: {},
		});

		expect(updated.map((parameter) => parameter.value)).toEqual([
			null,
			0,
			false,
			[],
			{},
		]);
		expect(updated[0]).toMatchObject({
			name: "api_key",
			type: "str",
			desc: "Authentication key",
			location: "header",
			required: true,
			default_value: null,
			is_secret: true,
			example: "secret-example",
		});
		expect(configurationParameters).toEqual(original);
		expect(updated).not.toBe(configurationParameters);
	});

	it("keeps current or default values when an edited key is absent", () => {
		const parameters: PluginConfigurationParameter[] = [
			{
				name: "explicit_empty",
				type: "str",
				required: false,
				default_value: "fallback",
				value: "",
			},
			{
				name: "default_zero",
				type: "int",
				required: false,
				default_value: 0,
			},
		];

		expect(
			applyPluginConfigurationValues(parameters, {}).map(
				(parameter) => parameter.value,
			),
		).toEqual(["", 0]);
	});

	it("orders non-default profiles before defaults without mutating input", () => {
		const nonDefaultA: PluginConfigurationRecord = {
			...configurationRecord,
			id: "non-default-a",
			name: "Non-default A",
			obj: { ...configurationRecord.obj, default: false },
		};
		const defaultB: PluginConfigurationRecord = {
			...configurationRecord,
			id: "default-b",
			name: "Default B",
			obj: { ...configurationRecord.obj, plugin: "second_family" },
		};
		const nonDefaultB: PluginConfigurationRecord = {
			...configurationRecord,
			id: "non-default-b",
			name: "Non-default B",
			obj: {
				...configurationRecord.obj,
				plugin: "second_family",
				default: false,
			},
		};
		const input = [configurationRecord, nonDefaultA, defaultB, nonDefaultB];

		const sorted = sortPluginConfigurationsForDeletion(input);

		expect(sorted.map((record) => record.id)).toEqual([
			"non-default-a",
			"non-default-b",
			"configuration-id",
			"default-b",
		]);
		expect(input.map((record) => record.id)).toEqual([
			"configuration-id",
			"non-default-a",
			"default-b",
			"non-default-b",
		]);
	});
});

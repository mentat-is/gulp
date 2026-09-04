import type { GulpDataset } from "@/class/Info";
import type { CustomParameters } from "@/components/CustomParameters";
import { QueryExternal } from "@/banners/QueryExternal.banner";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

interface CapturedEditorProps {
	plugin?: GulpDataset.PluginList.Interface;
	customParameters: CustomParameters.ValueMap;
	onValidityChange?: (isValid: boolean) => void;
}

let mockCapturedEditorProps: CapturedEditorProps | null = null;

jest.mock("@/context/Application.context", () => ({
	Application: { use: jest.fn() },
}));

jest.mock("@/context/Extension.context", () => ({
	Extension: {
		Slot: { QueryExternal: "query-external" },
		getBySlot: () => [],
	},
}));

jest.mock("@/banners/Preview.banner", () => ({ Preview: {} }));
jest.mock("@/banners/SelectFiles.banner", () => ({ SelectFiles: {} }));
jest.mock("@/components/AdvancedPluginParams", () => ({
	AdvancedPluginParams: (): null => null,
}));
jest.mock("@/locales", () => ({
	Locale: { use: () => ({ t: (key: string) => key }) },
}));
jest.mock("sonner", () => ({ toast: { success: jest.fn(), error: jest.fn() } }));
jest.mock("@/ui/Banner", () => ({ Banner: (): null => null }));
jest.mock("@/ui/Button", () => ({ Button: (): null => null }));
jest.mock("@/ui/Icon", () => ({ Icon: (): null => null }));
jest.mock("@/ui/Label", () => ({ Label: (): null => null }));
jest.mock("@/ui/Select", () => ({ Select: {} }));
jest.mock("@/ui/Stack", () => ({ Stack: (): null => null }));
jest.mock("@/ui/Textarea", () => ({ Textarea: (): null => null }));

jest.mock("@/components/CustomParameters", () => ({
	CustomParameters: {
		Editor: (props: CapturedEditorProps) => {
			mockCapturedEditorProps = props;
			return <div data-custom-parameters-editor />;
		},
	},
}));

const externalPlugin: GulpDataset.PluginList.Interface = {
	display_name: "External query",
	type: ["external"],
	desc: "External query plugin",
	path: ".",
	data: {},
	filename: "query_external",
	sigma_support: [],
	custom_parameters: [
		{
			name: "limit",
			type: "int",
			desc: "Result limit",
			location: "query",
			required: false,
			default_value: 0,
		},
	],
	depends_on: [],
	tags: [],
	version: "1.0.0",
};

describe("QueryExternal custom parameter integration", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockCapturedEditorProps = null;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("delegates external plugin fields to the shared editor", () => {
		const setParams = jest.fn();
		const onValidityChange = jest.fn();
		const params: CustomParameters.ValueMap = { limit: 0 };

		act(() => {
			root.render(
				<QueryExternal.DynamicRequestBuilder
					selectedOption={externalPlugin}
					params={params}
					setParams={setParams}
					onValidityChange={onValidityChange}
				/>,
			);
		});

		expect(container.querySelector("[data-custom-parameters-editor]")).not.toBeNull();
		expect(mockCapturedEditorProps?.plugin).toBe(externalPlugin);
		expect(mockCapturedEditorProps?.customParameters).toBe(params);
		mockCapturedEditorProps?.onValidityChange?.(false);
		expect(onValidityChange).toHaveBeenCalledWith(false);
	});

	it("blocks query and preview actions until a plugin exists and dict drafts are valid", () => {
		expect(QueryExternal.isSubmissionDisabled(null, true)).toBe(true);
		expect(QueryExternal.isSubmissionDisabled(externalPlugin, false)).toBe(true);
		expect(QueryExternal.isSubmissionDisabled(externalPlugin, true)).toBe(false);
	});

	it("serializes restored object queries without changing existing strings", () => {
		const query = { query: { match_all: {} } };
		expect(QueryExternal.restoredQueryText(query)).toBe(
			JSON.stringify(query, null, 2),
		);
		expect(QueryExternal.restoredQueryText("{\"query\":{}}")).toBe(
			"{\"query\":{}}",
		);
	});
});

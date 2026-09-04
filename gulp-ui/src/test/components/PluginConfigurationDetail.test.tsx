import React, { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import type { Glyph } from "@/entities/Glyph";
import type { PluginConfigurationRecord } from "@/plugins/plugin-configuration/PluginConfiguration.model";
import {
	DisplayPluginConfigurationDetailDialog,
	PluginConfigurationMetadataBanner,
	PluginConfigurationObjectBanner,
} from "@/plugins/plugin-configuration/PluginConfigurationDetail";

(globalThis as typeof globalThis & { IS_REACT_ACT_ENVIRONMENT: boolean })
	.IS_REACT_ACT_ENVIRONMENT = true;

const SERVER_SECRET_MASK = "********";
const mockDestroyBanner = jest.fn<void, []>();
const mockSpawnBanner = jest.fn<void, [React.ReactElement]>();
const mockToastError = jest.fn<void, [string]>();
const mockToastSuccess = jest.fn<void, [string]>();
let mockUpdateResult: PluginConfigurationRecord | null = null;
let mockGetResult: PluginConfigurationRecord | null = null;
let mockDeleteResult = false;
const mockUpdatePluginConfiguration = jest.fn<
	void,
	[string, Record<string, unknown>]
>();
const mockGetPluginConfiguration = jest.fn<void, [string]>();
const mockDeletePluginConfiguration = jest.fn<void, [string]>();

const mockTranslate = (key: string): string => key;

jest.mock("@/context/Application.context", () => ({
	Application: {
		use: () => ({
			destroyBanner: mockDestroyBanner,
			spawnBanner: mockSpawnBanner,
		}),
	},
}));

jest.mock("@/plugins/plugin-configuration/PluginConfiguration.api", () => ({
	deletePluginConfiguration: (objId: string) => {
		mockDeletePluginConfiguration(objId);
		return Promise.resolve(mockDeleteResult);
	},
	getPluginConfiguration: (objId: string) => {
		mockGetPluginConfiguration(objId);
		return Promise.resolve(mockGetResult);
	},
	updatePluginConfiguration: (
		objId: string,
		update: Record<string, unknown>,
	) => {
		mockUpdatePluginConfiguration(objId, update);
		return Promise.resolve(mockUpdateResult);
	},
}));

jest.mock("sonner", () => ({
	toast: {
		error: (message: string) => mockToastError(message),
		success: (message: string) => mockToastSuccess(message),
	},
}));

jest.mock("@/locales", () => ({
	Locale: { use: () => ({ t: mockTranslate }) },
}));

jest.mock("@/entities/Glyph", () => ({
	Glyph: {
		List: { get: () => "AcronymJson" },
		Chooser: ({
			icon,
			setIcon,
			label,
		}: {
			icon: string | null;
			setIcon: (icon: string | null) => void;
			label: string;
		}) => (
			<button
				type="button"
				data-testid="glyph-chooser"
				data-glyph={icon ?? ""}
				onClick={() => setIcon("replacement-glyph")}
			>
				{label}
			</button>
		),
	},
}));

jest.mock("@/ui/Banner", () => ({
	Banner: ({
		title,
		done,
		children,
	}: {
		title?: ReactNode;
		done?: ReactNode;
		children?: ReactNode;
	}) => (
		<section data-banner-title={String(title ?? "")}>
			{children}
			<div data-banner-done>{done}</div>
		</section>
	),
}));

jest.mock("@/ui/Button", () => ({
	Button: ({
		children,
		icon,
		variant: _variant,
		loading: _loading,
		...props
	}: React.ButtonHTMLAttributes<HTMLButtonElement> & {
		icon?: string;
		variant?: string;
		loading?: boolean;
	}) => (
		<button
			type="button"
			data-icon={icon}
			{...props}
		>
			{children ?? icon}
		</button>
	),
}));

jest.mock("@/ui/Input", () => ({
	Input: ({
		label,
		icon,
		endIcon,
		onEndIconClick,
		endIconTitle,
		variant,
		valid: _valid,
		skeleton: _skeleton,
		...props
	}: React.InputHTMLAttributes<HTMLInputElement> & {
		label?: string;
		icon?: string;
		endIcon?: string;
		onEndIconClick?: () => void;
		endIconTitle?: string;
		variant?: string;
		valid?: boolean;
		skeleton?: boolean;
	}) => (
		<label>
			{label}
			<input
				aria-label={label}
				data-icon={icon ?? ""}
				data-variant={variant ?? ""}
				{...props}
			/>
			{endIcon ? (
				<button
					type="button"
					data-end-icon={endIcon}
					title={endIconTitle}
					onClick={onEndIconClick}
				/>
			) : null}
		</label>
	),
}));

jest.mock("@/ui/Label", () => ({
	Label: ({
		value,
		htmlFor,
		className,
	}: {
		value?: string;
		htmlFor?: string;
		className?: string;
	}) => (
		<label
			htmlFor={htmlFor}
			className={className}
		>
			{value}
		</label>
	),
}));

jest.mock("@/ui/Stack", () => ({
	Stack: ({
		children,
		dir: _dir,
		gap: _gap,
		ai: _ai,
		jc: _jc,
		noShrink: _noShrink,
		...props
	}: React.HTMLAttributes<HTMLDivElement> & {
		dir?: string;
		gap?: number;
		ai?: string;
		jc?: string;
		noShrink?: boolean;
	}) => <div {...props}>{children}</div>,
}));

jest.mock("@/ui/Switch", () => ({
	Switch: ({
		checked,
		disabled,
		name,
		onCheckedChange,
		"aria-label": ariaLabel,
	}: {
		checked?: boolean;
		disabled?: boolean;
		name?: string;
		onCheckedChange?: (checked: boolean) => void;
		"aria-label"?: string;
	}) => (
		<input
			type="checkbox"
			name={name}
			aria-label={ariaLabel}
			checked={checked}
			disabled={disabled}
			onChange={(event) => onCheckedChange?.(event.currentTarget.checked)}
		/>
	),
}));

jest.mock("@/ui/Textarea", () => ({
	Textarea: ({
		error: _error,
		...props
	}: React.TextareaHTMLAttributes<HTMLTextAreaElement> & {
		error?: boolean;
	}) => <textarea {...props} />,
}));

jest.mock("@/ui/Select", () => ({
	Select: {
		Root: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
		Trigger: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
		Value: ({ placeholder }: { placeholder?: string }) => <span>{placeholder}</span>,
		Content: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
		Item: ({ children }: { children?: ReactNode }) => <span>{children}</span>,
	},
}));

jest.mock("@/ui/Badge", () => ({
	Badge: ({ children }: { children?: ReactNode }) => <span>{children}</span>,
}));

jest.mock("@/ui/DeleteConfirmationBanner", () => ({
	DeleteConfirmationBanner: ({ children }: { children?: ReactNode }) => (
		<div>{children}</div>
	),
}));

jest.mock("@/ui/Dialog", () => ({
	Dialog: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
}));

jest.mock("@/ui/Icon", () => ({
	Icon: ({ name }: { name: string }) => <span data-icon-name={name} />,
}));

const configurationRecord: PluginConfigurationRecord = {
	obj_id: "configuration-id",
	type: "shared_object",
	obj_type: "plugin_configuration",
	name: "Original name",
	description: "Original description",
	tags: ["original"],
	glyph_id: "original-glyph" as Glyph.Id,
	obj: {
		plugin: "query_family",
		default: false,
		configuration: [
			{
				name: "api_key",
				type: "str",
				desc: "Authentication key",
				location: "header",
				required: true,
				default_value: null,
				is_secret: true,
				example: "secret-example",
				value: SERVER_SECRET_MASK,
				transport_hint: "preserve-me",
			},
			{
				name: "enabled",
				type: "bool",
				desc: "Enable queries",
				location: "query",
				required: false,
				default_value: true,
				value: false,
			},
		],
	},
};

/**
 * Dispatches a React-compatible input event after invoking the native setter.
 * @param element Input or textarea whose controlled value is changed.
 * @param value New textual value.
 * @returns Nothing.
 */
function changeValue(
	element: HTMLInputElement | HTMLTextAreaElement,
	value: string,
): void {
	const prototype =
		element instanceof HTMLTextAreaElement
			? HTMLTextAreaElement.prototype
			: HTMLInputElement.prototype;
	const setter = Object.getOwnPropertyDescriptor(prototype, "value")?.set;
	setter?.call(element, value);
	element.dispatchEvent(new Event("input", { bubbles: true }));
}

describe("PluginConfiguration paid banners", () => {
	let container: HTMLDivElement;
	let root: Root;

	beforeEach(() => {
		mockDestroyBanner.mock.calls.splice(0);
		mockSpawnBanner.mock.calls.splice(0);
		mockToastError.mock.calls.splice(0);
		mockToastSuccess.mock.calls.splice(0);
		mockUpdatePluginConfiguration.mock.calls.splice(0);
		mockGetPluginConfiguration.mock.calls.splice(0);
		mockDeletePluginConfiguration.mock.calls.splice(0);
		mockUpdateResult = configurationRecord;
		mockGetResult = configurationRecord;
		mockDeleteResult = true;
		container = document.createElement("div");
		document.body.appendChild(container);
		root = createRoot(container);
	});

	afterEach(() => {
		act(() => root.unmount());
		container.remove();
	});

	it("renders plugin readonly, keeps the server mask in a password field, and promotes default", async () => {
		const onSaved = jest.fn<Promise<void>, []>(() => Promise.resolve());
		act(() => {
			root.render(
				<PluginConfigurationObjectBanner
					configuration={configurationRecord}
					onSaved={onSaved}
				/>,
			);
		});

		const pluginInput = container.querySelector<HTMLInputElement>(
			'input[aria-label="plugins.pluginConfiguration.plugin"]',
		);
		const secretInput = container.querySelector<HTMLInputElement>(
			'input[name="api_key"]',
		);
		const defaultSwitch = container.querySelector<HTMLInputElement>(
			'input[aria-label="plugins.pluginConfiguration.default"]',
		);
		expect(pluginInput?.value).toBe("query_family");
		expect(pluginInput?.disabled).toBe(true);
		expect(pluginInput?.dataset.icon).toBe("");
		expect(pluginInput?.dataset.variant).toBe("");
		expect(secretInput?.type).toBe("password");
		expect(secretInput?.value).toBe(SERVER_SECRET_MASK);
		const revealSecretButton = container.querySelector<HTMLButtonElement>(
			'button[data-end-icon="Eye"]',
		);
		expect(revealSecretButton?.title).toBe("auth.showPassword");
		act(() => revealSecretButton?.click());
		expect(secretInput?.type).toBe("text");
		expect(defaultSwitch?.checked).toBe(false);
		expect(defaultSwitch?.disabled).toBe(false);

		act(() => defaultSwitch?.click());
		const saveButton = container.querySelector<HTMLButtonElement>(
			'button[data-icon="Check"]',
		);
		expect(saveButton?.disabled).toBe(false);
		await act(async () => {
			saveButton?.click();
			await Promise.resolve();
			await Promise.resolve();
		});

		expect(mockUpdatePluginConfiguration.mock.calls).toHaveLength(1);
		expect(mockUpdatePluginConfiguration.mock.calls[0]).toEqual([
			"configuration-id",
			{
				configuration: configurationRecord.obj.configuration,
				default: true,
			},
		]);
		expect(
			(
				mockUpdatePluginConfiguration.mock.calls[0][1]
					.configuration as PluginConfigurationRecord["obj"]["configuration"]
			)[0],
		).toMatchObject({
			desc: "Authentication key",
			location: "header",
			required: true,
			default_value: null,
			is_secret: true,
			example: "secret-example",
			value: SERVER_SECRET_MASK,
			transport_hint: "preserve-me",
		});
		expect(onSaved).toHaveBeenCalledTimes(1);
		expect(mockDestroyBanner).toHaveBeenCalledTimes(1);
	});

	it("lets the backend validate unsetting an existing default", async () => {
		const defaultConfiguration: PluginConfigurationRecord = {
			...configurationRecord,
			obj: { ...configurationRecord.obj, default: true },
		};
		act(() => {
			root.render(
				<PluginConfigurationObjectBanner
					configuration={defaultConfiguration}
					onSaved={() => undefined}
				/>,
			);
		});

		const defaultSwitch = container.querySelector<HTMLInputElement>(
			'input[aria-label="plugins.pluginConfiguration.default"]',
		);
		expect(defaultSwitch?.checked).toBe(true);
		expect(defaultSwitch?.disabled).toBe(false);
		expect(container.textContent?.includes("defaultLocked")).toBe(false);

		act(() => defaultSwitch?.click());
		const saveButton = container.querySelector<HTMLButtonElement>(
			'button[data-icon="Check"]',
		);
		expect(saveButton?.disabled).toBe(false);
		await act(async () => {
			saveButton?.click();
			await Promise.resolve();
			await Promise.resolve();
		});

		expect(mockUpdatePluginConfiguration.mock.calls[0]?.[1]).toMatchObject({
			default: false,
		});
	});

	it("updates and clears metadata with one dedicated PATCH payload", async () => {
		const onSaved = jest.fn<Promise<void>, []>(() => Promise.resolve());
		act(() => {
			root.render(
				<PluginConfigurationMetadataBanner
					configuration={configurationRecord}
					onSaved={onSaved}
				/>,
			);
		});

		const nameInput = container.querySelector<HTMLInputElement>(
			'input[placeholder="common.name"]',
		);
		const descriptionInput = container.querySelector<HTMLTextAreaElement>(
			"#plugin-configuration-description",
		);
		const tagsInput = container.querySelector<HTMLInputElement>(
			'input[placeholder="common.tags"]',
		);
		const clearIconButton = container.querySelector<HTMLButtonElement>(
			'button[title="plugins.pluginConfiguration.clearIcon"]',
		);
		expect(nameInput).not.toBeNull();
		expect(descriptionInput).not.toBeNull();
		expect(tagsInput).not.toBeNull();
		expect(clearIconButton?.disabled).toBe(false);

		act(() => {
			if (nameInput) changeValue(nameInput, "  Renamed configuration  ");
			if (descriptionInput) changeValue(descriptionInput, "   ");
			if (tagsInput) changeValue(tagsInput, "alpha, , beta");
			clearIconButton?.click();
		});
		const saveButton = container.querySelector<HTMLButtonElement>(
			'button[data-icon="Check"]',
		);
		expect(saveButton?.disabled).toBe(false);
		await act(async () => {
			saveButton?.click();
			await Promise.resolve();
			await Promise.resolve();
		});

		expect(mockUpdatePluginConfiguration.mock.calls).toEqual([
			[
				"configuration-id",
				{
					name: "Renamed configuration",
					glyph_id: null,
					description: null,
					tags: ["alpha", "beta"],
				},
			],
		]);
		expect(onSaved).toHaveBeenCalledTimes(1);
		expect(mockDestroyBanner).toHaveBeenCalledTimes(1);
		expect(mockToastError).not.toHaveBeenCalled();
	});

	it("loads structured detail fields and deletes through the dedicated action", async () => {
		const onClose = jest.fn<void, []>();
		const onUpdated = jest.fn<Promise<void>, []>(() => Promise.resolve());
		await act(async () => {
			root.render(
				<DisplayPluginConfigurationDetailDialog
					objId="configuration-id"
					fallbackName="Fallback name"
					onClose={onClose}
					onUpdated={onUpdated}
				/>,
			);
			await Promise.resolve();
			await Promise.resolve();
		});

		expect(mockGetPluginConfiguration.mock.calls).toEqual([
			["configuration-id"],
		]);
		expect(container.textContent?.includes("Original name")).toBe(true);
		expect(container.textContent?.includes("Original description")).toBe(true);
		expect(container.textContent?.includes("query_family")).toBe(true);
		expect(container.textContent?.includes("api_key")).toBe(true);
		expect(container.textContent?.includes("Authentication key")).toBe(true);
		expect(container.textContent?.includes(SERVER_SECRET_MASK)).toBe(true);
		expect(container.textContent?.includes("transport_hint")).toBe(false);
		const defaultSwitch = container.querySelector<HTMLInputElement>(
			'input[aria-label="plugins.pluginConfiguration.default"]',
		);
		expect(defaultSwitch?.checked).toBe(false);
		expect(defaultSwitch?.disabled).toBe(true);

		act(() => {
			container
				.querySelector<HTMLButtonElement>('button[data-icon="Trash2"]')
				?.click();
		});
		expect(mockSpawnBanner.mock.calls).toHaveLength(1);
		const deleteBanner = mockSpawnBanner.mock.calls[0][0] as React.ReactElement<{
			onConfirm: () => Promise<boolean | void>;
		}>;
		await act(async () => {
			await deleteBanner.props.onConfirm();
		});

		expect(mockDeletePluginConfiguration.mock.calls).toEqual([
			["configuration-id"],
		]);
		expect(onUpdated).toHaveBeenCalledTimes(1);
		expect(onClose).toHaveBeenCalledTimes(1);
		expect(mockToastSuccess).toHaveBeenCalledWith(
			"plugins.pluginConfiguration.deleted",
		);
	});

	it("renders the localized failure state when the dedicated get returns no record", async () => {
		mockGetResult = null;
		await act(async () => {
			root.render(
				<DisplayPluginConfigurationDetailDialog
					objId="missing-configuration"
					fallbackName="Missing configuration"
					onClose={() => undefined}
					onUpdated={() => undefined}
				/>,
			);
			await Promise.resolve();
			await Promise.resolve();
		});

		expect(mockGetPluginConfiguration.mock.calls).toEqual([
			["missing-configuration"],
		]);
		expect(
			container.textContent?.includes(
				"plugins.pluginConfiguration.loadFailed",
			),
		).toBe(true);
		expect(mockToastError).toHaveBeenCalledWith(
			"plugins.pluginConfiguration.loadFailed",
		);
		expect(
			container.querySelector<HTMLButtonElement>('button[data-icon="Trash2"]'),
		).toBeNull();
	});
});

import type { ComponentType } from "react";
import type { LocaleTables } from "@/locales/core";

/**
 * Public plugin contract: re-export the actual UI implementations.
 * Keep this API version unchanged across UI releases unless this contract changes.
 */
export { toast } from "sonner";
export { Application } from "@/context/Application.context";
export { AIAssistant } from "@/banners/AIAssistant.banner";
export { FilterFileBanner } from "@/banners/FilterFile.banner";
export { GulpIndexedDB } from "@/class/IndexedDB";
export { SmartSocket } from "@/class/SmartSocket";
export { Info } from "@/class/Info";
export { QueriesHistory } from "@/components/QueriesHistory";
export { OpenSearchQueryBuilder } from "@/components/QueryBuilder";
export { Table } from "@/components/Table";
export { DisplayEventDialog, EventIndicator } from "@/dialogs/Event.dialog";
export { Default } from "@/dto/Dataset";
export { Logger } from "@/dto/Logger.class";
export { EventJsonTransforms } from "@/lib/EventJsonTransforms";
export { WindowBridge } from "@/lib/WindowBridge";
export { scrollStore } from "@/store/scroll.store";
export { Locale, translate } from "@/locales";
export { cn, copy, download, generateUUID, HASH_FUNCTIONS } from "@/ui/utils";
export type { HashFunctionName } from "@/ui/utils";
export { ColorPicker, ColorPickerTrigger, ColorPickerPopover } from "@/ui/Color";
export { ContextMenu, ContextMenuContent, ContextMenuItem, ContextMenuSeparator, ContextMenuTrigger } from "@/ui/ContextMenu";
export { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/ui/Tooltip";
export { App } from "@/entities/App";
export { Color } from "@/entities/Color";
export { Context } from "@/entities/Context";
export { Doc } from "@/entities/Doc";
export { Filter } from "@/entities/Filter";
export { Glyph } from "@/entities/Glyph";
export { Group } from "@/entities/Group";
export { Highlight } from "@/entities/Highlight";
export { Link } from "@/entities/Link";
export { Note } from "@/entities/Note";
export { Operation } from "@/entities/Operation";
export type { Query } from "@/entities/Query";
export { Source } from "@/entities/Source";
export { User } from "@/entities/User";
export { Internal } from "@/entities/addon/Internal";
export { Badge } from "@/ui/Badge";
export { Banner } from "@/ui/Banner";
export { Button } from "@/ui/Button";
export { Checkbox } from "@/ui/Checkbox";
export { Dialog } from "@/ui/Dialog";
export { Icon } from "@/ui/Icon";
export { Input } from "@/ui/Input";
export { Label } from "@/ui/Label";
export { Markdown } from "@/ui/Markdown";
export { Notification } from "@/ui/Notification";
export { Popover } from "@/ui/Popover";
export { Select } from "@/ui/Select";
export { Separator } from "@/ui/Separator";
export { Shimmer } from "@/ui/Shimmer";
export { Stack } from "@/ui/Stack";
export { Switch } from "@/ui/Switch";
export { Textarea } from "@/ui/Textarea";
export { Toggle } from "@/ui/Toggle";
export { api } from "@/class/API";
export type { ResponseBase } from "@/class/API";
export type { SendDataPluginRef } from "@/banners/SendData.banner";
export { Canvas } from "@/utils";

/** Compatibility contract, independent of the UI application release version. */
export interface UiPluginApiVersion {
	major: number;
	minor: number;
}

/** Plugin-owned locale tables; identical keys in different plugins are isolated. */
export type UiPluginLocales = LocaleTables;

/** Services available to activation hooks, including before authentication. */
export interface UiPluginActivationContext {
	server: string;
	t: (key: string, vars?: Record<string, string | number>) => string;
}

/** Exports produced by the licensing builder for one UI entrypoint. */
export interface UiPluginModule {
	default: ComponentType<Record<string, unknown>>;
	locales?: UiPluginLocales;
	activate?: (context: UiPluginActivationContext) => void | (() => void);
}

/** Runtime additions to the existing UI component/slot metadata. */
export interface UiPluginRuntimeMetadata {
	id: string;
	requires_ui_api: UiPluginApiVersion;
	build_hash: string;
	entry_url: string;
	css_url?: string;
}

export const UI_PLUGIN_API_VERSION: UiPluginApiVersion = { major: 1, minor: 0 };

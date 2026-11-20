# Plugins

gULP UI supports UI plugins loaded from `src/plugins` and registered by the
backend through `/ui_plugin_list`.

## Saved Backend Plugin Configurations

Open **Plugin Configuration** from Home to inspect a saved configuration. The
detail dialog shows one **Configuration** heading followed by read-only
parameter values. The plugin/default summary and the intermediate **Parameters**
heading are hidden in both details and the edit banner. Their JSX remains
commented out, and the saved plugin/default metadata is preserved when editing.
When `obj.configuration` is a non-empty list of `GulpPluginCustomParameter`
definitions with unique names and supported types (`str`, `int`, `float`, `bool`,
`dict`, and `list`), the dialog reuses the typed controls from External Query.
Descriptions and required-field indicators remain visible, while definition
metadata such as `type`, `location`, and `is_secret` is not shown as nested JSON.
Text and dictionary fields are read-only; switches and selects are disabled.
Secrets are masked initially and retain the existing show/hide toggle.

The configuration edit action opens the same typed controls in editable mode.
Values come from `value` when that property exists, otherwise `default_value`,
including explicit `false`, `0`, empty strings, and `null`. Saving updates only
changed values in their original field and preserves parameter order, metadata,
and unrelated JSON fields. Invalid dictionary drafts block saving, and opening
the editor or reverting edits does not mark the configuration as changed.

Custom configurations such as MISP retain the recursive JSON display and
value-only editor, including nested objects and arrays. Empty or missing
parameter lists retain the empty state. Mixed lists,
duplicate parameter names, and unsupported definitions also use the JSON
fallback. Editors preserve object keys, array shape, and JSON value types when
saving through the shared-object API.

## Runtime Model

The frontend loads plugin metadata from the backend. For each returned plugin, it
imports:

```ts
import(`@/plugins/${plugin.filename}`);
```

The imported file must provide a default export React component. The metadata
JSON identifies where the component can be mounted.

Supported UI slots include:

- `operation-menu`: renders an entry in the operation menu Plugins section.
- `send-data`: renders as a Send IOC destination from the event detail view.
- `event-actions`: renders in the event action area.
- `sigma-upload-mode`: adds an upload mode to the Sigma banner.
- `dashboard-view`: mounts dashboard-related UI.
- `ai-assistant-window`: reserved for assistant windows.

## UI Plugins

### Coconut Example

`coconut.tsx` is an example UI plugin. It shows the minimum pattern: default
export a button, open a banner, and close the banner from inside the plugin UI.

## Creating a Custom UI Plugin

Create a component file and a metadata JSON file in `src/plugins`.

Example files:

```text
src/plugins/MyPlugin.tsx
src/plugins/MyPlugin.tsx.json
```

The component file must default export a React component:

```tsx
import { Application } from "@/context/Application.context";
import { Button } from "@/ui/Button";

export default function MyPlugin() {
	const { spawnBanner } = Application.use();

	return (
		<Button
			icon="Puzzle"
			variant="secondary"
			onClick={() => spawnBanner(<div>My plugin UI</div>)}
		/>
	);
}
```

The metadata file must describe the plugin:

```json
{
	"display_name": "My Plugin",
	"plugin": "my_backend_plugin",
	"extension": true,
	"version": "1.0.0",
	"desc": "Adds a custom gULP UI action.",
	"path": ".",
	"filename": "MyPlugin.tsx",
	"type": ["operation-menu"]
}
```

Use existing UI components from `src/ui`, CSS modules for non-trivial styling,
and locale JSON files for visible text. Do not hardcode user-facing strings when
the text belongs in localization.

## Send Data Plugin Contract

A plugin targeting `send-data` must expose an `onDone()` method through
`forwardRef` and `useImperativeHandle`. The Send IOC banner mounts the plugin and
calls that method when the analyst confirms.

The plugin receives the current event in its props and owns destination-specific
validation, API calls, and success/error handling.

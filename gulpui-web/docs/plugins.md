# Plugins

gulpui-web supports UI plugins loaded from `src/plugins` and registered by the
backend through `/ui_plugin_list`.

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
- `ai-assistant-window`: reserved for assistant windows; paid/pro assistant
  plugins are not documented here.

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
	"desc": "Adds a custom gulpui-web action.",
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

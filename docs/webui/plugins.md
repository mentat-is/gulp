# Plugins

gULP UI supports UI plugins loaded from `src/plugins` and registered by the
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
- `ai-assistant-window`: reserved for assistant windows.
- `home-section`: adds a routed section to the authenticated Home shell.

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

## Home Section Plugin Contract

A `home-section` plugin adds one or more navigation targets to the Home shell.
The Home shell continues to own the built-in Operations, Users, and Groups
sections. Plugin sections supply their menu metadata and content through the
manifest, so the shell does not import or name individual plugins.

Each Home target uses these fields:

| Field | Required | Behavior |
| --- | --- | --- |
| `slot` | yes | Must be `home-section`. |
| `route` | yes | Absolute route owned by the plugin section. |
| `labelKey` | yes | Locale key for the Home menu entry. |
| `titleKey` | yes | Locale key for the section heading. |
| `icon` | yes | Icon name rendered by the Home menu. |
| `group` | no | Locale key for the menu category; defaults to `common.plugins`. |
| `requiredPermissions` | no | Permission names that the user must all possess. |
| `order` | no | Numeric ascending menu and route precedence; omitted values sort last. |

The extension manifest must also retain the normal UI plugin fields. For
example:

```json
{
  "display_name": "Extension home example",
  "plugin": "example_plugin",
  "extension": true,
  "version": "1.0.0",
  "desc": "description about example plugin",
  "path": ".",
  "filename": "example.plugin.tsx",
  "type": ["home-section"],
  "targets": [{
    "slot": "home-section",
    "route": "/example-plugin",
    "labelKey": "example.plugin.menu",
    "titleKey": "example.plugin.title",
    "icon": "AcronymJson",
    "group": "common.administration",
    "requiredPermissions": ["admin"],
    "order": 100
  }]
}
```

Routes are trimmed, trailing slashes are removed, and matching is exact. Routes
must contain only alphanumeric path segments plus `_` and `-`; empty, relative,
double-slash, query-bearing, and fragment-bearing values are invalid. Core routes
(`/`, `/login`, `/users`, `/groups`, `/renderer-test`, `/operations` and its
children, and `/detached` and its children) are reserved. Invalid targets are
logged and skipped.

Targets are ordered by `order`, then plugin display name, then filename. If two
targets normalize to the same route, only the first ordered target is retained
and the collision is logged. Every value in `requiredPermissions` must be present
on the authenticated user. A target that fails this check is absent from the
menu and cannot be opened directly.

The authenticated catch-all route resolves only registered Home targets. It
waits for both UI plugin discovery and Home/user initialization before deciding.
Unknown routes, missing or failed plugin imports, and unauthorized targets are
redirected to `/` with history replacement. This also makes refresh and direct
navigation use the same resolver as menu navigation.

The component default export receives the selected slot and normalized target:

```tsx
interface HomeSectionPluginProps {
  slot: "home-section";
  target: Extension.HomeSectionTarget;
}
```

The component owns the main section body. The Home shell owns the surrounding
menu, localized heading, back navigation, and optional docked dialog area.
input.

## Send Data Plugin Contract

A plugin targeting `send-data` must expose an `onDone()` method through
`forwardRef` and `useImperativeHandle`. The Send IOC banner mounts the plugin and
calls that method when the analyst confirms.

The plugin receives the current event in its props and owns destination-specific
validation, API calls, and success/error handling.

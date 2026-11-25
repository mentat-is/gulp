# Runtime UI plugins

The public UI image can load precompiled plugins from the selected gULP backend.
The same image works for customers with different plugin selections. Bundle
compilation runs on a build machine; servers need only the resulting files and
compatible backend/UI images. Manifests and compiled assets all live under
the working directory's `plugins/ui` tree.

## Two supported sources

Custom plugins in `gulp-ui/src/plugins` keep the source plus companion JSON
format. The UI discovers these pairs itself, including nested directories.
The source filename is derived from the JSON filename, rather than the optional
`filename` field inside that JSON. Each source must default-export a React
component. Its current internal imports and CSS modules continue working.
These plugins are compiled into that developer's UI build and load even when
the backend list is empty, unavailable, or does not mention their names.

Runtime plugins are enabled by JSON manifests in
`$GULP_WORKING_DIR/plugins/ui`. The backend lists complete, valid packages from
that directory and its built-in UI manifest directory. Working-directory entries
take precedence over built-ins with the same id or filename. The UI loads each
bundle from its returned URL. A locally compiled plugin takes precedence over a
runtime plugin with the same logical `filename`, and is not mounted twice.

Both sources use the existing component registry, slots, props and send-data refs.
The optional filename/slot filters for detached clients still apply to both.
The new `login` slot mounts in the login page and authentication banner alongside
the existing login methods. Discovery and asset delivery require no token.
Authenticated business endpoints keep their own permission checks.

## Compiled package layout

Obtain a compiled package compatible with the host UI API. Copy its `plugins/ui`
tree into the backend working directory, preserving this layout:

```text
customer-ui/
  plugins/ui/TestUiPlugin.plugin.json
  plugins/ui/TestUiPlugin.plugin/<sha256>/plugin.js
  plugins/ui/TestUiPlugin.plugin/<sha256>/plugin.css
```

CSS is optional. Static resources are embedded, JavaScript has no extra chunks,
and required dependency license comments remain in the JavaScript. The build
hash covers both declared files, including the embedded translations. A manifest
contains the existing display/slot metadata plus:

```json
{
  "id": "TestUiPlugin.plugin",
  "filename": "TestUiPlugin.plugin.tsx",
  "requires_ui_api": { "major": 1, "minor": 0 },
  "build_hash": "<64 lowercase hexadecimal characters>",
  "assets": ["plugin.js", "plugin.css"]
}
```

## Backend delivery and Docker operations

`GET /ui_plugin_list` returns a JSend `data` array. Each package includes a
backend-generated `entry_url` and an optional `css_url`. The list uses
`Cache-Control: no-cache`. Metadata discovery does not open a collaboration DB
session. Invalid manifests and incomplete packages are logged and skipped
individually. The backend no longer requires source files beside the manifests.

`GET /ui_plugin_asset/{id}/{hash}/{asset}` serves the declared JavaScript or CSS
without login, using the correct content type and public immutable caching.
Only packages with an installed activation manifest are served. Undeclared files,
source maps, incorrect hashes and symlinks escaping the bundle root are rejected.
No Python extension is needed for asset delivery.

Copy the `plugins/ui/<id>/<hash>` asset directories first and then install the
activation JSON files in `plugins/ui`. The existing working-directory mount
exposes both to the backend. The nginx UI container needs no additional mount or
customer build.
When changing the selected plugin set, remove activation manifests for plugins
that are no longer included; merging a new directory does not delete old files.
Reload the UI after changing manifests or packages. For rollback, restore the
previous manifest and retain its corresponding bundle directory. Removing a
manifest disables subsequent discovery and downloads; browser caches already
holding public assets are not a licensing/revocation mechanism.

The UI appends backend-relative asset paths to the configured server URL, including
any reverse-proxy prefix. A proxy with CSP must permit scripts and styles from
the selected backend; its CORS configuration must allow module requests from the
UI origin. This design uses native ESM URLs and does not need Blob or eval policies.

Remove previously generated paid mirrors from `src/plugins` when preparing a
public source build. The old copy script is no longer run by `prestart` or
`prebuild`; locally authored custom files continue to belong there.

## Public UI API and lifecycle

Runtime source imports host capabilities from `@gulp/ui-plugin-sdk/v1`. The
contract is defined in `gulp-ui/src/plugin-api/v1.ts`, the only file in that
directory. It contains type definitions and re-exports the actual existing UI
components and services. It has no parallel implementations to synchronize.
Current paid plugins use these exports instead of private `@/` imports or
application globals. Canvas export/navigation methods belong to the existing
`src/utils.tsx` module. The builder rejects private UI imports and unknown
contract exports, including declarations written directly in the contract.

The host provides React and JSX runtimes, its application contexts, UI components,
API client, shared registries and notification service. Bundles receive these
same objects, so hooks/context/state are shared with the host. Private third-party
dependencies are bundled. The UI installs its host facade in each document,
including detached windows, before evaluating a runtime module. Discovery,
module/CSS ownership and the small runtime bridge belong to
`context/Extension.context.tsx`; no separate loader or host implementation is kept
under `plugin-api`.

Compatibility is independent of application release and plugin version numbers.
For example, a bundle released as 2.0.1 can run on UI 2.1.0 unchanged when the
components/services it uses remain compatible. A new UI release alone requires
neither a plugin version change nor a bundle rebuild.

The plugin's required API major must match the host and its minimum minor must
not exceed the host minor. Keep the API marker in `v1.ts` unchanged for compatible
UI-only changes. Additions to the public contract can increment the minor;
breaking signatures/behavior or an incompatible React ABI require a new major
and rebuilding affected plugins. `v1.ts` lists the shared dependency surface to
review; the re-exports use the current implementations directly. Local custom
plugins are not required to migrate to this contract.

An entrypoint default-exports its component and can export a synchronous
`activate(context)` function returning cleanup. Its context contains the selected
backend `server` and a scoped `t(key, variables?)` translator. Registration side
effects belong in activation rather than module evaluation. Login plugins can
activate without a user; operational registrations activate once a session exists.
On logout or backend changes the owning provider disposes registrations and CSS.
Stale pending downloads are released without mounting their components. Module
downloads are cached by backend asset URL; CSS ownership is reference counted.

## Translation ownership

The builder traverses local imports and includes companion
`<source-stem>.locale.<language>.json` files for the entrypoint and dependencies.
Each file retains the `{ "strings": { "key": "value" } }` format. If any locales
exist, English is required. Conflicting values for the same language/key within
one package are a build error. Separate bundles can use identical keys safely.

The existing locale engine accepts optional owner-scoped tables:
`resolveMessage(language, key, scope)`, `translate(key, variables, scope)`, and
`Locale.use(scope)`. The runtime bridge binds the bundle's dictionaries to these
same functions before component evaluation. Lookup uses the bundle's selected
language, then its English table, then the core fallback.

The host language/direction and React context remain shared. Bound dictionaries
also follow plugin components mounted by host banner/dialog services, so portals
retain translations. There is no second translation engine or global dictionary
merge. Local custom locale discovery continues through the existing UI build
mechanism.

## Validation

Start/check a test backend following `docs/testing.md`, then run:

```bash
.venv/bin/python -m pytest -v tests/unit/test_ui_plugin_packages.py
GULP_UI_TEST_WORKING_DIR=/path/to/test-working-dir \
  .venv/bin/python -m pytest -v -s -x tests/integration/test_ui_plugin_runtime.py
cd gulp-ui
node_modules/.bin/tsc --noEmit
CI=true node_modules/.bin/react-scripts test --watchAll=false --runInBand \
  --runTestsByPath src/test/integration/UiPlugins.test.tsx
```

Builder coverage requires internal build tooling. Live integration
coverage installs uniquely named fixture packages into the explicitly selected
test working directory and removes them afterward. It does not reset the DB.
To include Chromium coverage, also set `GULP_UI_BROWSER_URL` to a running UI build
containing the source/manifest pair from `tests/fixtures/ui_local_plugins` copied into its
`src/plugins` directory. The test refreshes `/login`, so serve this build with the
project's nginx configuration or equivalent SPA route fallback. Set
`GULP_PLAYWRIGHT_MODULE` to an installed Playwright module. Playwright is an
external test tool rather than a project dependency. The browser test checks
anonymous native module loading, CSS, shared context, translated portals, RTL,
local-source independence and backend-change disposal. Test the same archived
bundle against subsequent compatible UI builds as part of release validation.
Set `GULP_UI_PREBUILT_PACKAGE` to an archived fixture package directory to run
the same compiled bytes against multiple compatible UI builds without rebuilding
the fixture between runs. Set `GULP_UI_EXPECTED_VERSION` to check the tested UI's
public version file as well; this does not change the plugin API requirements.

To validate an existing full paid package in the browser as well, set
`GULP_UI_PAID_BROWSER_PACKAGE` to its output directory. The test installs only
those manifests/bundles into the designated test working directory, imports each
entrypoint against the real host, checks Story's transitive translations and
CyberSousChef registration disposal, and removes the installed packages afterward.

/** Validate every prebuilt paid entrypoint against the real public UI host API. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require(process.env.GULP_PLAYWRIGHT_MODULE);

async function main() {
  const directory = path.join(process.env.GULP_UI_PAID_BROWSER_PACKAGE, 'plugins/ui');
  const ids = fs.readdirSync(directory, { withFileTypes: true })
    .filter(entry => entry.isFile() && entry.name.endsWith('.json'))
    .map(entry => JSON.parse(fs.readFileSync(path.join(directory, entry.name))).id);
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    const backend = process.env.GULP_BASE_URL || 'http://localhost:8080';
    await page.addInitScript(server => {
      localStorage.setItem('__server', server);
      localStorage.setItem('settings.ui_language', 'it');
    }, backend);
    await page.goto(process.env.GULP_UI_BROWSER_URL);
    await page.waitForFunction(() => Boolean(globalThis.__gulpUiPluginHost));
    const result = await page.evaluate(async ({ backend, ids }) => {
      const { data } = await (await fetch(backend + '/ui_plugin_list')).json();
      const entries = data.filter(plugin => ids.includes(plugin.id));
      if (entries.length !== ids.length) throw new Error('Missing paid runtime manifest');
      const loaded = await Promise.all(entries.map(async entry => {
        const module = await import(backend + entry.entry_url);
        if (!module.default) throw new Error('Missing component: ' + entry.filename);
        return { entry, module };
      }));
      const host = globalThis.__gulpUiPluginHost;
      const api = host.createApi({});
      const surface = document.createElement('div');
      const canvas = document.createElement('canvas');
      canvas.id = 'canvas';
      canvas.width = canvas.height = 2;
      surface.appendChild(canvas);
      document.body.appendChild(surface);
      const sameCanvas = api.Canvas.snapshot()?.canvas === canvas;
      const originalClass = canvas.className;
      const png = api.Canvas.png();
      const cursorRestored = canvas.className === originalClass;
      surface.remove();
      const chef = loaded.find(item => item.entry.filename === 'cybersouschef.tsx');
      if (!chef?.module.activate) throw new Error('Missing CyberSousChef activation');
      const before = api.EventJsonTransforms.hasAny();
      const dispose = chef.module.activate({ server: backend, t: api.translate });
      const registered = api.EventJsonTransforms.hasAny();
      dispose();
      const after = api.EventJsonTransforms.hasAny();
      const story = loaded.find(item => item.entry.filename === 'Story.popover.tsx');
      return {
        count: loaded.length, before, registered, after, sameCanvas, cursorRestored,
        png: png?.startsWith('data:image/png;base64,'),
        storyEntry: story?.module.locales.en['plugins.story.addOrConnect'],
        storyDependency: story?.module.locales.en['plugins.story.changesSaved'],
        scopedOne: host.createApi({ en: { collision: 'one' } }).translate('collision'),
        scopedTwo: host.createApi({ en: { collision: 'two' } }).translate('collision'),
        core: api.translate('collision'),
      };
    }, { backend, ids });
    assert.equal(result.count, ids.length);
    assert.equal(result.before, false);
    assert.equal(result.registered, true);
    assert.equal(result.after, false);
    assert.equal(result.sameCanvas, true);
    assert.equal(result.png, true);
    assert.equal(result.cursorRestored, true);
    assert.equal(result.storyEntry, 'Add or connect to story');
    assert.ok(result.storyDependency);
    assert.equal(result.scopedOne, 'one');
    assert.equal(result.scopedTwo, 'two');
    assert.equal(result.core, 'collision');
    process.stdout.write(`Validated ${result.count} paid ESM modules, scoped dictionaries and CyberSousChef cleanup.\n`);
  } finally {
    await browser.close();
  }
}

main().catch(error => { process.stderr.write(error.stack + '\n'); process.exitCode = 1; });

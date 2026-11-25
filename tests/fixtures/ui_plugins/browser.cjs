/** Exercise native ESM loading against the real application and a live backend. */
const assert = require('node:assert/strict');
const { chromium } = require(process.env.GULP_PLAYWRIGHT_MODULE);

async function main() {
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    const uiUrl = process.env.GULP_UI_BROWSER_URL;
    const backend = process.env.GULP_BASE_URL || 'http://localhost:8080';
    if (process.env.GULP_UI_EXPECTED_VERSION) {
      const version = await page.request.get(new URL('/version', uiUrl).href);
      assert.ok(version.ok());
      assert.equal((await version.text()).trim(), process.env.GULP_UI_EXPECTED_VERSION);
    }
    const errors = [];
    const consoleErrors = [];
    const navigations = [];
    page.on('framenavigated', frame => {
      if (frame === page.mainFrame()) navigations.push(frame.url());
    });
    const assetRequests = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('console', message => {
      if (message.type() === 'error') consoleErrors.push(message.text());
    });
    page.on('request', request => {
      if (request.url().includes('/ui_plugin_asset/')) assetRequests.push(request);
    });
    await page.addInitScript(({ backend }) => {
      localStorage.setItem('__server', backend);
      localStorage.setItem('settings.ui_language', window.name || 'it');
      window.__pluginLifecycle = { activated: 0, disposed: 0 };
      window.addEventListener('runtime-fixture-activated', () => window.__pluginLifecycle.activated++);
      window.addEventListener('runtime-fixture-disposed', () => window.__pluginLifecycle.disposed++);
    }, { backend });

    await page.goto(uiUrl);
    await page.locator('[data-local-fixture]').waitFor();
    await page.getByRole('button', { name: 'Accesso runtime', exact: true }).waitFor();
    assert.equal(await page.locator('[data-runtime-login]').getAttribute('data-authenticated'), 'false');
    assert.equal(await page.locator('[data-runtime-fallback]').textContent(), 'English fallback');
    assert.equal(await page.locator('[data-runtime-login]').evaluate(element => getComputedStyle(element).outlineColor), 'rgb(12, 34, 56)');
    assert.equal(await page.evaluate(() => window.__pluginLifecycle.activated), 1);
    await page.getByRole('button', { name: 'Accesso runtime', exact: true }).click();
    assert.equal(await page.locator('[data-runtime-portal]').textContent(), 'Portale tradotto');

    await page.evaluate(() => { window.name = 'ar'; });
    await page.reload();
    await page.getByRole('button', { name: 'تسجيل الدخول', exact: true }).waitFor();
    await page.waitForURL('**/login**');
    assert.equal(await page.locator('html').getAttribute('dir'), 'rtl');

    // A backend change must release old registrations and acquire a fresh bundle/CSS.
    const alternateBackend = backend.includes('localhost') ? backend.replace('localhost', '127.0.0.1') : backend.replace('127.0.0.1', 'localhost');
    const beforeSwitch = await page.evaluate(() => ({
      lifecycle: window.__pluginLifecycle,
      server: localStorage.getItem('__server'),
      styles: Array.from(document.querySelectorAll('link[href*="ui_plugin_asset"]'), link => link.href),
    }));
    assert.deepEqual(beforeSwitch.lifecycle, { activated: 1, disposed: 0 }, JSON.stringify({ navigations }));
    assert.equal(beforeSwitch.server, backend);
    assert.ok(beforeSwitch.styles.every(url => url.startsWith(backend + '/')));
    await page.evaluate(server => {
      localStorage.setItem('__server', server);
      window.dispatchEvent(new Event('gulp-server-changed'));
    }, alternateBackend);
    try {
      await page.waitForFunction(() => window.__pluginLifecycle.activated === 2);
    } catch (error) {
      const state = await page.evaluate(() => ({
        lifecycle: window.__pluginLifecycle,
        server: localStorage.getItem('__server'),
        styles: Array.from(document.querySelectorAll('link[href*="ui_plugin_asset"]'), link => link.href),
      }));
      throw new Error(`${error.message}\nPlugin diagnostics: ${JSON.stringify({ beforeSwitch, state, navigations, errors, consoleErrors })}`);
    }
    assert.equal(await page.evaluate(() => window.__pluginLifecycle.disposed), 1);
    assert.equal(await page.locator('link[href*="ui_plugin_asset"]').count(), 1);

    await page.route('**/ui_plugin_list', route => route.abort());
    await page.evaluate(() => window.dispatchEvent(new Event('gulp-server-changed')));
    await page.waitForFunction(() => window.__pluginLifecycle.disposed === 2);
    await page.locator('[data-local-fixture]').waitFor();
    assert.equal(await page.locator('[data-runtime-login]').count(), 0);
    assert.equal(await page.locator('link[href*="ui_plugin_asset"]').count(), 0);
    assert.ok(assetRequests.length >= 4);
    for (const request of assetRequests) assert.equal(request.headers().token, undefined);
    assert.deepEqual(errors, []);
    process.stdout.write('Browser runtime, local discovery, context, CSS, locales, RTL and lifecycle checks passed.\n');
  } finally {
    await browser.close();
  }
}

main().catch(error => { process.stderr.write(error.stack + '\n'); process.exitCode = 1; });

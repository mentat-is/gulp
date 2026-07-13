import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const localeDir = join(root, "src/locales");
const pluginDir = join(root, "src/plugins");
const locales = Object.fromEntries(
  readdirSync(localeDir)
    .filter((file) => file.endsWith(".json"))
    .map((file) => [file.replace(/\.json$/, ""), JSON.parse(readFileSync(join(localeDir, file), "utf8"))]),
);

for (const file of readdirSync(pluginDir).filter((file) => file.includes(".locale.") && file.endsWith(".json"))) {
  const language = file.replace(/^.+\.locale\./, "").replace(/\.json$/, "");
  if (!locales[language]) continue;
  const pluginLocale = JSON.parse(readFileSync(join(pluginDir, file), "utf8"));
  Object.assign(locales[language].strings, pluginLocale.strings ?? {});
}

const en = locales.en;

assert.ok(en, "missing default locale: en");
assert.ok(en.strings, "default locale en is missing strings");

const placeholders = (value) => [...String(value).matchAll(/\{([^{}]+)\}/g)].map((match) => match[1]);
const table = { "settings.title": undefined };
const t = (key) => table[key] ?? en.strings[key] ?? key;
const keys = (locale) => Object.keys(locale.strings ?? {}).sort();

assert.equal(t("settings.title"), "Settings");
assert.equal(t("plugins.story.addOrConnect"), "Add or connect to story");
assert.equal(t("missing.key"), "missing.key");
for (const [code, locale] of Object.entries(locales)) {
  assert.equal(typeof locale.metadata?.["@label"], "string", `${code} is missing metadata.@label`);
  assert.match(locale.metadata?.["@dir"], /^(ltr|rtl)$/, `${code} has invalid metadata.@dir`);
  assert.ok(locale.strings && typeof locale.strings === "object", `${code} is missing strings`);
  if (locale.metadata?.["@sample"] !== "true") {
    assert.deepEqual(keys(locale), keys(en), `${code} locale keys differ from en`);
    for (const key of Object.keys(en.strings)) {
      assert.deepEqual(
        placeholders(locale.strings[key]),
        placeholders(en.strings[key]),
        `${code} locale placeholder mismatch for ${key}`,
      );
    }
  }
}

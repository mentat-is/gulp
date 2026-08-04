import enLocale from "./en.json";

export const defaultLanguage = "en";
export type Language = string;
export type LocaleDir = "ltr" | "rtl";

export interface LocaleDefinition {
  code: Language;
  label: string;
  dir: LocaleDir;
}

type LocaleTable = Record<string, string>;
type LocaleFile = {
  metadata?: Record<string, string>;
  strings?: LocaleTable;
};

interface LocaleModuleContext {
  keys: () => string[];
  <T = unknown>(id: string): T;
}

declare const require: {
  context: (path: string, deep?: boolean, filter?: RegExp) => LocaleModuleContext;
};

/** Returns a Jest-safe single-module context for the required English fallback. */
function createEnglishFallbackContext(): LocaleModuleContext {
  const context = (<T = unknown>(id: string): T => {
    if (id !== "./en.json") throw new Error(`Unknown fallback locale module: ${id}`);
    return enLocale as T;
  }) as LocaleModuleContext;
  context.keys = () => ["./en.json"];
  return context;
}

/** Returns an empty module context when plugin discovery is unavailable outside Webpack. */
function createEmptyLocaleContext(): LocaleModuleContext {
  const context = (<T = unknown>(id: string): T => {
    throw new Error(`Unknown plugin locale module: ${id}`);
  }) as LocaleModuleContext;
  context.keys = () => [];
  return context;
}

const localeContext = () =>
  process.env.NODE_ENV === "test"
    ? createEnglishFallbackContext()
    : require.context("./", false, /^\.\/.+\.json$/);

const pluginLocaleContext = () =>
  process.env.NODE_ENV === "test"
    ? createEmptyLocaleContext()
    : require.context("../plugins", false, /^\.\/.+\.locale\.[^.]+\.json$/);

const languageStorageKey = "settings.ui_language";

function codeFromFile(file: string): Language {
  return file.replace(/^\.\//, "").replace(/\.json$/, "");
}

function languageFromPluginLocaleFile(file: string): Language {
  return file.replace(/^\.\//, "").replace(/^.+\.locale\./, "").replace(/\.json$/, "");
}

function normalizeModule(module: LocaleFile | { default: LocaleFile }): LocaleFile {
  const maybeDefault = (module as { default?: unknown }).default;
  return maybeDefault && typeof maybeDefault === "object" ? maybeDefault as LocaleFile : module as LocaleFile;
}

function normalizeDir(value: string | undefined): LocaleDir {
  return value === "rtl" ? "rtl" : "ltr";
}

export function readStoredLanguage(): Language | null {
  if (typeof window === "undefined") return null;
  const value = window.localStorage.getItem(languageStorageKey);
  return isLanguage(value) ? value : null;
}

export function writeStoredLanguage(language: Language) {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(languageStorageKey, language);
}

function titleCaseCode(code: string): string {
  return code.replace(/[-_]/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function loadLocaleTablesFromDirectory() {
  const context = localeContext();
  const tables: Record<Language, LocaleTable> = {};
  const locales = context.keys().flatMap((file) => {
    const code = codeFromFile(file);
    const locale = normalizeModule(context<LocaleFile | { default: LocaleFile }>(file));
    const metadata = locale.metadata ?? {};
    if (metadata["@sample"] === "true") return [];

    const table = locale.strings ?? {};
    tables[code] = table;
    return [{
      code,
      label: metadata["@label"] ?? titleCaseCode(code),
      dir: normalizeDir(metadata["@dir"]),
    }];
  }).sort((left, right) => {
    if (left.code === defaultLanguage) return -1;
    if (right.code === defaultLanguage) return 1;
    return left.label.localeCompare(right.label);
  });

  const pluginContext = pluginLocaleContext();
  pluginContext.keys().forEach((file) => {
    const code = languageFromPluginLocaleFile(file);
    if (!tables[code]) return;
    const locale = normalizeModule(pluginContext<LocaleFile | { default: LocaleFile }>(file));
    Object.assign(tables[code], locale.strings ?? {});
  });

  return { tables, locales };
}

const loadedLocales = loadLocaleTablesFromDirectory();
const tables = loadedLocales.tables;
export const localeList: LocaleDefinition[] = loadedLocales.locales;
export const localeByCode = Object.fromEntries(localeList.map((locale) => [locale.code, locale])) as Record<Language, LocaleDefinition>;
let activeLanguage: Language = defaultLanguage;

if (!tables[defaultLanguage]) {
  throw new Error(`Missing default locale: ${defaultLanguage}`);
}

export function isLanguage(value: unknown): value is Language {
  return typeof value === "string" && value in tables;
}

export function resolveMessage(language: Language, key: string): string {
  return tables[language]?.[key] ?? tables[defaultLanguage][key] ?? key;
}

export function formatMessage(message: string, vars?: Record<string, string | number>) {
  if (!vars) return message;
  return message.replace(/\{(\w+)\}/g, (match, name) => String(vars[name] ?? match));
}

export function setActiveLanguage(language: Language) {
  activeLanguage = isLanguage(language) ? language : defaultLanguage;
}

export function translate(key: string, vars?: Record<string, string | number>): string {
  return formatMessage(resolveMessage(activeLanguage, key), vars);
}

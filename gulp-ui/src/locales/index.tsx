// This module provides localization utilities and a React context for managing the application's current language and text direction. 
// It loads locale definitions from JSON files in the same directory, allowing for easy addition of new languages by simply adding new JSON files. 
// The `Locale.Provider` component wraps the application and provides the current language, text direction, a function to change the language, and a translation function to its children via context.

// to use strings from this module, add them to a JSON file in the same directory (e.g., `en.json`) with the structure:
// {
//   "metadata": {
//     "@label": "English",
//     "@dir": "ltr"
//   },
//   "strings": {
//     "key1": "Translated string 1",
//     "key2": "Translated string 2 with a variable: {var}"
//   }
// }

// Then, in your React components, you can use the `Locale.use()` hook to access the current language and translation function:

// import { Locale } from "@/locales";

// function MyComponent() {
//   const { t } = Locale.use();
//   return <div>{t("key1")}</div>;
// }

import { Application } from "@/context/Application.context";
import { createContext, ReactNode, useCallback, useContext, useEffect, useMemo, useState } from "react";
import {
  defaultLanguage,
  formatMessage,
  isLanguage,
  Language,
  LocaleDir,
  localeByCode,
  localeList,
  resolveMessage,
  readStoredLanguage,
  setActiveLanguage,
  translate,
  writeStoredLanguage,
} from "./core";

export {
  defaultLanguage,
  isLanguage,
  localeList,
  resolveMessage,
  translate,
};
export type { Language, LocaleDir, LocaleDefinition } from "./core";

export namespace Locale {
  export interface ContextValue {
    language: Language;
    dir: LocaleDir;
    setLanguage: (language: Language) => void;
    t: (key: string, vars?: Record<string, string | number>) => string;
  }

  export const Context = createContext<ContextValue>({
    language: defaultLanguage,
    dir: "ltr",
    setLanguage: () => { },
    t: (key) => resolveMessage(defaultLanguage, key),
  });

  export function Provider({ children }: { children: ReactNode }) {
    const { app, Info } = Application.use();
    const userLanguage = app.general.user?.user_data?.ui_language;
    const [language, setLanguageState] = useState<Language>(() => (
      isLanguage(userLanguage)
        ? userLanguage
        : readStoredLanguage() ?? defaultLanguage
    ));

    useEffect(() => {
      if (isLanguage(userLanguage)) {
        writeStoredLanguage(userLanguage);
        if (language !== userLanguage) {
          setLanguageState(userLanguage);
        }
        return;
      }

      const storedLanguage = readStoredLanguage();
      if (!app.general.user) {
        const nextLanguage = storedLanguage ?? defaultLanguage;
        writeStoredLanguage(nextLanguage);
        if (language !== nextLanguage) {
          setLanguageState(nextLanguage);
        }
      }
    }, [app.general.user, language, userLanguage]);

    const locale = localeByCode[language] ?? localeByCode[defaultLanguage] ?? { code: defaultLanguage, label: defaultLanguage, dir: "ltr" };

    useEffect(() => {
      setActiveLanguage(language);
      document.documentElement.lang = language;
      document.documentElement.dir = locale.dir;
    }, [language, locale.dir]);

    const setLanguage = useCallback((next: Language) => {
      if (!isLanguage(next)) return;
      setLanguageState(next);
      writeStoredLanguage(next);
      if (app.general.user && app.general.user.user_data?.ui_language !== next) {
        void Info.user_set_data("ui_language", next);
      }
    }, [Info, app.general.user]);

    const t = useCallback<ContextValue["t"]>(
      (key, vars) => formatMessage(resolveMessage(language, key), vars),
      [language],
    );

    const value = useMemo(() => ({
      language,
      dir: locale.dir,
      setLanguage,
      t,
    }), [language, locale.dir, setLanguage, t]);

    return <Locale.Context.Provider value={value}>{children}</Locale.Context.Provider>;
  }

  export const use = () => useContext(Locale.Context);
}

import { Locale } from "@/locales";

/** A custom source plugin deliberately absent from the backend's runtime list. */
export default function LocalFixture() {
  const { t } = Locale.use();
  return <span data-local-fixture>{t("auth.login")}</span>;
}

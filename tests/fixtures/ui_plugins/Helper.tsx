import { Banner, Locale } from "@gulp/ui-plugin-sdk/v1";

/** Uses a transitive locale from a component mounted by the host banner service. */
export function Helper() {
  const { t } = Locale.use();
  return <Banner title={t("fixture.helper")}><span data-runtime-portal>{t("fixture.helper")}</span></Banner>;
}

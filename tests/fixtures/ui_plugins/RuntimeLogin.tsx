import { Application, Button, Locale } from "@gulp/ui-plugin-sdk/v1";
import type { UiPluginActivationContext } from "@gulp/ui-plugin-sdk/v1";
import { Helper } from "./Helper";
import styles from "./RuntimeLogin.module.css";

/** Exercises the public API, host context and translated portal before login. */
export default function RuntimeLogin() {
  const { app, spawnBanner } = Application.use();
  const { t } = Locale.use();
  return (
    <div className={styles.marker} data-runtime-login data-authenticated={Boolean(app.general.user)}>
      <Button onClick={() => spawnBanner(<Helper />)}>{t("fixture.login")}</Button>
      <span data-runtime-fallback>{t("fixture.fallback")}</span>
    </div>
  );
}

/** Announces activation so browser coverage can verify pre-auth lifecycle cleanup. */
export function activate(_context: UiPluginActivationContext): () => void {
  window.dispatchEvent(new Event("runtime-fixture-activated"));
  return () => window.dispatchEvent(new Event("runtime-fixture-disposed"));
}

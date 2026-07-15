import type { App } from "@/entities/App";

/**
 * Creates an authentication-free renderer fixture that cannot initialize user services.
 * @param app Existing application fixture state.
 * @returns Application fixture with skipped authentication and no active user.
 */
export function createOfflineRendererTestApp(app: App.Type): App.Type {
	return {
		...app,
		general: {
			...app.general,
			skippedAuth: true,
			user: null,
		},
	};
}

import { createOfflineRendererTestApp } from "@/page/RendererTest.fixture";
import type { App } from "@/entities/App";
import type { User } from "@/entities/User";

describe("RendererTest offline fixture", () => {
	it("sets skipped authentication and removes the active user", () => {
		const authenticatedUser = { id: "user-1" as User.Id } as User.Type;
		const app = {
			general: {
				user: authenticatedUser,
				skippedAuth: false,
			},
		} as App.Type;

		const offlineApp = createOfflineRendererTestApp(app);

		expect(offlineApp.general.user).toBeNull();
		expect(offlineApp.general.skippedAuth).toBe(true);
		expect(app.general.user).toBe(authenticatedUser);
	});
});

import type { User } from "@/entities/User";
import {
	createUserEventStoreCacheNamespaceId,
	EVENT_STORE_USER_ID_STORAGE_KEY,
	normalizeEventStoreServerIdentity,
	resolveEventStoreCacheNamespaceId,
} from "@/event-store/EventStore.namespace";

describe("EventStore cache namespace", () => {
	it("is stable across tab coordinators for the same backend user", () => {
		const userId = "user-one" as User.Id;
		const first = resolveEventStoreCacheNamespaceId(
			userId,
			"https://gulp.example.test/",
			"tab-one",
			null,
		);
		const second = resolveEventStoreCacheNamespaceId(
			userId,
			"https://gulp.example.test",
			"tab-two",
			null,
		);

		expect(first).toBe(second);
		expect(first).toBe(
			createUserEventStoreCacheNamespaceId(
				userId,
				"https://gulp.example.test",
			),
		);
	});

	it("isolates different users and backend authorities", () => {
		const firstUser = createUserEventStoreCacheNamespaceId(
			"user-one",
			"https://gulp.example.test",
		);
		const secondUser = createUserEventStoreCacheNamespaceId(
			"user-two",
			"https://gulp.example.test",
		);
		const secondServer = createUserEventStoreCacheNamespaceId(
			"user-one",
			"https://other-gulp.example.test",
		);

		expect(firstUser).not.toBe(secondUser);
		expect(firstUser).not.toBe(secondServer);
	});

	it("uses persisted login identity only when authenticated state is unavailable", () => {
		const storage = {
			getItem: (key: string): string | null =>
				key === EVENT_STORE_USER_ID_STORAGE_KEY ? "restored-user" : null,
		};

		expect(
			resolveEventStoreCacheNamespaceId(
				null,
				"https://gulp.example.test",
				"new-tab",
				storage,
			),
		).toBe(
			createUserEventStoreCacheNamespaceId(
				"restored-user",
				"https://gulp.example.test",
			),
		);
		expect(
			resolveEventStoreCacheNamespaceId(
				"authenticated-user",
				"https://gulp.example.test",
				"new-tab",
				storage,
			),
		).toBe(
			createUserEventStoreCacheNamespaceId(
				"authenticated-user",
				"https://gulp.example.test",
			),
		);
	});

	it("falls back to an isolated tab namespace when user recovery is unavailable", () => {
		const blockedStorage = {
			getItem: (): string | null => {
				throw new DOMException("blocked");
			},
		};
		const first = resolveEventStoreCacheNamespaceId(
			null,
			"https://gulp.example.test",
			"tab-one",
			blockedStorage,
		);
		const second = resolveEventStoreCacheNamespaceId(
			null,
			"https://gulp.example.test",
			"tab-two",
			null,
		);

		expect(first).not.toBe(second);
		expect(first.includes("coordinator:tab-one")).toBe(true);
	});

	it("canonicalizes default ports, case, and trailing slashes", () => {
		expect(
			normalizeEventStoreServerIdentity("HTTPS://GULP.EXAMPLE.TEST:443/api/"),
		).toBe("https://gulp.example.test/api");
	});
});

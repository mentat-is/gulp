import type { User } from "@/entities/User";

export const EVENT_STORE_USER_ID_STORAGE_KEY = "__user_id";
const EVENT_STORE_USER_NAMESPACE_PREFIX = "user";
const EVENT_STORE_FALLBACK_NAMESPACE_PREFIX = "coordinator";

interface EventStoreNamespaceStorage {
	getItem(key: string): string | null;
}

/**
 * Canonicalizes the configured backend so equivalent trailing-slash URLs share
 * a cache while distinct backend origins or base paths remain isolated.
 * @param server Configured gULP backend base URL.
 * @returns Stable backend identity used inside the cache namespace.
 */
export function normalizeEventStoreServerIdentity(server: string): string {
	const normalizedServer = server.trim();
	if (normalizedServer.length === 0) {
		throw new TypeError("EventStore server identity cannot be empty.");
	}
	try {
		const parsed = new URL(normalizedServer);
		const pathname = parsed.pathname.replace(/\/+$/, "");
		return `${parsed.origin}${pathname}`;
	} catch {
		return normalizedServer.replace(/\/+$/, "");
	}
}

/**
 * Creates the persistent EventStore namespace owned by one authenticated user.
 * @param userId Backend-authenticated user identity.
 * @param server Configured backend whose event authority owns the cache.
 * @returns Stable cache namespace shared by that user across browser sessions.
 */
export function createUserEventStoreCacheNamespaceId(
	userId: User.Id | string,
	server: string,
): string {
	const normalizedUserId = String(userId).trim();
	if (normalizedUserId.length === 0) {
		throw new TypeError("EventStore user id cannot be empty.");
	}
	const serverIdentity = normalizeEventStoreServerIdentity(server);
	return `server:${encodeURIComponent(
		serverIdentity,
	)}:${EVENT_STORE_USER_NAMESPACE_PREFIX}:${encodeURIComponent(
		normalizedUserId,
	)}`;
}

/**
 * Resolves the EventStore identity from authenticated state, reload recovery,
 * or an isolated coordinator fallback when no user can be proven.
 * @param authenticatedUserId Authoritative user from the current application state.
 * @param server Configured backend whose event authority owns the cache.
 * @param fallbackCoordinatorId Tab-scoped identity used only without authenticated state.
 * @param storage Same-origin user-id persistence, injectable for tests.
 * @returns A non-empty namespace safe for IndexedDB, revision channels, and locks.
 */
export function resolveEventStoreCacheNamespaceId(
	authenticatedUserId: User.Id | string | null | undefined,
	server: string,
	fallbackCoordinatorId: string,
	storage?: EventStoreNamespaceStorage | null,
): string {
	const normalizedAuthenticatedUserId = authenticatedUserId
		? String(authenticatedUserId).trim()
		: "";
	if (normalizedAuthenticatedUserId.length > 0) {
		return createUserEventStoreCacheNamespaceId(
			normalizedAuthenticatedUserId,
			server,
		);
	}

	let selectedStorage = storage;
	if (storage === undefined) {
		try {
			selectedStorage = globalThis.localStorage;
		} catch {
			selectedStorage = null;
		}
	}
	let storedUserId: string | null = null;
	try {
		storedUserId =
			selectedStorage?.getItem(EVENT_STORE_USER_ID_STORAGE_KEY) ?? null;
	} catch {
		// Storage can be blocked in hardened/private contexts. Keep the realm isolated.
	}
	if (storedUserId?.trim()) {
		return createUserEventStoreCacheNamespaceId(storedUserId, server);
	}

	const normalizedCoordinatorId = fallbackCoordinatorId.trim();
	if (normalizedCoordinatorId.length === 0) {
		throw new TypeError("EventStore fallback coordinator id cannot be empty.");
	}
	return `server:${encodeURIComponent(
		normalizeEventStoreServerIdentity(server),
	)}:${EVENT_STORE_FALLBACK_NAMESPACE_PREFIX}:${encodeURIComponent(
		normalizedCoordinatorId,
	)}`;
}

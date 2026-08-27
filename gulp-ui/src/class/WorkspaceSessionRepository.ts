import type { App } from "@/entities/App";
import type { Context } from "@/entities/Context";
import type { Doc } from "@/entities/Doc";
import type { Icon } from "@/ui/Icon";
import type { Operation } from "@/entities/Operation";
import type { Query } from "@/entities/Query";
import type { Source } from "@/entities/Source";
import type { User } from "@/entities/User";

export const WORKSPACE_SESSION_KEY_PREFIX = "ui_workspace_sessions_v2:";
export const LEGACY_AUTOSAVE_PREFIX = "Autosaved session ";

export type WorkspaceSessionKind = "autosave" | "named";

export interface WorkspaceSessionSourceSetting
	extends Partial<Source.Type["settings"]> {
	pinned?: boolean;
}

export interface WorkspaceSessionSnapshot {
	selected: {
		files: Source.Id[];
		contexts: Context.Id[];
	};
	timeline: {
		scale: number;
		frame: { min: number; max: number };
		scroll: { x: number; y: number };
		filter: string;
		target: Doc.Type | null;
	};
	filters: Partial<Record<Source.Id, Query.Type>>;
	hidden: App.Type["hidden"];
	source_settings: {
		source_settings: Record<Source.Id, WorkspaceSessionSourceSetting>;
	};
}

export interface StoredWorkspaceSession {
	id: string;
	operation_id: Operation.Id;
	kind: WorkspaceSessionKind;
	name: string;
	icon: Icon.Name;
	color: string;
	created_at_msec: number;
	updated_at_msec: number;
	snapshot: WorkspaceSessionSnapshot;
}

export interface OperationWorkspaceSessionBucket {
	version: 2;
	autosave: StoredWorkspaceSession | null;
	named: Record<string, StoredWorkspaceSession>;
}

interface WorkspaceSessionUser {
	id: User.Id;
	user_data?: Record<string, unknown>;
}

interface WorkspaceSessionBroadcastMessage {
	source_id: string;
	operation_id: Operation.Id;
	bucket: OperationWorkspaceSessionBucket;
}

interface OperationWriteState {
	active: boolean;
	pending: OperationWorkspaceSessionBucket | null;
	waiters: Array<(success: boolean) => void>;
}

export type WorkspaceSessionBucketWriter = (
	userId: User.Id,
	key: string,
	bucket: OperationWorkspaceSessionBucket,
) => Promise<WorkspaceSessionUser | null | undefined>;

/**
 * Raised when a named session duplicates another name in the same operation.
 */
export class WorkspaceSessionNameExistsError extends Error {
	public constructor(name: string) {
		super(`Workspace session already exists: ${name}`);
		this.name = "WorkspaceSessionNameExistsError";
	}
}

/**
 * Checks whether an unknown JSON value is a non-array object.
 * @param value JSON value to inspect.
 * @returns True when the value can be read as a record.
 */
function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Checks whether an unknown JSON value is a finite number.
 * @param value JSON value to inspect.
 * @returns True when the value is a finite number.
 */
function isFiniteNumber(value: unknown): value is number {
	return typeof value === "number" && Number.isFinite(value);
}

/**
 * Checks whether an unknown JSON value is an array containing only strings.
 * @param value JSON value to inspect.
 * @returns True when every array entry is a string.
 */
function isStringArray(value: unknown): value is string[] {
	return Array.isArray(value) && value.every((entry) => typeof entry === "string");
}

/**
 * Generates a client-owned identifier without importing UI helpers that depend on Info.
 * @returns A UUID-like session identifier.
 */
function createSessionId(): string {
	if (typeof globalThis.crypto?.randomUUID === "function") {
		return globalThis.crypto.randomUUID();
	}

	return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (token) => {
		const random = Math.floor(Math.random() * 16);
		const value = token === "x" ? random : (random & 0x3) | 0x8;
		return value.toString(16);
	});
}

/**
 * Builds a deterministic identifier for a legacy array entry.
 * @param seed Stable legacy session identity fields.
 * @returns A deterministic client identifier.
 */
function createLegacySessionId(seed: string): string {
	let hash = 0x811c9dc5;
	for (let index = 0; index < seed.length; index++) {
		hash ^= seed.charCodeAt(index);
		hash = Math.imul(hash, 0x01000193);
	}
	return `legacy-${(hash >>> 0).toString(16).padStart(8, "0")}`;
}

/**
 * Converts application state into JSON-compatible data without retaining mutable references.
 * @param value Value to clone.
 * @returns A detached JSON-compatible clone.
 */
export function cloneWorkspaceSessionValue<T>(value: T): T {
	return JSON.parse(
		JSON.stringify(value, (_key, nestedValue) =>
			typeof nestedValue === "bigint" ? nestedValue.toString() : nestedValue,
		),
	) as T;
}

/**
 * Serializes semantic session data with stable object-key ordering.
 * @param value Session value to serialize.
 * @returns Stable JSON used to suppress unchanged writes.
 */
export function serializeWorkspaceSessionValue(value: unknown): string {
	const normalize = (nestedValue: unknown): unknown => {
		if (typeof nestedValue === "bigint") return nestedValue.toString();
		if (Array.isArray(nestedValue)) return nestedValue.map(normalize);
		if (!isRecord(nestedValue)) return nestedValue;

		return Object.fromEntries(
			Object.keys(nestedValue)
				.sort()
				.map((key) => [key, normalize(nestedValue[key])]),
		);
	};

	return JSON.stringify(normalize(value));
}

/**
 * Returns the top-level JSONB key assigned to one operation.
 * @param operationId Operation owning the session bucket.
 * @returns Stable user_data key.
 */
export function workspaceSessionKey(operationId: Operation.Id): string {
	return `${WORKSPACE_SESSION_KEY_PREFIX}${operationId}`;
}

/**
 * Parses a persisted session snapshot using the minimum structural requirements.
 * @param value Unknown persisted value.
 * @returns A typed detached snapshot, or null when required state is missing.
 */
function parseSnapshot(value: unknown): WorkspaceSessionSnapshot | null {
	if (!isRecord(value)) return null;
	if (!isRecord(value.selected) || !isRecord(value.timeline)) return null;
	const frame = value.timeline.frame;
	const scroll = value.timeline.scroll;
	if (
		!isStringArray(value.selected.files) ||
		!isStringArray(value.selected.contexts) ||
		!isRecord(frame) ||
		!isRecord(scroll) ||
		!isFiniteNumber(value.timeline.scale) ||
		!isFiniteNumber(frame.min) ||
		!isFiniteNumber(frame.max) ||
		!isFiniteNumber(scroll.x) ||
		!isFiniteNumber(scroll.y) ||
		typeof value.timeline.filter !== "string"
	) {
		return null;
	}

	const persistedSourceSettings =
		isRecord(value.source_settings) &&
		isRecord(value.source_settings.source_settings)
			? value.source_settings.source_settings
			: {};
	const sourceSettings = Object.entries(persistedSourceSettings).reduce<
		Record<Source.Id, WorkspaceSessionSourceSetting>
	>((settings, [sourceId, sourceSetting]) => {
		if (isRecord(sourceSetting)) {
			settings[sourceId as Source.Id] =
				sourceSetting as unknown as WorkspaceSessionSourceSetting;
		}
		return settings;
	}, {});
	const snapshot: WorkspaceSessionSnapshot = {
		selected: {
			files: value.selected.files as Source.Id[],
			contexts: value.selected.contexts as Context.Id[],
		},
		timeline: {
			scale: value.timeline.scale,
			frame: { min: frame.min, max: frame.max },
			scroll: { x: scroll.x, y: scroll.y },
			filter: value.timeline.filter,
			target: isRecord(value.timeline.target)
				? (value.timeline.target as unknown as Doc.Type)
				: null,
		},
		filters: (isRecord(value.filters) ? value.filters : {}) as Partial<
			Record<Source.Id, Query.Type>
		>,
		hidden: (isRecord(value.hidden) ? value.hidden : {}) as App.Type["hidden"],
		source_settings: { source_settings: sourceSettings },
	};

	return cloneWorkspaceSessionValue(snapshot);
}

/**
 * Parses one v2 stored session and enforces its operation scope.
 * @param value Unknown persisted session.
 * @param operationId Expected operation.
 * @returns A valid stored session or null.
 */
function parseStoredSession(
	value: unknown,
	operationId: Operation.Id,
): StoredWorkspaceSession | null {
	if (!isRecord(value)) return null;
	if (
		typeof value.id !== "string" ||
		typeof value.name !== "string" ||
		typeof value.icon !== "string" ||
		typeof value.color !== "string" ||
		(value.kind !== "autosave" && value.kind !== "named")
	) {
		return null;
	}

	const snapshot = parseSnapshot(value.snapshot);
	if (!snapshot) return null;

	return {
		id: value.id,
		operation_id: operationId,
		kind: value.kind,
		name: value.name,
		icon: value.icon as Icon.Name,
		color: value.color,
		created_at_msec:
			typeof value.created_at_msec === "number" ? value.created_at_msec : 0,
		updated_at_msec:
			typeof value.updated_at_msec === "number" ? value.updated_at_msec : 0,
		snapshot,
	};
}

/**
 * Parses an operation bucket while skipping only malformed individual sessions.
 * @param value Unknown user_data value.
 * @param operationId Expected operation.
 * @returns A normalized v2 bucket, or null when the bucket is not v2.
 */
export function parseWorkspaceSessionBucket(
	value: unknown,
	operationId: Operation.Id,
): OperationWorkspaceSessionBucket | null {
	if (!isRecord(value) || value.version !== 2 || !isRecord(value.named)) {
		return null;
	}

	const named = Object.values(value.named).reduce<
		Record<string, StoredWorkspaceSession>
	>((sessions, candidate) => {
		const parsed = parseStoredSession(candidate, operationId);
		if (parsed?.kind === "named") sessions[parsed.id] = parsed;
		return sessions;
	}, {});
	const autosave = parseStoredSession(value.autosave, operationId);

	return {
		version: 2,
		autosave: autosave?.kind === "autosave" ? autosave : null,
		named,
	};
}

/**
 * Converts one legacy flat session into a v2 stored session.
 * @param value Legacy session value.
 * @param operationId Operation selected by the legacy session.
 * @param index Original array position used for stable fallback identity.
 * @returns Converted session or null when required state is malformed.
 */
function convertLegacySession(
	value: unknown,
	operationId: Operation.Id,
	index: number,
): StoredWorkspaceSession | null {
	if (!isRecord(value) || typeof value.name !== "string") return null;
	const selected = isRecord(value.selected) ? value.selected : null;
	if (selected?.operations !== operationId) return null;

	const snapshot = parseSnapshot({
		selected: {
			files: selected.files,
			contexts: selected.contexts,
		},
		timeline: value.timeline,
		filters: value.filters,
		hidden: value.hidden,
		source_settings: value.source_settings,
	});
	if (!snapshot) return null;

	const kind: WorkspaceSessionKind = value.name.startsWith(
		LEGACY_AUTOSAVE_PREFIX,
	)
		? "autosave"
		: "named";
	const parsedTimestamp =
		kind === "autosave"
			? Date.parse(value.name.slice(LEGACY_AUTOSAVE_PREFIX.length))
			: Number.NaN;
	const timestamp = Number.isFinite(parsedTimestamp) ? parsedTimestamp : index;

	return {
		id: createLegacySessionId(
			`${operationId}:${kind}:${value.name}:${index}`,
		),
		operation_id: operationId,
		kind,
		name: value.name,
		icon:
			typeof value.icon === "string"
				? (value.icon as Icon.Name)
				: ("Status" as Icon.Name),
		color: typeof value.color === "string" ? value.color : "var(--green-700)",
		created_at_msec: timestamp,
		updated_at_msec: timestamp,
		snapshot,
	};
}

/**
 * Derives one operation bucket from the legacy user_data.sessions array.
 * @param value Unknown legacy sessions value.
 * @param operationId Operation whose sessions should be selected.
 * @returns Converted bucket with the newest autosave retained.
 */
export function convertLegacyWorkspaceSessions(
	value: unknown,
	operationId: Operation.Id,
): OperationWorkspaceSessionBucket {
	const converted = (Array.isArray(value) ? value : [])
		.map((session, index) => convertLegacySession(session, operationId, index))
		.filter((session): session is StoredWorkspaceSession => session !== null);
	const autosave =
		converted
			.filter((session) => session.kind === "autosave")
			.sort((left, right) => right.updated_at_msec - left.updated_at_msec)[0] ??
		null;
	const named = converted
		.filter((session) => session.kind === "named")
		.reduce<Record<string, StoredWorkspaceSession>>((sessions, session) => {
			sessions[session.id] = session;
			return sessions;
		}, {});

	return { version: 2, autosave, named };
}

/**
 * Owns typed workspace-session state without publishing persistence changes through React.
 */
export class WorkspaceSessionRepository {
	private userId: User.Id | null = null;
	private userData: Record<string, unknown> = {};
	private readonly buckets = new Map<
		Operation.Id,
		OperationWorkspaceSessionBucket
	>();
	private readonly writes = new Map<Operation.Id, OperationWriteState>();
	private readonly listeners = new Map<Operation.Id, Set<() => void>>();
	private channel: BroadcastChannel | null = null;
	private readonly sourceId = createSessionId();

	public constructor(private readonly writeBucket: WorkspaceSessionBucketWriter) {}

	/**
	 * Initializes the repository for an authenticated user without replacing a same-user cache.
	 * @param user Authenticated user profile containing user_data.
	 * @returns Nothing.
	 */
	public initialize(user: WorkspaceSessionUser): void {
		if (this.userId === user.id) return;

		this.channel?.close();
		this.userId = user.id;
		this.userData = isRecord(user.user_data) ? { ...user.user_data } : {};
		this.buckets.clear();
		this.writes.clear();
		this.listeners.clear();

		if (typeof globalThis.BroadcastChannel !== "function") {
			this.channel = null;
			return;
		}

		this.channel = new BroadcastChannel(`gulp-workspace-sessions:${user.id}`);
		this.channel.onmessage = (event: MessageEvent<unknown>) => {
			this.applyBroadcastMessage(event.data);
		};
	}

	/**
	 * Replaces cached user_data after an explicit server refresh.
	 * @param user Refreshed authenticated user.
	 * @returns Nothing.
	 */
	public refresh(user: WorkspaceSessionUser): void {
		if (this.userId !== user.id) {
			this.initialize(user);
			return;
		}
		this.userData = isRecord(user.user_data) ? { ...user.user_data } : {};
		this.buckets.clear();
		this.notifyAll();
	}

	/**
	 * Releases browser resources owned by the repository.
	 * @returns Nothing.
	 */
	public close(): void {
		this.channel?.close();
		this.channel = null;
		this.userId = null;
		this.userData = {};
		this.buckets.clear();
		this.writes.clear();
		this.listeners.clear();
	}

	/**
	 * Subscribes to operation session changes from successful writes or other tabs.
	 * @param operationId Operation to observe.
	 * @param listener Listener invoked after the cached bucket changes.
	 * @returns Unsubscribe callback.
	 */
	public subscribe(
		operationId: Operation.Id,
		listener: () => void,
	): () => void {
		const operationListeners =
			this.listeners.get(operationId) ?? new Set<() => void>();
		operationListeners.add(listener);
		this.listeners.set(operationId, operationListeners);
		return () => operationListeners.delete(listener);
	}

	/**
	 * Lists cached sessions for one operation without a backend read.
	 * @param operationId Operation whose sessions should be listed.
	 * @returns Autosave followed by named sessions ordered by recent update.
	 */
	public list(operationId: Operation.Id): StoredWorkspaceSession[] {
		const bucket = this.getBucket(operationId);
		return [
			...(bucket.autosave ? [bucket.autosave] : []),
			...Object.values(bucket.named).sort(
				(left, right) => right.updated_at_msec - left.updated_at_msec,
			),
		].map(cloneWorkspaceSessionValue);
	}

	/**
	 * Creates one named session and persists the complete operation bucket.
	 * @param operationId Owning operation.
	 * @param metadata Named-session display metadata.
	 * @param snapshot Detached operation snapshot.
	 * @returns Persisted session, or null when the request fails.
	 */
	public async createNamed(
		operationId: Operation.Id,
		metadata: { name: string; icon: Icon.Name; color: string },
		snapshot: WorkspaceSessionSnapshot,
	): Promise<StoredWorkspaceSession | null> {
		const name = metadata.name.trim();
		const current = this.getBucket(operationId);
		if (
			Object.values(current.named).some((session) => session.name === name)
		) {
			throw new WorkspaceSessionNameExistsError(name);
		}

		const timestamp = Date.now();
		const session: StoredWorkspaceSession = {
			id: createSessionId(),
			operation_id: operationId,
			kind: "named",
			name,
			icon: metadata.icon,
			color: metadata.color,
			created_at_msec: timestamp,
			updated_at_msec: timestamp,
			snapshot: cloneWorkspaceSessionValue(snapshot),
		};
		const next: OperationWorkspaceSessionBucket = {
			...current,
			named: { ...current.named, [session.id]: session },
		};

		return (await this.commit(operationId, current, next)) ? session : null;
	}

	/**
	 * Replaces the rolling autosave for one operation.
	 * @param operationId Owning operation.
	 * @param snapshot Detached operation snapshot.
	 * @returns True when the bucket is persisted.
	 */
	public async upsertAutosave(
		operationId: Operation.Id,
		snapshot: WorkspaceSessionSnapshot,
	): Promise<boolean> {
		const current = this.getBucket(operationId);
		const timestamp = Date.now();
		const autosave: StoredWorkspaceSession = {
			id: current.autosave?.id ?? createSessionId(),
			operation_id: operationId,
			kind: "autosave",
			name: `${LEGACY_AUTOSAVE_PREFIX}${new Date(timestamp).toUTCString()}`,
			icon: "RefreshClockwise" as Icon.Name,
			color: "var(--green-700)",
			created_at_msec: current.autosave?.created_at_msec ?? timestamp,
			updated_at_msec: timestamp,
			snapshot: cloneWorkspaceSessionValue(snapshot),
		};
		const next: OperationWorkspaceSessionBucket = {
			...current,
			autosave,
		};

		return this.commit(operationId, current, next);
	}

	/**
	 * Renames one named session within its operation.
	 * @param operationId Owning operation.
	 * @param sessionId Stable session identifier.
	 * @param requestedName New display name.
	 * @returns True when the renamed bucket is persisted.
	 */
	public async rename(
		operationId: Operation.Id,
		sessionId: string,
		requestedName: string,
	): Promise<boolean> {
		const name = requestedName.trim();
		const current = this.getBucket(operationId);
		const session = current.named[sessionId];
		if (!session) return false;
		if (
			Object.values(current.named).some(
				(candidate) => candidate.id !== sessionId && candidate.name === name,
			)
		) {
			throw new WorkspaceSessionNameExistsError(name);
		}
		if (session.name === name) return true;

		const next: OperationWorkspaceSessionBucket = {
			...current,
			named: {
				...current.named,
				[sessionId]: {
					...session,
					name,
					updated_at_msec: Date.now(),
				},
			},
		};
		return this.commit(operationId, current, next);
	}

	/**
	 * Deletes several sessions from one operation with one bucket write.
	 * @param operationId Owning operation.
	 * @param sessionIds Stable identifiers selected for deletion.
	 * @returns True when the updated bucket is persisted.
	 */
	public async delete(
		operationId: Operation.Id,
		sessionIds: string[],
	): Promise<boolean> {
		const current = this.getBucket(operationId);
		const selectedIds = new Set(sessionIds);
		const named = Object.fromEntries(
			Object.entries(current.named).filter(([id]) => !selectedIds.has(id)),
		);
		const next: OperationWorkspaceSessionBucket = {
			...current,
			autosave:
				current.autosave && selectedIds.has(current.autosave.id)
					? null
					: current.autosave,
			named,
		};
		if (
			next.autosave === current.autosave &&
			Object.keys(next.named).length === Object.keys(current.named).length
		) {
			return true;
		}

		return this.commit(operationId, current, next);
	}

	/**
	 * Applies one immutable maintenance transform to every session in an operation.
	 * @param operationId Owning operation.
	 * @param transform Session transform.
	 * @returns True when no write is needed or the changed bucket is persisted.
	 */
	public async updateAll(
		operationId: Operation.Id,
		transform: (session: StoredWorkspaceSession) => StoredWorkspaceSession,
	): Promise<boolean> {
		const current = this.getBucket(operationId);
		const autosave = current.autosave
			? transform(cloneWorkspaceSessionValue(current.autosave))
			: null;
		const named = Object.fromEntries(
			Object.values(current.named).map((session) => {
				const transformed = transform(cloneWorkspaceSessionValue(session));
				return [transformed.id, transformed];
			}),
		);
		const next: OperationWorkspaceSessionBucket = {
			version: 2,
			autosave,
			named,
		};
		if (
			serializeWorkspaceSessionValue(current) ===
			serializeWorkspaceSessionValue(next)
		) {
			return true;
		}
		return this.commit(operationId, current, next);
	}

	/**
	 * Returns an operation bucket from v2 data or a lazy legacy conversion.
	 * @param operationId Operation whose bucket should be read.
	 * @returns Cached normalized bucket.
	 */
	private getBucket(
		operationId: Operation.Id,
	): OperationWorkspaceSessionBucket {
		const cached = this.buckets.get(operationId);
		if (cached) return cached;

		const key = workspaceSessionKey(operationId);
		const bucket =
			parseWorkspaceSessionBucket(this.userData[key], operationId) ??
			convertLegacyWorkspaceSessions(this.userData.sessions, operationId);
		this.buckets.set(operationId, bucket);
		return bucket;
	}

	/**
	 * Applies a local mutation, queues its write, and rolls it back when still current after failure.
	 * @param operationId Operation being changed.
	 * @param previous Bucket before the mutation.
	 * @param next Bucket to persist.
	 * @returns True when persistence succeeds.
	 */
	private async commit(
		operationId: Operation.Id,
		previous: OperationWorkspaceSessionBucket,
		next: OperationWorkspaceSessionBucket,
	): Promise<boolean> {
		this.buckets.set(operationId, next);
		const success = await this.enqueueWrite(operationId, next);
		if (!success && this.buckets.get(operationId) === next) {
			this.buckets.set(operationId, previous);
		}
		if (success) this.notify(operationId);
		return success;
	}

	/**
	 * Coalesces queued operation writes while keeping at most one request active.
	 * @param operationId Operation being persisted.
	 * @param bucket Latest desired bucket.
	 * @returns Promise resolved by the write that includes this mutation.
	 */
	private enqueueWrite(
		operationId: Operation.Id,
		bucket: OperationWorkspaceSessionBucket,
	): Promise<boolean> {
		const state = this.writes.get(operationId) ?? {
			active: false,
			pending: null,
			waiters: [],
		};
		this.writes.set(operationId, state);

		const result = new Promise<boolean>((resolve) => {
			state.pending = cloneWorkspaceSessionValue(bucket);
			state.waiters.push(resolve);
		});
		if (!state.active) void this.drainWrites(operationId, state);
		return result;
	}

	/**
	 * Drains the latest queued bucket after each active write.
	 * @param operationId Operation being persisted.
	 * @param state Mutable operation queue state.
	 * @returns Promise settling after the queue becomes empty.
	 */
	private async drainWrites(
		operationId: Operation.Id,
		state: OperationWriteState,
	): Promise<void> {
		state.active = true;
		while (state.pending) {
			const bucket = state.pending;
			const waiters = state.waiters;
			state.pending = null;
			state.waiters = [];
			const success = await this.persist(operationId, bucket);
			waiters.forEach((resolve) => resolve(success));
		}
		state.active = false;
	}

	/**
	 * Writes one top-level operation key and updates only the private repository cache.
	 * @param operationId Operation being persisted.
	 * @param bucket Bucket value sent through /user_update.
	 * @returns True when the backend returns an updated user.
	 */
	private async persist(
		operationId: Operation.Id,
		bucket: OperationWorkspaceSessionBucket,
	): Promise<boolean> {
		if (!this.userId) return false;
		const key = workspaceSessionKey(operationId);
		try {
			const user = await this.writeBucket(this.userId, key, bucket);
			if (!user) return false;

			if (isRecord(user.user_data)) {
				this.userData = { ...this.userData, ...user.user_data };
			}
			this.buckets.forEach((cachedBucket, cachedOperationId) => {
				this.userData[workspaceSessionKey(cachedOperationId)] = cachedBucket;
			});
			this.userData[key] = bucket;
			this.channel?.postMessage({
				source_id: this.sourceId,
				operation_id: operationId,
				bucket,
			} satisfies WorkspaceSessionBroadcastMessage);
			return true;
		} catch {
			return false;
		}
	}

	/**
	 * Applies a valid same-user update received from another browser tab.
	 * @param value Broadcast payload.
	 * @returns Nothing.
	 */
	private applyBroadcastMessage(value: unknown): void {
		if (!isRecord(value) || value.source_id === this.sourceId) return;
		if (typeof value.operation_id !== "string") return;
		const operationId = value.operation_id as Operation.Id;
		const writeState = this.writes.get(operationId);
		if (writeState?.active || writeState?.pending) return;

		const bucket = parseWorkspaceSessionBucket(value.bucket, operationId);
		if (!bucket) return;
		this.buckets.set(operationId, bucket);
		this.userData[workspaceSessionKey(operationId)] = bucket;
		this.notify(operationId);
	}

	/**
	 * Notifies listeners for one operation.
	 * @param operationId Changed operation.
	 * @returns Nothing.
	 */
	private notify(operationId: Operation.Id): void {
		this.listeners.get(operationId)?.forEach((listener) => listener());
	}

	/**
	 * Notifies all currently subscribed operations after an explicit refresh.
	 * @returns Nothing.
	 */
	private notifyAll(): void {
		this.listeners.forEach((listeners) =>
			listeners.forEach((listener) => listener()),
		);
	}
}

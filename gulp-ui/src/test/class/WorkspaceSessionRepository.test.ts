import {
	LEGACY_AUTOSAVE_PREFIX,
	WorkspaceSessionNameExistsError,
	WorkspaceSessionRepository,
	type OperationWorkspaceSessionBucket,
	type WorkspaceSessionSnapshot,
	workspaceSessionKey,
} from "@/class/WorkspaceSessionRepository";
import type { Context } from "@/entities/Context";
import type { Operation } from "@/entities/Operation";
import type { Source } from "@/entities/Source";
import type { User } from "@/entities/User";

const USER_ID = "analyst" as User.Id;
const OPERATION_A = "operation-a" as Operation.Id;
const OPERATION_B = "operation-b" as Operation.Id;
const SOURCE_ID = "source-a" as Source.Id;
const CONTEXT_ID = "context-a" as Context.Id;

/**
 * Builds a complete JSON-compatible workspace snapshot for repository tests.
 * @param marker Value used to distinguish snapshots.
 * @returns Typed session snapshot.
 */
function snapshot(marker: number): WorkspaceSessionSnapshot {
	return {
		selected: {
			files: [SOURCE_ID],
			contexts: [CONTEXT_ID],
		},
		timeline: {
			scale: marker,
			frame: { min: marker, max: marker + 100 },
			scroll: { x: marker + 1, y: marker + 2 },
			filter: `filter-${marker}`,
			target: null,
		},
		filters: {},
		hidden: {
			notes: false,
			links: false,
			sourcesWithNoEvents: true,
			toasts: false,
		},
		source_settings: {
			source_settings: {
				[SOURCE_ID]: {
					offset: marker,
					field: "_id",
					hash_function: "fnv1a",
					render_color_palette: "plasma",
					render_engine: "default",
					frequency_sample: 1000,
					color_palette: {
						id: "palette",
						name: "Palette",
						interval: 10,
					},
					color_override: {
						field: "kind",
						values: { alert: "#ff0000" },
						lookup: { 1: "#ff0000" },
					},
					pinned: true,
				},
			},
		},
	};
}

/**
 * Converts a v2 snapshot into the flat shape stored by the legacy UI.
 * @param operationId Owning operation.
 * @param name Legacy display name.
 * @param marker Snapshot marker.
 * @returns Legacy session JSON.
 */
function legacySession(
	operationId: Operation.Id,
	name: string,
	marker: number,
): Record<string, unknown> {
	const value = snapshot(marker);
	return {
		name,
		icon: "Status",
		color: "#ffffff",
		selected: {
			...value.selected,
			operations: operationId,
		},
		timeline: value.timeline,
		filters: value.filters,
		hidden: value.hidden,
		source_settings: value.source_settings,
	};
}

describe("WorkspaceSessionRepository", () => {
	it("recovers from malformed JSONB and persists one valid autosave bucket", async () => {
		const writer = jest.fn(
			async (
				userId: User.Id,
				key: string,
				bucket: OperationWorkspaceSessionBucket,
			) => ({ id: userId, user_data: { [key]: bucket } }),
		);
		const repository = new WorkspaceSessionRepository(writer);
		repository.initialize({
			id: USER_ID,
			user_data: {
				[workspaceSessionKey(OPERATION_A)]: {
					version: 2,
					autosave: "invalid",
					named: ["invalid"],
				},
				sessions: "invalid",
			},
		});

		expect(repository.list(OPERATION_A)).toEqual([]);
		expect(await repository.upsertAutosave(OPERATION_A, snapshot(9))).toBe(true);
		expect(writer).toHaveBeenCalledTimes(1);
		expect(writer.mock.calls[0][1]).toBe(workspaceSessionKey(OPERATION_A));
		expect(repository.list(OPERATION_A)[0].snapshot).toEqual(snapshot(9));
	});

	it("lazily promotes only the current operation and keeps the newest legacy autosave", async () => {
		const writes: Array<{
			key: string;
			bucket: OperationWorkspaceSessionBucket;
		}> = [];
		const userData = {
			sessions: [
				legacySession(
					OPERATION_A,
					`${LEGACY_AUTOSAVE_PREFIX}Mon, 01 Jan 2024 00:00:00 GMT`,
					1,
				),
				legacySession(OPERATION_A, "Investigation", 2),
				legacySession(
					OPERATION_A,
					`${LEGACY_AUTOSAVE_PREFIX}Tue, 02 Jan 2024 00:00:00 GMT`,
					3,
				),
				legacySession(OPERATION_B, "Other operation", 4),
			],
		};
		const repository = new WorkspaceSessionRepository(
			async (userId, key, bucket) => {
				writes.push({ key, bucket });
				return { id: userId, user_data: { ...userData, [key]: bucket } };
			},
		);
		repository.initialize({ id: USER_ID, user_data: userData });

		const operationASessions = repository.list(OPERATION_A);
		expect(operationASessions).toHaveLength(2);
		expect(operationASessions[0].kind).toBe("autosave");
		expect(operationASessions[0].snapshot.timeline.scale).toBe(3);
		expect(repository.list(OPERATION_B).map((session) => session.name)).toEqual([
			"Other operation",
		]);

		await repository.createNamed(
			OPERATION_A,
			{ name: "Second", icon: "Status", color: "#000000" },
			snapshot(5),
		);

		expect(writes).toHaveLength(1);
		expect(writes[0].key).toBe(workspaceSessionKey(OPERATION_A));
		expect(
			Object.values(writes[0].bucket.named)
				.map((session) => session.name)
				.sort(),
		).toEqual(["Investigation", "Second"]);
		expect(userData.sessions).toHaveLength(4);
	});

	it("uses stable IDs, operation-local names, and one write for bulk deletion", async () => {
		const writer = jest.fn(
			async (
				userId: User.Id,
				key: string,
				bucket: OperationWorkspaceSessionBucket,
			) => ({ id: userId, user_data: { [key]: bucket } }),
		);
		const repository = new WorkspaceSessionRepository(writer);
		repository.initialize({ id: USER_ID, user_data: {} });

		const first = await repository.createNamed(
			OPERATION_A,
			{ name: "Review", icon: "Status", color: "#ffffff" },
			snapshot(1),
		);
		const otherOperation = await repository.createNamed(
			OPERATION_B,
			{ name: "Review", icon: "Status", color: "#ffffff" },
			snapshot(2),
		);

		expect(typeof first?.id).toBe("string");
		expect(typeof otherOperation?.id).toBe("string");
		expect(first?.id).not.toBe(otherOperation?.id);
		expect(repository.list(OPERATION_A)[0].snapshot).toEqual(snapshot(1));
		let duplicateError: unknown;
		try {
			await repository.createNamed(
				OPERATION_A,
				{ name: "Review", icon: "Status", color: "#ffffff" },
				snapshot(3),
			);
		} catch (error) {
			duplicateError = error;
		}
		expect(duplicateError).toBeInstanceOf(WorkspaceSessionNameExistsError);

		expect(await repository.rename(OPERATION_A, first!.id, "Review renamed")).toBe(
			true,
		);
		expect(repository.list(OPERATION_A)[0].name).toBe("Review renamed");
		const writesBeforeDelete = writer.mock.calls.length;
		expect(await repository.delete(OPERATION_A, [first!.id])).toBe(true);
		expect(writer.mock.calls.length - writesBeforeDelete).toBe(1);
		expect(repository.list(OPERATION_A)).toEqual([]);
		expect(repository.list(OPERATION_B)).toHaveLength(1);
	});

	it("replaces only the requested operation autosave", async () => {
		const writer = async (
			userId: User.Id,
			key: string,
			bucket: OperationWorkspaceSessionBucket,
		) => ({ id: userId, user_data: { [key]: bucket } });
		const repository = new WorkspaceSessionRepository(writer);
		repository.initialize({ id: USER_ID, user_data: {} });

		await repository.upsertAutosave(OPERATION_A, snapshot(1));
		await repository.upsertAutosave(OPERATION_B, snapshot(2));
		const operationBAutosaveId = repository.list(OPERATION_B)[0].id;
		await repository.upsertAutosave(OPERATION_A, snapshot(3));

		expect(repository.list(OPERATION_A)).toHaveLength(1);
		expect(repository.list(OPERATION_A)[0].snapshot.timeline.scale).toBe(3);
		expect(repository.list(OPERATION_B)).toHaveLength(1);
		expect(repository.list(OPERATION_B)[0].id).toBe(operationBAutosaveId);
		expect(repository.list(OPERATION_B)[0].snapshot.timeline.scale).toBe(2);
	});

	it("keeps one request active and coalesces an in-flight update into one later write", async () => {
		const resolvers: Array<
			(value: { id: User.Id; user_data: Record<string, unknown> }) => void
		> = [];
		let activeWrites = 0;
		let maximumActiveWrites = 0;
		const writer = jest.fn(
			(
				userId: User.Id,
				key: string,
				bucket: OperationWorkspaceSessionBucket,
			) =>
				new Promise<{ id: User.Id; user_data: Record<string, unknown> }>(
					(resolve) => {
						activeWrites++;
						maximumActiveWrites = Math.max(maximumActiveWrites, activeWrites);
						resolvers.push((value) => {
							activeWrites--;
							resolve(value);
						});
					},
				),
		);
		const repository = new WorkspaceSessionRepository(writer);
		repository.initialize({ id: USER_ID, user_data: {} });

		const first = repository.upsertAutosave(OPERATION_A, snapshot(1));
		await Promise.resolve();
		const second = repository.upsertAutosave(OPERATION_A, snapshot(2));
		expect(writer).toHaveBeenCalledTimes(1);

		const firstBucket = writer.mock.calls[0][2];
		resolvers[0]({
			id: USER_ID,
			user_data: { [workspaceSessionKey(OPERATION_A)]: firstBucket },
		});
		expect(await first).toBe(true);
		await Promise.resolve();
		expect(writer).toHaveBeenCalledTimes(2);

		const secondBucket = writer.mock.calls[1][2];
		resolvers[1]({
			id: USER_ID,
			user_data: { [workspaceSessionKey(OPERATION_A)]: secondBucket },
		});
		expect(await second).toBe(true);
		expect(maximumActiveWrites).toBe(1);
		expect(repository.list(OPERATION_A)[0].snapshot.timeline.scale).toBe(2);
	});

	it("synchronizes a successful operation bucket across same-user browser tabs", async () => {
		const originalBroadcastChannel = globalThis.BroadcastChannel;
		const channels = new Map<string, Set<FakeBroadcastChannel>>();

		class FakeBroadcastChannel {
			public onmessage: ((event: MessageEvent<unknown>) => void) | null = null;

			public constructor(public readonly name: string) {
				const peers = channels.get(name) ?? new Set<FakeBroadcastChannel>();
				peers.add(this);
				channels.set(name, peers);
			}

			public postMessage(message: unknown): void {
				channels.get(this.name)?.forEach((peer) => {
					if (peer !== this) {
						peer.onmessage?.({ data: message } as MessageEvent<unknown>);
					}
				});
			}

			public close(): void {
				channels.get(this.name)?.delete(this);
			}

			public addEventListener(): void {}
			public removeEventListener(): void {}
			public dispatchEvent(): boolean {
				return true;
			}
		}

		Object.defineProperty(globalThis, "BroadcastChannel", {
			configurable: true,
			value: FakeBroadcastChannel,
		});

		try {
			const writer = async (
				userId: User.Id,
				key: string,
				bucket: OperationWorkspaceSessionBucket,
			) => ({ id: userId, user_data: { [key]: bucket } });
			const firstTab = new WorkspaceSessionRepository(writer);
			const secondTab = new WorkspaceSessionRepository(writer);
			firstTab.initialize({ id: USER_ID, user_data: {} });
			secondTab.initialize({ id: USER_ID, user_data: {} });
			expect(secondTab.list(OPERATION_A)).toEqual([]);

			await firstTab.createNamed(
				OPERATION_A,
				{ name: "Shared tab state", icon: "Status", color: "#ffffff" },
				snapshot(7),
			);

			expect(secondTab.list(OPERATION_A).map((session) => session.name)).toEqual([
				"Shared tab state",
			]);
			firstTab.close();
			secondTab.close();
		} finally {
			Object.defineProperty(globalThis, "BroadcastChannel", {
				configurable: true,
				value: originalBroadcastChannel,
			});
		}
	});
});

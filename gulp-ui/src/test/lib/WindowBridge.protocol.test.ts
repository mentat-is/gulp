import type { Doc } from "@/entities/Doc";
import type { Operation } from "@/entities/Operation";
import type { Query } from "@/entities/Query";
import { WindowBridge } from "@/lib/WindowBridge";

/**
 * Synchronous in-memory BroadcastChannel implementation used to verify channel
 * naming and routing without relying on the browser scheduler.
 */
class FakeBroadcastChannel {
	public static readonly channels = new Map<
		string,
		Set<FakeBroadcastChannel>
	>();

	public onmessage: ((event: MessageEvent) => void) | null = null;
	public onmessageerror: ((event: MessageEvent) => void) | null = null;

	public constructor(public readonly name: string) {
		const peers =
			FakeBroadcastChannel.channels.get(name) ??
			new Set<FakeBroadcastChannel>();
		peers.add(this);
		FakeBroadcastChannel.channels.set(name, peers);
	}

	public postMessage(message: unknown): void {
		FakeBroadcastChannel.channels.get(this.name)?.forEach((peer) => {
			if (peer !== this) {
				peer.onmessage?.({ data: message } as MessageEvent);
			}
		});
	}

	public close(): void {
		FakeBroadcastChannel.channels.get(this.name)?.delete(this);
	}

	public addEventListener(): void {}
	public removeEventListener(): void {}
	public dispatchEvent(): boolean {
		return true;
	}
}

describe("WindowBridge detached-window protocol", () => {
	const originalBroadcastChannel = globalThis.BroadcastChannel;
	const bridges: WindowBridge.Bridge[] = [];

	/**
	 * Retains a bridge so every test closes all of its channel endpoints.
	 */
	function retain<TBridge extends WindowBridge.Bridge>(
		bridge: TBridge,
	): TBridge {
		bridges.push(bridge);
		return bridge;
	}

	beforeEach(() => {
		FakeBroadcastChannel.channels.clear();
		Object.defineProperty(globalThis, "BroadcastChannel", {
			configurable: true,
			value: FakeBroadcastChannel,
		});
	});

	afterEach(() => {
		bridges.splice(0).forEach((bridge) => bridge.destroy());
		Object.defineProperty(globalThis, "BroadcastChannel", {
			configurable: true,
			value: originalBroadcastChannel,
		});
	});

	it("isolates control and private traffic by coordinator namespace", () => {
		const firstControlMessages: WindowBridge.Message[] = [];
		const secondControlMessages: WindowBridge.Message[] = [];
		const firstPrivateMessages: WindowBridge.Message[] = [];
		const secondPrivateMessages: WindowBridge.Message[] = [];

		retain(
			WindowBridge.create(
				"first-main",
				(message) => firstControlMessages.push(message),
				"coordinator-one",
			),
		);
		retain(
			WindowBridge.create(
				"second-main",
				(message) => secondControlMessages.push(message),
				"coordinator-two",
			),
		);
		retain(
			WindowBridge.createClient(
				"same-client-id",
				"first-client",
				(message) => firstPrivateMessages.push(message),
				"coordinator-one",
			),
		);
		retain(
			WindowBridge.createClient(
				"same-client-id",
				"second-client",
				(message) => secondPrivateMessages.push(message),
				"coordinator-two",
			),
		);

		retain(
			WindowBridge.create(
				"first-control-sender",
				() => {},
				"coordinator-one",
			),
		).send(WindowBridge.MessageType.EVENT_TARGET_REQUEST, {
			docId: "event-one" as Doc.Id,
		});
		retain(
			WindowBridge.createClient(
				"same-client-id",
				"first-private-sender",
				() => {},
				"coordinator-one",
			),
		).send(WindowBridge.MessageType.EVENT_TARGET_CHANGED, {
			docId: "event-two" as Doc.Id,
		});

		expect(firstControlMessages).toHaveLength(1);
		expect(firstPrivateMessages).toHaveLength(1);
		expect(secondControlMessages).toEqual([]);
		expect(secondPrivateMessages).toEqual([]);
	});

	it("delivers a complete theme snapshot only to its intended client", () => {
		const controlMessages: WindowBridge.Message[] = [];
		const intendedMessages: WindowBridge.Message[] = [];
		const unrelatedMessages: WindowBridge.Message[] = [];
		const theme: WindowBridge.ThemeChangePayload = {
			theme: "classic-light",
			mode: "light",
			cssVariables: {
				"--accent": "#123456",
				"--theme-mode": "light",
			},
		};

		retain(
			WindowBridge.create(
				"control-listener",
				(message) => controlMessages.push(message),
				"coordinator",
			),
		);
		retain(
			WindowBridge.createClient(
				"notes-client",
				"notes-client",
				(message) => intendedMessages.push(message),
				"coordinator",
			),
		);
		retain(
			WindowBridge.createClient(
				"table-client",
				"table-client",
				(message) => unrelatedMessages.push(message),
				"coordinator",
			),
		);

		retain(
			WindowBridge.createClient(
				"notes-client",
				"main",
				() => {},
				"coordinator",
			),
		).send(WindowBridge.MessageType.THEME_CHANGE, theme);

		expect(intendedMessages).toEqual([
			{
				type: WindowBridge.MessageType.THEME_CHANGE,
				payload: theme,
				senderId: "main",
			},
		]);
		expect(unrelatedMessages).toEqual([]);
		expect(controlMessages).toEqual([]);
	});

	it("routes established detached actions privately without waking control listeners", () => {
		const coordinatorId = "coordinator";
		const clientId = "assistant-client";
		const controlMessages: WindowBridge.Message[] = [];
		const privateMessages: WindowBridge.Message[] = [];

		retain(
			WindowBridge.create(
				"main-control",
				(message) => controlMessages.push(message),
				coordinatorId,
			),
		);
		retain(
			WindowBridge.create(
				"unrelated-detached-control",
				(message) => controlMessages.push(message),
				coordinatorId,
			),
		);
		retain(
			WindowBridge.createClient(
				clientId,
				"main-private",
				(message) => privateMessages.push(message),
				coordinatorId,
			),
		);
		const clientPrivateBridge = retain(
			WindowBridge.createClient(
				clientId,
				clientId,
				() => {},
				coordinatorId,
			),
		);
		const unregister = WindowBridge.registerPrivateActionSender(
			coordinatorId,
			clientPrivateBridge,
		);

		WindowBridge.sendControlMessage(
			WindowBridge.MessageType.OPEN_FILTER_REQUEST,
			{
				sourceIds: [],
				query: {
					string: "",
					text_filter: "",
					raw: {},
					isManual: true,
					filters: [],
				} as Query.Type,
			},
			coordinatorId,
		);
		unregister();

		expect(privateMessages).toHaveLength(1);
		expect(privateMessages[0]?.type).toBe(
			WindowBridge.MessageType.OPEN_FILTER_REQUEST,
		);
		expect(controlMessages).toEqual([]);
	});

	it("keeps assistant-history invalidations off unrelated channels", () => {
		const matchingHistoryMessages: WindowBridge.Message[] = [];
		const otherHistoryMessages: WindowBridge.Message[] = [];
		const controlMessages: WindowBridge.Message[] = [];
		const coordinatorId = "coordinator";
		const operationId = "operation-one" as Operation.Id;

		retain(
			WindowBridge.create(
				"control-listener",
				(message) => controlMessages.push(message),
				coordinatorId,
			),
		);
		retain(
			WindowBridge.createAssistantHistory(
				"matching-listener",
				operationId,
				(message) => matchingHistoryMessages.push(message),
				coordinatorId,
			),
		);
		retain(
			WindowBridge.createAssistantHistory(
				"other-listener",
				"operation-two" as Operation.Id,
				(message) => otherHistoryMessages.push(message),
				coordinatorId,
			),
		);

		retain(
			WindowBridge.createAssistantHistory(
				"history-sender",
				operationId,
				() => {},
				coordinatorId,
			),
		).send(WindowBridge.MessageType.AI_HISTORY_UPDATED, {});

		expect(matchingHistoryMessages).toHaveLength(1);
		expect(otherHistoryMessages).toEqual([]);
		expect(controlMessages).toEqual([]);
	});

	it("supports a reconnect request followed by a fresh private bootstrap", () => {
		const readyMessages: WindowBridge.DetachedReadyPayload[] = [];
		const heartbeatMessages: WindowBridge.Message[] = [];
		const bootstrapMessages: WindowBridge.DetachedBootstrapPayload[] = [];
		const clientId = "notes-client";
		const coordinatorId = "coordinator";
		const theme: WindowBridge.ThemeChangePayload = {
			theme: "classic-dark",
			mode: "dark",
			cssVariables: { "--theme-mode": "dark" },
		};
		const mainPrivateBridge = retain(
			WindowBridge.createClient(
				clientId,
				"main",
				(message) => {
					if (
						message.type ===
						WindowBridge.MessageType.DETACHED_HEARTBEAT
					) {
						heartbeatMessages.push(message);
					}
				},
				coordinatorId,
			),
		);

		retain(
			WindowBridge.create(
				"main",
				(message) => {
					if (message.type !== WindowBridge.MessageType.DETACHED_READY) {
						return;
					}
					const payload =
						message.payload as WindowBridge.DetachedReadyPayload;
					readyMessages.push(payload);
					if (payload.needsBootstrap) {
						mainPrivateBridge.send(
							WindowBridge.MessageType.DETACHED_BOOTSTRAP,
							{
								status: "active",
								theme,
							},
						);
					}
				},
				coordinatorId,
			),
		);
		const firstPrivateReceiver = retain(
			WindowBridge.createClient(
				clientId,
				clientId,
				(message) => {
					if (
						message.type ===
						WindowBridge.MessageType.DETACHED_BOOTSTRAP
					) {
						bootstrapMessages.push(
							message.payload as WindowBridge.DetachedBootstrapPayload,
						);
					}
				},
				coordinatorId,
			),
		);
		const firstControlSender = retain(
			WindowBridge.create(clientId, () => {}, coordinatorId),
		);

		firstControlSender.send(WindowBridge.MessageType.DETACHED_READY, {
			kind: "notes",
			needsBootstrap: true,
		});
		firstPrivateReceiver.send(
			WindowBridge.MessageType.DETACHED_HEARTBEAT,
			{},
		);

		expect(readyMessages.map((message) => message.needsBootstrap)).toEqual([
			true,
		]);
		expect(heartbeatMessages).toEqual([
			{
				type: WindowBridge.MessageType.DETACHED_HEARTBEAT,
				payload: {},
				senderId: clientId,
			},
		]);
		expect(bootstrapMessages).toEqual([
			{
				status: "active",
				theme,
			},
		]);

		firstPrivateReceiver.destroy();
		firstControlSender.destroy();
		const reconnectedPrivateReceiver = retain(
			WindowBridge.createClient(
				clientId,
				clientId,
				(message) => {
					if (
						message.type ===
						WindowBridge.MessageType.DETACHED_BOOTSTRAP
					) {
						bootstrapMessages.push(
							message.payload as WindowBridge.DetachedBootstrapPayload,
						);
					}
				},
				coordinatorId,
			),
		);
		const reconnectedControlSender = retain(
			WindowBridge.create(clientId, () => {}, coordinatorId),
		);

		reconnectedControlSender.send(
			WindowBridge.MessageType.DETACHED_READY,
			{
				kind: "notes",
				needsBootstrap: true,
			},
		);

		expect(readyMessages.map((message) => message.needsBootstrap)).toEqual([
			true,
			true,
		]);
		expect(bootstrapMessages).toEqual([
			{ status: "active", theme },
			{ status: "active", theme },
		]);
		expect(reconnectedPrivateReceiver.destroy).not.toBeUndefined();
	});
});

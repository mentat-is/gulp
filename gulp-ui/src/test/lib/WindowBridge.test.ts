import type { App } from "@/entities/App";
import type { Operation } from "@/entities/Operation";
import { WindowBridge } from "@/lib/WindowBridge";
import { DataStore } from "@/store/DataStore";

const OPERATION_ID = "operation" as Operation.Id;

interface SentBridgeMessage {
	type: WindowBridge.MessageType;
	payload: unknown;
}

/**
 * Creates one selected operation fixture for detached snapshot tests.
 * @returns Selected operation entity.
 */
function createOperation(): Operation.Type {
	return {
		id: OPERATION_ID,
		name: "Operation",
		selected: true,
	} as Operation.Type;
}

/**
 * Creates a minimal app fixture with a scale that must not cross the bridge.
 * @returns Application fixture.
 */
function createApp(): App.Type {
	return {
		target: {
			operations: [createOperation()],
			contexts: [],
			files: [],
			events: new Map(),
			filters: {},
			notes: [],
			links: [],
			highlights: [],
			glyphs: [],
			mappings: [],
			plugins: [],
		},
		timeline: {
			frame: { min: 100, max: 200 },
			target: null,
			filter: "needle",
			scale: 42,
			cache: { data: new Map(), filters: {} },
			isScrollReversed: false,
			dialogSize: 320,
			pointers: [],
			renderVersion: 7,
		},
		hidden: {
			notes: false,
			links: false,
			sourcesWithNoEvents: true,
			toasts: false,
		},
		general: {
			server: "",
			ws_id: "",
			glyphs_syncronized: false,
			requests: [],
			user: null,
			loadings: {
				byRequestId: new Map(),
				byFileId: new Map(),
			},
			tableViewSource: null,
		},
		settings: {},
	} as App.Type;
}

/**
 * Creates an in-memory bridge that records sent messages.
 * @param sentMessages Mutable message sink.
 * @returns Bridge implementation for unit tests.
 */
function createRecordingBridge(sentMessages: SentBridgeMessage[]): WindowBridge.Bridge {
	return {
		send<TType extends WindowBridge.MessageType>(
			type: TType,
			payload: WindowBridge.MessagePayload[TType],
		): void {
			sentMessages.push({ type, payload });
		},
		destroy(): void {},
	};
}

describe("WindowBridge detached app snapshots", () => {
	beforeEach(() => {
		DataStore.resetRenderOperation(OPERATION_ID);
	});

	it("does not include movement-coupled scale or viewport source IDs", () => {
		const app = createApp();
		const sentMessages: SentBridgeMessage[] = [];

		WindowBridge.sendMainContext(
			createRecordingBridge(sentMessages),
			app,
			"active",
			OPERATION_ID,
		);

		const snapshotMessage = sentMessages.find(
			(message) => message.type === WindowBridge.MessageType.APP_SNAPSHOT,
		);
		const snapshot =
			snapshotMessage?.payload as WindowBridge.AppSnapshotPayload | undefined;

		expect(snapshot === undefined).toBe(false);
		if (!snapshot) {
			throw new Error("APP_SNAPSHOT was not sent");
		}

		expect(Object.prototype.hasOwnProperty.call(snapshot, "visibleSourceIds")).toBe(
			false,
		);
		expect(snapshot.timeline).toEqual({
			frame: app.timeline.frame,
			filter: app.timeline.filter,
			renderVersion: app.timeline.renderVersion,
		});
		expect(Object.prototype.hasOwnProperty.call(snapshot.timeline, "scale")).toBe(
			false,
		);
	});
});

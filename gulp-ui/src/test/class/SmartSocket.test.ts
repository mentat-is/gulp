import { SmartSocket } from "@/class/SmartSocket";

jest.mock("@/dto/Logger.class", () => ({
	Logger: { log: () => {} },
}));
jest.mock("@/entities/addon/Internal", () => ({
	Internal: {
		Settings: {
			server: "ws://gulp.test",
			token: "test-token",
		},
	},
}));
jest.mock("@/entities/Doc", () => ({ Doc: {} }));
jest.mock("@/entities/Group", () => ({ Group: {} }));
jest.mock("@/entities/Operation", () => ({ Operation: {} }));
jest.mock("@/entities/Request", () => ({ Request: {} }));
jest.mock("@/entities/User", () => ({ User: {} }));
jest.mock("@/ui/Icon", () => ({ Icon: () => null }));

class FakeWebSocket {
	public static readonly OPEN = 1;
	public readonly url: string;
	public readyState = 0;
	public onopen: ((event: Event) => void) | null = null;
	public onclose: ((event: CloseEvent) => void) | null = null;
	public onerror: ((event: Event) => void) | null = null;
	public onmessage: ((event: MessageEvent) => void) | null = null;
	public closeCalls = 0;

	public constructor(url: string) {
		this.url = url;
	}

	public send(): void {}

	public close(): void {
		this.closeCalls += 1;
	}
}

describe("SmartSocket lifecycle", () => {
	const originalWebSocket = globalThis.WebSocket;

	beforeEach(() => {
		Reflect.deleteProperty(SmartSocket.Class, "instance");
		Object.defineProperty(globalThis, "WebSocket", {
			configurable: true,
			value: FakeWebSocket,
		});
	});

	afterEach(() => {
		const socket = Reflect.get(
			SmartSocket.Class,
			"instance",
		) as SmartSocket.Class | undefined;
		socket?.destroy();
		Object.defineProperty(globalThis, "WebSocket", {
			configurable: true,
			value: originalWebSocket,
		});
	});

	it("releases the closed singleton so a detached route can reconnect", () => {
		const first = new SmartSocket.Class("detached-one");
		expect(SmartSocket.Class.instance).toBe(first);

		first.destroy();
		expect(
			Reflect.get(SmartSocket.Class, "instance"),
		).toBeUndefined();

		const second = new SmartSocket.Class("detached-two");
		expect(second).not.toBe(first);
		expect(SmartSocket.Class.instance).toBe(second);
	});
});

type WindowBridgeNamespace =
	typeof import("@/lib/WindowBridge")["WindowBridge"];

const COORDINATOR_SESSION_KEY = "gulp-window-bridge:coordinator";
const COORDINATOR_OWNERSHIP_SESSION_KEY =
	"gulp-window-bridge:coordinator-ownership";
const DOCUMENT_ID_GLOBAL_KEY = "__gulpWindowBridgeDocumentId__";

/**
 * Loads WindowBridge in a fresh module realm, matching a newly loaded document.
 * @returns Fresh WindowBridge namespace.
 */
function loadWindowBridge(): WindowBridgeNamespace {
	const moduleJest = jest as typeof jest & {
		isolateModules(action: () => void): void;
		requireActual<TModule>(moduleName: string): TModule;
	};
	let bridge: WindowBridgeNamespace | undefined;
	moduleJest.isolateModules(() => {
		bridge = (
			moduleJest.requireActual<typeof import("@/lib/WindowBridge")>(
				"@/lib/WindowBridge",
			)
		).WindowBridge;
	});
	if (!bridge) throw new Error("WindowBridge module did not load");
	return bridge;
}

/**
 * Simulates the new Window global supplied to a new top-level document.
 */
function replaceDocumentRealm(): void {
	delete (
		globalThis as typeof globalThis & {
			[DOCUMENT_ID_GLOBAL_KEY]?: string;
		}
	)[DOCUMENT_ID_GLOBAL_KEY];
}

describe("WindowBridge coordinator ownership", () => {
	beforeEach(() => {
		sessionStorage.clear();
		replaceDocumentRealm();
		window.history.replaceState({}, "", "/");
	});

	afterEach(() => {
		sessionStorage.clear();
		replaceDocumentRealm();
		window.history.replaceState({}, "", "/");
	});

	it("mints a different coordinator when a new top-level context clones active session state", () => {
		const originalMain = loadWindowBridge();
		const originalCoordinator =
			originalMain.getOrCreateCoordinatorId();

		// Duplicate tabs and opener-created windows receive a sessionStorage
		// snapshot, but execute in a different Window/document realm.
		replaceDocumentRealm();
		const clonedMain = loadWindowBridge();
		const clonedCoordinator = clonedMain.getOrCreateCoordinatorId();

		expect(clonedCoordinator).not.toBe(originalCoordinator);
		expect(sessionStorage.getItem(COORDINATOR_SESSION_KEY)).toBe(
			clonedCoordinator,
		);
		expect(
			JSON.parse(
				sessionStorage.getItem(COORDINATOR_OWNERSHIP_SESSION_KEY) ?? "{}",
			),
		).toMatchObject({
			coordinatorId: clonedCoordinator,
			phase: "active",
		});
	});

	it("retains the coordinator across a same-tab reload hand-off", () => {
		const firstDocument = loadWindowBridge();
		const coordinatorId =
			firstDocument.getOrCreateCoordinatorId();

		window.dispatchEvent(new Event("pagehide"));
		expect(
			JSON.parse(
				sessionStorage.getItem(COORDINATOR_OWNERSHIP_SESSION_KEY) ?? "{}",
			),
		).toMatchObject({
			coordinatorId,
			phase: "transitioning",
		});

		replaceDocumentRealm();
		const reloadedDocument = loadWindowBridge();

		expect(reloadedDocument.getOrCreateCoordinatorId()).toBe(
			coordinatorId,
		);
		expect(
			JSON.parse(
				sessionStorage.getItem(COORDINATOR_OWNERSHIP_SESSION_KEY) ?? "{}",
			),
		).toMatchObject({
			coordinatorId,
			phase: "active",
		});
	});

	it("reuses active ownership across duplicate module instances in one document", () => {
		const firstBundle = loadWindowBridge();
		const coordinatorId = firstBundle.getOrCreateCoordinatorId();
		const secondBundle = loadWindowBridge();

		expect(secondBundle.getOrCreateCoordinatorId()).toBe(coordinatorId);
	});

	it("makes a detached route adopt its explicit main coordinator", () => {
		sessionStorage.setItem(
			COORDINATOR_SESSION_KEY,
			"cloned-unrelated-coordinator",
		);
		sessionStorage.setItem(
			COORDINATOR_OWNERSHIP_SESSION_KEY,
			JSON.stringify({
				coordinatorId: "cloned-unrelated-coordinator",
				documentId: "unrelated-main-document",
				phase: "active",
			}),
		);
		window.history.replaceState(
			{},
			"",
			"/detached/notes?clientId=notes&coordinatorId=main-coordinator",
		);

		const detachedDocument = loadWindowBridge();

		expect(detachedDocument.getOrCreateCoordinatorId()).toBe(
			"main-coordinator",
		);
		expect(sessionStorage.getItem(COORDINATOR_SESSION_KEY)).toBe(
			"main-coordinator",
		);
		expect(
			JSON.parse(
				sessionStorage.getItem(COORDINATOR_OWNERSHIP_SESSION_KEY) ?? "{}",
			),
		).toMatchObject({
			coordinatorId: "cloned-unrelated-coordinator",
			phase: "active",
		});
	});
});

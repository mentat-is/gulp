import { ownsIndependentDetachedDataStore } from "@/context/detachedDataStore";

describe("detached DataStore ownership", () => {
	it("treats a standalone detached route as an independent module realm", () => {
		const runtimeWindow = {} as Window;
		const detachedDocument = { defaultView: runtimeWindow } as Document;

		expect(
			ownsIndependentDetachedDataStore(detachedDocument, runtimeWindow),
		).toBe(true);
	});

	it("keeps opener-rendered popup roots on the shared canonical DataStore", () => {
		const runtimeWindow = {} as Window;
		const popupWindow = {} as Window;
		const detachedDocument = { defaultView: popupWindow } as Document;

		expect(
			ownsIndependentDetachedDataStore(detachedDocument, runtimeWindow),
		).toBe(false);
	});

	it("does not claim ownership for a document without an attached window", () => {
		const runtimeWindow = {} as Window;
		const detachedDocument = { defaultView: null } as Document;

		expect(
			ownsIndependentDetachedDataStore(detachedDocument, runtimeWindow),
		).toBe(false);
	});
});

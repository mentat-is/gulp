import { Doc } from "@/entities/Doc";
import { WindowBridge } from "@/lib/WindowBridge";

describe("WindowBridge detached dialog canonicalization", () => {
	it("replaces a single-event descriptor with the projected canonical event", () => {
		const originalEvent = {
			_id: "event-one",
			gulp_timestamp: 100,
		} as Doc.Type;
		const canonicalEvent = {
			...originalEvent,
			gulp_timestamp: 200,
		} as Doc.Type;

		const descriptor = WindowBridge.canonicalizeDetachedDialogDescriptor(
			{ kind: "event", event: originalEvent },
			new Map([[canonicalEvent._id, canonicalEvent]]),
		);

		expect(descriptor).toEqual({
			kind: "event",
			event: canonicalEvent,
		});
	});

	it("canonicalizes grouped events while preserving their order and kind", () => {
		const firstOriginal = {
			_id: "event-one",
			gulp_timestamp: 100,
		} as Doc.Type;
		const secondOriginal = {
			_id: "event-two",
			gulp_timestamp: 200,
		} as Doc.Type;
		const firstCanonical = {
			...firstOriginal,
			gulp_timestamp: 300,
		} as Doc.Type;
		const secondCanonical = {
			...secondOriginal,
			gulp_timestamp: 400,
		} as Doc.Type;

		const descriptor = WindowBridge.canonicalizeDetachedDialogDescriptor(
			{
				kind: "group",
				events: [secondOriginal, firstOriginal],
				groupKind: "links",
			},
			new Map([
				[firstCanonical._id, firstCanonical],
				[secondCanonical._id, secondCanonical],
			]),
		);

		expect(descriptor).toEqual({
			kind: "group",
			events: [secondCanonical, firstCanonical],
			groupKind: "links",
		});
	});
});

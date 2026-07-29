import { resolveTableNotePortalContainer } from "@/components/TableViewWindow.portal";

describe("TableViewWindow note portal", () => {
	it("uses the active detached document body instead of the table root", () => {
		const detachedDocument = document.implementation.createHTMLDocument(
			"Detached Table View",
		);
		const tableRoot = detachedDocument.createElement("div");
		detachedDocument.body.appendChild(tableRoot);

		const portalContainer =
			resolveTableNotePortalContainer(detachedDocument);

		expect(portalContainer).toBe(detachedDocument.body);
		expect(portalContainer).not.toBe(tableRoot);
	});
});

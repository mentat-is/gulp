import { isSelectFilesSourceVisible } from "@/banners/SelectFiles.visibility";

describe("Select Files source visibility", () => {
	const emptySource = { total: 0 };

	it("shows a newly created source while its ingestion is active", () => {
		expect(
			isSelectFilesSourceVisible(emptySource, false, {
				ingestionProgress: 0,
				isIngesting: true,
			}),
		).toBe(true);
	});

	it("shows websocket progress before the persisted total is refreshed", () => {
		expect(
			isSelectFilesSourceVisible(emptySource, false, {
				ingestionProgress: 18_000,
				isIngesting: false,
			}),
		).toBe(true);
	});

	it("continues hiding an idle empty source unless the user enables it", () => {
		const idleActivity = { ingestionProgress: 0, isIngesting: false };

		expect(
			isSelectFilesSourceVisible(emptySource, false, idleActivity),
		).toBe(false);
		expect(
			isSelectFilesSourceVisible(emptySource, true, idleActivity),
		).toBe(true);
	});

	it("shows a source whose persisted total is already non-zero", () => {
		expect(
			isSelectFilesSourceVisible(
				{ total: 1 },
				false,
				{ ingestionProgress: 0, isIngesting: false },
			),
		).toBe(true);
	});
});

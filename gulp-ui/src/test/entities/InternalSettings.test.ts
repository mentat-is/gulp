import { Internal } from "@/entities/addon/Internal";
import { DEFAULT_TIMESTAMP_TIME_ZONE } from "@/timeZone";

jest.mock("@/ui/Icon", () => ({ Icon: {} }));
jest.mock("@/dto/Logger.class", () => ({ Logger: {} }));
jest.mock("@/entities/Glyph", () => ({ Glyph: { List: new Map() } }));
jest.mock("@/entities/Source", () => ({ Source: {} }));

describe("Internal.Settings timestamp time zone", () => {
	beforeEach(() => {
		localStorage.clear();
	});

	it("persists UTC as the default preference", () => {
		expect(Internal.Settings.timestampTimeZone).toBe(DEFAULT_TIMESTAMP_TIME_ZONE);
		expect(
			localStorage.getItem(
				Internal.LocalStorageItemsList.TIMESTAMP_TIME_ZONE,
			),
		).toBe(DEFAULT_TIMESTAMP_TIME_ZONE);
	});

	it("persists supported zones and resets invalid values to UTC", () => {
		Internal.Settings.timestampTimeZone = "Europe/Rome";
		expect(Internal.Settings.timestampTimeZone).toBe("Europe/Rome");

		Internal.Settings.timestampTimeZone = "Mars/Base";
		expect(Internal.Settings.timestampTimeZone).toBe(DEFAULT_TIMESTAMP_TIME_ZONE);
	});
});

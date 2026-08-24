import { Logger } from "@/dto/Logger.class";
import { Icon } from "@/ui/Icon";
import {
	DEFAULT_TIMESTAMP_TIME_ZONE,
	isSupportedTimestampTimeZone,
} from "@/timeZone";
import { Glyph } from "../Glyph";
import { Source } from "../Source";

export namespace Internal {
	export enum LocalStorageItemsList {
		GENERAL_SERVER_VALUE = "__server",
		GENERAL_TOKEN_VALUE = "__token",
		TIMESTAMP_TIME_ZONE = "__timestamp_time_zone",
	}

	export namespace Sync {
		export interface Options {
			contexts?: boolean;
			files?: boolean;
		}
	}

	export class Settings {
		static default: Source.Type["settings"] = {
			offset: 0,
			field: "gulp.event_code",
			hash_function: "fnv1a",
			render_color_palette: "plasma",
			render_engine: "default",
			timeline_label: "filename",
			frequency_sample: 1000,
		};

		public static get server(): string {
			const engine = localStorage.getItem(
				Internal.LocalStorageItemsList.GENERAL_SERVER_VALUE,
			);

			if (engine) {
				return engine;
			}

			Internal.Settings.server = "http://localhost:8080";

			return Internal.Settings.server;
		}

		public static set server(server: string) {
			localStorage.setItem(
				Internal.LocalStorageItemsList.GENERAL_SERVER_VALUE,
				server,
			);
		}

		public static get token(): string {
			const token = localStorage.getItem(
				Internal.LocalStorageItemsList.GENERAL_TOKEN_VALUE,
			);

			if (token) {
				return token;
			}

			Internal.Settings.token = "-";

			return Internal.Settings.token;
		}

		public static set token(token: string) {
			localStorage.setItem(
				Internal.LocalStorageItemsList.GENERAL_TOKEN_VALUE,
				token,
			);
		}

		/**
		 * Persists the time zone used to display event timestamps.
		 * @param timeZone Candidate IANA time-zone identifier.
		 */
		public static set timestampTimeZone(timeZone: string) {
			localStorage.setItem(
				Internal.LocalStorageItemsList.TIMESTAMP_TIME_ZONE,
				isSupportedTimestampTimeZone(timeZone)
					? timeZone
					: DEFAULT_TIMESTAMP_TIME_ZONE,
			);
		}

		/** Returns the persisted, validated timestamp time zone or UTC by default. */
		public static get timestampTimeZone(): string {
			const value = localStorage.getItem(
				Internal.LocalStorageItemsList.TIMESTAMP_TIME_ZONE,
			);

			if (value && isSupportedTimestampTimeZone(value)) {
				return value;
			}

			Internal.Settings.timestampTimeZone = DEFAULT_TIMESTAMP_TIME_ZONE;
			return DEFAULT_TIMESTAMP_TIME_ZONE;
		}
	}

	export class IconExtractor {
		public static activate = <T extends { glyph_id: Glyph.Id } | null>(
			defaultValue: Icon.Name,
		): ((obj: T) => Icon.Name) => {
			return (obj: T) => {
				if (obj?.glyph_id) {
					return Glyph.List.get(obj.glyph_id) ?? defaultValue;
				}

				return defaultValue;
			};
		};
	}

	export class Transformator {
		public static toTimestamp = (
			timestamp: string | number | Date | bigint,
			roundTo: keyof Pick<Math, "ceil" | "floor" | "round"> = "round",
		): number =>
			new Date(
				Math[roundTo](Number(this.toNanos(timestamp)) / 1_000_000),
			).valueOf();

		public static toNanos(
			value:
				| string
				| number
				| Date
				| bigint
				| { source: string; parsedValue: number },
		): bigint {
			try {
				if (typeof value === "bigint") return value;

				if (value instanceof Date) return BigInt(value.getTime()) * 1_000_000n;

				if (typeof value === "object" && value !== null && "source" in value) {
					return BigInt(value.source);
				}

				if (typeof value === "number") {
					const str = String(Math.floor(value));
					if (str.length === 19) return BigInt(str); // already nanos
					if (str.length === 16) return BigInt(str) * 1_000n; // micros
					if (str.length === 13) return BigInt(str) * 1_000_000n; // millis
					if (str.length <= 10) return BigInt(str) * 1_000_000_000n; // seconds
					return BigInt(value);
				}

				if (typeof value === "string") {
					if (/^\d+$/.test(value)) return this.toNanos(Number(value));
					const parsed = Date.parse(value);
					if (!isNaN(parsed)) return BigInt(parsed) * 1_000_000n;
					return 0n;
				}

				return 0n;
			} catch {
				return 0n;
			}
		}

		public static toISO = (
			timestamp: string | number | Date | bigint,
		): string => {
			if (timestamp instanceof Date) return timestamp.toISOString();
			if (typeof timestamp === "number" || typeof timestamp === "bigint")
				return new Date(this.toTimestamp(timestamp)).toISOString();
			const parsed = Date.parse(timestamp);
			if (isNaN(parsed)) {
				Logger.error(
					`Invalid transformation to ISO from ${timestamp}`,
					Transformator.name,
				);
				return new Date().toISOString();
			}
			return new Date(parsed).toISOString();
		};

		public static toAsync = <T extends any>(value: T): Promise<T> => {
			return new Promise((resolve) => resolve(value));
		};
	}

}

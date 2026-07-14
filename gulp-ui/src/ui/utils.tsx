import { toast } from "sonner";
type UUID = string;
import { Info, MinMax, MinMaxBase } from "@/class/Info";
import { ChangeEvent, RefObject } from "react";
import { XY, XYBase } from "@/dto/XY.dto";
import { SetState } from "@/class/API";
import { Logger } from "@/dto/Logger.class";
import { Request } from "@/entities/Request";
import { Source } from "@/entities/Source";
import { Doc } from "@/entities/Doc";
import { App } from "@/entities/App";
import { Color as EntityColor } from "@/entities/Color";
import { Internal } from "@/entities/addon/Internal";
import { translate } from "@/locales";

export type Callback<T> = (data: T) => void;

export enum Side {
	TOP = "top",
	RIGHT = "right",
	BOTTOM = "bottom",
	LEFT = "left",
}

type ColorVar = `var(--${string})`;
type ColorHex =
	| `#${string}`
	| `#${string}${string}`
	| `#${string}${string}${string}`
	| `#${string}${string}${string}${string}`
	| `#${string}${string}${string}${string}${string}`
	| `#${string}${string}${string}${string}${string}${string}`;

export type Color = ColorHex | ColorVar;

export type MaybeArray<K> = K | K[];

type ClassDictionary = Record<string, unknown>;

export type ClassValue =
	| string
	| number
	| boolean
	| null
	| undefined
	| ClassDictionary
	| ClassValue[];

/**
 * Uppercases the first character of a string and preserves the remaining content.
 * @param str Text to transform.
 * @returns Text with the first character uppercased.
 */
export function capitalize(str: string): string {
	return str.substring(0, 1).toUpperCase() + str.substring(1);
}

/**
 * Normalizes a single value or an array into an array.
 * @param unknown Value that may already be an array.
 * @returns Empty array for undefined, the original array for arrays, or a single-item array.
 */
export function toArray<K>(unknown: MaybeArray<K> | undefined): K[] {
	if (Array.isArray(unknown)) return unknown;
	return typeof unknown === "undefined" ? [] : [unknown];
}

/**
 * Throws a newly constructed error instance.
 * @param Exception Error constructor to instantiate.
 * @returns Never returns because it always throws.
 */
export function λthrow(Exception: new () => Error): never {
	throw new Exception();
}

/**
 * Builds a className string from strings, arrays, and conditional object maps.
 * @param inputs Class values to flatten and filter.
 * @returns Space-separated className string.
 */
export function cn(...inputs: ClassValue[]): string {
	return collectClassNames(inputs).join(" ");
}

/**
 * Recursively flattens class values into renderable class tokens.
 * @param inputs Class values to inspect.
 * @returns Ordered list of class tokens.
 */
function collectClassNames(inputs: ClassValue[]): string[] {
	return inputs.flatMap((input) => {
		if (!input) return [];

		if (typeof input === "string" || typeof input === "number") {
			return [String(input)];
		}

		if (Array.isArray(input)) {
			return collectClassNames(input);
		}

		if (typeof input === "object") {
			return Object.entries(input).flatMap(([className, isEnabled]) =>
				isEnabled ? [className] : [],
			);
		}

		return [];
	});
}

export const parseTokensFromCookies = (tokens: string) => {
	try {
		return JSON.parse(tokens as string);
	} catch (_) {
		return Array.isArray(tokens) ? tokens : [];
	}
};

const colorCache = new Map<string, EntityColor.Type>();

export const stringToHexColor = (str: string): EntityColor.Type => {
	if (colorCache.has(str)) return colorCache.get(str)!;

	let hash = 0;
	for (let i = 0; i < str.length; i++) {
		hash = (hash << 5) - hash + str.charCodeAt(i);
		hash |= 0;
	}

	const color = `#${[0, 1, 2]
		.map((i) => ((hash >> (i * 8)) & 0xff).toString(16).padStart(2, "0"))
		.join("")}` as EntityColor.Type;

	colorCache.set(str, color);
	return color;
};

export const parse = (str: string) => parseFloat(str.replace("px", ""));

export type JsonString<T> = string & { __jsonStringBrand: T };

export const copy = (value: string) => {
	const performCopy = async () => {
		try {
			if (!navigator.clipboard) {
				throw new Error("Clipboard API not available");
			}
			await navigator.clipboard.writeText(value);
			toast(translate("clipboard.copied"), {
				description: translate("clipboard.pasteHint"),
			});
		} catch (error) {
			// Fallback for lack of user activation or missing API
			try {
				const textArea = document.createElement("textarea");
				textArea.value = value;
				textArea.style.position = "fixed";
				textArea.style.left = "-9999px";
				textArea.style.top = "0";
				document.body.appendChild(textArea);
				textArea.focus();
				textArea.select();
				const successful = document.execCommand("copy");
				document.body.removeChild(textArea);

				if (successful) {
					toast(translate("clipboard.copied"), {
						description: translate("clipboard.pasteHint"),
					});
				} else {
					throw new Error("Fallback copy failed");
				}
			} catch (err) {
				toast.error(translate("clipboard.noAccess"));
			}
		}
	};

	performCopy();
};

export const throwableByTimestamp = (
	timestamp: MinMax | number,
	limits: MinMax,
	app: App.Type,
	offset = 0,
): boolean => {
	const time: number | MinMax =
		typeof timestamp === "number"
			? timestamp + offset
			: {
					min: timestamp.min + offset,
					max: timestamp.max + offset,
				};

	return typeof time === "number"
		? time < limits.min ||
				time > limits.max ||
				time < (app.timeline.frame.min || 0) ||
				time > (app.timeline.frame.max || Infinity)
		: time.max < limits.min ||
				time.min > limits.max ||
				time.max < (app.timeline.frame.min || 0) ||
				time.min > (app.timeline.frame.max || Infinity);
};

export function generateUUID<T>(prefix?: Request.Prefix): T {
	const base = "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
		const r = (Math.random() * 16) | 0;
		const v = c === "x" ? r : (r & 0x3) | 0x8;
		return v.toString(16) as UUID;
	});

	if (prefix) {
		return `${prefix}-${base}` as T;
	}

	return base as T;
}

export const getLimits = (
	app: App.Type,
	Info: Info,
	timeline: RefObject<HTMLDivElement>,
	scrollX: number,
): MinMax => {
	if (!app.timeline.frame) {
		return app.timeline.frame;
	}

	const min =
		app.timeline.frame.min +
		(scrollX / Info.width) * (app.timeline.frame.max - app.timeline.frame.min);

	const max =
		app.timeline.frame.min +
		((scrollX + (timeline.current?.clientWidth ?? 0)) / Info.width) *
			(app.timeline.frame.max - app.timeline.frame.min);

	return { min, max };
};

export const arrayToLinearGradientCSS = (gradient: string[]): string =>
	`linear-gradient(to right, ${gradient.map((g) => "#" + g).join(", ")})`;

export const getDateFormat = (diffInMilliseconds: number) => {
	const diffInDays = diffInMilliseconds / (1000 * 60 * 60 * 24);
	const diffInHours = diffInMilliseconds / (1000 * 60 * 60);
	const diffInMinutes = diffInMilliseconds / (1000 * 60);

	if (diffInDays >= 30) return "dd.MM.yyyy";
	if (diffInDays >= 1) return "dd.MM.yyyy";
	if (diffInHours >= 1) return "HH:mm dd.MM";
	if (diffInMinutes >= 1) return "HH:mm:ss dd.MM";
	return "HH:mm:ss dd.MM.yyyy";
};

export const getTimestamp = (x: number, info: Info) => {
	const { min, max } = info.app.timeline.frame;

	return Math.round(min + (x / info.width) * (max - min));
};

export const formatBytes = (bytes: number): string => {
	if (bytes >= 1024 * 1024) {
		return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
	} else {
		return `${(bytes / 1024).toFixed(2)} KB`;
	}
};

export const between = (num: number, min: number, max: number) =>
	num >= min && num <= max;

export type HashFunctionName = "fnv1a" | "djb2" | "sdbm";

export const HASH_FUNCTIONS: HashFunctionName[] = ["fnv1a", "djb2", "sdbm"];

export function numericRepresentationOfAnyString(
	input: string,
	hashFunction: HashFunctionName = "fnv1a",
): number {
	return Refractor.string.toNumber(input, hashFunction);
}

export function numericRepresentationOfAnyValueOnlyForInternalUsageOfRenderEngine(
	file: Source.Type,
	event: Doc.Type,
): number {
	let key: unknown = Refractor.get(event, file.settings.field);

	if (typeof key === "object" && key !== null) {
		key = Object.values(key).reduce((sum, value) => {
			return (
				sum +
				(parseInt(value.toString(), 10) ||
					numericRepresentationOfAnyString(
						value.toString(),
						file.settings.hash_function,
					))
			);
		}, 0);
	}

	return (
		(typeof key === "string" &&
			numericRepresentationOfAnyString(key, file.settings.hash_function)) ||
		(typeof key === "number" ? key : NaN) ||
		0
	);
}

export function download(content: string, type: string, name: string) {
	const blob = new Blob([content], { type });
	const url = URL.createObjectURL(blob);
	const a = document.createElement("a");
	a.href = url;
	a.download = name;
	document.body.appendChild(a);
	a.click();
	document.body.removeChild(a);
	URL.revokeObjectURL(url);
}

export namespace Algorhithm {
	export interface Constructor {
		frame: MinMax;
		scroll: XY;
		width: number;
		scale: number;
	}
}

export class Algorhithm implements Algorhithm.Constructor {
	frame: MinMax = MinMaxBase;
	scroll: XY = XYBase(0);
	width = 1;
	scale = 1;

	constructor(constructor: Algorhithm.Constructor) {
		Object.assign(this, constructor);
	}

	abs_x_from_timestamp = (timestamp: number) =>
		Math.round(
			((timestamp - this.frame.min) / (this.frame.max - this.frame.min)) *
				this.width,
		);

	rel_x_from_timestamp = (timestamp: number, scroll: XY = this.scroll) =>
		scroll ? this.abs_x_from_timestamp(timestamp) - scroll.x : -1;

	center_scroll_from_timestamp = (timestamp: number) =>
		Math.round(
			this.abs_x_from_timestamp(timestamp) - this.width / (2 * this.scale),
		);

	timestamp_from_rel_x = (relX: number, scroll: XY = this.scroll) => {
		const absX = relX + scroll.x;
		const ratio = absX / this.width;
		return Math.round(
			this.frame.min + ratio * (this.frame.max - this.frame.min),
		);
	};
}

export type Maybe<T> = T | null;

export type Sometimes<T> = Maybe<T> | undefined;

export type NotSure<T> = T[] | T;

export type Usual<T> = Sometimes<T> & NotSure<T>;

export const bich =
	(setState: SetState<string>) => (event: ChangeEvent<HTMLInputElement>) => {
		return setState(event.target.value);
	};

export const fws = { width: "100%" };

export async function sleep(ms = 0) {
	return new Promise((res) => setTimeout(res, ms));
}

export type NodeFile = NonNullable<
	NonNullable<ChangeEvent<HTMLInputElement>["target"]["files"]>[0]
>;

export class Refractor {
	private static readonly HASH_MOD = 8000;

	private static readonly hash = {
		fnv1a: (str: string) => {
			let hash = 0x811c9dc5;
			for (let i = 0; i < str.length; i++) {
				hash ^= str.charCodeAt(i);
				hash = Math.imul(hash, 0x01000193);
			}
			return hash >>> 0;
		},
		djb2: (str: string) => {
			let hash = 5381;
			for (let i = 0; i < str.length; i++) {
				hash = Math.imul(hash, 33) + str.charCodeAt(i);
			}
			return hash >>> 0;
		},
		sdbm: (str: string) => {
			let hash = 0;
			for (let i = 0; i < str.length; i++) {
				hash = str.charCodeAt(i) + Math.imul(hash, 65599);
			}
			return hash >>> 0;
		},
	};

	public static readonly reflect = {
		toVar: <T extends Record<string, any>>(obj: T) => {
			const reflection: Record<string, any> = {};
			Object.keys(obj).forEach((key) => {
				reflection[`--${key}`] = obj[key];
			});
			return reflection as { [K in keyof T as `--${string & K}`]: T[K] };
		},
	};

	public static readonly string = {
		toNumber: (str: string, hashFunction: HashFunctionName = "fnv1a") => {
			const algorithm = Refractor.hash[hashFunction] ?? Refractor.hash.fnv1a;
			return algorithm(str) % Refractor.HASH_MOD;
		},
	};

	public static readonly any = {
		toNumber: (
			value: any,
			hashFunction: HashFunctionName = "fnv1a",
		): number => {
			switch (typeof value) {
				case "string":
					return Refractor.string.toNumber(value, hashFunction);
				case "number":
					return value;
				case "bigint":
					return Internal.Transformator.toTimestamp(value);
				default:
					const result = Number(value);
					if (isNaN(result)) {
						return 0;
					}
					return result;
			}
		},
	};

	/**
	 * Safe nested property accessor using dot notation.
	 * @param obj Object to traverse
	 * @param path Dot-separated path (e.g. "gulp.unmapped.rempip")
	 * @returns Value at path or undefined
	 */
	public static get = (obj: any, path: string): any => {
		if (!path || !obj) return undefined;
		if (obj[path] !== undefined) return obj[path];

		const parts = path.split(".");
		for (let i = parts.length - 1; i > 0; i--) {
			const head = parts.slice(0, i).join(".");
			const tail = parts.slice(i).join(".");
			if (obj[head] !== undefined) {
				return Refractor.get(obj[head], tail);
			}
		}

		let current = obj;
		for (const part of parts) {
			if (current === null || current === undefined) return undefined;
			current = current[part];
		}
		return current;
	};

	/**
	 * Use this function to trigger react dependents rendering that uses this object
	 * @param obj Object
	 * @returns Object
	 */
	public static object = <T extends object>(obj: T) => ({ ...obj }) as T;

	/**
	 * Use this function to trigger react dependents rendering that uses this array
	 * @param obj Object
	 * @returns Object
	 */
	public static array = <T extends object>(...obj: T[]) => [...obj] as T[];
}

export interface RGB {
	r: number;
	g: number;
	b: number;
}

export function getSortOrder<T>(
	arr: T[],
	compareFn: (a: T, b: T) => number,
): "asc" | "desc" | "unsorted" {
	let asc = true;
	let desc = true;

	for (let i = 1; i < arr.length; i++) {
		const cmp = compareFn(arr[i - 1], arr[i]);
		if (cmp > 0) asc = false;
		if (cmp < 0) desc = false;
		if (!asc && !desc) return "unsorted";
	}

	if (asc) return "asc";
	if (desc) return "desc";
	return "unsorted";
}

export const formatTimestampToReadableString = (
	value: Date | number | string,
) => {
	try {
		const date = new Date(value);

		const pad = (n: number, z = 2) => ("00" + n).slice(-z);

		const getters = {
			year: Internal.Settings.isUTCTimestamps
				? date.getUTCFullYear()
				: date.getFullYear(),
			month: Internal.Settings.isUTCTimestamps
				? date.getUTCMonth() + 1
				: date.getMonth() + 1,
			day: Internal.Settings.isUTCTimestamps
				? date.getUTCDate()
				: date.getDate(),
			hour: Internal.Settings.isUTCTimestamps
				? date.getUTCHours()
				: date.getHours(),
			minute: Internal.Settings.isUTCTimestamps
				? date.getUTCMinutes()
				: date.getMinutes(),
			second: Internal.Settings.isUTCTimestamps
				? date.getUTCSeconds()
				: date.getSeconds(),
			ms: Internal.Settings.isUTCTimestamps
				? date.getUTCMilliseconds()
				: date.getMilliseconds(),
		};

		return "yyyy.MM.dd HH:mm:ss SSS"
			.replace("yyyy", getters.year.toString())
			.replace("MM", pad(getters.month))
			.replace("dd", pad(getters.day))
			.replace("HH", pad(getters.hour))
			.replace("mm", pad(getters.minute))
			.replace("ss", pad(getters.second))
			.replace("SSS", pad(getters.ms, 3));
	} catch (error) {
		Logger.error(
			`Invalid time value. Expected number | string | Date, got ${value}`,
			"Timestamp",
		);
		return "";
	}
};

/**
 * Checks if a value is a plain object.
 * @param value The value to check
 */
export const isPlainObject = (
	value: unknown,
): value is Record<string, unknown> =>
	Object.prototype.toString.call(value) === "[object Object]";

/**
 * Recursively sorts the keys of an object or array of objects.
 * @param value The object or array to sort
 */
export const sortObjectKeysRecursively = (value: unknown): unknown => {
	if (Array.isArray(value)) {
		return value.map((item) => sortObjectKeysRecursively(item));
	}

	if (!isPlainObject(value)) {
		return value;
	}

	const entries = Object.entries(value)
		.sort(([leftKey, leftValue], [rightKey, rightValue]) => {
			const leftIsObject = isPlainObject(leftValue);
			const rightIsObject = isPlainObject(rightValue);
			if (leftIsObject !== rightIsObject) {
				return leftIsObject ? 1 : -1;
			}
			return leftKey.localeCompare(rightKey);
		})
		.map(
			([key, nestedValue]) =>
				[key, sortObjectKeysRecursively(nestedValue)] as const,
		);

	return Object.fromEntries(entries);
};

/**
 * Parses a multi-line string into a key-value record by looking for colons.
 * @param raw Input string containing "key: value" lines
 */
export const parseLineToKeyValue = (raw: string): Record<string, string> => {
	const result: Record<string, string> = {};
	for (const line of raw.split("\n")) {
		const cleaned = line.trim();
		if (cleaned.length === 0) continue;

		const index = line.indexOf(":");
		if (index === -1) continue;

		const key = line
			.slice(0, index)
			.trim()
			.replace(/^"+|"+$/g, "");
		const value = line
			.slice(index + 1)
			.trim()
			.replace(/^"+|"+$/g, "")
			.replace(/[,"]+$/, "");
		result[key] = value || "*";
	}
	return result;
};

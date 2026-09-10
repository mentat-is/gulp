import { type Callback } from "@/ui/utils";
import { Logger } from "@/dto/Logger.class";
import { Icon } from "@/ui/Icon";
import { Request } from "@/entities/Request";
import { Internal } from "@/entities/addon/Internal";
import { toast } from "sonner";
import { translate } from "@/locales/core";
import { WindowBridge } from "@/lib/WindowBridge";

export interface ResponseBase<T = any> {
	status: "success" | "error" | "pending";
	timestamp: Date;
	req_id: Request.Id;
	data: T;
}

export interface ResponseErrorBody {
	request: {
		path: string;
		method: string;
		query: string;
		headers: {
			[key: string]: string;
		};
		body: string;
	};
	__error: {
		name: string;
		msg: string;
		trace: string;
	};
}

export type ResponseError = ResponseBase<ResponseErrorBody>;

export class ResponseHandler<T extends ResponseBase<any>> {
	status: "success" | "error" | "pending";
	req_id: Request.Id;
	timestamp: Date;
	data: T["data"];

	constructor(payload: T = {} as T) {
		this.status = payload.status ?? "error";
		this.req_id = payload.req_id ?? ("" as Request.Id);
		this.timestamp = payload.timestamp ?? Date.now();
		this.data = payload.data;

		if (this.status === "error" && !this.req_id && !this.data) {
			toast.error(translate("api.connectionFailed"), {
				description: translate("api.connectionCheck"),
				richColors: true,
				icon: <Icon name="Warning" />,
			});
		}
	}
}

export type SetState<T> = React.Dispatch<React.SetStateAction<T>>;

type Toast<T> =
	| ((data: ResponseBase<T>) => string | number | undefined)
	| null
	| undefined;

export interface RequestOptions<T> {
	toast?: {
		onSuccess?: Toast<T>;
		onError?: Toast<ResponseErrorBody>;
	};
	setLoading?: SetState<boolean>;
	query?: Record<string, any>;
	body?: Record<string, any> | RequestInit["body"];
	deassign?: boolean;
	/**
	 * Shows a centralized API error toast when a request fails.
	 */
	showErrorToast?: boolean;
	/**
	 * Localized title used by the centralized API error toast.
	 */
	errorToastTitle?: string;
	/**
	 * Redirects to login when the backend returns HTTP 401.
	 */
	authRedirect?: boolean;
	/**
	 * @deprecated MissingPermission is now always treated as a request failure.
	 */
	suppressMissingPermissionRedirect?: boolean;
}

type RawTrueOptions<T> = Omit<RequestInit, "body"> & {
	raw: true;
} & RequestOptions<T>;
type RawFalseOptions<T> = Omit<RequestInit, "body"> & {
	raw?: false;
} & RequestOptions<T>;
type AnyOptions<T> = Omit<RequestInit, "body"> & {
	raw?: boolean;
} & RequestOptions<T>;

export type Api = {
	/**
	 * @param setLoading: SetState<boolean>
	 *
	 * @param toast: keyof Locale | boolean
	 */
	<T>(
		path: string,
		options: RawTrueOptions<T>,
	): Promise<ResponseHandler<ResponseBase<T>>>;
	<T>(path: string, options?: RawFalseOptions<T>): Promise<T>;
	<T>(
		path: string,
		options?: AnyOptions<T>,
	): Promise<ResponseHandler<ResponseBase<T>> | T>;

	<T>(
		path: string,
		options: RawTrueOptions<T>,
		callback: Callback<ResponseHandler<ResponseBase<T>>>,
	): Promise<ResponseHandler<ResponseBase<T>>>;
	<T>(
		path: string,
		options?: RawFalseOptions<T>,
		callback?: Callback<T>,
	): Promise<T>;
	<T>(
		path: string,
		options?: AnyOptions<T>,
		callback?: Callback<ResponseHandler<ResponseBase<T>> | T>,
	): Promise<ResponseHandler<ResponseBase<T>> | T>;

	<T>(
		path: string,
		callback: Callback<ResponseHandler<ResponseBase<T>>>,
		options: RawTrueOptions<T>,
	): Promise<ResponseHandler<ResponseBase<T>>>;
	<T>(
		path: string,
		callback: Callback<T>,
		options?: RawFalseOptions<T>,
	): Promise<T>;
	<T>(
		path: string,
		callback: Callback<ResponseHandler<ResponseBase<T>> | T>,
		options?: AnyOptions<T>,
	): Promise<ResponseHandler<ResponseBase<T>> | T>;
};

type unresolwedArgument<T> =
	| (RequestInit & RequestOptions<T> & { raw?: boolean })
	| Callback<T>
	| undefined;

/**
 * Normalizes the overloaded API call arguments into fetch options and callback.
 * @param a First overloaded API argument.
 * @param b Second overloaded API argument.
 * @param _path Request path before leading slash normalization.
 * @returns Normalized request options, callback, query, and path.
 */
export function parseApiOptions<T>(
	a: unresolwedArgument<T>,
	b: unresolwedArgument<T>,
	_path: string,
) {
	const options: RequestInit & RequestOptions<T> & { raw?: boolean } = {};
	let callback: Callback<T> | undefined;

	if (typeof a === "function") {
		callback = a;
		if (b && typeof b === "object") {
			Object.assign(options, b);
		}
	} else if (typeof a === "object") {
		Object.assign(options, a);
		if (b && typeof b === "function") {
			callback = b;
		}
	}

	const headers: Record<string, string> = {
		token: Internal.Settings.token,
	};

	if (!options.deassign) {
		headers["Content-Type"] = "application/json";
	}

	options.headers = Object.assign(options.headers || {}, headers);

	if (typeof options.body === "object" && !(options.body instanceof FormData)) {
		options.body = JSON.stringify(options.body, (_, v) =>
			typeof v === "bigint" ? v.toString() : v,
		);
	}

	const path = _path.startsWith("/") ? _path : `/${_path}`;

	const toStringObject = (obj: typeof options.query) =>
		obj
			? Object.fromEntries(
					Object.entries(options.query || {}).map(([key, value]) => [
						key,
						String(value),
					]),
				)
			: "";

	const query = new URLSearchParams(toStringObject(options.query));

	return {
		options,
		callback,
		query,
		path,
	};
}

export function soft<T>(value: T, func?: Callback<T>) {
	if (!func) {
		return;
	}

	return func(value);
}

/**
 * Checks whether a response payload contains backend error details.
 *
 * @param data - Raw response data returned by the backend.
 * @returns True when the payload has the standard backend error shape.
 */
function isResponseErrorBody(data: unknown): data is ResponseErrorBody {
	if (typeof data !== "object" || data === null) {
		return false;
	}

	const error = (data as { __error?: unknown }).__error;
	return typeof error === "object" && error !== null;
}

/**
 * Extracts a readable backend error message from a raw API response.
 *
 * @param response - API response wrapper produced for a failed request.
 * @returns Backend error name/message text when the response exposes it.
 */
function getResponseErrorDescription(
	response: ResponseHandler<ResponseBase<unknown>>,
): string | undefined {
	if (!isResponseErrorBody(response.data)) {
		return undefined;
	}

	const { name, msg } = response.data.__error;
	const parts = [name, msg].filter(
		(part): part is string => typeof part === "string" && part.length > 0,
	);
	return parts.length > 0 ? parts.join(": ") : undefined;
}

/**
 * Shows the shared API failure toast unless the request opted out.
 *
 * @param response - API response wrapper produced for a failed request.
 * @param options - Request options controlling toast behavior.
 */
function showApiErrorToast<T>(
	response: ResponseHandler<ResponseBase<T>>,
	options: RequestOptions<T>,
) {
	if (options.showErrorToast === false) {
		return;
	}

	toast.error(options.errorToastTitle ?? translate("api.requestFailed"), {
		description: getResponseErrorDescription(
			response as ResponseHandler<ResponseBase<unknown>>,
		),
		icon: <Icon name="Stop" />,
		richColors: true,
	});
}

/**
 * Clears local authentication state and redirects the main window to login.
 */
export function redirectToLogin(): void {
	WindowBridge.persistMainStatus("auth_lost");
	if (window.location.pathname.startsWith("/detached/")) {
		WindowBridge.sendControlMessage(
			WindowBridge.MessageType.AUTH_LOST,
			{},
		);
	}
	window.dispatchEvent(new CustomEvent("gulp-auth-lost"));
	localStorage.removeItem(Internal.LocalStorageItemsList.GENERAL_TOKEN_VALUE);
	localStorage.removeItem("__user_id");
	Logger.warn("Session has been expired", "API", {
		icon: <Icon name="Warning" />,
		richColors: true,
	});
	if (window.location.pathname !== "/login") {
		const redirectUrl = encodeURIComponent(
			window.location.pathname + window.location.search,
		);
		window.location.href = `/login?redirect=${redirectUrl}`;
	}
}

/**
 * Builds a fallback API envelope when an HTTP response has no JSON body.
 *
 * @param response - Fetch response returned by the backend.
 * @param path - Normalized API request path.
 * @param query - Serialized query parameters sent with the request.
 * @param options - Fetch options used for the request.
 * @returns A response envelope compatible with ResponseHandler.
 */
function createFallbackResponse<T>(
	response: Response,
	path: string,
	query: URLSearchParams,
	options: RequestInit,
): ResponseBase<T | ResponseErrorBody> {
	if (response.ok) {
		return {
			status: "success",
			timestamp: new Date(),
			req_id: "" as Request.Id,
			data: undefined as T,
		};
	}

	return {
		status: "error",
		timestamp: new Date(),
		req_id: "" as Request.Id,
		data: {
			request: {
				path,
				method: options.method ?? "GET",
				query: query.toString(),
				headers: {},
				body: typeof options.body === "string" ? options.body : "",
			},
			__error: {
				name: response.statusText || "HTTPError",
				msg: `HTTP ${response.status}`,
				trace: "",
			},
		},
	};
}

/**
 * Executes an authenticated backend request with optional response unwrapping.
 * @param _path Backend API path.
 * @param arg2 Options or callback from the overloaded API signature.
 * @param arg3 Options or callback from the overloaded API signature.
 * @returns The unwrapped response data or a raw ResponseHandler.
 */
const api: Api = async function <T>(
	_path: string,
	arg2?: any,
	arg3?: any,
): Promise<ResponseHandler<ResponseBase<T>> | T> {
	const { options, callback, query, path } = parseApiOptions<T>(
		arg2,
		arg3,
		_path,
	);

	soft(() => true, options.setLoading);

	let aborted = false;
	const url = `${Internal.Settings.server}${path}${query ? `?${query}` : ""}`;
	const response = await fetch(url, {
		...options,
	}).catch((error) => {
		if (error instanceof DOMException && error.name === "AbortError") {
			aborted = true;
		}
		return undefined;
	});

	if (aborted) {
		soft(() => false, options.setLoading);
		return void 0 as T;
	}

	let json: any;
	try {
		json = await (response?.json() ?? Promise.resolve(undefined));
	} catch (err) {
		// Gracefully handle non-JSON or malformed responses
	}

	if (response?.status === 401 && options.authRedirect !== false) {
		soft(() => false, options.setLoading);
		redirectToLogin();
		return void 0 as T;
	}

	const payload =
		json ??
		(response ? createFallbackResponse<T>(response, path, query, options) : json);
	const res = new ResponseHandler(payload as ResponseBase<T>);
	const isHttpError = response ? !response.ok : false;
	const hasBackendError = isResponseErrorBody(res.data);
	const isSuccessResponse =
		!isHttpError &&
		(["success", "pending"].includes(res.status) ||
			Boolean(res.data && !hasBackendError));

	if (!response) {
		Logger.error(res, "API");
		soft(() => false, options.setLoading);
		return options.raw ? res : (void 0 as T);
	}

	if (isSuccessResponse) {
		if (options.toast?.onSuccess) {
			options.toast?.onSuccess(res);
			Logger.log(res, "API");
		}
		// @ts-ignore
		soft(options.raw ? res : res.data, callback);
	} else {
		if (options.toast?.onError) {
			try {
				options.toast.onError(res as ResponseError);
			} catch (error) {
				Logger.error(error, "API onError");
			}
		}
		showApiErrorToast(res, options);
		if (
			hasBackendError &&
			(res.data as ResponseErrorBody).__error.name === "MissingPermission"
		) {
			Logger.warn("Request declined by server", api);
		} else {
			Logger.error(res, "API");
		}
		soft(() => false, options.setLoading);
		if (options.raw) {
			// @ts-ignore
			soft(res, callback);
			return res;
		}
		return void 0 as T;
	}

	soft(() => false, options.setLoading);
	return options.raw ? res : res.data;
};

globalThis.api = api;

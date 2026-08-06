/** Callable mock function exposed by React Scripts' bundled Jest runtime. */
interface JestMock<TResult, TArguments extends unknown[]> {
	(...arguments_: TArguments): TResult;
	readonly mock: { calls: TArguments[] };
}

/** Minimal method-spy surface used by lifecycle tests. */
interface JestSpy {
	/** Resolves asynchronous calls with the supplied value. */
	mockResolvedValue(value: unknown): JestSpy;
	/** Returns the supplied value from subsequent calls. */
	mockReturnValue(value: unknown): JestSpy;
	readonly mock: { calls: unknown[][] };
}

/** Minimal synchronous matcher surface used by the repository's dependency-free tests. */
interface JestMatchers<TActual> {
	readonly not: JestMatchers<TActual>;
	readonly resolves: JestAsyncMatchers;
	readonly rejects: JestAsyncMatchers;
	toBe(expected: unknown): void;
	toEqual(expected: unknown): void;
	toMatchObject(expected: object): void;
	toBeInstanceOf(expected: object): void;
	toBeUndefined(): void;
	toBeNull(): void;
	toHaveLength(expected: number): void;
	toHaveBeenCalled(): void;
	/** Checks that a mock received the expected arguments in at least one call. */
	toHaveBeenCalledWith(...expected: unknown[]): void;
	toHaveBeenCalledTimes(expected: number): void;
}

/** Minimal asynchronous matcher surface used for rejected worker operations. */
interface JestAsyncMatchers {
	toThrow(expected?: string | RegExp): Promise<void>;
}

/** Creates an expectation for a value under the React Scripts Jest runtime. */
declare function expect<TActual>(actual: TActual): JestMatchers<TActual>;

/** Groups related tests. */
declare function describe(name: string, action: () => void): void;

/** Registers one test case. */
declare function it(
	name: string,
	action: () => void | Promise<void>,
): void;

/** Registers setup executed once before a test group. */
declare function beforeAll(action: () => void | Promise<void>): void;

/** Registers cleanup executed once after a test group. */
declare function afterAll(action: () => void | Promise<void>): void;

/** Registers setup executed before each test. */
declare function beforeEach(action: () => void | Promise<void>): void;

/** Registers cleanup executed after each test. */
declare function afterEach(action: () => void | Promise<void>): void;

/** Minimal Jest mock factory bundled transitively by React Scripts. */
declare const jest: {
	/** Replaces one imported module with a deterministic test factory. */
	mock(moduleName: string, factory: () => unknown): void;
	fn<TResult = void, TArguments extends unknown[] = []>(
		implementation?: (...arguments_: TArguments) => TResult,
	): JestMock<TResult, TArguments>;
	/** Replaces one object method while retaining call history. */
	spyOn<TObject extends object>(
		target: TObject,
		method: keyof TObject,
	): JestSpy;
	/** Replaces native timers with the deterministic Jest timer implementation. */
	useFakeTimers(): void;
	/** Restores the native timer implementation. */
	useRealTimers(): void;
	/** Advances deterministic timers by the supplied duration. */
	advanceTimersByTime(milliseconds: number): void;
};

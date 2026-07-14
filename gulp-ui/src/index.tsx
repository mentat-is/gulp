import ReactDOM from "react-dom/client";
import "./global.css";
import { Application } from "./context/Application.context";
import { Toaster } from "./ui/Toaster";
import { Api } from "./class/API";
import { Extension } from "./context/Extension.context";
import { Logger } from "./dto/Logger.class";
import { Auth } from "./page/Auth.page";
import { Boundary } from "./context/Boundary.context";
import { Theme } from "./context/Theme.context";
import { RendererTest } from "./page/RendererTest.page";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { OperationView } from "./page/OperationView.page";
import { Home } from "./page/Home.page";
import { Locale } from "./locales";
import { DetachedWindow } from "./context/DetachedWindow.context";
import { DetachedRouteShell } from "./page/DetachedRoute.page";

const root = document.getElementById("root");

ReactDOM.createRoot(root!).render(Root());

declare global {
	var api: Api;
}

function RequireAuth({ children }: { children: React.ReactNode }) {
	const { app } = Application.use();
	const hasToken =
		!!localStorage.getItem("__token") &&
		localStorage.getItem("__token") !== "-";
	const isAuthenticated =
		!!app.general.user || hasToken || app.general.skippedAuth;

	if (!isAuthenticated) {
		const redirectUrl = encodeURIComponent(
			window.location.pathname + window.location.search,
		);
		return (
			<Navigate
				to={`/login?redirect=${redirectUrl}`}
				replace
			/>
		);
	}

	return <>{children}</>;
}

/**
 * Redirects already-authenticated users away from the login page to the home page.
 * Prevents logged-in users from landing on /login via direct navigation or history.
 */
function RedirectIfAuthenticated({ children }: { children: React.ReactNode }) {
	const { app } = Application.use();
	const hasToken =
		!!localStorage.getItem("__token") &&
		localStorage.getItem("__token") !== "-";
	const isAuthenticated =
		!!app.general.user || hasToken || app.general.skippedAuth;

	if (isAuthenticated) {
		const redirectParam = new URLSearchParams(window.location.search).get(
			"redirect",
		);
		console.warn("Already authenticated, redirecting to /" + redirectParam);
		if (redirectParam) {
			return (
				<Navigate
					to={`${redirectParam}`}
					replace
				/>
			);
		}
		return (
			<Navigate
				to="/"
				replace
			/>
		);
	}

	return <>{children}</>;
}

/**
 * Renders the authenticated main application routes under the detached-window coordinator.
 * @returns Main application route tree.
 */
function MainApplicationRoutes() {
	return (
		<DetachedWindow.Provider>
			<Routes>
				<Route
					path="/renderer-test"
					element={<RendererTest.Page />}
				/>
				<Route
					path="/login"
					element={
						<RedirectIfAuthenticated>
							<Auth.Page />
						</RedirectIfAuthenticated>
					}
				/>
				<Route
					path="/"
					element={
						<RequireAuth>
							<Home.Page section="operations" />
						</RequireAuth>
					}
				/>
				<Route
					path="/users"
					element={
						<RequireAuth>
							<Home.Page section="users" />
						</RequireAuth>
					}
				/>
				<Route
					path="/groups"
					element={
						<RequireAuth>
							<Home.Page section="groups" />
						</RequireAuth>
					}
				/>
				<Route
					path="/plugin-configurations"
					element={
						<RequireAuth>
							<Home.Page section="pluginConfigurations" />
						</RequireAuth>
					}
				/>
				<Route
					path="/operations/:operation_id"
					element={
						<RequireAuth>
							<OperationView />
						</RequireAuth>
					}
				/>
				<Route
					path="*"
					element={
						<Navigate
							to="/login"
							replace
						/>
					}
				/>
			</Routes>
		</DetachedWindow.Provider>
	);
}

function Root() {
	if (window.onerror) {
		window.onerror = function (...props) {
			Logger.error("[Global Error]", props.join("\n"));
		};
	}

	window.onerror = function (msg: any, src, line, col, err) {
		Logger.error("[Global Error]", msg);
		const inst = Boundary.Provider.instance;
		if (err && inst) {
			inst.showError(err);
		}
	};

	window.onunhandledrejection = function (event) {
		Logger.error("[Unhandled Rejection]", event.reason);
		const inst = Boundary.Provider.instance;
		if (event.reason && inst) {
			const error =
				event.reason instanceof Error
					? event.reason
					: new Error(String(event.reason));
			inst.showError(error);
		}
	};

	return (
		<>
			<Theme.Provider>
				<Toaster />
				<BrowserRouter>
					<Application.Provider>
						<Locale.Provider>
							<Boundary.Provider>
								<Extension.Provider>
									<Routes>
										<Route
											path="/detached/:kind"
											element={<DetachedRouteShell />}
										/>
										<Route
											path="/*"
											element={<MainApplicationRoutes />}
										/>
									</Routes>
								</Extension.Provider>
							</Boundary.Provider>
						</Locale.Provider>
					</Application.Provider>
				</BrowserRouter>
			</Theme.Provider>
		</>
	);
}

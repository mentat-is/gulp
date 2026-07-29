import { applyDetachedThemeSnapshot } from "@/context/detachedTheme";

describe("detached theme synchronization", () => {
	afterEach(() => {
		document.documentElement.removeAttribute("data-theme");
		document.documentElement.style.removeProperty("--accent");
		document.documentElement.style.removeProperty("--background-100");
		document.documentElement.style.removeProperty("--stale-theme-value");
	});

	it("applies the targeted theme and removes stale computed variables", () => {
		document.documentElement.style.setProperty("--stale-theme-value", "old");

		const appliedVariables = applyDetachedThemeSnapshot(
			document,
			{
				theme: "classic-light",
				mode: "light",
				cssVariables: {
					"--accent": "#111111",
					"--background-100": "#ffffff",
				},
			},
			new Set(["--stale-theme-value"]),
		);

		expect(document.documentElement.getAttribute("data-theme")).toBe(
			"classic-light",
		);
		expect(
			document.documentElement.style.getPropertyValue("--accent"),
		).toBe("#111111");
		expect(
			document.documentElement.style.getPropertyValue(
				"--stale-theme-value",
			),
		).toBe("");
		expect([...appliedVariables].sort()).toEqual([
			"--accent",
			"--background-100",
		]);
	});
});

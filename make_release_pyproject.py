#!/usr/bin/env python3
"""
make_release_pyproject.py

Reads a pyproject.toml, finds all unpinned dependencies, and pins them
to the versions currently installed in the active virtual environment.
Produces a new pyproject.toml with pinned versions.

Usage:
    python make_release_pyproject.py [input.toml] [output.toml] [--exclude pkg1,pkg2,...]
"""

import argparse
import re
import sys
import subprocess
from pathlib import Path


def get_installed_versions() -> dict[str, str]:
    """Return a mapping of package name -> installed version from the current venv."""
    result = subprocess.run(
        [sys.executable, "-m", "pip", "freeze"],
        capture_output=True,
        text=True,
        check=True,
    )
    versions: dict[str, str] = {}
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Handle editable installs: -e git+...#egg=pkg==version
        if line.startswith("-e "):
            egg_match = re.search(r"#egg=([^=&]+)==([^=&]+)", line)
            if egg_match:
                pkg = egg_match.group(1).strip()
                ver = egg_match.group(2).strip()
                versions[pkg.lower()] = ver
            continue
        # Handle standard formats: pkg==version, pkg==version+local, pkg @ url
        if " @ " in line:
            # URL-based install — skip pinning
            continue
        # Split on ==, but be careful with extras like pkg[extra]==version
        m = re.match(r"([^=]+)==(.+)", line)
        if m:
            pkg = m.group(1).strip()
            ver = m.group(2).strip()
            # Strip extras for lookup key
            pkg_key = re.split(r"[\[;]", pkg)[0].strip().lower()
            versions[pkg_key] = ver
    return versions


def normalize_pkg_name(name: str) -> str:
    """PEP 503 normalization: lower-case and replace -/_ with -."""
    return re.sub(r"[-_.]+", "-", name.lower())


def parse_exclude_list(exclude_arg: str | None) -> set[str]:
    """Parse a comma-separated exclude list into a set of normalized names."""
    if not exclude_arg:
        return set()
    return {normalize_pkg_name(p.strip()) for p in exclude_arg.split(",") if p.strip()}


def pin_dependency_line(
    line: str, installed: dict[str, str], excluded: set[str]
) -> str | None:
    """
    Given a single dependency string from a TOML list,
    return a pinned version if it is unpinned and found in the venv.
    Returns None if the line should be left untouched.
    """
    # Match a quoted dependency string, possibly with trailing comma
    m = re.match(r'^(\s*")([^"]+)("\s*,?\s*)$', line)
    if not m:
        return None  # Not a standard dependency line

    prefix = m.group(1)
    dep_spec = m.group(2)
    suffix = m.group(3)

    # Already pinned?  (contains ==, >=, <=, ~=, <, >, !=, or @)
    if re.search(r"(==|>=|<=|~=|<|>|!=|@)", dep_spec):
        return None  # Leave already-constrained deps alone

    # Strip extras and environment markers for lookup
    pkg_name = re.split(r"[\[;]", dep_spec)[0].strip()
    norm_name = normalize_pkg_name(pkg_name)

    # Check exclusion list
    if norm_name in excluded:
        return None  # Leave excluded packages unpinned

    version = installed.get(norm_name)
    if version:
        # Preserve extras and markers, pin only the version part
        rest = dep_spec[len(pkg_name) :]
        pinned = f"{pkg_name}=={version}{rest}"
        return f"{prefix}{pinned}{suffix}"

    # Not installed in venv — leave as-is (print warning)
    print(f"⚠️  Not installed in venv, leaving unpinned: {dep_spec}", file=sys.stderr)
    return None


def process_pyproject(input_path: Path, output_path: Path, excluded: set[str]) -> None:
    """Read input pyproject.toml, pin deps, write output."""
    text = input_path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    installed = get_installed_versions()

    new_lines: list[str] = []
    in_deps = False

    for raw_line in lines:
        line = raw_line.rstrip("\n")
        stripped = line.strip()

        if stripped == "dependencies = [":
            in_deps = True
            new_lines.append(raw_line)
            continue

        if in_deps and stripped == "]":
            in_deps = False
            new_lines.append(raw_line)
            continue

        if in_deps:
            pinned = pin_dependency_line(line, installed, excluded)
            if pinned is not None:
                new_lines.append(pinned + "\n")
                continue

        new_lines.append(raw_line)

    output_path.write_text("".join(new_lines), encoding="utf-8")
    print(f"✅ Written pinned pyproject.toml to {output_path}")
    if excluded:
        print(f"   Excluded (left unpinned): {', '.join(sorted(excluded))}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pin pyproject.toml dependencies to versions installed in the current venv."
    )
    parser.add_argument(
        "input",
        nargs="?",
        default="pyproject.toml",
        help="Input pyproject.toml file (default: pyproject.toml)",
    )
    parser.add_argument(
        "output",
        nargs="?",
        default="pyproject.pinned.toml",
        help="Output file (default: pyproject.pinned.toml)",
    )
    parser.add_argument(
        "--exclude",
        metavar="PACKAGES",
        default="",
        help='Comma-separated list of package names to exclude from pinning (e.g. "gulp-sdk,muty-python")',
    )
    args = parser.parse_args()

    input_file = Path(args.input)
    output_file = Path(args.output)
    excluded = parse_exclude_list(args.exclude)

    if not input_file.exists():
        print(f"❌ Input file not found: {input_file}", file=sys.stderr)
        sys.exit(1)

    process_pyproject(input_file, output_file, excluded)


if __name__ == "__main__":
    main()

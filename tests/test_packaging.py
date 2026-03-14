"""
tests/test_packaging — Packaging hygiene guard.

Scans every source package via AST to find third-party imports, then
cross-references against pyproject.toml to ensure:

1. Every import is covered by a declared dependency (core or extra).
2. Every source directory with __init__.py ships in the wheel.
3. Every extra's deps can be resolved to a real PyPI package name.

No network access, no pip installs — pure static analysis.
"""

from __future__ import annotations

import ast
import sys
try:
    import tomllib
except ImportError:
    import tomli as tomllib
from pathlib import Path

import pytest

# ── Project root ──────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"

# ── Known import-name → PyPI-name translations ───────────────────────────
# Only needed when the import name differs from the pip package name.
_IMPORT_TO_PYPI: dict[str, str] = {
    "psycopg2": "psycopg2_binary",
    "bs4": "beautifulsoup4",
    "google": "google_genai",
    "perspective": "perspective_python",
    "deephaven": "deephaven_server",
    "deephaven_server": "deephaven_server",
    "pydeephaven": "pydeephaven",
    "pymupdf": "pymupdf",
    "cv2": "opencv_python",
    "PIL": "pillow",
    "yaml": "pyyaml",
    "sklearn": "scikit_learn",
}

# Which extra(s) are expected to cover a given source package.
# Packages not listed here default to [<package_name>].
# Empty list means the package should be covered by core deps only.
_PKG_TO_EXTRAS: dict[str, list[str]] = {
    "store": [],
    "reactive": [],
    "db": [],
    "workflow": [],
    "bridge": [],
    "scheduler": ["scheduler"],
    "models": [],
    "marketmodel": ["pricing"],
    "instruments": ["pricing"],
    "objectstore": ["lakehouse", "media"],
    "streaming": ["streaming"],
}

# Directories that are NOT source packages (skip them).
_SKIP_DIRS = {"tests", "demos", "examples", "venv", ".venv", "__pycache__",
              ".git", ".github", ".ruff_cache", ".mypy_cache", "node_modules"}

STDLIB = frozenset(sys.stdlib_module_names)


# ── Helpers ───────────────────────────────────────────────────────────────

def _load_pyproject() -> dict:
    with open(PYPROJECT, "rb") as f:
        return tomllib.load(f)


def _norm_dep(dep: str) -> str:
    """Normalize a pip requirement string to a bare package name."""
    return (dep.split("[")[0].split(">")[0].split("<")[0]
            .split("=")[0].split(";")[0].strip()
            .lower().replace("-", "_"))


def _norm_import(imp: str) -> str:
    """Map an import name to its normalized PyPI package name."""
    return _IMPORT_TO_PYPI.get(imp, imp).lower().replace("-", "_")


def _scan_imports(pkg_dir: Path, local_modules: frozenset[str]) -> dict[str, set[str]]:
    """AST-scan a package dir and return {import_name: {file, …}} for third-party imports."""
    result: dict[str, set[str]] = {}
    for py_file in pkg_dir.rglob("*.py"):
        try:
            tree = ast.parse(py_file.read_text(), filename=str(py_file))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            tops: list[str] = []
            if isinstance(node, ast.Import):
                tops = [alias.name.split(".")[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                tops = [node.module.split(".")[0]]
            for top in tops:
                if top not in local_modules and top not in STDLIB:
                    result.setdefault(top, set()).add(str(py_file.relative_to(ROOT)))
    return result


# ── Tests ─────────────────────────────────────────────────────────────────

class TestDependencyCoverage:
    """Every third-party import in a wheel package must be declared in pyproject.toml."""

    @pytest.fixture(scope="class")
    def config(self):
        return _load_pyproject()

    @pytest.fixture(scope="class")
    def wheel_packages(self, config) -> list[str]:
        return config["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"]

    @pytest.fixture(scope="class")
    def core_deps(self, config) -> frozenset[str]:
        return frozenset(_norm_dep(d) for d in config["project"]["dependencies"])

    @pytest.fixture(scope="class")
    def extras(self, config) -> dict[str, frozenset[str]]:
        return {
            k: frozenset(_norm_dep(d) for d in v)
            for k, v in config["project"].get("optional-dependencies", {}).items()
        }

    @pytest.fixture(scope="class")
    def local_modules(self, wheel_packages) -> frozenset[str]:
        return frozenset(wheel_packages) | _SKIP_DIRS

    def test_all_imports_declared(self, wheel_packages, core_deps, extras, local_modules):
        """Every third-party import must be covered by core deps or the correct extra."""
        gaps: list[str] = []

        for pkg in wheel_packages:
            pkg_dir = ROOT / pkg
            if not pkg_dir.exists():
                continue

            covering_extras = _PKG_TO_EXTRAS.get(pkg, [pkg])
            imports = _scan_imports(pkg_dir, local_modules)

            for imp, files in sorted(imports.items()):
                pypi = _norm_import(imp)

                # Check core
                if pypi in core_deps:
                    continue

                # Check covering extras
                if covering_extras and any(
                    pypi in extras.get(extra, frozenset())
                    for extra in covering_extras
                ):
                    continue

                # Core-only packages (empty covering_extras): already checked core above
                if not covering_extras:
                    pass  # fall through to gap

                file_list = ", ".join(sorted(files))
                extras_note = f" (should be in [{' or '.join(covering_extras)}])" if covering_extras else " (should be in core)"
                gaps.append(f"{pkg}/{imp} → {pypi}{extras_note}  used in: {file_list}")

        assert not gaps, (
            f"Found {len(gaps)} undeclared dependency(ies):\n" +
            "\n".join(f"  • {g}" for g in gaps)
        )


class TestWheelCompleteness:
    """Every source directory with __init__.py must ship in the wheel."""

    @pytest.fixture(scope="class")
    def config(self):
        return _load_pyproject()

    @pytest.fixture(scope="class")
    def wheel_packages(self, config) -> set[str]:
        return set(config["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"])

    def test_all_source_packages_in_wheel(self, wheel_packages):
        """No source package left behind."""
        missing: list[str] = []
        for d in sorted(ROOT.iterdir()):
            if (
                d.is_dir()
                and (d / "__init__.py").exists()
                and d.name not in _SKIP_DIRS
                and d.name not in wheel_packages
            ):
                missing.append(d.name)

        assert not missing, (
            f"Source packages missing from [tool.hatch.build.targets.wheel].packages:\n" +
            "\n".join(f"  • {m}/" for m in missing)
        )

    def test_wheel_packages_exist(self, wheel_packages):
        """Every declared wheel package dir actually exists."""
        phantoms = [p for p in sorted(wheel_packages) if not (ROOT / p).is_dir()]
        assert not phantoms, (
            f"Wheel declares packages that don't exist:\n" +
            "\n".join(f"  • {p}/" for p in phantoms)
        )


class TestExtrasConsistency:
    """Extras should be well-formed and not contain stale entries."""

    @pytest.fixture(scope="class")
    def config(self):
        return _load_pyproject()

    @pytest.fixture(scope="class")
    def extras(self, config) -> dict[str, list[str]]:
        return config["project"].get("optional-dependencies", {})

    def test_no_duplicate_deps_in_extra(self, extras):
        """No extra should list the same package twice."""
        dupes: list[str] = []
        for name, deps in extras.items():
            normed = [_norm_dep(d) for d in deps]
            seen: set[str] = set()
            for n in normed:
                if n in seen:
                    dupes.append(f"[{name}]: {n}")
                seen.add(n)
        assert not dupes, (
            f"Duplicate deps in extras:\n" + "\n".join(f"  • {d}" for d in dupes)
        )

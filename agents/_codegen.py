"""
Agent Code Generation — persistent Python module writing + ephemeral sandbox.

Provides three tools for agents:
    - inspect_registry   — check which columns exist before generating code
    - define_module      — write a .py file to agent_generated/ and exec it
    - execute_python     — run ephemeral code in a sandboxed namespace

The ``define_module`` tool writes files to two auto-import directories:
    - store/columns/agent_generated/   — column definitions
    - models/agent_generated/          — Storable classes, @computed, @ticking

On startup, every .py in these directories is auto-imported, so agent-created
schemas survive process restarts.
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
import io
import json
import logging
import shutil
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from ai import tool

from agents._context import _PlatformContext

logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────

# Resolve project root relative to this file (agents/_codegen.py → project/)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent

COLUMNS_DIR = _PROJECT_ROOT / "store" / "columns" / "agent_generated"
MODELS_DIR = _PROJECT_ROOT / "models" / "agent_generated"

# ── Forbidden AST patterns ─────────────────────────────────────────

_FORBIDDEN_MODULES = frozenset({
    "os", "sys", "subprocess", "shutil", "socket", "http",
    "ftplib", "smtplib", "ctypes", "signal", "multiprocessing",
    "threading", "importlib", "pathlib", "tempfile", "glob",
    "pickle", "shelve", "webbrowser", "code", "codeop",
    "compileall", "py_compile",
})

_FORBIDDEN_BUILTINS = frozenset({
    "exec", "eval", "compile", "__import__", "open",
    "breakpoint", "exit", "quit",
})


class CodegenError(Exception):
    """Raised when code generation or validation fails."""


# ── AST Validation ─────────────────────────────────────────────────

def validate_code(source: str) -> list[str]:
    """Parse and validate Python source for safety.

    Returns a list of error strings.  Empty list = safe.
    """
    errors = []

    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        return [f"SyntaxError: {e}"]

    for node in ast.walk(tree):
        # Check import statements
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".")[0]
                if top in _FORBIDDEN_MODULES:
                    errors.append(
                        f"Forbidden import: '{alias.name}' "
                        f"(line {node.lineno})"
                    )

        elif isinstance(node, ast.ImportFrom):
            if node.module:
                top = node.module.split(".")[0]
                if top in _FORBIDDEN_MODULES:
                    errors.append(
                        f"Forbidden import: 'from {node.module}' "
                        f"(line {node.lineno})"
                    )

        # Check forbidden builtins
        elif isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id in _FORBIDDEN_BUILTINS:
                errors.append(
                    f"Forbidden builtin: '{func.id}()' "
                    f"(line {node.lineno})"
                )

    return errors


# ── Sandbox Namespace ──────────────────────────────────────────────

def _build_sandbox_namespace(ctx: _PlatformContext | None = None) -> dict:
    """Build a restricted namespace for exec'ing agent code."""
    import dataclasses
    import json as json_mod
    import math

    from store import Storable
    from store import REGISTRY
    from store import ColumnDef

    ns = {
        # Column registry
        "REGISTRY": REGISTRY,
        "ColumnDef": ColumnDef,

        # Data modeling
        "Storable": Storable,
        "dataclass": dataclasses.dataclass,
        "field": dataclasses.field,
        "dataclasses": dataclasses,

        # Stdlib (safe subset)
        "math": math,
        "datetime": datetime,
        "date": date,
        "Decimal": Decimal,
        "json": json_mod,

        # Builtins: AST validation is the real safety layer (blocks os/sys/subprocess/exec/eval/open).
        # Using full builtins avoids breaking Python's metaclass/dataclass internals.
    }

    # Reactive (lazy — may not always be needed)
    try:
        from reactive.computed import computed, effect
        ns["computed"] = computed
        ns["effect"] = effect
    except ImportError:
        pass

    # Streaming ticking decorator (lazy)
    try:
        from streaming.decorator import ticking
        ns["ticking"] = ticking
    except ImportError:
        pass

    # Platform context if provided
    if ctx is not None:
        ns["ctx"] = ctx

    return ns


def _safe_builtins() -> dict:
    """A restricted set of builtins — no open/exec/eval/__import__."""
    import builtins
    safe = {}
    allowed = {
        # Types
        "bool", "int", "float", "str", "bytes", "list", "tuple",
        "dict", "set", "frozenset", "type", "object",
        # Functions
        "abs", "all", "any", "bin", "chr", "divmod", "enumerate",
        "filter", "format", "getattr", "hasattr", "hash", "hex",
        "id", "isinstance", "issubclass", "iter", "len", "map",
        "max", "min", "next", "oct", "ord", "pow", "print",
        "property", "range", "repr", "reversed", "round",
        "setattr", "slice", "sorted", "staticmethod", "sum",
        "super", "zip", "classmethod",
        # Exceptions
        "Exception", "ValueError", "TypeError", "KeyError",
        "IndexError", "AttributeError", "RuntimeError",
        "StopIteration", "NotImplementedError", "ZeroDivisionError",
        "ArithmeticError", "LookupError", "OverflowError",
    }
    for name in allowed:
        if hasattr(builtins, name):
            safe[name] = getattr(builtins, name)
    # None, True, False must be available
    safe["None"] = None
    safe["True"] = True
    safe["False"] = False
    # Allow imports — AST validation is the real guard against forbidden modules
    import builtins as _b
    safe["__import__"] = _b.__import__
    # __build_class__ is required for `class` statements in exec'd code
    safe["__build_class__"] = _b.__build_class__
    # __name__ is required by dataclass/metaclass machinery
    safe["__name__"] = "__agent_sandbox__"
    return safe


# ── Auto-import ────────────────────────────────────────────────────

def _ensure_dirs() -> None:
    """Create the agent_generated directories and __init__.py files."""
    for d in (COLUMNS_DIR, MODELS_DIR):
        d.mkdir(parents=True, exist_ok=True)
        init = d / "__init__.py"
        if not init.exists():
            init.write_text(
                '"""Agent-generated modules — auto-imported on startup."""\n'
            )


def load_agent_modules(directory: Path) -> list[str]:
    """Import all .py modules from an agent_generated directory.

    Returns list of module names that were loaded.
    """
    loaded: list[str] = []
    if not directory.exists():
        return loaded

    for py_file in sorted(directory.glob("*.py")):
        if py_file.name.startswith("_"):
            continue

        module_name = f"_agent_gen_{directory.parent.name}_{py_file.stem}"

        # Remove stale module if already in sys.modules
        if module_name in sys.modules:
            del sys.modules[module_name]

        spec = importlib.util.spec_from_file_location(module_name, py_file)
        if spec and spec.loader:
            try:
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                loaded.append(py_file.stem)
            except Exception as e:
                logger.error(
                    "Failed to load agent module %s: %s", py_file, e
                )

    return loaded


def load_all_agent_modules() -> dict[str, list[str]]:
    """Load all agent-generated column and model modules.

    Returns dict of {"columns": [...], "models": [...]}.
    """
    _ensure_dirs()
    return {
        "columns": load_agent_modules(COLUMNS_DIR),
        "models": load_agent_modules(MODELS_DIR),
    }


# ── Tool Factories ─────────────────────────────────────────────────

def create_codegen_tools(ctx: _PlatformContext) -> list:
    """Create the three codegen tools bound to a PlatformContext."""

    @tool
    def inspect_registry(columns_json: str = "[]") -> str:
        """Check which columns exist in the column registry.

        Use this BEFORE generating code to see which columns already
        exist and which ones you need to define.

        Args:
            columns_json: JSON list of column names to check.
                          Pass "[]" to get a summary of all columns.
        """
        from store import REGISTRY

        try:
            columns = json.loads(columns_json)
        except json.JSONDecodeError:
            columns = []

        if not columns:
            # Summary mode: return all columns grouped by category
            all_cols = REGISTRY.all_columns()
            summary: dict[str, list[dict[str, str]]] = {}
            for name, col in all_cols.items():
                cat = col.category or "uncategorized"
                if cat not in summary:
                    summary[cat] = []
                summary[cat].append({
                    "name": name,
                    "type": col.python_type.__name__,
                    "role": col.role,
                    "description": col.description[:80],
                })
            return json.dumps({
                "total_columns": len(all_cols),
                "categories": summary,
            }, default=str)

        # Check specific columns
        result = {}
        for name in columns:
            if REGISTRY.has(name):
                col = REGISTRY.get(name)
                result[name] = {
                    "exists": True,
                    "type": col.python_type.__name__,
                    "role": col.role,
                    "description": col.description,
                    "unit": col.unit,
                    "category": col.category,
                    "sensitivity": col.sensitivity,
                }
            else:
                result[name] = {"exists": False}

        return json.dumps(result, default=str)

    @tool
    def define_module(
        module_name: str,
        code: str,
        module_type: str = "columns",
        description: str = "",
    ) -> str:
        """Write a Python module to the platform's auto-import directory.

        The module is written to disk AND exec'd immediately so it takes
        effect in the current session.  On restart, it will be auto-imported.

        Use module_type="columns" for REGISTRY.define() calls.
        Use module_type="models" for Storable class definitions.

        Args:
            module_name: snake_case name (e.g. "trade_signals").
            code: Python source code for the module.
            module_type: "columns" or "models".
            description: What this module defines.
        """
        # Validate module_type
        if module_type not in ("columns", "models"):
            return json.dumps({
                "status": "error",
                "error": "module_type must be 'columns' or 'models'",
            })

        # Validate module_name
        if not module_name.replace("_", "").isalnum():
            return json.dumps({
                "status": "error",
                "error": f"Invalid module name: '{module_name}' "
                         f"(must be alphanumeric/underscores)",
            })

        # AST validation
        errors = validate_code(code)
        if errors:
            return json.dumps({
                "status": "error",
                "errors": errors,
            })

        # Determine target directory
        target_dir = COLUMNS_DIR if module_type == "columns" else MODELS_DIR
        _ensure_dirs()

        target_file = target_dir / f"{module_name}.py"

        # Backup if overwriting
        if target_file.exists():
            backup = target_file.with_suffix(
                f".bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            shutil.copy2(target_file, backup)
            logger.info("Backed up %s → %s", target_file, backup)

        # Write the file
        header = f'"""{description or module_name} — generated by agent."""\n'
        full_code = header + code if not code.startswith('"""') else code
        target_file.write_text(full_code)
        logger.info("Wrote agent module: %s", target_file)

        # Exec immediately in sandbox namespace
        ns = _build_sandbox_namespace(ctx)
        stdout_capture = io.StringIO()

        try:
            compiled = compile(full_code, str(target_file), "exec")
            # Redirect prints via namespace override
            ns["print"] = lambda *a, **kw: print(
                *a, file=stdout_capture, **kw
            )
            exec(compiled, ns)
        except Exception as e:
            # Remove the broken file
            target_file.unlink(missing_ok=True)
            return json.dumps({
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__,
                "message": "File was NOT written (exec failed). "
                           "Fix the code and try again.",
            })

        # Collect what was created
        created_types = []
        for k, v in ns.items():
            if isinstance(v, type) and k != "Storable" and not k.startswith("_"):
                try:
                    from store import Storable as _S
                    if issubclass(v, _S) and v is not _S:
                        created_types.append(k)
                        ctx.register_storable_type(k, v)
                except TypeError:
                    pass

        return json.dumps({
            "status": "success",
            "file": str(target_file),
            "module_type": module_type,
            "output": stdout_capture.getvalue(),
            "created_types": created_types,
            "message": f"Module '{module_name}' written to {target_dir.name}/ "
                       f"and loaded into current session.",
        })

    @tool
    def execute_python(code: str, description: str = "") -> str:
        """Execute Python code in a sandboxed platform environment.

        This is for EPHEMERAL work — queries, analysis, computations.
        Nothing is saved to disk.  For persistent schemas or models,
        use define_module() instead.

        The code runs with access to REGISTRY, Storable, @computed,
        @ticking, math, datetime, json — but NOT os, sys, subprocess.

        Args:
            code: Python source code to execute.
            description: What this code does (for logging).
        """
        # AST validation
        errors = validate_code(code)
        if errors:
            return json.dumps({
                "status": "error",
                "errors": errors,
            })

        ns = _build_sandbox_namespace(ctx)
        stdout_capture = io.StringIO()

        # Redirect prints via namespace override
        ns["print"] = lambda *a, **kw: print(
            *a, file=stdout_capture, **kw
        )

        try:
            compiled = compile(code, "<agent>", "exec")
            exec(compiled, ns)
        except Exception as e:
            return json.dumps({
                "status": "error",
                "error": str(e),
                "error_type": type(e).__name__,
            })

        # Collect result — look for a 'result' variable
        output = stdout_capture.getvalue()
        result_val = ns.get("result")

        response = {
            "status": "success",
            "output": output,
        }
        if result_val is not None:
            try:
                response["result"] = json.loads(json.dumps(result_val, default=str))
            except (TypeError, ValueError):
                response["result"] = str(result_val)

        return json.dumps(response, default=str)

    return [inspect_registry, define_module, execute_python]

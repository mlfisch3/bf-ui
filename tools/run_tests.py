from __future__ import annotations

import importlib.util
import pathlib
import subprocess
import sys
from types import ModuleType


def run_pytest_if_available() -> int | None:
    try:
        probe = subprocess.run(
            [sys.executable, "-m", "pytest", "--version"],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return None
    if probe.returncode != 0:
        return None
    run = subprocess.run([sys.executable, "-m", "pytest", "-q"], check=False)
    return run.returncode


def load_module(path: pathlib.Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(path.stem, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_function_style_tests() -> int:
    root = pathlib.Path(__file__).resolve().parents[1]
    tests_dir = root / "tests"
    failures = 0
    skipped = 0
    for path in sorted(tests_dir.glob("test_*.py")):
        try:
            module = load_module(path)
        except ModuleNotFoundError as exc:
            skipped += 1
            print(f"SKIP {path.name} -> missing dependency: {exc}")
            continue
        for name in dir(module):
            if not name.startswith("test_"):
                continue
            fn = getattr(module, name)
            if not callable(fn):
                continue
            try:
                fn()
                print(f"PASS {path.name}::{name}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"FAIL {path.name}::{name} -> {exc}")
    if skipped:
        print(f"Skipped modules: {skipped}")
    return 1 if failures else 0


def main() -> int:
    root = pathlib.Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    compile_target = root / "app.py"
    compile_result = subprocess.run([sys.executable, "-m", "py_compile", str(compile_target)], check=False)
    if compile_result.returncode != 0:
        return compile_result.returncode

    pytest_result = run_pytest_if_available()
    if pytest_result is not None:
        return pytest_result

    print("pytest not available; running fallback function-style tests")
    return run_function_style_tests()


if __name__ == "__main__":
    raise SystemExit(main())

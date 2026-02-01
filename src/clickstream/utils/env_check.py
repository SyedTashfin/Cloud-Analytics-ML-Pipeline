from __future__ import annotations

import platform
import subprocess
import sys
from pathlib import Path
from typing import Optional


def print_python_info() -> None:
    """Print the Python executable path and version."""
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {platform.python_version()}")


def get_java_version() -> Optional[str]:
    """Return the Java version string if available."""
    try:
        result = subprocess.run(
            ["java", "-version"],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None

    output = result.stderr or result.stdout
    if result.returncode != 0 or not output:
        return None

    lines = [line.strip() for line in output.splitlines() if line.strip()]
    return lines[0] if lines else None


def print_java_availability() -> bool:
    """Check for Java and print a summary line."""
    version_line = get_java_version()
    if version_line is None:
        print("Java: not found")
        return False

    print(f"Java: {version_line}")
    return True


def print_spark_version() -> bool:
    """Import pyspark and print the Spark version."""
    try:
        import pyspark
    except ModuleNotFoundError:
        print("Spark: pyspark not installed")
        return False

    print(f"Spark: {pyspark.__version__}")
    return True


def project_root() -> Path:
    """Resolve the repository root based on this file location."""
    resolved = Path(__file__).resolve()
    return resolved.parents[2] if len(resolved.parents) > 2 else resolved.parent


def check_filesystem() -> bool:
    """Attempt to write and delete a temp file in data/interim."""
    interim_dir = project_root() / "data" / "interim"
    try:
        interim_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        print(f"Filesystem: cannot create {interim_dir} ({exc})")
        return False

    test_file = interim_dir / ".env_check.tmp"
    try:
        test_file.write_text("ok", encoding="utf-8")
        _ = test_file.read_text(encoding="utf-8")
        test_file.unlink()
        print(f"Filesystem: write OK ({interim_dir})")
        return True
    except Exception as exc:
        print(f"Filesystem: write FAILED ({interim_dir}) - {exc}")
        if test_file.exists():
            try:
                test_file.unlink()
            except Exception:
                pass
        return False


def main() -> None:
    """Run environment checks for local Spark usage."""
    failed = False

    print_python_info()

    if not print_java_availability():
        failed = True

    if not print_spark_version():
        failed = True

    if not check_filesystem():
        failed = True

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

from pathlib import Path
import os
import subprocess
import sys


def resolve_venv_python(repo_root: Path, venv_name: str) -> str | None:
    candidates = [
        repo_root / venv_name / "Scripts" / "python.exe",
        repo_root / venv_name / "bin" / "python",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


def main() -> int:
    repo_root = Path(__file__).resolve().parent

    transcribe_python = (
        resolve_venv_python(repo_root, ".venv")
        or sys.executable
    )

    env = os.environ.copy()

    cmd = [transcribe_python, str(repo_root / "pipeline.py"), *sys.argv[1:]]
    result = subprocess.run(cmd, env=env, check=False)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())

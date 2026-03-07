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

    transcribe_python = resolve_venv_python(repo_root, "venv_transcribe") or sys.executable
    translate_python = os.environ.get("TRANSLATE_PYTHON", "").strip()
    if not translate_python:
        translate_python = resolve_venv_python(repo_root, "venv_translate") or ""

    env = os.environ.copy()
    if translate_python:
        env["TRANSLATE_PYTHON"] = translate_python

    cmd = [transcribe_python, str(repo_root / "pipeline.py"), *sys.argv[1:]]
    result = subprocess.run(cmd, env=env, check=False)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())

from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    pipeline_root = repo_root / "pipeline"
    dirs = [
        pipeline_root / "0_src",
        pipeline_root / "1_srt-jp",
        pipeline_root / "2_qc",
        pipeline_root / "3_srt-en",
        pipeline_root / "4_srt-sp",
        pipeline_root / "5_output",
    ]

    for directory in dirs:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"ready: {directory.relative_to(repo_root)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

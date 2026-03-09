import sys
from pathlib import Path
import os
import sys

from faster_whisper import WhisperModel


def main() -> int:

    # Whisper model validation
    WhisperModel("small", compute_type="int8")
    print("Transcribe runtime OK")

    # OpenAI API validation
    import pipeline

    pipeline.load_local_env()

    openai_api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not openai_api_key:
        print("OPENAI_API_KEY is not set.")
        return 1
    
    print("Translation API OK")
    
    # Initialize pipeline structure
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
        print(f"Ready: {directory.relative_to(repo_root)}")

    return 0

if __name__ == "__main__":
    sys.exit(main())

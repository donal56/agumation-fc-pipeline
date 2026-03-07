import sys

from faster_whisper import WhisperModel


def main() -> int:
    WhisperModel("small", compute_type="int8")
    print("transcribe runtime OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())

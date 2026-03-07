from pathlib import Path
import sys


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo_root))

    import pipeline

    ja_en, en_es = pipeline.get_translators()
    if ja_en and en_es:
        print("translate runtime OK")
        return 0

    print("translate runtime FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(main())

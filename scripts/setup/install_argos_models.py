from pathlib import Path
import sys
from urllib.request import urlretrieve

import argostranslate.package


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    models_dir = repo_root / "argos_models"
    models_dir.mkdir(parents=True, exist_ok=True)
    model_specs = [
        (
            "translate-en_es-1_0.argosmodel",
            "https://argos-net.com/v1/translate-en_es-1_0.argosmodel",
        ),
        (
            "translate-ja_en-1_1.argosmodel",
            "https://argos-net.com/v1/translate-ja_en-1_1.argosmodel",
        ),
    ]

    model_files = []
    for filename, url in model_specs:
        model_path = models_dir / filename
        model_files.append(model_path)
        if model_path.exists():
            print(f"Using cached model: {model_path.name}")
            continue
        print(f"Downloading: {filename}")
        try:
            urlretrieve(url, str(model_path))
        except Exception as err:
            print(f"Failed downloading {filename}: {err}")
            return 1

    for model in model_files:
        print(f"Installing: {model.name}")
        argostranslate.package.install_from_path(str(model))

    print("Argos models installed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

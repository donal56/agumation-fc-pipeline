import sys

import pipeline_utils as pu
from pipeline_stages.hardsub import stage_hardsub
from pipeline_stages.qc import stage_qc
from pipeline_stages.transcribe import stage_transcribe
from pipeline_stages.translate_en import stage_translate_en
from pipeline_stages.translate_es import stage_translate_es


def main():

    pu.ensure_dirs()

    if len(sys.argv) >= 2 and sys.argv[1] == "__translate_file":
        if len(sys.argv) != 5:
            print(
                "Usage: pipeline.py __translate_file [ja_en|en_es] <input_srt> <output_srt>"
            )
            sys.exit(2)
        try:
            code = pu.worker_translate_file(sys.argv[2], sys.argv[3], sys.argv[4])
            sys.exit(code)
        except Exception as err:
            print(f"Worker translation error: {err}")
            sys.exit(1)

    possibleStages = {
        "all",
        "transcribe",
        "qc",
        "translate_en",
        "translate_es",
        "hardsub",
    }
    usageHelp = (
        "Usage: pipeline.py [all|transcribe|qc|translate_en|translate_es|hardsub]"
    )

    if len(sys.argv) != 2:
        print(usageHelp)
        print(
            "Add files to /pipeline/0_src and run the script to begin a video pipeline"
        )
        return

    stage = sys.argv[1]

    if stage not in possibleStages:
        print(usageHelp)
        return

    if (
        stage in {"all", "translate_en", "translate_es"}
        and not pu.validate_translate_python()
    ):
        return

    if stage == "transcribe":
        stage_transcribe()

    elif stage == "qc":
        stage_qc()

    elif stage == "translate_en":
        stage_translate_en()

    elif stage == "translate_es":
        stage_translate_es()

    elif stage == "hardsub":
        stage_hardsub()

    elif stage == "all":

        stage_transcribe()
        stage_qc()
        stage_translate_en()
        stage_translate_es()
        stage_hardsub()


def load_local_env() -> None:
    pu.load_local_env()


if __name__ == "__main__":
    main()

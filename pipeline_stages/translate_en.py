import os

import pipeline_utils as pu


def stage_translate_en():

    print("\nSTAGE: JP -> EN\n")

    translate_python = os.environ.get("TRANSLATE_PYTHON", "").strip()
    if not translate_python:
        ja_en, _ = pu.get_translators()
        if ja_en is None:
            return

    for f in os.listdir(pu.JP):
        if not f.lower().endswith(".srt"):
            pu.log_file_status("translate_en", f, "Skipped", "Unsupported extension")
            continue
        pu.log_file_start("translate_en", f)

        name = os.path.splitext(f)[0]

        jp = os.path.join(pu.JP, f)
        en_out = os.path.join(pu.EN, name + ".srt")
        qc_path = os.path.join(pu.QC, name + ".txt")

        if os.path.exists(en_out):
            pu.log_file_status("translate_en", f, "Skipped", "Already translated")
            continue

        if not os.path.exists(qc_path):
            pu.log_file_status("translate_en", f, "Skipped", "Missing QC file")
            continue

        with open(qc_path, "r", encoding="utf8") as qc_file:
            qc_contents = qc_file.read().strip()
        if qc_contents:
            pu.log_file_status("translate_en", f, "Skipped", "QC has issues")
            continue

        if pu.is_silent_source_case(name):
            pu.log_file_status(
                "translate_en",
                f,
                "Success",
                "Silent source, translation skipped (No SRT generated)",
            )
            continue

        try:
            result = pu.run_translate_file_worker("ja_en", jp, en_out)
            if result.returncode == 0:
                pu.log_file_status("translate_en", f, "Success")
            else:
                detail = pu.summarize_worker_failure(result)
                pu.log_file_status("translate_en", f, "Failed", detail)
        except Exception as err:
            pu.log_file_status("translate_en", f, "Failed", str(err))

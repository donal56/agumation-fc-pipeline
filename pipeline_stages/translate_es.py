import os

import pipeline_utils as pu


def stage_translate_es():

    print("\nSTAGE: EN -> ES\n")

    translate_python = os.environ.get("TRANSLATE_PYTHON", "").strip()
    if not translate_python:
        _, en_es = pu.get_translators()
        if en_es is None:
            return

    for f in os.listdir(pu.JP):
        if not f.lower().endswith(".srt"):
            pu.log_file_status("translate_es", f, "Skipped", "unsupported extension")
            continue
        pu.log_file_start("translate_es", f)

        name = os.path.splitext(f)[0]

        en = os.path.join(pu.EN, name + ".srt")
        es_out = os.path.join(pu.ES, name + ".srt")

        if os.path.exists(es_out):
            pu.log_file_status("translate_es", f, "Skipped", "Already translated")
            continue

        if pu.is_silent_source_case(name):
            pu.log_file_status(
                "translate_es",
                f,
                "Success",
                "Silent source, translation skipped (No SRT generated)",
            )
            continue

        if not os.path.exists(en):
            pu.log_file_status("translate_es", f, "Skipped", "Missing EN subtitle")
            continue

        try:
            result = pu.run_translate_file_worker("en_es", en, es_out)
            if result.returncode == 0:
                pu.log_file_status("translate_es", f, "Success")
            else:
                detail = pu.summarize_worker_failure(result)
                pu.log_file_status("translate_es", f, "Failed", detail)
        except Exception as err:
            pu.log_file_status("translate_es", f, "Failed", str(err))

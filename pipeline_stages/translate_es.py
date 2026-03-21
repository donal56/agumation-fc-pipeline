import os

import pipeline_utils as pu


def stage_translate_es():

    print("\nSTAGE: EN -> ES\n")

    translate_python = os.environ.get("TRANSLATE_PYTHON", "").strip()
    if not translate_python:
        sucess = pu.init_translation_process()
        if not sucess:
            return

    for f in os.scandir(pu.JP):
        if not f.is_file():
            break

        if not f.name.lower().endswith(".srt"):
            pu.log_file_status("translate_es", f.name, "Skipped", "unsupported extension")
            continue
        pu.log_file_start("translate_es", f.name)

        raw_name = os.path.splitext(f.name)[0]

        en = os.path.join(pu.EN, raw_name + ".srt")
        es_out = os.path.join(pu.ES, raw_name + ".srt")

        if os.path.exists(es_out):
            pu.log_file_status("translate_es", f.name, "Skipped", "Already translated")
            continue

        if not os.path.exists(en):
            pu.log_file_status("translate_es", f.name, "Skipped", "Missing EN subtitle")
            continue

        try:
            result = pu.run_translate_file_worker("en_es", en, es_out)
            if result.returncode == 0:
                pu.log_file_status("translate_es", f.name, "Success")
            elif result.returncode == 2:
                detail = pu.summarize_worker_failure(result)
                pu.log_file_status("translate_es", f.name, "Skipped", detail)
            else:
                detail = pu.summarize_worker_failure(result)
                pu.log_file_status("translate_es", f.name, "Failed", detail)
        except Exception as err:
            pu.log_file_status("translate_es", f.name, "Failed", str(err))

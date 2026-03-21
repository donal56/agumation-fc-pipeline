import os

import pipeline_utils as pu


def stage_translate_en():

    print("\nSTAGE: JP -> EN\n")

    translate_python = os.environ.get("TRANSLATE_PYTHON", "").strip()
    if not translate_python:
        sucess = pu.init_translation_process()
        if not sucess:
            return

    for f in os.scandir(pu.JP):
        if not f.is_file():
            break

        if not f.name.lower().endswith(".srt"):
            pu.log_file_status("translate_en", f.name, "Skipped", "Unsupported extension")
            continue
        pu.log_file_start("translate_en", f.name)

        raw_name = os.path.splitext(f.name)[0]

        jp = os.path.join(pu.JP, f.name)
        en_out = os.path.join(pu.EN, raw_name + ".srt")
        qc_path = os.path.join(pu.QC, raw_name + ".txt")

        if os.path.exists(en_out):
            pu.log_file_status("translate_en", f.name, "Skipped", "Already translated")
            continue

        if not os.path.exists(qc_path):
            pu.log_file_status("translate_en", f.name, "Skipped", "Missing QC file")
            continue

        with open(qc_path, "r", encoding="utf8") as qc_file:
            qc_contents = qc_file.read().strip()
        if qc_contents:
            pu.log_file_status("translate_en", f.name, "Skipped", "QC has issues")
            continue

        try:
            result = pu.run_translate_file_worker("ja_en", jp, en_out)
            if result.returncode == 0:
                pu.log_file_status("translate_en", f.name, "Success")
            elif result.returncode == 2:
                detail = pu.summarize_worker_failure(result)
                pu.log_file_status("translate_en", f.name, "Skipped", detail)
            else:
                detail = pu.summarize_worker_failure(result)
                pu.log_file_status("translate_en", f.name, "Failed", detail)
        except Exception as err:
            pu.log_file_status("translate_en", f.name, "Failed", str(err))

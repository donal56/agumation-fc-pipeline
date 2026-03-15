import os

import pipeline_utils as pu


def stage_qc():

    print("\nSTAGE: QC\n")

    for f in os.listdir(pu.JP):
        if not f.lower().endswith(".srt"):
            pu.log_file_status("qc", f, "Skipped", "Unsupported extension")
            continue
        pu.log_file_start("qc", f)

        name = os.path.splitext(f)[0]

        path = os.path.join(pu.JP, f)
        out = os.path.join(pu.QC, name + ".txt")
        try:
            problems = pu.qc_check_srt(path)
            with open(out, "w", encoding="utf8") as qc_file:
                for p in problems:
                    qc_file.write(p + "\n")
            if problems:
                pu.log_file_status("qc", f, "Failed", f"Found {len(problems)} issues")
            else:
                pu.log_file_status("qc", f, "Success")
        except Exception as err:
            pu.log_file_status("qc", f, "Failed", str(err))

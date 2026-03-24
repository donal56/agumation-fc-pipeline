import os
import shutil

import pipeline_utils as pu

def stage_hardsub():

    print("\nSTAGE: HARDSUB\n")

    for f in os.scandir(pu.JP):
        if not f.is_file():
            break

        pu.log_file_start("hardsub", f.name)
        raw_name = os.path.splitext(f.name)[0]
        video = None

        for ext in pu.VIDEO_EXT:

            candidate = os.path.join(pu.SRC, raw_name + ext)

            if os.path.exists(candidate):
                video = candidate
                break

        if video is None:
            pu.log_file_status("hardsub", f.name, "Skipped", "Source video not found")
            continue

        out = os.path.join(pu.OUT, raw_name + "_sub.mp4")

        if os.path.exists(out):
            pu.log_file_status("hardsub", f.name, "Skipped", "Already generated")
            continue

        srt_filename = raw_name + ".srt"
        srt1 = os.path.join(pu.EN, srt_filename)
        srt2 = os.path.join(pu.ES, srt_filename)
        qc =  os.path.join(pu.QC, raw_name + ".txt")
        
        if not os.path.exists(srt1) or not os.path.exists(srt2):
            if not os.path.exists(srt1) and not os.path.exists(srt2) and os.path.exists(qc) and pu.is_empty_text_file(qc):
                shutil.copyfile(video, out)
                pu.log_file_status("hardsub", f.name, "Skipped", "Both translated subtitles empty, copied without subtitles")
                continue
            else:
                pu.log_file_status("hardsub", f.name, "Skipped", "Missing translated subtitles")
                continue

        try:
            ok, detail = pu.hardsub(video, srt1, srt2, out)
            if ok:
                pu.log_file_status("hardsub", f.name, "Success")
            else:
                pu.log_file_status("hardsub", f.name, "Failed", detail)
        except Exception as err:
            pu.log_file_status("hardsub", f.name, "Failed", str(err))

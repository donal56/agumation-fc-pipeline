import os
import shutil

import pipeline_utils as pu


def stage_hardsub():

    print("\nSTAGE: HARDSUB\n")

    for f in os.listdir(pu.SRC):
        if not f.lower().endswith(pu.VIDEO_EXT):
            pu.log_file_status("hardsub", f, "Skipped", "Unsupported extension")
            continue
        pu.log_file_start("hardsub", f)

        name = os.path.splitext(f)[0]

        video = None

        for ext in pu.VIDEO_EXT:

            candidate = os.path.join(pu.SRC, name + ext)

            if os.path.exists(candidate):
                video = candidate
                break

        if video is None:
            pu.log_file_status("hardsub", f, "Skipped", "Source video not found")
            continue

        out = os.path.join(pu.OUT, name + "_sub.mp4")

        if os.path.exists(out):
            pu.log_file_status("hardsub", f, "Skipped", "Already generated")
            continue

        def copy_video(reason: str) -> None:
            try:
                shutil.copyfile(video, out)
                pu.log_file_status("hardsub", f, "Success", reason)
            except Exception as err:
                pu.log_file_status("hardsub", f, "Failed", str(err))

        if pu.is_silent_source_case(name):
            copy_video("Silent source, copied without subtitles")
            continue

        srt_filename = name + ".srt"
        srt1 = os.path.join(pu.EN, srt_filename)
        srt2 = os.path.join(pu.ES, srt_filename)
        if not os.path.exists(srt1) or not os.path.exists(srt2):
            pu.log_file_status("hardsub", f, "Skipped", "Missing translated subtitles")
            continue
        if not pu.read_srt(srt1) and not pu.read_srt(srt2):
            copy_video("Both translated subtitles empty, copied without subtitles")
            continue
        try:
            ok, detail = pu.hardsub(video, srt1, srt2, out)
            if ok:
                pu.log_file_status("hardsub", f, "Success")
            else:
                pu.log_file_status("hardsub", f, "Failed", detail)
        except Exception as err:
            pu.log_file_status("hardsub", f, "Failed", str(err))

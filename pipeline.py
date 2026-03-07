import os
import sys
import subprocess
import types
import warnings

warnings.filterwarnings(
    "ignore",
    message=r"pkg_resources is deprecated as an API\..*",
    category=UserWarning,
)

PIPELINE_ROOT = "pipeline"
SRC = os.path.join(PIPELINE_ROOT, "0_src")
JP = os.path.join(PIPELINE_ROOT, "1_srt-jp")
QC = os.path.join(PIPELINE_ROOT, "2_qc")
EN = os.path.join(PIPELINE_ROOT, "3_srt-en")
ES = os.path.join(PIPELINE_ROOT, "4_srt-sp")
OUT = os.path.join(PIPELINE_ROOT, "5_output")

VIDEO_EXT = (".mp4", ".mkv", ".mov", ".webm")

model = None
_ja_en = None
_en_es = None


def log_file_status(stage, filename, status, detail=""):
    suffix = f" ({detail})" if detail else ""
    print(f"[{stage}] {filename}: {status}{suffix}")


def validate_translate_python():
    translate_python = os.environ.get("TRANSLATE_PYTHON", "").strip()
    if not translate_python:
        return True
    if not os.path.exists(translate_python):
        print(f"Error: TRANSLATE_PYTHON does not exist: {translate_python}")
        print("Set TRANSLATE_PYTHON to a valid python.exe path for the translation venv.")
        return False
    if not os.path.isfile(translate_python):
        print(f"Error: TRANSLATE_PYTHON is not a file: {translate_python}")
        return False
    return True


def format_time(t):
    h = int(t // 3600)
    m = int((t % 3600) // 60)
    s = t % 60
    return f"{h:02}:{m:02}:{s:06.3f}".replace(".", ",")


def write_srt(segments, path):

    with open(path, "w", encoding="utf8") as f:

        for i, seg in enumerate(segments, 1):

            f.write(str(i) + "\n")
            f.write(format_time(seg.start) + " --> " + format_time(seg.end) + "\n")
            f.write(seg.text.strip() + "\n\n")


def translate_srt(input_path, output_path, translator):

    with open(input_path, "r", encoding="utf8") as f:
        lines = f.readlines()

    out = []

    for line in lines:

        if "-->" in line or line.strip().isdigit() or line.strip() == "":
            out.append(line)
        else:
            out.append(translator.translate(line.strip()) + "\n")

    with open(output_path, "w", encoding="utf8") as f:
        f.writelines(out)


def run_translate_file_worker(direction, input_path, output_path):
    translate_python = os.environ.get("TRANSLATE_PYTHON", "").strip() or sys.executable
    cmd = [
        translate_python,
        os.path.abspath(__file__),
        "__translate_file",
        direction,
        input_path,
        output_path,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def worker_translate_file(direction, input_path, output_path):
    ja_en, en_es = get_translators()
    if direction == "ja_en":
        if ja_en is None:
            return 2
        translate_srt(input_path, output_path, ja_en)
        return 0
    if direction == "en_es":
        if en_es is None:
            return 2
        translate_srt(input_path, output_path, en_es)
        return 0
    return 3


def get_translators():
    global _ja_en
    global _en_es

    if _ja_en is not None and _en_es is not None:
        return _ja_en, _en_es

    # Keep Argos runtime data inside the project to avoid broken user-level config paths.
    local_config_root = os.path.join(os.getcwd(), ".runtime")
    os.makedirs(local_config_root, exist_ok=True)
    os.environ.setdefault("XDG_CONFIG_HOME", local_config_root)
    os.environ.setdefault("ARGOS_STANZA_AVAILABLE", "1")

    try:
        import argostranslate.translate
    except OSError as err:
        print("Error loading Argos runtime (Torch DLL initialization failed).")
        print("Likely Windows dependency issue in your venv.")
        print("Try reinstalling CPU wheels:")
        print("  python -m pip uninstall -y torch torchvision torchaudio")
        print("  python -m pip install --index-url https://download.pytorch.org/whl/cpu torch torchvision torchaudio")
        print(f"Details: {err}")
        return None, None
    except ModuleNotFoundError as err:
        if "stanza" in str(err):
            # Torch-free fallback: provide a minimal stanza module so argostranslate can import.
            stanza_stub = types.ModuleType("stanza")

            class _StubPipeline:
                def __init__(self, *args, **kwargs):
                    pass

                def __call__(self, text):
                    return types.SimpleNamespace(sentences=[types.SimpleNamespace(text=text)])

            stanza_stub.Pipeline = _StubPipeline
            sys.modules["stanza"] = stanza_stub
            try:
                import argostranslate.translate
            except Exception as second_err:
                print(f"Error importing argostranslate after stanza fallback: {second_err}")
                return None, None
            # Continue with normal flow using imported module.
            langs = argostranslate.translate.get_installed_languages()
            try:
                ja = [l for l in langs if l.code == "ja"][0]
                en = [l for l in langs if l.code == "en"][0]
                es = [l for l in langs if l.code == "es"][0]
            except IndexError:
                print("Error: Required language models not installed. Run setup commands from README.")
                return None, None

            _ja_en = ja.get_translation(en)
            _en_es = en.get_translation(es)
            return _ja_en, _en_es
        print(f"Error importing argostranslate: {err}")
        return None, None
    except Exception as err:
        print(f"Error importing argostranslate: {err}")
        return None, None

    langs = argostranslate.translate.get_installed_languages()
    try:
        ja = [l for l in langs if l.code == "ja"][0]
        en = [l for l in langs if l.code == "en"][0]
        es = [l for l in langs if l.code == "es"][0]
    except IndexError:
        print("Error: Required language models not installed. Run setup commands from README.")
        return None, None

    _ja_en = ja.get_translation(en)
    _en_es = en.get_translation(es)
    return _ja_en, _en_es


def qc_check_srt(path):

    problems = []

    with open(path, "r", encoding="utf8") as f:
        lines = f.readlines()

    i = 0

    while i < len(lines):

        if "-->" in lines[i]:

            time_line = lines[i].strip()
            text_line = lines[i + 1].strip()

            start, end = time_line.split("-->")

            def to_sec(t):

                h, m, s = t.strip().replace(",", ".").split(":")
                return int(h) * 3600 + int(m) * 60 + float(s)

            dur = to_sec(end) - to_sec(start)

            chars = len(text_line)

            cps = chars / dur if dur > 0 else 999

            if chars > 42:
                problems.append(f"Linea larga ({chars} chars): {text_line}")

            if dur < 0.7:
                problems.append(f"Duracion muy corta {dur:.2f}s: {text_line}")

            if dur > 7:
                problems.append(f"Duracion muy larga {dur:.2f}s: {text_line}")

            if cps > 20:
                problems.append(f"Lectura rapida {cps:.1f} cps: {text_line}")

        i += 1

    return problems


def hardsub(video, srt1, srt2, out):

    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        video,
        "-filter_complex",
        f"[0]subtitles={srt1}:force_style='Fontname=Calibri,PrimaryColour=&HFFFFFF&,Fontsize=21,Alignment=6'[a]; [a]subtitles={srt2}:force_style='Fontname=Calibri,PrimaryColour=&HFFFFFF&,Fontsize=21'",
        out,
    ]

    result = subprocess.run(cmd, check=False)
    return result.returncode == 0


def stage_transcribe():

    print("\nSTAGE: TRANSCRIBE\n")
    global model

    if model is None:
        try:
            from faster_whisper import WhisperModel
        except OSError as err:
            print("Error loading faster-whisper runtime (Torch DLL initialization failed).")
            print("Likely Windows dependency issue in your venv.")
            print("Try reinstalling CPU wheels:")
            print("  python -m pip uninstall -y torch torchvision torchaudio")
            print("  python -m pip install --index-url https://download.pytorch.org/whl/cpu torch torchvision torchaudio")
            print(f"Details: {err}")
            return
        except Exception as err:
            print(f"Error importing faster-whisper: {err}")
            return

        try:
            model = WhisperModel("small", compute_type="int8")
        except Exception as err:
            msg = str(err)
            if "LocalEntryNotFoundError" in msg or "cannot find the appropriate snapshot folder" in msg:
                print("Error: Whisper model files are not available locally and auto-download failed.")
                print("Check internet/proxy access to Hugging Face, then retry transcribe stage.")
                print("Alternative: pre-download the model cache in an environment with internet.")
            else:
                print(f"Error initializing Whisper model: {err}")
            return

    for f in os.listdir(SRC):

        if not f.lower().endswith(VIDEO_EXT):
            log_file_status("transcribe", f, "Skipped", "unsupported extension")
            continue

        name = os.path.splitext(f)[0]

        jp_path = os.path.join(JP, name + ".srt")

        if os.path.exists(jp_path):
            log_file_status("transcribe", f, "Skipped", "already transcribed")
            continue

        video = os.path.join(SRC, f)
        try:
            segments, _ = model.transcribe(video, language="ja")
            write_srt(list(segments), jp_path)
            log_file_status("transcribe", f, "Success")
        except Exception as err:
            log_file_status("transcribe", f, "Failed", str(err))


def stage_qc():

    print("\nSTAGE: QC\n")

    for f in os.listdir(JP):
        if not f.lower().endswith(".srt"):
            log_file_status("qc", f, "Skipped", "unsupported extension")
            continue

        name = os.path.splitext(f)[0]

        path = os.path.join(JP, f)
        out = os.path.join(QC, name + ".txt")
        try:
            problems = qc_check_srt(path)
            with open(out, "w", encoding="utf8") as qc_file:
                for p in problems:
                    qc_file.write(p + "\n")
            if problems:
                log_file_status("qc", f, "Failed", f"{len(problems)} issues")
            else:
                log_file_status("qc", f, "Success")
        except Exception as err:
            log_file_status("qc", f, "Failed", str(err))


def stage_translate_en():

    print("\nSTAGE: JP -> EN\n")

    translate_python = os.environ.get("TRANSLATE_PYTHON", "").strip()
    if not translate_python:
        ja_en, _ = get_translators()
        if ja_en is None:
            return

    for f in os.listdir(JP):
        if not f.lower().endswith(".srt"):
            log_file_status("translate_en", f, "Skipped", "unsupported extension")
            continue

        name = os.path.splitext(f)[0]

        jp = os.path.join(JP, f)
        en_out = os.path.join(EN, name + ".srt")
        qc_path = os.path.join(QC, name + ".txt")

        if os.path.exists(en_out):
            log_file_status("translate_en", f, "Skipped", "already translated")
            continue

        if not os.path.exists(qc_path):
            log_file_status("translate_en", f, "Skipped", "missing QC file")
            continue

        with open(qc_path, "r", encoding="utf8") as qc_file:
            qc_contents = qc_file.read().strip()
        if qc_contents:
            log_file_status("translate_en", f, "Skipped", "QC has issues")
            continue

        try:
            result = run_translate_file_worker("ja_en", jp, en_out)
            if result.returncode == 0:
                log_file_status("translate_en", f, "Success")
            else:
                detail = (result.stderr or result.stdout).strip().splitlines()
                detail = detail[-1] if detail else f"worker exit code {result.returncode}"
                log_file_status("translate_en", f, "Failed", detail)
        except Exception as err:
            log_file_status("translate_en", f, "Failed", str(err))


def stage_translate_es():

    print("\nSTAGE: EN -> ES\n")

    translate_python = os.environ.get("TRANSLATE_PYTHON", "").strip()
    if not translate_python:
        _, en_es = get_translators()
        if en_es is None:
            return

    for f in os.listdir(EN):
        if not f.lower().endswith(".srt"):
            log_file_status("translate_es", f, "Skipped", "unsupported extension")
            continue

        name = os.path.splitext(f)[0]

        en = os.path.join(EN, f)
        es_out = os.path.join(ES, name + ".srt")

        if os.path.exists(es_out):
            log_file_status("translate_es", f, "Skipped", "already translated")
            continue

        try:
            result = run_translate_file_worker("en_es", en, es_out)
            if result.returncode == 0:
                log_file_status("translate_es", f, "Success")
            else:
                detail = (result.stderr or result.stdout).strip().splitlines()
                detail = detail[-1] if detail else f"worker exit code {result.returncode}"
                log_file_status("translate_es", f, "Failed", detail)
        except Exception as err:
            log_file_status("translate_es", f, "Failed", str(err))


def stage_hardsub():

    print("\nSTAGE: HARDSUB\n")

    for f in os.listdir(ES):
        if not f.lower().endswith(".srt"):
            log_file_status("hardsub", f, "Skipped", "unsupported extension")
            continue

        name = os.path.splitext(f)[0]

        video = None

        for ext in VIDEO_EXT:

            candidate = os.path.join(SRC, name + ext)

            if os.path.exists(candidate):
                video = candidate
                break

        if video is None:
            log_file_status("hardsub", f, "Skipped", "source video not found")
            continue

        out = os.path.join(OUT, name + "_sub.mp4")

        if os.path.exists(out):
            log_file_status("hardsub", f, "Skipped", "already generated")
            continue

        srt1 = os.path.join(EN, f)
        srt2 = os.path.join(ES, f)
        try:
            if hardsub(video, srt1, srt2, out):
                log_file_status("hardsub", f, "Success")
            else:
                log_file_status("hardsub", f, "Failed", "ffmpeg returned non-zero")
        except Exception as err:
            log_file_status("hardsub", f, "Failed", str(err))


def ensure_dirs():

    for d in [SRC, JP, QC, EN, ES, OUT]:
        os.makedirs(d, exist_ok=True)


def main():

    ensure_dirs()

    if len(sys.argv) >= 2 and sys.argv[1] == "__translate_file":
        if len(sys.argv) != 5:
            print("Usage: pipeline.py __translate_file [ja_en|en_es] <input_srt> <output_srt>")
            sys.exit(2)
        try:
            code = worker_translate_file(sys.argv[2], sys.argv[3], sys.argv[4])
            sys.exit(code)
        except Exception as err:
            print(f"Worker translation error: {err}")
            sys.exit(1)

    possibleStages = {"all", "transcribe", "qc", "translate_en", "translate_es", "hardsub"}
    usageHelp = "Usage: pipeline.py [all|transcribe|qc|translate_en|translate_es|hardsub]";

    if len(sys.argv) != 2:
        print(usageHelp)
        print("Add files to /pipeline/0_src and run the script to begin a video pipeline")
        return

    stage = sys.argv[1]

    if stage not in possibleStages:
        print(usageHelp)
        return

    if stage in {"all", "translate_en", "translate_es"} and not validate_translate_python():
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


if __name__ == "__main__":
    main()

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import os
import re
import subprocess
import sys
import warnings
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parent
PIPELINE_ROOT = ROOT / "pipeline"
SRC = str(PIPELINE_ROOT / "0_src")
JP = str(PIPELINE_ROOT / "1_srt-jp")
QC = str(PIPELINE_ROOT / "2_qc")
EN = str(PIPELINE_ROOT / "3_srt-en")
ES = str(PIPELINE_ROOT / "4_srt-sp")
OUT = str(PIPELINE_ROOT / "5_output")

VIDEO_EXT = (".mp4", ".mov", ".mkv", ".avi", ".webm")

model = None
_last_status_len = 0


warnings.filterwarnings(
    "ignore",
    message=r"pkg_resources is deprecated as an API\..*",
    category=UserWarning,
)


def ensure_dirs() -> None:
    for path in (SRC, JP, QC, EN, ES, OUT):
        Path(path).mkdir(parents=True, exist_ok=True)


def load_local_env(override: bool = False) -> None:
    env_path = ROOT / ".env.local"
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if override or key not in os.environ:
            os.environ[key] = val


def get_env_int(key: str, default: int) -> int:
    raw = os.environ.get(key, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def get_env_float(key: str, default: float) -> float:
    raw = os.environ.get(key, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def load_json_config(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf8") as handle:
            return json.load(handle)
    except Exception:
        return default


def log_file_start(stage: str, filename: str) -> None:
    global _last_status_len
    line = f"[{stage}] {filename} -> Processing..."
    _last_status_len = len(line)
    print(line, end="", flush=True)


def log_file_status(stage: str, filename: str, status: str, detail: str | None = None) -> None:
    global _last_status_len
    suffix = f" ({detail})" if detail else ""
    line = f"[{stage}] {filename} -> {status}{suffix}"
    padding = " " * max(0, _last_status_len - len(line))
    print("\r" + line + padding)
    _last_status_len = 0


@dataclass
class SrtCue:
    index: int
    start: float
    end: float
    text: str


def _parse_timestamp(ts: str) -> float:
    h, m, rest = ts.split(":")
    s, ms = rest.split(",")
    return int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000.0


def _format_timestamp(seconds: float) -> str:
    seconds = max(0.0, seconds)
    ms = int(round((seconds - int(seconds)) * 1000))
    total = int(seconds)
    s = total % 60
    total //= 60
    m = total % 60
    h = total // 60
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def read_srt(path: str) -> list[SrtCue]:
    content = Path(path).read_text(encoding="utf8").replace("\r\n", "\n").strip()
    if content.startswith("\ufeff"):
        content = content.lstrip("\ufeff")
    if not content:
        return []
    cues: list[SrtCue] = []
    for block in content.split("\n\n"):
        lines = [line for line in block.split("\n") if line.strip() != ""]
        if not lines:
            continue
        idx = None
        if re.match(r"^\d+$", lines[0].strip()):
            idx = int(lines[0].strip())
            lines = lines[1:]
        if not lines:
            continue
        m = re.match(r"^\s*(\d\d:\d\d:\d\d,\d\d\d)\s*-->\s*(\d\d:\d\d:\d\d,\d\d\d)", lines[0])
        if not m:
            continue
        start = _parse_timestamp(m.group(1))
        end = _parse_timestamp(m.group(2))
        text = "\n".join(lines[1:]).strip()
        cue = SrtCue(index=idx or (len(cues) + 1), start=start, end=end, text=text)
        cues.append(cue)
    return cues


def write_srt(cues: Iterable[dict[str, Any] | SrtCue], path: str) -> None:
    lines: list[str] = []
    for i, cue in enumerate(cues, start=1):
        if isinstance(cue, dict):
            start = cue.get("start", 0.0)
            end = cue.get("end", 0.0)
            text = cue.get("text", "")
        else:
            start = cue.start
            end = cue.end
            text = cue.text
        lines.append(str(i))
        lines.append(f"{_format_timestamp(start)} --> {_format_timestamp(end)}")
        lines.append(text.strip())
        lines.append("")
    Path(path).write_text("\n".join(lines).strip() + "\n", encoding="utf8")


def _segment_attr(seg: Any, name: str, default: Any = None) -> Any:
    if isinstance(seg, dict):
        return seg.get(name, default)
    return getattr(seg, name, default)


def split_segments_for_srt(
    segments: Iterable[Any],
    max_cue_seconds: float,
    max_cue_chars: int,
) -> list[dict[str, Any]]:
    if max_cue_seconds <= 0 and max_cue_chars <= 0:
        return [
            {"start": _segment_attr(s, "start"), "end": _segment_attr(s, "end"), "text": _segment_attr(s, "text", "").strip()}
            for s in segments
        ]

    output: list[dict[str, Any]] = []
    for seg in segments:
        start = float(_segment_attr(seg, "start", 0.0))
        end = float(_segment_attr(seg, "end", 0.0))
        text = str(_segment_attr(seg, "text", "")).strip()
        words = _segment_attr(seg, "words", None)
        if words:
            current: list[Any] = []
            cue_start = None
            for word in words:
                w_text = _segment_attr(word, "word", "")
                w_start = float(_segment_attr(word, "start", start))
                w_end = float(_segment_attr(word, "end", end))
                if cue_start is None:
                    cue_start = w_start
                current.append(word)
                cue_text = "".join(_segment_attr(w, "word", "") for w in current).strip()
                duration = w_end - (cue_start or w_start)
                too_long = (max_cue_seconds > 0 and duration >= max_cue_seconds) or (
                    max_cue_chars > 0 and len(cue_text) >= max_cue_chars
                )
                if too_long:
                    output.append({"start": cue_start, "end": w_end, "text": cue_text})
                    current = []
                    cue_start = None
            if current:
                cue_start = float(_segment_attr(current[0], "start", start))
                cue_end = float(_segment_attr(current[-1], "end", end))
                cue_text = "".join(_segment_attr(w, "word", "") for w in current).strip()
                output.append({"start": cue_start, "end": cue_end, "text": cue_text})
            continue

        if max_cue_chars > 0 and len(text) > max_cue_chars:
            words = text.split()
            if not words:
                continue
            chunks: list[str] = []
            current = ""
            for word in words:
                candidate = f"{current} {word}".strip()
                if len(candidate) > max_cue_chars and current:
                    chunks.append(current)
                    current = word
                else:
                    current = candidate
            if current:
                chunks.append(current)
            if len(chunks) == 1:
                output.append({"start": start, "end": end, "text": chunks[0]})
            else:
                total = max(1, len(chunks))
                dur = max(0.1, end - start)
                slice_dur = dur / total
                for idx, chunk in enumerate(chunks):
                    c_start = start + idx * slice_dur
                    c_end = start + (idx + 1) * slice_dur
                    output.append({"start": c_start, "end": c_end, "text": chunk})
        else:
            output.append({"start": start, "end": end, "text": text})
    return output


def apply_short_cue_remediation(
    segments: list[dict[str, Any]], short_cue_rules: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    if not segments or not short_cue_rules:
        return segments

    rules = sorted(short_cue_rules, key=lambda r: r.get("chars_per_line", 0))

    def min_seconds_for_text(text: str) -> float:
        max_line = max((len(line) for line in text.splitlines()), default=0)
        applicable = [r for r in rules if r.get("chars_per_line", 0) <= max_line]
        if not applicable:
            return rules[0].get("min_seconds", 0.0)
        return applicable[-1].get("min_seconds", 0.0)

    output: list[dict[str, Any]] = []
    i = 0
    while i < len(segments):
        seg = segments[i]
        duration = float(seg.get("end", 0.0)) - float(seg.get("start", 0.0))
        min_seconds = min_seconds_for_text(seg.get("text", ""))
        if duration < min_seconds and i + 1 < len(segments):
            nxt = segments[i + 1]
            merged_text = (seg.get("text", "") + " " + nxt.get("text", "")).strip()
            output.append(
                {
                    "start": seg.get("start", 0.0),
                    "end": nxt.get("end", seg.get("end", 0.0)),
                    "text": merged_text,
                }
            )
            i += 2
            continue
        output.append(seg)
        i += 1
    return output


def qc_check_srt(path: str) -> list[str]:
    cues = read_srt(path)
    problems: list[str] = []
    if not cues:
        problems.append("Empty SRT file")
        return problems
    prev_end = 0.0
    for cue in cues:
        if cue.start >= cue.end:
            problems.append(f"Cue {cue.index}: start time >= end time")
        if cue.text.strip() == "":
            problems.append(f"Cue {cue.index}: empty text")
        if cue.start < prev_end:
            problems.append(f"Cue {cue.index}: overlaps previous cue")
        prev_end = max(prev_end, cue.end)
    return problems


def _openai_call(messages: list[dict[str, str]], model: str, temperature: float) -> str:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    try:
        from openai import OpenAI  # type: ignore

        client = OpenAI(api_key=api_key)
        if hasattr(client, "chat") and hasattr(client.chat, "completions"):
            resp = client.chat.completions.create(
                model=model, messages=messages, temperature=temperature
            )
            return resp.choices[0].message.content.strip()
    except Exception:
        pass

    try:
        import openai  # type: ignore

        openai.api_key = api_key
        resp = openai.ChatCompletion.create(
            model=model, messages=messages, temperature=temperature
        )
        return resp["choices"][0]["message"]["content"].strip()
    except Exception:
        pass

    base_url = os.environ.get("OPENAI_BASE_URL", os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1"))
    url = base_url.rstrip("/") + "/chat/completions"
    payload = json.dumps(
        {"model": model, "messages": messages, "temperature": temperature}
    ).encode("utf8")
    req = None
    try:
        import urllib.request

        req = urllib.request.Request(
            url,
            data=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf8"))
            return data["choices"][0]["message"]["content"].strip()
    except Exception as err:
        raise RuntimeError(f"OpenAI request failed: {err}") from err


def _lang_name(code: str) -> str:
    lookup = {"ja": "Japanese", "en": "English", "es": "Spanish"}
    return lookup.get(code.lower(), code)


def _build_translation_prompt(
    source_lang: str, target_lang: str, texts: list[str]
) -> list[dict[str, str]]:
    context = os.environ.get("OPENAI_CONTEXT", "").strip()
    pair_key = f"OPENAI_CONTEXT_{source_lang.upper()}_{target_lang.upper()}"
    pair_context = os.environ.get(pair_key, "").strip()
    context_line = pair_context or context
    base = (
        "You are a professional subtitle translator. "
        "Translate from {source} to {target}. "
        "Keep meaning faithful and natural. "
        "Do not add or remove items. "
        "Return ONLY a JSON array of strings, same length and order as input."
    ).format(source=_lang_name(source_lang), target=_lang_name(target_lang))
    if context_line:
        base += f" Context: {context_line}"
    user = "Input JSON:\n" + json.dumps(texts, ensure_ascii=False)
    return [
        {"role": "system", "content": base},
        {"role": "user", "content": user},
    ]


def _translate_texts(
    source_lang: str, target_lang: str, texts: list[str]
) -> list[str]:
    model_name = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini").strip()
    batch_limit = max(500, get_env_int("OPENAI_BATCH_CHARS", 4000))
    translated: list[str] = []

    def translate_batch(batch: list[str]) -> list[str]:
        messages = _build_translation_prompt(source_lang, target_lang, batch)
        content = _openai_call(messages, model_name, temperature=0.2)
        try:
            data = json.loads(content)
            if isinstance(data, list) and len(data) == len(batch):
                return [str(x) for x in data]
        except Exception:
            pass
        lines = [line.strip() for line in content.splitlines() if line.strip()]
        if len(lines) == len(batch):
            return lines
        raise RuntimeError("OpenAI response could not be parsed as expected JSON.")

    current: list[str] = []
    current_len = 0
    for text in texts:
        size = len(text)
        if current and current_len + size > batch_limit:
            translated.extend(translate_batch(current))
            current = []
            current_len = 0
        current.append(text)
        current_len += size
    if current:
        translated.extend(translate_batch(current))

    return translated


def worker_translate_file(pair: str, input_srt: str, output_srt: str) -> int:
    load_local_env()
    if "_" not in pair:
        print(f"Invalid language pair: {pair}")
        return 1
    source_lang, target_lang = pair.split("_", 1)
    cues = read_srt(input_srt)
    if not cues:
        print("No cues to translate.")
        return 1
    try:
        texts = [cue.text for cue in cues]
        translated = _translate_texts(source_lang, target_lang, texts)
    except Exception as err:
        print(f"Translation failed: {err}")
        return 1

    out_cues = [
        {"start": cue.start, "end": cue.end, "text": translated[idx]}
        for idx, cue in enumerate(cues)
    ]
    write_srt(out_cues, output_srt)
    return 0


def validate_translate_python() -> bool:
    load_local_env()
    translate_python = os.environ.get("TRANSLATE_PYTHON", "").strip()
    if translate_python and not Path(translate_python).exists():
        print(f"TRANSLATE_PYTHON not found: {translate_python}")
        return False
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        print("OPENAI_API_KEY is not set. Configure it in .env.local.")
        return False
    return True


def get_translators() -> tuple[object | None, object | None]:
    if not validate_translate_python():
        return None, None
    return object(), object()


def run_translate_file_worker(pair: str, input_srt: str, output_srt: str) -> subprocess.CompletedProcess[str]:
    translate_python = os.environ.get("TRANSLATE_PYTHON", "").strip() or sys.executable
    cmd = [
        translate_python,
        str(ROOT / "pipeline.py"),
        "__translate_file",
        pair,
        input_srt,
        output_srt,
    ]
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    return subprocess.run(cmd, text=True, capture_output=True, env=env, check=False)


def summarize_worker_failure(result: subprocess.CompletedProcess[str]) -> str:
    detail = (result.stderr or result.stdout or "").strip()
    if not detail:
        return f"Worker failed with exit code {result.returncode}"
    lines = [line for line in detail.splitlines() if line.strip()]
    return lines[-1] if lines else f"Worker failed with exit code {result.returncode}"


def is_silent_source_case(name: str) -> bool:
    tags_raw = os.environ.get("SILENT_SOURCE_TAGS", "_silent,-silent,.silent")
    tags = [t.strip().lower() for t in tags_raw.split(",") if t.strip()]
    lower = name.lower()
    return any(tag in lower for tag in tags)


def _ffmpeg_escape(path: str) -> str:
    return Path(path).resolve().as_posix().replace(":", "\\:")


def hardsub(video_path: str, en_srt: str, es_srt: str, out_path: str) -> tuple[bool, str]:
    en_filter = f"subtitles='{_ffmpeg_escape(en_srt)}':force_style='Alignment=2'"
    es_filter = f"subtitles='{_ffmpeg_escape(es_srt)}':force_style='Alignment=8'"
    vf = f"{es_filter},{en_filter}"
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        video_path,
        "-vf",
        vf,
        "-c:a",
        "copy",
        out_path,
    ]
    result = subprocess.run(cmd, text=True, capture_output=True, check=False)
    if result.returncode == 0:
        return True, ""
    detail = (result.stderr or result.stdout or "").strip()
    return False, detail.splitlines()[-1] if detail else "ffmpeg failed"

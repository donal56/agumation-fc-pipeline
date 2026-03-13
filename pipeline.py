import os
import sys
import subprocess
import json
import re
import time
import shutil
import warnings
import urllib.error
import urllib.request

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
_local_env_loaded = False

# Suppress known upstream warning from ctranslate2 importing pkg_resources.
warnings.filterwarnings(
    "ignore",
    message=r"pkg_resources is deprecated as an API\..*",
    category=UserWarning,
    module=r"ctranslate2(\..*)?$",
)


def log_file_status(stage, filename, status, detail=""):
    suffix = f" ({detail})" if detail else ""
    print(f"[{stage}] {filename}: {status}{suffix}")


def log_file_start(stage, filename):
    # Print a progress line that will be overwritten by the final status.
    print(f"[{stage}] {filename}: Processing...", end="\r", flush=True)


def read_text_file(path):
    with open(path, "r", encoding="utf8") as f:
        return f.read()


def is_blank_text_file(path):
    if not os.path.exists(path):
        return False
    return read_text_file(path).strip() == ""


def ensure_blank_file(path):
    with open(path, "w", encoding="utf8") as f:
        f.write("")


def is_silent_source_case(name):
    jp_path = os.path.join(JP, name + ".srt")
    qc_path = os.path.join(QC, name + ".txt")
    return is_blank_text_file(jp_path) and is_blank_text_file(qc_path)


def load_local_env():
    global _local_env_loaded
    if _local_env_loaded:
        return

    env_path = os.path.join(os.getcwd(), ".env.local")
    if not os.path.exists(env_path):
        _local_env_loaded = True
        return

    with open(env_path, "r", encoding="utf8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                value = value[1:-1]
            os.environ.setdefault(key, value)

    _local_env_loaded = True


def validate_translate_python():
    load_local_env()
    translate_python = os.environ.get("TRANSLATE_PYTHON", "").strip()
    if not translate_python:
        return True
    if not os.path.exists(translate_python):
        print(f"Error: TRANSLATE_PYTHON does not exist: {translate_python}")
        print(
            "Set TRANSLATE_PYTHON to a valid python.exe path for the translation venv."
        )
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


def get_env_float(name, default):
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def get_env_int(name, default):
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def load_json_config(path, default):
    if not path or not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf8") as f:
            return json.load(f)
    except Exception:
        return default


def _segment_value(segment, key, fallback=None):
    if isinstance(segment, dict):
        return segment.get(key, fallback)
    return getattr(segment, key, fallback)


SENTENCE_END_RE = re.compile(r"[.!?。！？][\"'”」』）\)\]]*$")
CLAUSE_END_RE = re.compile(r"[,;、，；][\"'”」』）\)\]]*$")


def _words_text(words):
    return "".join(_segment_value(word, "word", "") for word in words).strip()


def _build_word_chunk(words):
    if not words:
        return None
    start = _segment_value(words[0], "start", 0)
    end = _segment_value(words[-1], "end", start)
    return {
        "start": start,
        "end": end,
        "text": _words_text(words),
        "words": words,
    }


def _exceeds_limits(words, max_cue_seconds, max_cue_chars):
    if not words:
        return False
    start = _segment_value(words[0], "start", 0)
    end = _segment_value(words[-1], "end", start)
    text = _words_text(words)
    exceeds_duration = max_cue_seconds > 0 and (end - start) > max_cue_seconds
    exceeds_chars = max_cue_chars > 0 and len(text) > max_cue_chars
    return exceeds_duration or exceeds_chars


def _find_sentence_break(words, max_tail_words=6):
    if len(words) < 2:
        return None
    start_i = max(0, len(words) - 1 - max_tail_words)
    for i in range(len(words) - 2, start_i - 1, -1):
        token = _segment_value(words[i], "word", "").strip()
        if token and SENTENCE_END_RE.search(token):
            return i + 1
    return None


def _find_clause_break(words, max_tail_words=8):
    if len(words) < 2:
        return None
    start_i = max(0, len(words) - 1 - max_tail_words)
    for i in range(len(words) - 2, start_i - 1, -1):
        token = _segment_value(words[i], "word", "").strip()
        if token and CLAUSE_END_RE.search(token):
            return i + 1
    return None


def _find_pause_break(words, min_gap_seconds, max_tail_words=8):
    if len(words) < 2 or min_gap_seconds <= 0:
        return None
    start_i = max(0, len(words) - 1 - max_tail_words)
    for i in range(len(words) - 2, start_i - 1, -1):
        end_i = _segment_value(words[i], "end", None)
        start_next = _segment_value(words[i + 1], "start", None)
        if end_i is None or start_next is None:
            continue
        if (start_next - end_i) >= min_gap_seconds:
            return i + 1
    return None


def split_segments_for_srt(segments, max_cue_seconds, max_cue_chars):
    if max_cue_seconds <= 0 and max_cue_chars <= 0:
        return segments

    min_gap_seconds = max(0.0, get_env_float("SRT_SPLIT_GAP_SECONDS", 0.6))

    split = []
    for segment in segments:
        words = _segment_value(segment, "words", None) or []
        if not words:
            split.append(segment)
            continue

        chunk_words = []
        chunk_start = None
        chunk_end = None

        def flush_chunk():
            if not chunk_words:
                return
            chunk = _build_word_chunk(chunk_words)
            if chunk is not None:
                split.append(chunk)

        for word in words:
            word_start = _segment_value(word, "start", None)
            word_end = _segment_value(word, "end", None)
            if word_start is None or word_end is None:
                continue

            if not chunk_words:
                chunk_words = [word]
                chunk_start = word_start
                chunk_end = word_end
                continue

            candidate_words = chunk_words + [word]
            candidate_text = "".join(
                _segment_value(w, "word", "") for w in candidate_words
            ).strip()
            candidate_end = word_end
            candidate_duration = candidate_end - chunk_start

            exceeds_duration = (
                max_cue_seconds > 0 and candidate_duration > max_cue_seconds
            )
            exceeds_chars = max_cue_chars > 0 and len(candidate_text) > max_cue_chars

            if exceeds_duration or exceeds_chars:
                break_at = _find_sentence_break(candidate_words)
                if break_at is None:
                    break_at = _find_clause_break(candidate_words)
                if break_at is None:
                    break_at = _find_pause_break(candidate_words, min_gap_seconds)
                if break_at is not None:
                    first_words = candidate_words[:break_at]
                    tail_words = candidate_words[break_at:]
                    if tail_words and not _exceeds_limits(
                        first_words, max_cue_seconds, max_cue_chars
                    ):
                        first_chunk = _build_word_chunk(first_words)
                        if first_chunk is not None:
                            split.append(first_chunk)
                        chunk_words = tail_words
                        chunk_start = _segment_value(chunk_words[0], "start", None)
                        chunk_end = _segment_value(chunk_words[-1], "end", None)
                        continue

                flush_chunk()
                chunk_words = [word]
                chunk_start = word_start
                chunk_end = word_end
            else:
                chunk_words = candidate_words
                chunk_end = candidate_end

        flush_chunk()

    return split


def apply_short_cue_remediation(segments, rules):
    if not rules:
        return segments
    prefix_rule = {
        "prefix_punct_max_chars": 3,
        "prefix_punct_max_seconds": 0.7,
        "prefix_punct_delimiters": ".,?!。！？",
    }
    prefix_punct_set = set(prefix_rule["prefix_punct_delimiters"])

    remediated = []
    i = 0
    while i < len(segments):
        seg = segments[i]
        text = str(_segment_value(seg, "text", "")).strip()
        start = _segment_value(seg, "start", 0)
        end = _segment_value(seg, "end", start)
        duration = max(0.0, end - start)
        length = len(text)

        # If the line starts with a very short sentence that ends early, move it to the previous cue.
        if remediated and text:
            words = _segment_value(seg, "words", None)
            if words:
                prefix_words = []
                prefix_text = ""
                prefix_end = None
                remainder_words = []
                for word in words:
                    w_text = str(_segment_value(word, "word", ""))
                    prefix_words.append(word)
                    prefix_text = (prefix_text + w_text).strip()
                    for ch in w_text:
                        if ch in prefix_punct_set:
                            prefix_end = _segment_value(word, "end", None)
                            remainder_words = words[len(prefix_words) :]
                            break
                    if prefix_end is not None:
                        break
                if prefix_end is not None:
                    prefix_len = len(prefix_text.replace(" ", ""))
                    prefix_start = _segment_value(prefix_words[0], "start", start)
                    prefix_duration = max(0.0, prefix_end - prefix_start)
                    if (
                        0 < prefix_len <= prefix_rule["prefix_punct_max_chars"]
                        and prefix_duration <= prefix_rule["prefix_punct_max_seconds"]
                    ):
                        prev = remediated.pop()
                        merged_prev = {
                            "start": _segment_value(prev, "start", start),
                            "end": _segment_value(prev, "end", start),
                            "text": (
                                str(_segment_value(prev, "text", "")).strip()
                                + " "
                                + prefix_text
                            ).strip(),
                            "words": (_segment_value(prev, "words", None) or []) + prefix_words,
                        }
                        remediated.append(merged_prev)
                        if remainder_words:
                            seg = {
                                "start": _segment_value(remainder_words[0], "start", start),
                                "end": _segment_value(remainder_words[-1], "end", end),
                                "text": _words_text(remainder_words),
                                "words": remainder_words,
                            }
                            text = str(_segment_value(seg, "text", "")).strip()
                            start = _segment_value(seg, "start", start)
                            end = _segment_value(seg, "end", end)
                            length = len(text)
                        else:
                            i += 1
                            continue
            else:
                split_at = None
                for idx, ch in enumerate(text):
                    if ch in prefix_punct_set:
                        split_at = idx
                        break
                if split_at is not None:
                    prefix = text[: split_at + 1]
                    prefix_len = len(prefix.replace(" ", ""))
                    if 0 < prefix_len <= prefix_rule["prefix_punct_max_chars"]:
                        prev = remediated.pop()
                        merged_prev = {
                            "start": _segment_value(prev, "start", start),
                            "end": _segment_value(prev, "end", start),
                            "text": (
                                str(_segment_value(prev, "text", "")).strip()
                                + " "
                                + prefix
                            ).strip(),
                        }
                        remediated.append(merged_prev)
                        text = text[split_at + 1 :].lstrip()
                        length = len(text)
                        if not text:
                            i += 1
                            continue

        should_merge = False
        merge_into_previous = False
        for rule in rules:
            try:
                charsPerLine = int(rule.get("chars_per_line", -1))
                minSeconds = float(rule.get("min_seconds", -1))
            except Exception:
                continue
            if charsPerLine >= 0 and minSeconds >= 0:
                duration_block = True if minSeconds == 0 else duration <= minSeconds
                if length <= charsPerLine and duration_block:
                    should_merge = True
                    break

        punct_chars = prefix_rule["prefix_punct_delimiters"]
        max_prefix_chars = int(prefix_rule["prefix_punct_max_chars"])
        max_prefix_seconds = float(prefix_rule["prefix_punct_max_seconds"])
        if max_prefix_chars > 0 and max_prefix_seconds >= 0:
            duration_ok = True if max_prefix_seconds == 0 else duration <= max_prefix_seconds
            if length <= max_prefix_chars and duration_ok:
                if length > 0 and text and text[-1] in punct_chars:
                    should_merge = True
                    merge_into_previous = True

        if not should_merge:
            remediated.append(seg)
            i += 1
            continue

        if merge_into_previous and remediated:
            prev = remediated.pop()
            merged = {
                "start": _segment_value(prev, "start", start),
                "end": end,
                "text": (str(_segment_value(prev, "text", "")).strip() + " " + text).strip(),
            }
            remediated.append(merged)
            i += 1
            continue

        # Prefer merging with the next segment.
        if i + 1 < len(segments):
            nxt = segments[i + 1]
            merged = {
                "start": start,
                "end": _segment_value(nxt, "end", end),
                "text": (text + " " + str(_segment_value(nxt, "text", "")).strip()).strip(),
                "words": (_segment_value(seg, "words", None) or [])
                + (_segment_value(nxt, "words", None) or []),
            }
            remediated.append(merged)
            i += 2
            continue

        # Fallback: merge into previous if this is the last segment.
        if remediated:
            prev = remediated.pop()
            merged = {
                "start": _segment_value(prev, "start", start),
                "end": end,
                "text": (str(_segment_value(prev, "text", "")).strip() + " " + text).strip(),
                "words": (_segment_value(prev, "words", None) or [])
                + (_segment_value(seg, "words", None) or []),
            }
            remediated.append(merged)
        else:
            remediated.append(seg)
        i += 1

    return remediated


def write_srt(segments, path):

    with open(path, "w", encoding="utf8") as f:

        for i, seg in enumerate(segments, 1):
            start = _segment_value(seg, "start", 0)
            end = _segment_value(seg, "end", start)
            text = _segment_value(seg, "text", "")

            f.write(str(i) + "\n")
            f.write(format_time(start) + " --> " + format_time(end) + "\n")
            f.write(str(text).strip() + "\n\n")


def translate_srt(input_path, output_path, translator):
    with open(input_path, "r", encoding="utf8") as f:
        source_srt = f.read()

    if hasattr(translator, "translate_full_srt"):
        translated_srt = translator.translate_full_srt(source_srt)
        validate_srt_timemarks(source_srt, translated_srt)
        with open(output_path, "w", encoding="utf8") as f:
            f.write(translated_srt.rstrip() + "\n")
        return

    with open(input_path, "r", encoding="utf8") as f:
        lines = f.readlines()

    out = []
    text_inputs = []
    i = 0

    # Translate subtitle cues as blocks to preserve context and intent better.
    while i < len(lines):
        is_cue_start = (
            i + 1 < len(lines) and lines[i].strip().isdigit() and "-->" in lines[i + 1]
        )

        if not is_cue_start:
            out.append(("meta", lines[i]))
            i += 1
            continue

        out.append(("meta", lines[i]))  # cue index
        out.append(("meta", lines[i + 1]))  # time range
        i += 2

        cue_text_lines = []
        while i < len(lines) and lines[i].strip() != "":
            cue_text_lines.append(lines[i].rstrip("\n"))
            i += 1

        if cue_text_lines:
            text_inputs.append("\n".join(cue_text_lines))
            out.append(("text", None))

        if i < len(lines) and lines[i].strip() == "":
            out.append(("meta", lines[i]))
            i += 1

    if hasattr(translator, "translate_many"):
        translated_texts = translator.translate_many(text_inputs)
    else:
        translated_texts = [translator.translate(text) for text in text_inputs]

    text_i = 0
    rendered = []
    for entry_type, value in out:
        if entry_type == "meta":
            rendered.append(value)
        else:
            translated = translated_texts[text_i].rstrip("\n")
            rendered.append(translated + "\n")
            text_i += 1

    with open(output_path, "w", encoding="utf8") as f:
        f.writelines(rendered)


SRT_TIMEMARK_RE = re.compile(r"^\d{2}:\d{2}:\d{2},\d{3}\s-->\s\d{2}:\d{2}:\d{2},\d{3}$")


def extract_srt_timemarks(srt_text):
    timemarks = []
    for line in srt_text.splitlines():
        stripped = line.strip()
        if "-->" in stripped:
            timemarks.append(stripped)
    return timemarks


def validate_srt_timemarks(source_srt, translated_srt):
    source_timemarks = extract_srt_timemarks(source_srt)
    translated_timemarks = extract_srt_timemarks(translated_srt)

    if len(source_timemarks) != len(translated_timemarks):
        raise RuntimeError(
            f"Timestamp count mismatch after translation: expected {len(source_timemarks)}, got {len(translated_timemarks)}"
        )

    for i, (src, dst) in enumerate(
        zip(source_timemarks, translated_timemarks), start=1
    ):
        if not SRT_TIMEMARK_RE.match(dst):
            raise RuntimeError(f"Invalid timestamp format in translated cue {i}: {dst}")
        if src != dst:
            raise RuntimeError(f"Timestamp changed in cue {i}: '{src}' -> '{dst}'")


class OpenAITranslator:
    @staticmethod
    def _normalize_lang_code(lang):
        value = (lang or "").strip().upper()
        aliases = {
            "JAPANESE": "JA",
            "JPN": "JA",
            "ENGLISH": "EN",
            "ENG": "EN",
            "SPANISH": "ES",
            "SPA": "ES",
        }
        return aliases.get(value, value)

    @classmethod
    def _resolve_role_context(cls, source_lang, target_lang):
        source_code = cls._normalize_lang_code(source_lang)
        target_code = cls._normalize_lang_code(target_lang)
        pair_key = f"{source_code}_{target_code}"
        base_context = os.environ.get("OPENAI_CONTEXT", "").strip()
        pair_context = os.environ.get(f"OPENAI_CONTEXT_{pair_key}", "").strip()
        return " ".join(part for part in [base_context, pair_context] if part)

    def __init__(self, source_lang, target_lang):
        load_local_env()
        self.source_lang = self._normalize_lang_code(source_lang)
        self.target_lang = self._normalize_lang_code(target_lang)
        self.api_key = os.environ.get("OPENAI_API_KEY", "").strip()
        self.api_url = "https://api.openai.com/v1/responses"
        self.model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini").strip()
        self.timeout = 20
        self.max_retries = 3
        self.retry_base_seconds = 2
        self.role_context = self._resolve_role_context(
            self.source_lang, self.target_lang
        )
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY is required for translation")

    @staticmethod
    def _extract_openai_error_message(body):
        if not body:
            return ""
        try:
            payload = json.loads(body)
            err = payload.get("error", {})
            if isinstance(err, dict):
                msg = err.get("message")
                err_type = err.get("type")
                if msg and err_type:
                    return f"{err_type}: {msg}"
                if msg:
                    return str(msg)
        except Exception:
            pass
        return " ".join(body.split())

    def _translate_full_srt(self, srt_text):
        system_prompt = (
            f"You translate subtitles from {self.source_lang} to {self.target_lang}. "
            "Return valid SRT only. Keep all cue indexes and timestamp lines exactly unchanged. "
            "Only translate subtitle text. Preserve line breaks per cue when possible. "
            "Prioritize natural speech over direct translations. "
            "The input may contain errors, feel free to make corrections if they're minimal."
            "If there is a figure of speech/untranslatable phrase, make an interpretation that keeps the intention and tone of the original."
        )
        if self.role_context:
            system_prompt += f" Context: {self.role_context}"

        if self.api_url.rstrip("/").endswith("/chat/completions"):
            body_data = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": srt_text},
                ],
            }
        else:
            body_data = {
                "model": self.model,
                "input": [
                    {
                        "role": "system",
                        "content": [{"type": "input_text", "text": system_prompt}],
                    },
                    {
                        "role": "user",
                        "content": [{"type": "input_text", "text": srt_text}],
                    },
                ],
            }
        payload = json.dumps(body_data).encode("utf-8")
        req = urllib.request.Request(
            self.api_url,
            data=payload,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        attempts = self.max_retries + 1
        raw = None
        for attempt in range(1, attempts + 1):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as response:
                    raw = response.read().decode("utf-8")
                break
            except urllib.error.HTTPError as err:
                body = err.read().decode("utf-8", errors="replace")
                message = self._extract_openai_error_message(body)
                should_retry = err.code == 429 or 500 <= err.code <= 599
                if should_retry and attempt < attempts:
                    retry_after = 0
                    try:
                        retry_after = int(err.headers.get("Retry-After", "0"))
                    except Exception:
                        retry_after = 0
                    wait_seconds = retry_after or (
                        self.retry_base_seconds * (2 ** (attempt - 1))
                    )
                    time.sleep(wait_seconds)
                    continue
                raise RuntimeError(f"OpenAI HTTP {err.code}: {message}") from err
            except urllib.error.URLError as err:
                should_retry = attempt < attempts
                if should_retry:
                    wait_seconds = self.retry_base_seconds * (2 ** (attempt - 1))
                    time.sleep(wait_seconds)
                    continue
                raise RuntimeError(f"OpenAI connection error: {err}") from err

        if raw is None:
            raise RuntimeError("OpenAI request failed before receiving any response")

        parsed = json.loads(raw)
        output_text = parsed.get("output_text", "")
        if output_text:
            return output_text

        if "choices" in parsed:
            choices = parsed.get("choices", [])
            if choices:
                message = choices[0].get("message", {})
                content = message.get("content", "")
                if isinstance(content, str) and content.strip():
                    return content
                if isinstance(content, list):
                    chunks = []
                    for part in content:
                        if isinstance(part, dict):
                            text_part = part.get("text")
                            if isinstance(text_part, str):
                                chunks.append(text_part)
                    joined = "".join(chunks).strip()
                    if joined:
                        return joined

        output = parsed.get("output", [])
        chunks = []
        for item in output:
            for content in item.get("content", []):
                text_value = content.get("text")
                if isinstance(text_value, str):
                    chunks.append(text_value)
        combined = "".join(chunks).strip()
        if combined:
            return combined

        raise RuntimeError("OpenAI response did not include translatable output_text")

    def translate_full_srt(self, srt_text):
        return self._translate_full_srt(srt_text)

    def translate(self, text):
        return self._translate_full_srt(text)


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


def summarize_worker_failure(result):
    combined = "\n".join([result.stderr or "", result.stdout or ""]).strip()
    if not combined:
        return f"worker exit code {result.returncode}"

    # Prefer the explicit worker error line when present.
    for line in reversed(combined.splitlines()):
        line = line.strip()
        if line.startswith("Worker translation error:"):
            return line
        if line.startswith("Error initializing OpenAI translators:"):
            return line

    # Extract a compact OpenAI API error message if output is JSON.
    start = combined.find("{")
    end = combined.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            payload = json.loads(combined[start : end + 1])
            err = payload.get("error", {})
            if isinstance(err, dict):
                msg = err.get("message")
                err_type = err.get("type")
                if msg and err_type:
                    return f"OpenAI error ({err_type}): {msg}"
                if msg:
                    return f"OpenAI error: {msg}"
        except Exception:
            pass

    lines = [line.strip() for line in combined.splitlines() if line.strip()]
    return lines[-1] if lines else f"worker exit code {result.returncode}"


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

    try:
        _ja_en = OpenAITranslator("JA", "EN")
        _en_es = OpenAITranslator("EN", "ES")
        return _ja_en, _en_es
    except Exception as err:
        print(f"Error initializing OpenAI translators: {err}")
        return None, None


def qc_check_srt(path):

    problems = []

    with open(path, "r", encoding="utf8") as f:
        lines = f.readlines()

    if "".join(lines).strip() == "":
        return ["Transcripcion vacia (sin cues en SRT)"]

    i = 0
    found_cue = False
    cue_counter = 0

    while i < len(lines):

        if "-->" in lines[i]:
            found_cue = True
            cue_counter += 1

            time_line = lines[i].strip()
            text_line = lines[i + 1].strip()
            cue_label = str(cue_counter)
            if i > 0 and lines[i - 1].strip().isdigit():
                cue_label = lines[i - 1].strip()

            start, end = time_line.split("-->")

            def to_sec(t):

                h, m, s = t.strip().replace(",", ".").split(":")
                return int(h) * 3600 + int(m) * 60 + float(s)

            dur = to_sec(end) - to_sec(start)

            chars = len(text_line)

            cps = chars / dur if dur > 0 else 999

            if chars > 42:
                problems.append(f"Cue {cue_label}: Linea larga ({chars} chars): {text_line}")

            if dur < 0.7:
                problems.append(f"Cue {cue_label}: Duracion muy corta {dur:.2f}s: {text_line}")

            if dur > 7:
                problems.append(f"Cue {cue_label}: Duracion muy larga {dur:.2f}s: {text_line}")

            if cps > 20:
                problems.append(f"Cue {cue_label}: Lectura rapida {cps:.1f} cps: {text_line}")

        i += 1

    if not found_cue:
        problems.append("SRT sin cues validos (no se encontraron timestamps)")

    return problems


def hardsub(video, srt1, srt2, out):
    def escape_filter_value(path):
        # FFmpeg filtergraph escaping (not shell escaping).
        normalized = path.replace("\\", "/")
        escaped = []
        for ch in normalized:
            if ch in "\\':,;[]":
                escaped.append("\\" + ch)
            else:
                escaped.append(ch)
        return "".join(escaped)

    tmp_dir = os.path.join(".runtime", "hardsub_tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    token = f"{int(time.time() * 1000)}_{os.getpid()}"
    srt1_tmp = os.path.join(tmp_dir, f"top_{token}.srt")
    srt2_tmp = os.path.join(tmp_dir, f"bottom_{token}.srt")
    shutil.copyfile(srt1, srt1_tmp)
    shutil.copyfile(srt2, srt2_tmp)

    srt1_escaped = escape_filter_value(srt1_tmp)
    srt2_escaped = escape_filter_value(srt2_tmp)
    top_style = "Fontname=Calibri,PrimaryColour=&HFFFFFF&,Fontsize=21,Alignment=6"
    bottom_style = "Fontname=Calibri,PrimaryColour=&HFFFFFF&,Fontsize=21"
    filter_complex = (
        f"[0]subtitles=filename='{srt1_escaped}':force_style='{top_style}'[a]; "
        f"[a]subtitles=filename='{srt2_escaped}':force_style='{bottom_style}'"
    )

    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        video,
        "-v",
        "error",
        "-stats",
        "-filter_complex",
        filter_complex,
        out,
    ]

    try:
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    finally:
        for tmp_path in (srt1_tmp, srt2_tmp):
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
    if result.returncode == 0:
        return True, ""

    combined = "\n".join([result.stderr or "", result.stdout or ""]).strip()
    lines = [line.strip() for line in combined.splitlines() if line.strip()]
    detail = lines[-1] if lines else "ffmpeg returned non-zero"
    return False, detail


def stage_transcribe():

    print("\nSTAGE: TRANSCRIBE\n")
    global model
    load_local_env()
    max_cue_seconds = max(0.0, get_env_float("SRT_MAX_CUE_SECONDS", 8.0))
    max_cue_chars = max(0, get_env_int("SRT_MAX_CUE_CHARS", 15))
    use_word_timestamps = max_cue_seconds > 0 or max_cue_chars > 0
    remediation_config_path = os.environ.get(
        "SRT_REMEDIATION_CONFIG", "srt_remediation.json"
    ).strip()
    remediation_config = load_json_config(remediation_config_path, {})
    short_cue_rules = remediation_config.get("short_cue_rules", [])

    if model is None:
        try:
            from faster_whisper import WhisperModel
        except OSError as err:
            print(
                "Error loading faster-whisper runtime (Torch DLL initialization failed)."
            )
            print("Likely Windows dependency issue in your venv.")
            print("Try reinstalling CPU wheels:")
            print("  python -m pip uninstall -y torch torchvision torchaudio")
            print(
                "  python -m pip install --index-url https://download.pytorch.org/whl/cpu torch torchvision torchaudio"
            )
            print(f"Details: {err}")
            return
        except Exception as err:
            print(f"Error importing faster-whisper: {err}")
            return

        try:
            model = WhisperModel("small", compute_type="int8")
        except Exception as err:
            msg = str(err)
            if (
                "LocalEntryNotFoundError" in msg
                or "cannot find the appropriate snapshot folder" in msg
            ):
                print(
                    "Error: Whisper model files are not available locally and auto-download failed."
                )
                print(
                    "Check internet/proxy access to Hugging Face, then retry transcribe stage."
                )
                print(
                    "Alternative: pre-download the model cache in an environment with internet."
                )
            else:
                print(f"Error initializing Whisper model: {err}")
            return

    for f in os.listdir(SRC):

        if not f.lower().endswith(VIDEO_EXT):
            log_file_status("transcribe", f, "Skipped", "unsupported extension")
            continue
        log_file_start("transcribe", f)

        name = os.path.splitext(f)[0]

        jp_path = os.path.join(JP, name + ".srt")

        if os.path.exists(jp_path):
            log_file_status("transcribe", f, "Skipped", "already transcribed")
            continue

        video = os.path.join(SRC, f)
        try:
            segments, _ = model.transcribe(
                video, language="ja", word_timestamps=use_word_timestamps
            )
            segment_list = list(segments)
            split_segments = split_segments_for_srt(
                segment_list,
                max_cue_seconds=max_cue_seconds,
                max_cue_chars=max_cue_chars,
            )
            remediated_segments = apply_short_cue_remediation(
                split_segments, short_cue_rules
            )
            write_srt(remediated_segments, jp_path)
            log_file_status("transcribe", f, "Success")
        except Exception as err:
            log_file_status("transcribe", f, "Failed", str(err))


def stage_qc():

    print("\nSTAGE: QC\n")

    for f in os.listdir(JP):
        if not f.lower().endswith(".srt"):
            log_file_status("qc", f, "Skipped", "unsupported extension")
            continue
        log_file_start("qc", f)

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
        log_file_start("translate_en", f)

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

        if is_silent_source_case(name):
            log_file_status(
                "translate_en",
                f,
                "Success",
                "silent source, translation skipped (no SRT generated)",
            )
            continue

        try:
            result = run_translate_file_worker("ja_en", jp, en_out)
            if result.returncode == 0:
                log_file_status("translate_en", f, "Success")
            else:
                detail = summarize_worker_failure(result)
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

    for f in os.listdir(JP):
        if not f.lower().endswith(".srt"):
            log_file_status("translate_es", f, "Skipped", "unsupported extension")
            continue
        log_file_start("translate_es", f)

        name = os.path.splitext(f)[0]

        en = os.path.join(EN, name + ".srt")
        es_out = os.path.join(ES, name + ".srt")

        if os.path.exists(es_out):
            log_file_status("translate_es", f, "Skipped", "already translated")
            continue

        if is_silent_source_case(name):
            log_file_status(
                "translate_es",
                f,
                "Success",
                "silent source, translation skipped (no SRT generated)",
            )
            continue

        if not os.path.exists(en):
            log_file_status("translate_es", f, "Skipped", "missing EN subtitle")
            continue

        try:
            result = run_translate_file_worker("en_es", en, es_out)
            if result.returncode == 0:
                log_file_status("translate_es", f, "Success")
            else:
                detail = summarize_worker_failure(result)
                log_file_status("translate_es", f, "Failed", detail)
        except Exception as err:
            log_file_status("translate_es", f, "Failed", str(err))


def stage_hardsub():

    print("\nSTAGE: HARDSUB\n")

    for f in os.listdir(SRC):
        if not f.lower().endswith(VIDEO_EXT):
            log_file_status("hardsub", f, "Skipped", "unsupported extension")
            continue
        log_file_start("hardsub", f)

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

        if is_silent_source_case(name):
            try:
                shutil.copyfile(video, out)
                log_file_status(
                    "hardsub",
                    f,
                    "Success",
                    "silent source, copied without subtitles",
                )
            except Exception as err:
                log_file_status("hardsub", f, "Failed", str(err))
            continue

        srt_filename = name + ".srt"
        srt1 = os.path.join(EN, srt_filename)
        srt2 = os.path.join(ES, srt_filename)
        if not os.path.exists(srt1) or not os.path.exists(srt2):
            log_file_status("hardsub", f, "Skipped", "missing translated subtitles")
            continue
        try:
            ok, detail = hardsub(video, srt1, srt2, out)
            if ok:
                log_file_status("hardsub", f, "Success")
            else:
                log_file_status("hardsub", f, "Failed", detail)
        except Exception as err:
            log_file_status("hardsub", f, "Failed", str(err))


def ensure_dirs():

    for d in [SRC, JP, QC, EN, ES, OUT]:
        os.makedirs(d, exist_ok=True)


def main():

    ensure_dirs()

    if len(sys.argv) >= 2 and sys.argv[1] == "__translate_file":
        if len(sys.argv) != 5:
            print(
                "Usage: pipeline.py __translate_file [ja_en|en_es] <input_srt> <output_srt>"
            )
            sys.exit(2)
        try:
            code = worker_translate_file(sys.argv[2], sys.argv[3], sys.argv[4])
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
        and not validate_translate_python()
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


if __name__ == "__main__":
    main()

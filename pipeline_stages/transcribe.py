import os

import pipeline_utils as pu


SENTENCE_PUNCT = {".", "。", ";", "；", ":", "：", "?", "？", "¿", "!", "！"}
SOFT_PUNCT = {",", "，", "、"}
LONG_WORD_SECONDS = 2.0
LONG_SILENCE_SECONDS = 1.0
SHORT_CUE_SECONDS = 0.6
SHORT_CUE_CHARS = 3
SHORT_CUE_MERGE_OVERFLOW_SECONDS = 0.75


def _word_text(word: object) -> str:
    final_word = ""
    if isinstance(word, dict):
        final_word = str(word.get("word", ""))
    else:
        final_word = str(getattr(word, "word", ""))
    # This is useful only for JP source transcriptions
    return final_word.replace(" ", "。")


def _word_start(word: object, default: float) -> float:
    if isinstance(word, dict):
        return float(word.get("start", default))
    return float(getattr(word, "start", default))


def _word_end(word: object, default: float) -> float:
    if isinstance(word, dict):
        return float(word.get("end", default))
    return float(getattr(word, "end", default))


def _split_text_on_sentence_punct(text: str) -> list[str]:
    stripped = text.strip()
    if not stripped:
        return []

    parts: list[str] = []
    current = ""
    for ch in stripped:
        current += ch
        if ch in SENTENCE_PUNCT:
            parts.append(current)
            current = ""
    if current:
        parts.append(current)
    return parts


def _build_timed_text_parts(text: str, start: float, end: float) -> list[dict]:
    parts = _split_text_on_sentence_punct(text)
    if not parts:
        return []
    if len(parts) == 1:
        return [{"text": parts[0], "start": start, "end": end}]

    duration = max(0.0, end - start)
    total_units = sum(max(1, len(part)) for part in parts)
    elapsed_units = 0
    output: list[dict] = []

    for idx, part in enumerate(parts):
        part_start = start if idx == 0 else output[-1]["end"]
        elapsed_units += max(1, len(part))
        if idx == len(parts) - 1:
            part_end = end
        else:
            part_end = start + (duration * elapsed_units / total_units)
        output.append({"text": part, "start": float(part_start), "end": float(part_end)})

    return output


def _segment_words(segment: object) -> list[dict]:
    words = getattr(segment, "words", None)
    if words is None and isinstance(segment, dict):
        words = segment.get("words")
    if words:
        output = []
        for word in words:
            output.extend(
                _build_timed_text_parts(
                    _word_text(word),
                    _word_start(word, 0.0),
                    _word_end(word, 0.0),
                )
            )
        return output

    text = getattr(segment, "text", "")
    if isinstance(segment, dict):
        text = segment.get("text", text)
    start = getattr(segment, "start", 0.0)
    end = getattr(segment, "end", 0.0)
    if isinstance(segment, dict):
        start = segment.get("start", start)
        end = segment.get("end", end)
    if text:
        return _build_timed_text_parts(str(text), float(start), float(end))
    return []


def _ends_with_punct(text: str, punct_set: set[str]) -> bool:
    stripped = text.strip()
    return bool(stripped) and stripped[-1] in punct_set


def _sentence_duration(words: list[dict]) -> float:
    if not words:
        return 0.0
    return float(words[-1]["end"]) - float(words[0]["start"])


def _chunk_duration(words: list[dict]) -> float:
    if not words:
        return 0.0
    return float(words[-1]["end"]) - float(words[0]["start"])


def _chunk_text(words: list[dict]) -> str:
    return "".join(w["text"] for w in words).strip()


def _is_short_chunk(words: list[dict]) -> bool:
    text = _chunk_text(words)
    return _chunk_duration(words) < SHORT_CUE_SECONDS or len(text) <= SHORT_CUE_CHARS


def _merge_short_trailing_chunk(
    chunks: list[list[dict]], max_cue_seconds: float
) -> list[list[dict]]:
    if len(chunks) < 2 or max_cue_seconds <= 0:
        return chunks

    trailing = chunks[-1]
    if not _is_short_chunk(trailing):
        return chunks

    merged_duration = float(trailing[-1]["end"]) - float(chunks[-2][0]["start"])
    if merged_duration > max_cue_seconds + SHORT_CUE_MERGE_OVERFLOW_SECONDS:
        return chunks

    return chunks[:-2] + [chunks[-2] + trailing]


def _split_sentence_on_soft_pauses(
    words: list[dict], max_cue_seconds: float
) -> list[list[dict]]:
    if max_cue_seconds <= 0:
        return [words]

    duration = _sentence_duration(words)
    if duration <= max_cue_seconds:
        return [words]

    def split_on_soft_punct(items: list[dict]) -> list[list[dict]]:
        chunks: list[list[dict]] = []
        start_idx = 0
        for idx, word in enumerate(items):
            if _ends_with_punct(word["text"], SOFT_PUNCT):
                chunks.append(items[start_idx : idx + 1])
                start_idx = idx + 1
        if start_idx < len(items):
            chunks.append(items[start_idx:])
        return chunks

    def split_by_duration(items: list[dict]) -> list[list[dict]]:
        if _sentence_duration(items) <= max_cue_seconds:
            return [items]
        output: list[list[dict]] = []
        start_idx = 0
        while start_idx < len(items):
            end_idx = start_idx
            while end_idx < len(items):
                seg_duration = float(items[end_idx]["end"]) - float(
                    items[start_idx]["start"]
                )
                if seg_duration <= max_cue_seconds:
                    end_idx += 1
                    continue
                break
            if end_idx == start_idx:
                output.append([items[start_idx]])
                start_idx += 1
            else:
                output.append(items[start_idx:end_idx])
                start_idx = end_idx
        return _merge_short_trailing_chunk(output, max_cue_seconds)

    def merge_chunks(items: list[list[dict]]) -> list[list[dict]]:
        merged: list[list[dict]] = []
        current: list[dict] = []
        for chunk in items:
            if not current:
                current = chunk
                continue
            candidate_duration = float(chunk[-1]["end"]) - float(
                current[0]["start"]
            )
            if candidate_duration <= max_cue_seconds:
                current = current + chunk
            else:
                merged.append(current)
                current = chunk
        if current:
            merged.append(current)
        return merged

    soft_chunks = split_on_soft_punct(words)
    if len(soft_chunks) == 1:
        return split_by_duration(words)

    merged = merge_chunks(soft_chunks)
    output: list[list[dict]] = []
    for chunk in merged:
        output.extend(split_by_duration(chunk))
    return output


def _build_sentence_cues(words: list[dict], max_cue_seconds: float) -> list[dict]:
    sentences: list[list[dict]] = []
    current: list[dict] = []
    for idx, word in enumerate(words):
        current.append(word)
        boundary = False

        if _ends_with_punct(word["text"], SENTENCE_PUNCT):
            boundary = True

        duration = float(word["end"]) - float(word["start"])
        if duration > LONG_WORD_SECONDS:
            boundary = True

        if idx + 1 < len(words):
            gap = float(words[idx + 1]["start"]) - float(word["end"])
            if gap > LONG_SILENCE_SECONDS:
                boundary = True

        if boundary:
            sentences.append(current)
            current = []

    if current:
        sentences.append(current)

    cues: list[dict] = []
    for sentence in sentences:
        chunks = _split_sentence_on_soft_pauses(sentence, max_cue_seconds)
        for chunk in chunks:
            text = _chunk_text(chunk)
            cues.append(
                {
                    "start": float(chunk[0]["start"]),
                    "end": float(chunk[-1]["end"]),
                    "text": text,
                }
            )

    if max_cue_seconds > 0:
        cues = _merge_adjacent_cues(cues, max_cue_seconds)

    return cues


def _merge_adjacent_cues(cues: list[dict], max_cue_seconds: float) -> list[dict]:
    if not cues:
        return []

    merged: list[dict] = []
    current = cues[0].copy()

    for nxt in cues[1:]:
        if _ends_with_punct(current["text"], SENTENCE_PUNCT):
            merged.append(current)
            current = nxt.copy()
            continue

        can_merge_on_soft = _ends_with_punct(current["text"], SOFT_PUNCT)
        candidate_duration = float(nxt["end"]) - float(current["start"])

        if can_merge_on_soft and candidate_duration <= max_cue_seconds:
            current["end"] = float(nxt["end"])
            current["text"] = f'{current["text"]}{nxt["text"]}'
        else:
            merged.append(current)
            current = nxt.copy()

    merged.append(current)
    return merged


def stage_transcribe():

    print("\nSTAGE: TRANSCRIBE\n")
    pu.load_local_env()
    max_cue_seconds = max(0.0, pu.get_env_float("SRT_MAX_CUE_SECONDS", 6.0))
    use_word_timestamps = True

    if pu.model is None:
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
            pu.model = WhisperModel("small", compute_type="int8")
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

    for f in os.listdir(pu.SRC):

        if not f.lower().endswith(pu.VIDEO_EXT):
            pu.log_file_status("transcribe", f, "Skipped", "Unsupported extension")
            continue
        pu.log_file_start("transcribe", f)

        name = os.path.splitext(f)[0]

        jp_path = os.path.join(pu.JP, name + ".srt")

        if os.path.exists(jp_path):
            pu.log_file_status("transcribe", f, "Skipped", "Already transcribed")
            continue

        video = os.path.join(pu.SRC, f)
        try:
            segments, _ = pu.model.transcribe(
                video, language="ja", word_timestamps=use_word_timestamps
            )
            segment_list = list(segments)
            words: list[dict] = []
            for segment in segment_list:
                words.extend(_segment_words(segment))
            cues = _build_sentence_cues(words, max_cue_seconds)
            pu.write_srt(cues, jp_path)
            pu.log_file_status("transcribe", f, "Success")
        except Exception as err:
            pu.log_file_status("transcribe", f, "Failed", str(err))

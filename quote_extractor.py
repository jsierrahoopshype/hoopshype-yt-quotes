"""
HoopsHype YouTube quote extractor — production pipeline.

Polls NBA YouTube channels via the YouTube Data API v3, filters out Shorts,
recaps, highlights, and short videos, then sends each remaining video to
Gemini 2.5 Flash and saves the top 12 quotes as markdown + raw JSON.

Usage (from repo root):
    python quote_extractor.py
"""

import json
import os
import re
import signal
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from google import genai
from google.genai import types

import slack_notify

load_dotenv()

MODEL = "gemini-2.5-flash"
ROOT = Path(__file__).parent
OUTPUT_DIR = ROOT / "output"
CONFIG_PATH = ROOT / "channels.json"
WATCH_URL_TEMPLATE = "https://www.youtube.com/watch?v={video_id}"
YT_API_BASE = "https://www.googleapis.com/youtube/v3"

PROMPT = """You are watching an NBA YouTube show. Extract the top 12 most controversial or insightful quotes about NBA trades, free agency, player movement, team dynamics, front office, contracts, player legacy, coaching, or playoff issues.

Hard rules — these are limits, not targets:

- LENGTH: Each quote MUST be between 60 and 220 words after cleanup. Quotes over 220 words must be split into separate ranked quotes or shortened. Do not exceed 220 words under any circumstance.
- PLAYER NAMES: Use standard NBA reporting spellings for player and team names. Examples: Mikal Bridges (not Michael), Karl-Anthony Towns or KAT (not Cat), Scottie Barnes (not Burns), Jrue Holiday, Donovan Mitchell, Mikael Pereira, Tyrese Maxey, Cade Cunningham, Jalen Brunson, Jaylen Brown, Jayson Tatum. Apply this to all player names across the league.
- ONE TOPIC PER QUOTE: A single quote covers a single subject — one player, one team, one story, one argument. If the speaker pivots to a new player, team, story, or argument, that is a new quote with its own rank, timestamp, and topic tags. Do not merge two topics into one quote even when they are spoken back-to-back.
- TAG COUNT: Each quote gets 1 to 3 topic tags. If you want more than 3 tags, the quote is covering too much ground — split it.
- MONOLOGUES: When a speaker delivers a 3-5 minute monologue covering several subjects (e.g. a series recap, a coaching firing, and a contract take all in one breath, common on NBA podcasts), do NOT include the full monologue. Extract the strongest 1-2 standalone takes from it as separate quotes, each scoped to one subject.

Editorial rules:

- Identify the speaker by name when shown on screen, named in chyrons, named in the video title, or clearly addressed by another speaker. Otherwise return "Unknown speaker". Do not guess.
- Provide a start timestamp in MM:SS or H:MM:SS format pointing at the moment the quote begins.
- Tag each quote using only these topics: trades, free agency, team dynamics, player legacy, rumors, front office, coaching, playoffs, contracts.
- Clean only obvious filler ("um", "uh", "you know", false starts) and punctuation. Preserve the speaker's meaning. Do not exaggerate their tone. Do not fabricate.
- Skip play-by-play recap, sponsor reads, intros, outros, generic opinions, and recycled talking points unless phrased forcefully.
- Add a one-sentence "why it matters" note framed for HoopsHype Rumors readers (NBA-savvy, want news value).
- Return up to 12 quotes ranked by news value. If fewer than 12 meet the bar, return fewer.

Return ONLY valid JSON, no surrounding text or markdown fences:

{
  "video_title_guess": "string",
  "speakers_seen": ["names you saw or heard, in order of appearance"],
  "quotes": [
    {
      "rank": 1,
      "speaker": "string",
      "timestamp": "MM:SS",
      "topic_tags": ["trades"],
      "quote": "cleaned quote text",
      "why_it_matters": "one sentence"
    }
  ]
}"""


SPLIT_PROMPT = """You are editing a single quote from an NBA YouTube show that is too long or covers too many topics. Split it into 1 to 4 standalone sub-quotes.

Hard rules:

- Each sub-quote MUST be between 60 and 220 words after cleanup. Do not exceed 220 words under any circumstance.
- PLAYER NAMES: Use standard NBA reporting spellings for player and team names. Examples: Mikal Bridges (not Michael), Karl-Anthony Towns or KAT (not Cat), Scottie Barnes (not Burns), Jrue Holiday, Donovan Mitchell, Mikael Pereira, Tyrese Maxey, Cade Cunningham, Jalen Brunson, Jaylen Brown, Jayson Tatum. Apply this to all player names across the league.
- Each sub-quote covers a single subject — one player, one team, one story, one argument.
- Each sub-quote gets 1 to 3 topic tags drawn ONLY from this vocabulary: trades, free agency, team dynamics, player legacy, rumors, front office, coaching, playoffs, contracts.
- Use the SAME speaker and the SAME timestamp as the original quote for every sub-quote.
- Clean only obvious filler ("um", "uh", "you know", false starts) and punctuation. Preserve the speaker's meaning. Do not exaggerate. Do not fabricate.
- Drop weak segments rather than padding. Returning fewer sub-quotes is better than returning weak ones.
- Add a one-sentence "why it matters" note for each sub-quote, framed for HoopsHype Rumors readers (NBA-savvy, want news value).

Return ONLY valid JSON, no surrounding text or markdown fences. The output is a JSON ARRAY at the top level (not an object):

[
  {
    "speaker": "string",
    "timestamp": "MM:SS",
    "topic_tags": ["trades"],
    "quote": "cleaned quote text",
    "why_it_matters": "one sentence"
  }
]"""

MAX_QUOTES_AFTER_SPLIT = 15
SPLIT_WORKERS = 4   # concurrent splitter calls within a single video
VIDEO_WORKERS = 3   # concurrent process_video calls across the queue
PER_VIDEO_TIMEOUT_SECS = 25 * 60      # hard upper bound per video
SCRIPT_TIMEOUT_SECS = 4 * 60 * 60     # hard upper bound for the whole run

# Substrings (uppercased) in an exception's str() that count as transient
# network/server errors and should be retried with the standard backoff.
_TRANSIENT_MESSAGE_PATTERNS = (
    "UNAVAILABLE",
    "RESOURCE_EXHAUSTED",
    "INTERNAL",
    "SERVER DISCONNECTED",
    "CONNECTION RESET",
    "CONNECTION ABORTED",
    "READ TIMED OUT",
    "REMOTE END CLOSED CONNECTION",
    "READERROR",
    "REMOTEPROTOCOLERROR",
    "CONNECTERROR",
    "READTIMEOUT",
    "CONNECTTIMEOUT",
)


# --------------------------------------------------------------------------- #
# Small utilities
# --------------------------------------------------------------------------- #

def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def timestamp_to_seconds(ts: str) -> int:
    parts = (ts or "").strip().split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return 0
    if len(nums) == 3:
        return nums[0] * 3600 + nums[1] * 60 + nums[2]
    if len(nums) == 2:
        return nums[0] * 60 + nums[1]
    return nums[0] if nums else 0


def video_id_from_url(url: str) -> str:
    m = re.search(r"(?:v=|youtu\.be/|/shorts/|/embed/)([A-Za-z0-9_-]{11})", url or "")
    return m.group(1) if m else ""


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def title_matches_skip_keyword(title: str, keywords: list) -> str:
    low = (title or "").lower()
    for kw in keywords:
        if kw.lower() in low:
            return kw
    return ""


# --------------------------------------------------------------------------- #
# State: filesystem-as-database
# --------------------------------------------------------------------------- #

def find_existing_artifact(video_id: str) -> Path | None:
    """Return the on-disk path that marks this video as already-processed, or None.

    The disk is the only source of truth — output/index.md and any other
    accumulated state are ignored. A video counts as processed only when one
    of these is observably true:

      - some output/<date>/<video_id>.md AND output/<date>/<video_id>.json
        both exist as regular files with size > 0 (a successful prior run
        for that date), OR
      - some output/<date>/<video_id>.SKIPPED-too-long exists, OR
      - some output/<date>/<video_id>.FAILED.txt exists.

    Date folders are scanned newest-first so the most recent prior outcome
    wins when a video has artifacts in more than one date directory.

    Returns the full Path that triggered the match so the caller can log it.
    Crucially, .md.tmp / .json.tmp partial files DO NOT count — they signal
    an interrupted run that should be retried. Empty .md or .json files DO
    NOT count either, for the same reason.
    """
    if not OUTPUT_DIR.exists():
        return None
    for day_dir in sorted(
        (p for p in OUTPUT_DIR.iterdir() if p.is_dir() and p.name != "digest"),
        reverse=True,
    ):
        md = day_dir / f"{video_id}.md"
        js = day_dir / f"{video_id}.json"
        try:
            if (
                md.is_file()
                and js.is_file()
                and md.stat().st_size > 0
                and js.stat().st_size > 0
            ):
                return md
        except OSError:
            pass
        for marker_name in (f"{video_id}.SKIPPED-too-long", f"{video_id}.FAILED.txt"):
            marker = day_dir / marker_name
            if marker.is_file():
                return marker
    return None


# --------------------------------------------------------------------------- #
# YouTube Data API v3
# --------------------------------------------------------------------------- #

class QuotaExceeded(Exception):
    """Raised when a YouTube Data API call returns 403 quotaExceeded."""


def youtube_api_get(endpoint: str, params: dict, api_key: str) -> dict:
    """GET against the YouTube Data API. Raises QuotaExceeded on quota errors."""
    full_params = {**params, "key": api_key}
    resp = requests.get(f"{YT_API_BASE}/{endpoint}", params=full_params, timeout=20)
    if resp.status_code == 403:
        try:
            body = resp.json()
        except ValueError:
            body = {}
        errors = body.get("error", {}).get("errors", [])
        if any(e.get("reason") in ("quotaExceeded", "rateLimitExceeded") for e in errors):
            raise QuotaExceeded(body.get("error", {}).get("message", "quotaExceeded"))
    resp.raise_for_status()
    return resp.json()


def get_uploads_playlist_id(channel_id: str, api_key: str) -> str:
    data = youtube_api_get("channels", {"part": "contentDetails", "id": channel_id}, api_key)
    items = data.get("items", [])
    if not items:
        return ""
    return items[0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads", "")


def list_recent_video_ids(playlist_id: str, api_key: str, max_results: int = 10) -> list:
    data = youtube_api_get(
        "playlistItems",
        {"part": "contentDetails", "playlistId": playlist_id, "maxResults": max_results},
        api_key,
    )
    out = []
    for item in data.get("items", []):
        vid = item.get("contentDetails", {}).get("videoId")
        if vid:
            out.append(vid)
    return out


def parse_iso8601_duration(s: str) -> int:
    """Convert YouTube's PT1H23M45S durations to seconds. Returns 0 on parse failure."""
    if not s:
        return 0
    m = re.match(
        r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$",
        s,
    )
    if not m:
        return 0
    days, hours, mins, secs = (int(x) if x else 0 for x in m.groups())
    return days * 86400 + hours * 3600 + mins * 60 + secs


def hydrate_video_metadata(video_ids: list, api_key: str) -> dict:
    """Batch-call videos.list (50 IDs per call). Returns {id: {title, duration}}."""
    out = {}
    unique_ids = list(dict.fromkeys(video_ids))  # de-dupe, preserve order
    for i in range(0, len(unique_ids), 50):
        chunk = unique_ids[i:i + 50]
        data = youtube_api_get(
            "videos",
            {"part": "contentDetails,snippet", "id": ",".join(chunk)},
            api_key,
        )
        for item in data.get("items", []):
            snippet = item.get("snippet", {}) or {}
            details = item.get("contentDetails", {}) or {}
            out[item.get("id", "")] = {
                "title": snippet.get("title", ""),
                "channel_title": snippet.get("channelTitle", ""),
                "duration": parse_iso8601_duration(details.get("duration", "")),
                "published_at": snippet.get("publishedAt", ""),
            }
    return out


# --------------------------------------------------------------------------- #
# Gemini
# --------------------------------------------------------------------------- #

def call_gemini(client, url: str) -> tuple[str, object]:
    """Send the video to Gemini and return (raw_text, usage_metadata).

    Parsing is done by the caller so the raw text is available for
    error-archiving even when JSON parsing fails.
    """
    response = client.models.generate_content(
        model=MODEL,
        contents=[
            types.Part.from_uri(file_uri=url, mime_type="video/mp4"),
            PROMPT,
        ],
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.3,
            media_resolution=types.MediaResolution.MEDIA_RESOLUTION_LOW,
        ),
    )
    return (response.text or ""), response.usage_metadata


def gemini_error_status(exc: Exception) -> int | None:
    code = getattr(exc, "code", None) or getattr(exc, "status_code", None)
    if isinstance(code, int):
        return code
    msg = str(exc)
    m = re.search(r"\b(4\d\d|5\d\d)\b", msg)
    return int(m.group(1)) if m else None


def is_token_limit_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "token" in msg and ("limit" in msg or "exceed" in msg or "too" in msg)


def _is_transient_gemini_error(exc: Exception) -> bool:
    """True for retryable errors: HTTP 429/500/503, httpx network errors,
    and any message containing one of _TRANSIENT_MESSAGE_PATTERNS.
    """
    status = gemini_error_status(exc)
    if status in (429, 500, 503):
        return True
    try:
        import httpx
        if isinstance(exc, (
            httpx.ReadError,
            httpx.RemoteProtocolError,
            httpx.ConnectError,
            httpx.ReadTimeout,
            httpx.ConnectTimeout,
        )):
            return True
    except ImportError:
        pass
    msg = str(exc).upper()
    for pattern in _TRANSIENT_MESSAGE_PATTERNS:
        if pattern in msg:
            return True
    return False


def call_gemini_with_retry(client, url: str) -> tuple[str, object]:
    """Wrap call_gemini with the same 5s/15s/45s/120s/300s retry policy as the splitter.

    Retries on 429, 500, 503, or messages containing UNAVAILABLE /
    RESOURCE_EXHAUSTED / INTERNAL. Non-retryable errors (400/token-limit,
    other 4xx) propagate immediately so the caller can map them to
    too-long or failed-other.
    """
    backoffs = [5, 15, 45, 120, 300]
    for attempt, sleep_s in enumerate(backoffs, start=1):
        try:
            return call_gemini(client, url)
        except Exception as e:
            if not _is_transient_gemini_error(e):
                raise
            if attempt < len(backoffs):
                log(f"  [main] transient error on attempt {attempt}/5, sleeping {sleep_s}s: {e}")
                time.sleep(sleep_s)
            else:
                log(f"  [main] giving up after 5 attempts: {e}")
                raise


# --------------------------------------------------------------------------- #
# Post-processing: split oversized quotes via a second, text-only Gemini call
# --------------------------------------------------------------------------- #

def _word_count(text: str) -> int:
    return len((text or "").split())


def _needs_split(quote: dict) -> bool:
    if _word_count(quote.get("quote", "")) > 220:
        return True
    if len(quote.get("topic_tags") or []) > 3:
        return True
    return False


def split_quote(client, quote: dict) -> tuple[list, str]:
    """Run a text-only Gemini call to split one oversized quote.

    Returns (sub_quotes, status) where status is:
      "split"     — splitter produced multiple sub-quotes
      "unchanged" — splitter ran but returned 0/1 usable sub-quotes,
                    or its response was unparseable
      "failed"    — API error: either a non-retryable 4xx, or transient
                    503/429 that didn't recover within 5 attempts

    On any non-"split" outcome, sub_quotes is [quote] so the caller can
    extend its list verbatim and the original quote survives.
    """
    speaker = quote.get("speaker") or "Unknown speaker"
    timestamp = quote.get("timestamp") or "0:00"
    text = quote.get("quote", "")
    user_text = (
        f"Speaker: {speaker}\n"
        f"Timestamp: {timestamp}\n"
        f"Original quote ({_word_count(text)} words):\n\n{text}"
    )

    backoffs = [5, 15, 45, 120, 300]
    response = None
    for attempt, sleep_s in enumerate(backoffs, start=1):
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=[SPLIT_PROMPT, user_text],
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.3,
                ),
            )
            break
        except Exception as e:
            if not _is_transient_gemini_error(e):
                log(f"  [split] non-retryable error, keeping original: {e}")
                return [quote], "failed"
            if attempt < len(backoffs):
                log(f"  [split] transient error on attempt {attempt}/5, sleeping {sleep_s}s: {e}")
                time.sleep(sleep_s)
            else:
                log(f"  [split] giving up after 5 attempts: {e}")
                return [quote], "failed"

    try:
        parsed = json.loads(response.text or "")
    except Exception as e:
        log(f"  [split] malformed JSON, keeping original: {e}")
        return [quote], "unchanged"

    if isinstance(parsed, dict):
        for key in ("quotes", "sub_quotes", "results"):
            if isinstance(parsed.get(key), list):
                parsed = parsed[key]
                break
    if not isinstance(parsed, list) or not parsed:
        log("  [split] returned no usable sub-quotes; keeping original")
        return [quote], "unchanged"

    out = []
    for sub in parsed:
        if not isinstance(sub, dict) or not sub.get("quote"):
            continue
        out.append({
            "speaker": sub.get("speaker") or speaker,
            "timestamp": sub.get("timestamp") or timestamp,
            "topic_tags": sub.get("topic_tags") or quote.get("topic_tags", []),
            "quote": sub["quote"],
            "why_it_matters": sub.get("why_it_matters", quote.get("why_it_matters", "")),
        })
    if len(out) > 1:
        return out, "split"
    if len(out) == 1:
        return out, "unchanged"
    return [quote], "unchanged"


def post_process_quotes(client, quotes: list) -> list:
    """Split oversized quotes (in parallel), then re-rank and cap.

    Splitter calls fan out across SPLIT_WORKERS threads. The final list is
    assembled in the original input order so ranks remain stable, then
    re-numbered 1..N and capped at MAX_QUOTES_AFTER_SPLIT.
    """
    quotes = quotes or []
    counts = {"split": 0, "unchanged": 0, "failed": 0}
    results: list = [None] * len(quotes)

    split_jobs = []
    for i, q in enumerate(quotes):
        if _needs_split(q):
            split_jobs.append((i, q))
        else:
            results[i] = [q]

    if split_jobs:
        with ThreadPoolExecutor(max_workers=SPLIT_WORKERS) as ex:
            future_to_job = {
                ex.submit(split_quote, client, q): (i, q) for i, q in split_jobs
            }
            for fut in as_completed(future_to_job):
                i, q = future_to_job[fut]
                try:
                    subs, status = fut.result()
                except Exception as e:
                    log(f"  [split] worker raised {e!r}; keeping original")
                    subs, status = [q], "failed"
                counts[status] += 1
                if status == "split":
                    wc = _word_count(q.get("quote", ""))
                    tc = len(q.get("topic_tags") or [])
                    log(f"  [split] quote {i + 1} ({wc} words, {tc} tags) -> {len(subs)} sub-quotes")
                results[i] = subs

    out = []
    for r in results:
        out.extend(r if r is not None else [])

    if sum(counts.values()):
        log(
            f"  [split] summary: {counts['split']} split, "
            f"{counts['unchanged']} unchanged, "
            f"{counts['failed']} failed after retries"
        )

    if len(out) > MAX_QUOTES_AFTER_SPLIT:
        log(f"  [split] capping {len(out)} quotes at {MAX_QUOTES_AFTER_SPLIT}")
        out = out[:MAX_QUOTES_AFTER_SPLIT]

    for i, q in enumerate(out):
        q["rank"] = i + 1
    return out


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #

def to_markdown(url: str, channel_name: str, data: dict) -> str:
    vid = video_id_from_url(url)
    lines = [
        f"# {data.get('video_title_guess', 'NBA quotes')}",
        "",
        f"Channel: {channel_name}",
        "",
        f"Source: {url}",
        "",
    ]
    if data.get("speakers_seen"):
        lines.append(f"_Speakers identified: {', '.join(data['speakers_seen'])}_")
        lines.append("")
    for q in data.get("quotes", []):
        secs = timestamp_to_seconds(q.get("timestamp", "0:00"))
        ts_link = f"https://www.youtube.com/watch?v={vid}&t={secs}s" if vid else url
        topics = ", ".join(q.get("topic_tags", []))
        lines.append(f"**{q.get('rank', '?')}. {q.get('speaker', 'Unknown speaker')}, on {topics}**")
        lines.append(f"[{q.get('timestamp', '?')}]({ts_link})")
        lines.append("")
        lines.append(f"\"{q.get('quote', '')}\"")
        lines.append("")
        lines.append(f"_Why it matters:_ {q.get('why_it_matters', '')}")
        lines.append("")
    return "\n".join(lines)


def write_outputs(video: dict, channel_name: str, data: dict, raw_text: str) -> Path:
    """Write <video_id>.md and <video_id>.json atomically.

    Both files are written first to a sibling .tmp path and then renamed onto
    the canonical name, so a crash mid-write can never leave an empty .md
    visible at the published path. An empty rendered markdown is treated as a
    bug and raises before any file is opened.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day_dir = OUTPUT_DIR / today
    day_dir.mkdir(parents=True, exist_ok=True)
    md_path = day_dir / f"{video['video_id']}.md"
    json_path = day_dir / f"{video['video_id']}.json"

    md_content = to_markdown(video["url"], channel_name, data)
    if not md_content:
        raise ValueError(f"to_markdown produced empty content for {video['video_id']}")
    json_content = raw_text or ""

    md_tmp = md_path.with_name(md_path.name + ".tmp")
    json_tmp = json_path.with_name(json_path.name + ".tmp")
    try:
        md_tmp.write_text(md_content, encoding="utf-8")
        json_tmp.write_text(json_content, encoding="utf-8")
        md_tmp.replace(md_path)
        json_tmp.replace(json_path)
    except Exception:
        for tmp in (md_tmp, json_tmp):
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
        raise
    return md_path


def regenerate_index() -> None:
    """Walk output/ and write a chronological index.md at the root."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for day_dir in sorted([p for p in OUTPUT_DIR.iterdir() if p.is_dir() and p.name != "digest"], reverse=True):
        for md_path in sorted(day_dir.glob("*.md")):
            video_id = md_path.stem
            try:
                first_line = md_path.read_text(encoding="utf-8").splitlines()[0]
                title = first_line.lstrip("# ").strip() or video_id
            except Exception:
                title = video_id
            rel = f"{day_dir.name}/{md_path.name}"
            rows.append((day_dir.name, title, rel))
    lines = ["# HoopsHype YouTube quotes — index", ""]
    current_day = None
    for day, title, rel in rows:
        if day != current_day:
            lines.append(f"## {day}")
            lines.append("")
            current_day = day
        lines.append(f"- [{title}]({rel})")
    if not rows:
        lines.append("_No videos processed yet._")
    (OUTPUT_DIR / "index.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _process_one_video(client, channel, video, today, lock, summary, processed_items) -> None:
    """Worker: run one video through Gemini and update shared state under lock.

    Any unhandled exception is caught and the full traceback is logged so a
    silent failure can never masquerade as a successful run. After a status
    of "ok" the on-disk .md is sanity-checked and the status is downgraded
    to "failed-other" if the file is missing or zero bytes.
    """
    name = channel.get("name", "?")
    video_id = video["video_id"]
    log(f"  -> {video_id} [{name}]: {video['title'][:80]}")

    # Run process_video on a dedicated single-thread executor so we can
    # walk away after PER_VIDEO_TIMEOUT_SECS if the worker hangs (e.g.
    # on a TCP read that never returns). Python can't truly cancel a
    # running thread; we accept that the abandoned thread keeps running
    # in the background until the runner reaps it.
    inner_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"vid-{video_id}")
    future = inner_pool.submit(process_video, client, video, name)
    try:
        try:
            status, data = future.result(timeout=PER_VIDEO_TIMEOUT_SECS)
        except FuturesTimeoutError:
            log(f"  [timeout] video {video_id} exceeded {PER_VIDEO_TIMEOUT_SECS // 60}min total, abandoning")
            status, data = "failed-other", None
        except Exception as e:
            log(f"  unexpected exception processing {video_id}: {e}")
            for line in traceback.format_exc().rstrip().splitlines():
                log(f"    {line}")
            status, data = "failed-other", None
    finally:
        inner_pool.shutdown(wait=False)

    if status == "ok":
        md_path = OUTPUT_DIR / today / f"{video_id}.md"
        try:
            size = md_path.stat().st_size
        except FileNotFoundError:
            size = -1
        if size <= 0:
            log(
                f"  [sanity] {video_id}.md is "
                f"{'missing' if size < 0 else 'zero bytes'} "
                "after process_video returned ok; downgrading to failed-other"
            )
            status, data = "failed-other", None

    log(f"     status [{video_id}]: {status}")
    with lock:
        summary[status] = summary.get(status, 0) + 1
        if status == "ok" and data:
            quotes = data.get("quotes") or []
            top = quotes[0] if quotes else {}
            processed_items.append({
                "video_id": video_id,
                "title": data.get("video_title_guess") or video.get("title") or video_id,
                "channel": name,
                "top_quote": top.get("quote", ""),
                "speaker": top.get("speaker", ""),
                "date": today,
            })


# --------------------------------------------------------------------------- #
# Main pipeline
# --------------------------------------------------------------------------- #

def filter_video(video: dict, channel: dict, config: dict) -> str:
    """Return '' if the video should be processed, otherwise a reason string."""
    existing = find_existing_artifact(video["video_id"])
    if existing:
        try:
            existing_str = str(existing.resolve())
        except OSError:
            existing_str = str(existing)
        return f"already processed (matched on disk: {existing_str})"

    if channel.get("bypass_filters"):
        return ""

    kw = title_matches_skip_keyword(video["title"], config.get("skip_title_keywords", []))
    if kw:
        return f"title contains '{kw}'"

    duration = video.get("duration", 0)
    min_seconds = int(config.get("min_duration_minutes", 15)) * 60
    if duration < min_seconds:
        return f"duration {duration}s below minimum"

    return ""


def process_video(client, video: dict, channel_name: str) -> tuple[str, dict | None]:
    """Run Gemini against one video, write outputs.

    Returns (status, data) where data is the parsed Gemini response on success
    and None otherwise.
    """
    url = video["url"]
    video_id = video["video_id"]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day_dir = OUTPUT_DIR / today
    day_dir.mkdir(parents=True, exist_ok=True)

    attempts_parse = 0
    last_raw = ""

    while True:
        try:
            raw_text, _usage = call_gemini_with_retry(client, url)
        except Exception as e:
            status = gemini_error_status(e)
            if status == 400 or is_token_limit_error(e):
                log(f"  token-limit / 400 on {video_id}: {e}")
                (day_dir / f"{video_id}.SKIPPED-too-long").write_text("", encoding="utf-8")
                return "too-long", None
            log(f"  unexpected Gemini error on {video_id}: {e}")
            (day_dir / f"{video_id}.FAILED.txt").write_text(str(e), encoding="utf-8")
            return "failed-other", None

        last_raw = raw_text
        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError as e:
            attempts_parse += 1
            log(f"  malformed JSON (attempt {attempts_parse}): {e}")
            if attempts_parse >= 2:
                (day_dir / f"{video_id}.FAILED.txt").write_text(
                    last_raw or str(e), encoding="utf-8"
                )
                return "failed-json", None
            continue

        data["quotes"] = post_process_quotes(client, data.get("quotes") or [])
        write_outputs(video, channel_name, data, raw_text)
        return "ok", data


def _install_script_timeout() -> None:
    """Schedule a SIGALRM after SCRIPT_TIMEOUT_SECS that force-exits the
    process. SIGALRM is Unix-only; on Windows this is a no-op so local
    test runs aren't affected. On Linux (the GitHub Actions runner) the
    handler runs between Python bytecodes / on syscall return and we
    use os._exit because ThreadPoolExecutor workers can be stuck on
    blocking I/O and would otherwise block clean interpreter shutdown.
    """
    if not hasattr(signal, "SIGALRM"):
        log("signal.SIGALRM not available on this platform; script-level timeout disabled")
        return

    def _handler(signum, frame):
        log(
            f"[timeout] script-level {SCRIPT_TIMEOUT_SECS // 3600}h budget exceeded, "
            "exiting with partial output"
        )
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass
        os._exit(3)

    signal.signal(signal.SIGALRM, _handler)
    signal.alarm(SCRIPT_TIMEOUT_SECS)


def main() -> int:
    _install_script_timeout()
    gemini_key = os.getenv("GEMINI_API_KEY")
    yt_key = os.getenv("YOUTUBE_API_KEY")
    missing = [name for name, val in (("GEMINI_API_KEY", gemini_key), ("YOUTUBE_API_KEY", yt_key)) if not val]
    if missing:
        log(f"ERROR: missing env vars: {', '.join(missing)}. Copy .env.example to .env and fill them in.")
        return 1

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    config = load_config()
    channels = [c for c in config.get("channels", []) if c.get("active", True)]
    if not channels:
        log("No active channels in channels.json. Add at least one and re-run.")
        return 0

    max_total = int(config.get("max_videos_per_run", 15))
    max_per_channel = int(config.get("max_videos_per_channel_per_run", 3))

    # Step 1: discover candidate video IDs per channel.
    log(f"Polling {len(channels)} channel(s) via YouTube Data API...")
    candidates = []  # list of (channel, video_id)
    try:
        for channel in channels:
            name = channel.get("name", channel.get("channel_id", "?"))
            cid = channel.get("channel_id", "")
            if not cid:
                log(f"  [{name}] no channel_id, skipping")
                continue
            try:
                uploads_id = get_uploads_playlist_id(cid, yt_key)
            except QuotaExceeded:
                raise
            except Exception as e:
                log(f"  [{name}] channels.list failed: {e}")
                continue
            if not uploads_id:
                log(f"  [{name}] no uploads playlist found (channel_id may be wrong)")
                continue
            try:
                video_ids = list_recent_video_ids(uploads_id, yt_key, max_results=10)
            except QuotaExceeded:
                raise
            except Exception as e:
                log(f"  [{name}] playlistItems.list failed: {e}")
                continue
            log(f"  [{name}] found {len(video_ids)} recent video(s)")
            for vid in video_ids:
                candidates.append((channel, vid))

        if not candidates:
            log("No candidate videos discovered.")
            regenerate_index()
            slack_notify.post_no_new_videos(today)
            return 0

        # Step 2: hydrate metadata in batches of 50.
        all_ids = [vid for _, vid in candidates]
        log(f"Fetching metadata for {len(set(all_ids))} unique video(s)...")
        meta = hydrate_video_metadata(all_ids, yt_key)
    except QuotaExceeded as e:
        log(f"ERROR: YouTube Data API quota exceeded: {e}")
        log("Daily quota resets at midnight Pacific Time. Increase quota or wait.")
        return 2

    # Step 3: filter and queue.
    queued = []
    per_channel_count = {}
    for channel, vid in candidates:
        name = channel.get("name", "?")
        m = meta.get(vid)
        if not m:
            log(f"  [{name}] skip {vid}: no metadata returned")
            continue
        video = {
            "video_id": vid,
            "url": WATCH_URL_TEMPLATE.format(video_id=vid),
            "title": m["title"],
            "duration": m["duration"],
            "published": m["published_at"],
        }
        cap_key = channel.get("channel_id", name)
        if per_channel_count.get(cap_key, 0) >= max_per_channel:
            continue
        reason = filter_video(video, channel, config)
        if reason:
            log(f"  [{name}] skip {vid} ({video['title'][:60]}): {reason}")
            continue
        queued.append((channel, video))
        per_channel_count[cap_key] = per_channel_count.get(cap_key, 0) + 1

    if not queued:
        log("Nothing new to process.")
        regenerate_index()
        slack_notify.post_no_new_videos(today)
        return 0

    if len(queued) > max_total:
        log(f"Capping queue from {len(queued)} to max_videos_per_run={max_total}")
        queued = queued[:max_total]

    # Step 4: process with Gemini, up to VIDEO_WORKERS in parallel.
    # google-genai's Client wraps an httpx transport that's safe for concurrent
    # use across threads, so one Client is shared by all workers.
    client = genai.Client(api_key=gemini_key)
    log(f"Processing {len(queued)} video(s) with Gemini (up to {VIDEO_WORKERS} in parallel)...")
    summary = {"ok": 0, "too-long": 0, "failed-json": 0, "failed-other": 0}
    processed_items: list = []
    state_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=VIDEO_WORKERS) as ex:
        futures = [
            ex.submit(_process_one_video, client, channel, video, today, state_lock, summary, processed_items)
            for channel, video in queued
        ]
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                log(f"  unhandled worker exception: {e}")

    regenerate_index()

    if processed_items:
        slack_notify.post_digest(processed_items, today)
    else:
        slack_notify.post_no_new_videos(today)

    log(f"Done. {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

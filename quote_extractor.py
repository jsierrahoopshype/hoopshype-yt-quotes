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
from datetime import datetime, timedelta, timezone
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

PROMPT = """You are watching an NBA YouTube show. Extract the editorially strongest quotes about NBA trades, free agency, player movement, team dynamics, front office, contracts, player legacy, coaching, or playoff issues.

Hard rules — these are limits, not targets:

- LENGTH: Each quote MUST be between 60 and 220 words after cleanup. Quotes over 220 words must be split into separate ranked quotes or shortened. Do not exceed 220 words under any circumstance.
- PLAYER NAMES: Use standard NBA reporting spellings for player and team names. Examples: Mikal Bridges (not Michael), Karl-Anthony Towns or KAT (not Cat), Scottie Barnes (not Burns), Jrue Holiday, Donovan Mitchell, Mikael Pereira, Tyrese Maxey, Cade Cunningham, Jalen Brunson, Jaylen Brown, Jayson Tatum. Apply this to all player names across the league.
- ONE TOPIC PER QUOTE: A single quote covers a single subject — one player, one team, one story, one argument. If the speaker pivots to a new player, team, story, or argument, that is a new quote with its own rank and timestamp. Do not merge two topics into one quote even when they are spoken back-to-back.
- CONTIGUOUS QUOTES ONLY: Each quote must be a single uninterrupted span of speech from one speaker (or, for dialogue, from two speakers exchanging contiguously). Do NOT join non-adjacent passages. Do NOT skip middle content and rejoin opening and closing fragments. Do NOT condense a long monologue by cutting the middle. If a passage is too long to quote in full, return a shorter contiguous excerpt of it (start at point X, end at point Y, both inside the same continuous speech segment) — never fabricate continuity by stitching separated text together. For multi-speaker quotes (text_blocks), each speaker's contribution within their block must also be contiguous — do not stitch a speaker's earlier and later statements into one block. Example —
  BAD (stitches non-adjacent passages from the same long monologue): "I think the Knicks have a real shot this year because Brunson is just on another level... and that's why I'd take them over the Cavs in a seven-game series."
  (Between those two sentences the speaker actually talked for 3 minutes about Mikal Bridges, Karl-Anthony Towns, and the coaching staff — none of which appears in the quote above.)
  GOOD (one short contiguous span): "I think the Knicks have a real shot this year because Brunson is just on another level. Look at what he did in Game 5 — 41 points, the closing stretch, the way he got to the rim. That's the kind of run no one was projecting from him this season."
- MONOLOGUES: When a speaker delivers a 3-5 minute monologue covering several subjects (e.g. a series recap, a coaching firing, and a contract take all in one breath, common on NBA podcasts), do NOT include the full monologue. Extract the strongest 1-2 standalone takes from it as separate quotes, each scoped to one subject AND contiguous within that subject.

How many quotes to return (scale to the video's length and substance — do NOT pad):

- Under 5 min:       0 to 2 quotes. If nothing is editorially worth pulling, return an empty array.
- 5 to 15 min:       3 to 6 quotes.
- 15 to 30 min:      6 to 10 quotes.
- 30 to 60 min:      9 to 15 quotes.
- 60 to 90 min:      12 to 18 quotes.
- 90+ min:           15 to 20 quotes.

Cap at 20 in all cases. An empty array is acceptable if the video has nothing worth quoting. Better to return 5 strong quotes than 12 mediocre ones.

Editorial rules:

- Identify the speaker by name when shown on screen, named in chyrons, named in the video title, or clearly addressed by another speaker. Otherwise return an empty string "" for the speaker. Never invent or guess a speaker name. Never use descriptors like "man with beard" or "guy in red hoodie" — that is not a speaker identification. Never use the literal value "Unknown" or "Unknown speaker" — leave the field empty.
- MULTI-SPEAKER QUOTES: Use the "text_blocks" field WHENEVER two or more identified speakers each contribute more than about 5 words to the same subject within the same minute of audio. This threshold is low on purpose — short back-and-forth exchanges should render as dialogue, not as a single squished block. Both speakers count as long as their contributions are substantive (an attributed setup question by a host like "What did you think of that play?" followed by a substantive answer DOES qualify). Record each contribution as a paragraph in chronological order; the top-level "speaker" field is the speaker with the longest contribution; the "speakers" array lists all contributing speakers in order; "quote" is left empty. If the speakers shift to a different player/team/story, split into separate quotes per the ONE TOPIC PER QUOTE rule — do not bundle multiple subjects under one multi-speaker quote. Example of a short exchange that MUST use text_blocks:
  Speaker A: "Did you see that no-call on Brunson at the end?"
  Speaker B: "Tony Brothers blew it. That's a foul every day of the week. They make the free throw, game over."
  -> render as two text_blocks, not a single combined quote.
- Provide a start timestamp in MM:SS or H:MM:SS format pointing at the moment the quote begins.
- For each quote, write a "summary_phrase": a 5-12 word headline that describes the SUBSTANCE of what the speaker is saying, in concrete terms. Examples:
    - "why Embiid can't have a 'legacy game' in round 1"
    - "Max Strus's defensive transformation in Cleveland"
    - "DeAndre Ayton on his last straw in Portland"
  Do NOT use generic category labels like "player legacy", "team dynamics", "trades", "coaching", "playoffs". The summary_phrase must name a specific player, team, story, or argument — not a category.
- For each quote, list every player, coach, or front-office name mentioned IN THE QUOTE TEXT in the "names_mentioned" array. Use the exact substring spelling that appears in your cleaned quote text. Do not include team names, hosts who don't appear in the quote body, or the speaker's own name unless they refer to themselves in third person inside the quote.
- For each quote, write an "excerpt" field: a verbatim string of UP TO 18 WORDS pulled directly from the quote body. Pick the punchiest / most quotable single line — the standalone sentence that would work as a pull-quote in a headline. The excerpt MUST appear word-for-word in the quote text (or, for multi-speaker quotes, in any one speaker's text_blocks contribution — pick the most quotable line, not necessarily the longest speaker's). Do not paraphrase, condense, or add punctuation that isn't there. If no obvious 18-word pull-quote exists (single-sentence quotes, all rambling, etc.), leave excerpt as an empty string "".
- FILLER CLEANUP: Strip conversational noise so the quote reads like lightly edited print. Remove "uh", "um", "er", "ah" and variants. Remove "you know" when used as filler (keep it when it's a real question or phrase). Remove "like" when used as filler or hedging (keep it as a real verb or comparison). Remove "I mean" when used to restart a thought. Remove "kind of" and "sort of" when used as filler. Collapse repeated words from false starts ("the the", "we we"). Resolve mid-sentence trailing-off restarts to the speaker's completed thought (e.g. "if he goes home, he's not -- he can be the guy" -> "if he goes home, he can be the guy"). Preserve the speaker's voice and emphasis. Do not paraphrase or change meaning. Example —
  Before: "If he leaves LA, uh, Cleveland. I think it's full circle, going home again, um, joining a team that, as we saw, uh, last night, um, once again, you know, obviously Donovan Mitchell going off..."
  After: "If he leaves LA, Cleveland. I think it's full circle, going home again, joining a team that, as we saw last night, once again, obviously Donovan Mitchell going off..."
- Preserve the speaker's meaning. Do not exaggerate their tone. Do not fabricate.
- Skip play-by-play recap, sponsor reads, intros, outros, generic opinions, and recycled talking points unless phrased forcefully.
- HALLUCINATION GUARD: Begin your JSON with a "video_title" field that ECHOES BACK EXACTLY the YouTube title supplied above (the line starting with "YouTube title:"). Preserve case, punctuation, and emoji. This is a sanity check — if the content of your analysis doesn't match the title we provided, the response will be rejected and the video will be retried.

Return ONLY valid JSON, no surrounding text or markdown fences:

{
  "video_title": "exact echo of the YouTube title supplied above",
  "speakers_seen": ["names you saw or heard, in order of appearance"],
  "quotes": [
    {
      "rank": 1,
      "speaker": "string (empty if not confidently identifiable; longest contributor for multi-speaker quotes)",
      "speakers": ["only populate for multi-speaker quotes; omit or leave empty otherwise"],
      "timestamp": "MM:SS",
      "summary_phrase": "5-12 word specific headline naming a player, team, or story",
      "names_mentioned": ["LeBron James", "Mikal Bridges"],
      "excerpt": "up to 18 words pulled verbatim from the quote body; empty string if no clean pull-quote",
      "quote": "cleaned quote text (leave empty when using text_blocks)",
      "text_blocks": [
        {"speaker": "X", "text": "X's contribution"},
        {"speaker": "Y", "text": "Y's contribution"}
      ]
    }
  ]
}"""


SPLIT_PROMPT = """You are editing a single quote from an NBA YouTube show that is too long or covers too many topics. Split it into 1 to 4 standalone sub-quotes.

Hard rules:

- Each sub-quote MUST be between 60 and 220 words after cleanup. Do not exceed 220 words under any circumstance.
- PLAYER NAMES: Use standard NBA reporting spellings for player and team names. Examples: Mikal Bridges (not Michael), Karl-Anthony Towns or KAT (not Cat), Scottie Barnes (not Burns), Jrue Holiday, Donovan Mitchell, Mikael Pereira, Tyrese Maxey, Cade Cunningham, Jalen Brunson, Jaylen Brown, Jayson Tatum. Apply this to all player names across the league.
- Each sub-quote covers a single subject — one player, one team, one story, one argument.
- CONTIGUOUS ONLY: Each sub-quote must be a single uninterrupted span of speech from the original. Do not stitch the opening and closing sentences of the original together while skipping middle content. If the substantive content for a subject is itself non-contiguous in the original, pick the strongest single contiguous span and drop the rest rather than fabricating continuity.
- Use the SAME timestamp as the original quote for every sub-quote. Use the SAME speaker (or speakers) unless the original was multi-speaker and a particular sub-quote only includes one of them.
- For each sub-quote, write a "summary_phrase": a 5-12 word headline naming the specific player, team, story, or argument. Do not use generic category labels.
- For each sub-quote, list every player, coach, or front-office name mentioned in the sub-quote text in "names_mentioned", using the exact substring spelling from your cleaned text.
- For each sub-quote, write an "excerpt" of up to 18 words pulled verbatim from the sub-quote body (or, for multi-speaker sub-quotes, from any speaker's contribution). Pick the punchiest standalone line. Empty string when no clean pull-quote exists.
- If a sub-quote is multi-speaker dialogue on the same subject, populate "text_blocks" with one entry per speaker contribution (in order) and leave "quote" empty. The "speaker" field becomes the longest contributor in that sub-quote; populate "speakers" with the list. Single-speaker sub-quotes use "quote" only.
- If the speaker is not confidently identifiable, leave "speaker" empty (do not write "Unknown").
- Clean only obvious filler ("um", "uh", "you know", false starts) and punctuation. Preserve the speaker's meaning. Do not exaggerate. Do not fabricate.
- Drop weak segments rather than padding. Returning fewer sub-quotes is better than returning weak ones.

Return ONLY valid JSON, no surrounding text or markdown fences. The output is a JSON ARRAY at the top level (not an object):

[
  {
    "speaker": "string",
    "speakers": ["only for multi-speaker sub-quotes"],
    "timestamp": "MM:SS",
    "summary_phrase": "5-12 word specific headline",
    "names_mentioned": ["LeBron James"],
    "excerpt": "up to 18 words pulled verbatim; empty string if no clean pull-quote",
    "quote": "cleaned quote text (empty if using text_blocks)",
    "text_blocks": [
      {"speaker": "X", "text": "X's contribution"},
      {"speaker": "Y", "text": "Y's contribution"}
    ]
  }
]"""

MAX_QUOTES_AFTER_SPLIT = 20

# Footer link appended to every published digest.md. Uses HTML so it opens
# in a new tab in both GitHub blob and GitHub Pages renderers, matching the
# style of the Source and timestamp links inside per-video markdown.
DIGEST_CLOSING_LINE = (
    '<a href="https://www.youtube.com/feed/subscriptions" target="_blank" '
    'rel="noopener">CHECK OTHER YOUTUBE PODCASTS HERE</a>'
)
SPLIT_WORKERS = 4   # concurrent splitter calls within a single video
VIDEO_WORKERS = 3   # concurrent process_video calls across the queue
PER_VIDEO_TIMEOUT_SECS = 25 * 60      # hard upper bound per video
SCRIPT_TIMEOUT_SECS = 4 * 60 * 60     # hard upper bound for the whole run
RECENT_DAYS = 14                      # videos older than this are skipped

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


def _extract_one_off_video_id(token: str) -> str:
    """Pull an 11-char YouTube video ID from a watch / youtu.be / shorts /
    embed URL, or from a bare 11-char ID. Returns '' on no match.
    """
    token = (token or "").strip()
    if not token:
        return ""
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", token):
        return token
    return video_id_from_url(token)


def _parse_extra_videos_env(raw: str) -> list:
    """Split EXTRA_VIDEOS on commas AND newlines, strip whitespace, drop
    empty tokens. Returns the list of raw input tokens for downstream
    parsing/error reporting.
    """
    if not raw:
        return []
    tokens = re.split(r"[,\n\r]+", raw)
    return [t.strip() for t in tokens if t.strip()]


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def title_matches_skip_keyword(title: str, keywords: list) -> str:
    low = (title or "").lower()
    for kw in keywords:
        if kw.lower() in low:
            return kw
    return ""


def _normalize_title(title: str) -> str:
    """Lowercase, strip punctuation/emoji/everything non-alphanumeric, collapse
    whitespace. Used for the hallucination guard's word-overlap comparison."""
    if not title:
        return ""
    lower = title.lower()
    stripped = re.sub(r"[^a-z0-9\s]+", " ", lower)
    return re.sub(r"\s+", " ", stripped).strip()


def _title_word_overlap(expected: str, got: str) -> float:
    """Fraction of expected's distinct words that also appear in got's words.
    Returns 1.0 when expected has no analyzable words (pure-emoji titles
    etc.), so the check doesn't false-positive on uncheckable inputs.
    """
    exp_words = set(_normalize_title(expected).split())
    if not exp_words:
        return 1.0
    got_words = set(_normalize_title(got).split())
    return len(exp_words & got_words) / len(exp_words)


HALLUCINATION_OVERLAP_THRESHOLD = 0.70  # strictly greater; 0.70 itself is rejected


RETRY_CAP = 2  # after this many cap-eligible failures, write FAILED.txt
# failure statuses that count toward the retry cap. failed-json is exempt:
# it always writes its own FAILED.txt with the raw response for debugging
# and shouldn't get tracked here.
_CAP_ELIGIBLE_FAILURES = frozenset({"failed-timeout", "failed-hallucination", "failed-other"})


def _attempts_path(day_dir: Path, video_id: str) -> Path:
    return day_dir / f"{video_id}.ATTEMPTS.txt"


def _read_attempts(day_dir: Path, video_id: str) -> int:
    try:
        return int(_attempts_path(day_dir, video_id).read_text(encoding="utf-8").strip() or "0")
    except (OSError, ValueError):
        return 0


def _record_failed_attempt(day_dir: Path, video_id: str, status: str) -> int:
    """Increment <video_id>.ATTEMPTS.txt and, if the cap has been reached,
    write <video_id>.FAILED.txt so find_existing_artifact stops re-queuing
    the video next cron. Returns the new attempt count.
    """
    if status not in _CAP_ELIGIBLE_FAILURES:
        return 0
    day_dir.mkdir(parents=True, exist_ok=True)
    attempts = _read_attempts(day_dir, video_id) + 1
    try:
        _attempts_path(day_dir, video_id).write_text(str(attempts), encoding="utf-8")
    except OSError:
        pass
    if attempts >= RETRY_CAP:
        failed_path = day_dir / f"{video_id}.FAILED.txt"
        if not failed_path.exists():
            try:
                failed_path.write_text(
                    f"persistent failure after {attempts} attempts; last status: {status}\n",
                    encoding="utf-8",
                )
            except OSError:
                pass
    return attempts


def _publish_date(video: dict) -> str:
    """Return the video's YYYY-MM-DD UTC publish date for routing output to
    output/<date>/. Falls back to today's UTC date if publishedAt is missing
    or unparseable so we never crash here.
    """
    iso = (video.get("published") or "").strip()
    if iso:
        try:
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


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
    """Batch-call videos.list (50 IDs per call). Returns {id: {title, duration}}.

    Each result is keyed by the API response's own item["id"] — never by
    request order — so reordering or missing items in the response can't
    misalign titles with the wrong video. Items without an "id" field are
    skipped with a log line.
    """
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
            item_id = (item.get("id") or "").strip()
            if not item_id:
                log(f"  [meta] WARN: videos.list returned an item with no id; skipping")
                continue
            snippet = item.get("snippet", {}) or {}
            details = item.get("contentDetails", {}) or {}
            out[item_id] = {
                "title": snippet.get("title", ""),
                "channel_title": snippet.get("channelTitle", ""),
                "duration": parse_iso8601_duration(details.get("duration", "")),
                "published_at": snippet.get("publishedAt", ""),
            }
    return out


# --------------------------------------------------------------------------- #
# Gemini
# --------------------------------------------------------------------------- #

def call_gemini(client, url: str, video_title: str = "", known_speakers: list | None = None) -> tuple[str, object]:
    """Send the video to Gemini and return (raw_text, usage_metadata).

    The actual YouTube title is prepended to the prompt so Gemini can echo
    it back in its JSON. The caller compares the echo against the expected
    title to catch hallucinated full-output responses.

    When the channel has a configured known_speakers list, a hint line is
    prepended to anchor recurring-host name spellings so Gemini doesn't
    substitute similar-sounding names (e.g. Allie Clifton -> Allie LaForce).
    """
    parts = [f"YouTube title: {video_title}"]
    if known_speakers:
        joined = ", ".join(known_speakers)
        parts.append(
            f"KNOWN HOSTS for this channel: {joined}. Other speakers may appear "
            "as guests, but when identifying the regular hosts, use these exact "
            "spellings. Do NOT substitute similar-sounding names (e.g., "
            "Allie LaForce, Allison Williams)."
        )
    prefixed_prompt = "\n\n".join(parts) + "\n\n" + PROMPT
    response = client.models.generate_content(
        model=MODEL,
        contents=[
            types.Part.from_uri(file_uri=url, mime_type="video/mp4"),
            prefixed_prompt,
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


def call_gemini_with_retry(client, url: str, video_title: str = "", known_speakers: list | None = None) -> tuple[str, object]:
    """Wrap call_gemini with the same 5s/15s/45s/120s/300s retry policy as the splitter.

    Retries on 429, 500, 503, or messages containing UNAVAILABLE /
    RESOURCE_EXHAUSTED / INTERNAL. Non-retryable errors (400/token-limit,
    other 4xx) propagate immediately so the caller can map them to
    too-long or failed-other.
    """
    backoffs = [5, 15, 45, 120, 300]
    for attempt, sleep_s in enumerate(backoffs, start=1):
        try:
            return call_gemini(client, url, video_title, known_speakers)
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


def _canonical_quote_text(quote: dict) -> str:
    """Return the full body of a quote regardless of single-speaker ("quote")
    or multi-speaker ("text_blocks") shape. Used for length, dedupe, and
    Slack top-quote extraction.
    """
    blocks = quote.get("text_blocks") or []
    if blocks:
        return " ".join(
            (b.get("text") or "").strip() for b in blocks if isinstance(b, dict)
        ).strip()
    return (quote.get("quote") or "").strip()


def _needs_split(quote: dict) -> bool:
    return _word_count(_canonical_quote_text(quote)) > 220


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
    speaker = quote.get("speaker") or ""
    timestamp = quote.get("timestamp") or "0:00"
    canonical = _canonical_quote_text(quote)
    blocks = quote.get("text_blocks") or []
    speakers_list = quote.get("speakers") or []
    if blocks:
        formatted_blocks = []
        for b in blocks:
            sp = (b.get("speaker") or "").strip()
            tx = (b.get("text") or "").strip()
            if sp:
                formatted_blocks.append(f"**{sp}:** \"{tx}\"")
            else:
                formatted_blocks.append(f'"{tx}"')
        body = "\n\n".join(formatted_blocks)
        speakers_line = f"Speakers: {', '.join(speakers_list or [b.get('speaker','') for b in blocks])}\n"
    else:
        body = canonical
        speakers_line = f"Speaker: {speaker}\n" if speaker else ""
    user_text = (
        f"{speakers_line}"
        f"Timestamp: {timestamp}\n"
        f"Original quote ({_word_count(canonical)} words):\n\n{body}"
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
        if not isinstance(sub, dict):
            continue
        sub_blocks = sub.get("text_blocks") or []
        sub_quote = sub.get("quote") or ""
        if not sub_blocks and not sub_quote.strip():
            continue
        sub_dict = {
            "speaker": sub.get("speaker") or speaker,
            "timestamp": sub.get("timestamp") or timestamp,
            "summary_phrase": sub.get("summary_phrase") or quote.get("summary_phrase", ""),
            "names_mentioned": sub.get("names_mentioned") or quote.get("names_mentioned", []),
            "excerpt": sub.get("excerpt") or "",
            "quote": sub_quote,
        }
        if sub_blocks:
            # Keep only well-formed blocks
            clean_blocks = [
                {"speaker": (b.get("speaker") or "").strip(), "text": (b.get("text") or "").strip()}
                for b in sub_blocks if isinstance(b, dict) and (b.get("text") or "").strip()
            ]
            if clean_blocks:
                sub_dict["text_blocks"] = clean_blocks
                sub_speakers = sub.get("speakers") or [b["speaker"] for b in clean_blocks if b["speaker"]]
                if sub_speakers:
                    sub_dict["speakers"] = sub_speakers
        out.append(sub_dict)
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
                    wc = _word_count(_canonical_quote_text(q))
                    log(f"  [split] quote {i + 1} ({wc} words) -> {len(subs)} sub-quotes")
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

    # Dedupe on the cleaned quote body only. Keep first occurrence. The
    # canonical text helper handles both single-speaker "quote" and
    # multi-speaker "text_blocks" shapes.
    seen_bodies = set()
    deduped = []
    for q in out:
        key = _canonical_quote_text(q)
        if key and key in seen_bodies:
            continue
        if key:
            seen_bodies.add(key)
        deduped.append(q)
    if len(deduped) != len(out):
        log(f"  [dedupe] removed {len(out) - len(deduped)} identical quote(s)")
    out = deduped

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
    title = data.get("video_title") or "NBA quotes"
    lines = [
        f"# {title} — *{channel_name}*",
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
        ts_label = q.get('timestamp', '?')
        rank = q.get('rank', '?')
        speaker = (q.get("speaker") or "").strip()
        summary = (q.get("summary_phrase") or "").strip()
        excerpt = (q.get("excerpt") or "").strip()
        if _is_unknown_speaker(speaker):
            speaker = ""
        # Header format: **N. Speaker — summary — "excerpt"** [timestamp](url)
        # Any of speaker/summary/excerpt may be empty; em-dashes join only
        # present pieces. Plain markdown link for the timestamp so GitHub's
        # renderer applies its own target=_blank rather than us emitting
        # HTML that the sanitizer strips.
        fragments = []
        if speaker:
            fragments.append(speaker)
        if summary:
            fragments.append(summary)
        if excerpt:
            fragments.append(f'"{excerpt}"')
        if fragments:
            inner = " — ".join(fragments)
            header = f"**{rank}. {inner}** [{ts_label}]({ts_link})"
        else:
            header = f"**{rank}.** [{ts_label}]({ts_link})"
        lines.append(header)
        lines.append("")
        names = q.get("names_mentioned") or []
        blocks = q.get("text_blocks") or []
        if blocks:
            for j, block in enumerate(blocks):
                block_speaker = (block.get("speaker") or "").strip()
                block_text = block.get("text") or ""
                bolded = _bold_names(block_text, names)
                if _is_unknown_speaker(block_speaker):
                    lines.append(f"\"{bolded}\"")
                else:
                    lines.append(f"**{block_speaker}:** \"{bolded}\"")
                if j < len(blocks) - 1:
                    lines.append("")  # blank line -> markdown paragraph break
        else:
            body = q.get("quote", "")
            bolded = _bold_names(body, names)
            lines.append(f"\"{bolded}\"")
        lines.append("")
    return "\n".join(lines)


def _is_unknown_speaker(speaker: str) -> bool:
    """True when the speaker field should be treated as no-attribution.

    Covers empty strings, the literal 'Unknown' / 'Unknown speaker', and
    variants Gemini sometimes returns even though the prompt forbids them
    ('Unknown speaker (man with beard)', 'Unknown host', etc.). The defensive
    check stays in place even after the prompt was updated to require an
    empty string for unidentified speakers.
    """
    if not speaker:
        return True
    lower = speaker.strip().lower()
    if lower.startswith("unknown"):
        return True
    return lower in ("speaker",)


def _bold_names(text: str, names: list) -> str:
    """Wrap every occurrence of each name in **bold** markers.

    Longest names first via a single regex pass so "LeBron James" is bolded
    as one unit instead of "**LeBron** James". Exact substring match,
    case-sensitive, matching the spec.
    """
    valid = [n for n in (names or []) if isinstance(n, str) and n.strip()]
    if not valid or not text:
        return text or ""
    sorted_names = sorted(set(valid), key=len, reverse=True)
    pattern = "|".join(re.escape(n) for n in sorted_names)
    return re.sub(pattern, lambda m: f"**{m.group(0)}**", text)


def write_outputs(video: dict, channel_name: str, data: dict, raw_text: str) -> Path:
    """Write <video_id>.md and <video_id>.json atomically.

    Both files are written first to a sibling .tmp path and then renamed onto
    the canonical name, so a crash mid-write can never leave an empty .md
    visible at the published path. An empty rendered markdown is treated as a
    bug and raises before any file is opened.
    """
    pub_date = _publish_date(video)
    day_dir = OUTPUT_DIR / pub_date
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


def write_daily_digest(date_str: str) -> Path | None:
    """Concatenate every per-video <video_id>.md in output/<date>/ into one
    output/<date>/digest.md and return its path. Returns None if there is no
    day folder or no per-video .md to include.

    Regenerates from scratch each call so the digest reflects ALL successful
    videos for that date across every run that day. The digest itself, plus
    .FAILED.txt and .SKIPPED-too-long marker files, are excluded from the
    concatenation. Per-video h1 headings are demoted to h2 so the document
    has a clean outline under the date-level h1.
    """
    day_dir = OUTPUT_DIR / date_str
    if not day_dir.is_dir():
        return None

    video_md_paths = sorted(
        p for p in day_dir.glob("*.md")
        if p.is_file() and p.name != "digest.md"
    )
    if not video_md_paths:
        return None

    parts = [f"# HoopsHype YT Quotes — {date_str}", ""]
    first = True
    for md_path in video_md_paths:
        try:
            content = md_path.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        if not content:
            continue
        # Item 6d: skip videos that processed cleanly but produced zero quotes.
        # Quote headers start with "**<n>. " in the rendered markdown.
        if not re.search(r'(?m)^\*\*\d+\.', content):
            continue
        if content.startswith("# "):
            content = "#" + content  # demote h1 to h2
        if not first:
            parts.append("---")
            parts.append("")
        first = False
        parts.append(content)
        parts.append("")

    if first:
        # Every per-video .md was quoteless. Nothing meaningful to publish.
        return None

    # Closing line, separated from the last video block by a horizontal rule.
    parts.append("---")
    parts.append("")
    parts.append(DIGEST_CLOSING_LINE)
    parts.append("")

    digest_path = day_dir / "digest.md"
    tmp = digest_path.with_name(digest_path.name + ".tmp")
    try:
        tmp.write_text("\n".join(parts), encoding="utf-8")
        tmp.replace(digest_path)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        raise
    return digest_path


def _process_one_video(client, channel, video, lock, summary, processed_items) -> None:
    """Worker: run one video through Gemini and update shared state under lock.

    Any unhandled exception is caught and the full traceback is logged so a
    silent failure can never masquerade as a successful run. After a status
    of "ok" the on-disk .md is sanity-checked and the status is downgraded
    to "failed-other" if the file is missing or zero bytes.

    Output files are routed by the video's publish date, not the run date,
    so a 4-day-old video processed today still lands under its own day's
    folder and digest.
    """
    name = channel.get("name", "?")
    video_id = video["video_id"]
    pub_date = _publish_date(video)
    day_dir = OUTPUT_DIR / pub_date
    log(f"  -> {video_id} [{name}]: {video['title'][:80]}")

    # Run process_video on a dedicated single-thread executor so we can
    # walk away after PER_VIDEO_TIMEOUT_SECS if the worker hangs (e.g.
    # on a TCP read that never returns). Python can't truly cancel a
    # running thread; we accept that the abandoned thread keeps running
    # in the background until the runner reaps it.
    inner_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"vid-{video_id}")
    known_speakers = channel.get("known_speakers") or []
    future = inner_pool.submit(process_video, client, video, name, known_speakers)
    try:
        try:
            status, data = future.result(timeout=PER_VIDEO_TIMEOUT_SECS)
        except FuturesTimeoutError:
            log(f"  [timeout] video {video_id} exceeded {PER_VIDEO_TIMEOUT_SECS // 60}min total, abandoning")
            status, data = "failed-timeout", None
        except Exception as e:
            log(f"  unexpected exception processing {video_id}: {e}")
            for line in traceback.format_exc().rstrip().splitlines():
                log(f"    {line}")
            status, data = "failed-other", None
    finally:
        inner_pool.shutdown(wait=False)

    if status == "ok":
        md_path = day_dir / f"{video_id}.md"
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

    # Retry-cap accounting. failed-json is intentionally excluded — it
    # writes its own FAILED.txt with raw content for debugging and we
    # want it to keep retrying without cap. Timeouts, hallucinations,
    # and generic failed-other all increment ATTEMPTS.txt; on the
    # RETRY_CAP-th cumulative failure a FAILED.txt marker is written
    # so the duplicate check stops re-queuing the video.
    if status in _CAP_ELIGIBLE_FAILURES:
        n = _record_failed_attempt(day_dir, video_id, status)
        log(f"  [attempts] {video_id} now at {n}/{RETRY_CAP} failed attempts")

    log(f"     status [{video_id}]: {status}")
    with lock:
        summary[status] = summary.get(status, 0) + 1
        if status == "ok" and data:
            quotes = data.get("quotes") or []
            top = quotes[0] if quotes else {}
            processed_items.append({
                "video_id": video_id,
                "title": data.get("video_title") or video.get("title") or video_id,
                "channel": name,
                "top_quote": _canonical_quote_text(top),
                "speaker": top.get("speaker", ""),
                "date": pub_date,
            })


# --------------------------------------------------------------------------- #
# Main pipeline
# --------------------------------------------------------------------------- #

def _age_skip_reason(published_iso: str) -> str:
    """Return a skip reason if a video was published more than RECENT_DAYS
    ago. Empty string when the video is recent OR when the timestamp can't
    be parsed (don't drop on bad metadata — let the rest of the pipeline
    decide).
    """
    if not published_iso:
        return ""
    try:
        pub_dt = datetime.fromisoformat(published_iso.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return ""
    age = datetime.now(timezone.utc) - pub_dt
    if age > timedelta(days=RECENT_DAYS):
        return f"published {age.days} days ago, older than {RECENT_DAYS}-day cutoff"
    return ""


def filter_video(video: dict, channel: dict, config: dict) -> str:
    """Return '' if the video should be processed, otherwise a reason string."""
    existing = find_existing_artifact(video["video_id"])
    if existing:
        try:
            existing_str = str(existing.resolve())
        except OSError:
            existing_str = str(existing)
        return f"already processed (matched on disk: {existing_str})"

    # Age cutoff applies to ALL channels, including bypass_filters ones,
    # for the same reason duration does (below) — rarely-posting channels
    # would otherwise stale-pollute the digest.
    age_reason = _age_skip_reason(video.get("published") or "")
    if age_reason:
        return age_reason

    # Duration cutoff also applies to ALL channels, including bypass_filters.
    # Previously bypass_filters short-circuited before this check, which let
    # a 26-second Short slip through on a bypass channel. Shorts have their
    # own video IDs and show up in uploads playlists — the duration filter
    # is the one defense against them.
    duration = video.get("duration", 0)
    try:
        duration = int(duration)
    except (TypeError, ValueError):
        duration = 0
    min_seconds = int(config.get("min_duration_minutes", 5)) * 60
    if duration < min_seconds:
        return f"duration {duration}s below {min_seconds}s minimum"

    if channel.get("bypass_filters"):
        log(f"  [accept] {video['video_id']} duration={duration}s (bypass channel)")
        return ""

    kw = title_matches_skip_keyword(video["title"], config.get("skip_title_keywords", []))
    if kw:
        return f"title contains '{kw}'"

    log(f"  [accept] {video['video_id']} duration={duration}s")
    return ""


def process_video(client, video: dict, channel_name: str, known_speakers: list | None = None) -> tuple[str, dict | None]:
    """Run Gemini against one video, write outputs.

    Returns (status, data) where data is the parsed Gemini response on success
    and None otherwise. known_speakers (optional) primes the prompt with
    the channel's recurring-host names so Gemini doesn't substitute
    similar-sounding ones.
    """
    url = video["url"]
    video_id = video["video_id"]
    pub_date = _publish_date(video)
    day_dir = OUTPUT_DIR / pub_date
    day_dir.mkdir(parents=True, exist_ok=True)

    attempts_parse = 0
    last_raw = ""

    expected_title = video.get("title") or ""
    known_speakers = list(known_speakers or [])
    if known_speakers:
        log(f"  [meta] {video_id} publish={pub_date} title={expected_title!r} known_hosts={known_speakers}")
    else:
        log(f"  [meta] {video_id} publish={pub_date} title={expected_title!r}")

    while True:
        try:
            raw_text, _usage = call_gemini_with_retry(client, url, expected_title, known_speakers)
        except Exception as e:
            status = gemini_error_status(e)
            if status == 400 or is_token_limit_error(e):
                log(f"  token-limit / 400 on {video_id}: {e}")
                (day_dir / f"{video_id}.SKIPPED-too-long").write_text("", encoding="utf-8")
                return "too-long", None
            # failed-other no longer writes FAILED.txt here — _process_one_video
            # increments the attempt counter and writes FAILED.txt only after
            # the retry cap is hit. This lets transient errors retry on the
            # next cron without a permanent marker after one bad outcome.
            log(f"  unexpected Gemini error on {video_id}: {e}")
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

        # Hallucination guard: confirm Gemini analyzed the video we sent by
        # checking that its echoed video_title overlaps the actual YouTube
        # title we provided. Skip writing any output on mismatch so the
        # video can be retried next cron.
        echoed_title = (data.get("video_title") or "").strip()
        overlap = _title_word_overlap(expected_title, echoed_title)
        if overlap <= HALLUCINATION_OVERLAP_THRESHOLD:
            log(
                f"  [hallucination] video {video_id} title mismatch "
                f"(overlap {overlap:.0%}): expected {expected_title!r}, "
                f"got {echoed_title!r}"
            )
            return "failed-hallucination", None

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

    # Step 2.5: one-off queue from EXTRA_VIDEOS env (workflow_dispatch input).
    # These bypass ALL standard filters — duration min, age cutoff, skip
    # keywords, and the already-processed check — because the user
    # explicitly requested them. The hallucination guard, splitter, retry
    # logic, and per-video timeout still apply normally.
    extra_queue = []
    extra_ids_set = set()
    extra_raw = os.getenv("EXTRA_VIDEOS", "")
    extra_tokens = _parse_extra_videos_env(extra_raw)
    if extra_tokens:
        log(f"One-off input: {len(extra_tokens)} token(s) from EXTRA_VIDEOS")
        seen = set()
        ids_to_fetch = []
        for token in extra_tokens:
            vid = _extract_one_off_video_id(token)
            if not vid:
                log(f"  [oneoff] skip {token!r}: could not parse video ID")
                continue
            if vid in seen:
                log(f"  [oneoff] skip {token!r}: duplicate of earlier one-off ID")
                continue
            seen.add(vid)
            ids_to_fetch.append(vid)
        if ids_to_fetch:
            try:
                extra_meta = hydrate_video_metadata(ids_to_fetch, yt_key)
            except QuotaExceeded as e:
                log(f"ERROR: YouTube Data API quota exceeded: {e}")
                log("Daily quota resets at midnight Pacific Time. Increase quota or wait.")
                return 2
            except Exception as e:
                log(f"  [oneoff] metadata fetch failed: {e}")
                extra_meta = {}
            for vid in ids_to_fetch:
                m = extra_meta.get(vid)
                if not m:
                    log(f"  [oneoff] skip {vid}: no metadata returned (private/deleted/invalid?)")
                    continue
                channel_title = m.get("channel_title") or "(one-off)"
                synthetic_channel = {
                    "name": channel_title,
                    "channel_id": "",
                    "bypass_filters": True,
                    "active": True,
                    "known_speakers": [],
                }
                video = {
                    "video_id": vid,
                    "url": WATCH_URL_TEMPLATE.format(video_id=vid),
                    "title": m.get("title", ""),
                    "duration": m.get("duration", 0),
                    "published": m.get("published_at", ""),
                }
                extra_queue.append((synthetic_channel, video))
                extra_ids_set.add(vid)
                log(f"  [oneoff] queued {vid} ({video['title'][:60]}) from {channel_title}")

    # Step 3: filter and queue the channel rotation.
    rotation_queue = []
    per_channel_count = {}
    for channel, vid in candidates:
        name = channel.get("name", "?")
        if vid in extra_ids_set:
            log(f"  [{name}] skip {vid}: also requested as one-off (processing once)")
            continue
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
        rotation_queue.append((channel, video))
        per_channel_count[cap_key] = per_channel_count.get(cap_key, 0) + 1

    # One-offs go first so they grab the first parallel slots.
    queued = extra_queue + rotation_queue

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
    summary = {
        "ok": 0,
        "too-long": 0,
        "failed-json": 0,
        "failed-hallucination": 0,
        "failed-timeout": 0,
        "failed-other": 0,
    }
    processed_items: list = []
    state_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=VIDEO_WORKERS) as ex:
        futures = [
            ex.submit(_process_one_video, client, channel, video, state_lock, summary, processed_items)
            for channel, video in queued
        ]
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                log(f"  unhandled worker exception: {e}")

    regenerate_index()

    # A video processed today might publish-date to a different folder
    # (e.g. a 4-day-old upload that finally got through). Regenerate the
    # digest.md for every publish date that received new content this run.
    publish_dates_touched = sorted({it.get("date") for it in processed_items if it.get("date")})
    digest_dates = []
    for pd in publish_dates_touched:
        if write_daily_digest(pd) is not None:
            digest_dates.append(pd)

    if processed_items:
        # Count one-offs that successfully processed (subset of processed_items).
        one_off_count = sum(1 for it in processed_items if it.get("video_id") in extra_ids_set)
        slack_notify.post_digest(
            processed_items,
            today,
            digest_dates=digest_dates,
            one_off_count=one_off_count,
        )
    else:
        slack_notify.post_no_new_videos(today)

    log(f"Done. {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

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
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

MODEL = "gemini-2.5-flash"
ROOT = Path(__file__).parent
OUTPUT_DIR = ROOT / "output"
CONFIG_PATH = ROOT / "channels.json"
WATCH_URL_TEMPLATE = "https://www.youtube.com/watch?v={video_id}"
YT_API_BASE = "https://www.googleapis.com/youtube/v3"

PROMPT = """You are watching an NBA YouTube show. Extract the top 12 most controversial or insightful quotes about NBA trades, free agency, player movement, team dynamics, front office, contracts, player legacy, coaching, or playoff issues.

Rules:
- Each quote should run roughly 100 to 180 words after light cleanup. Preserve the speaker's meaning. Clean only obvious filler ("um", "uh", "you know", false starts) and punctuation.
- Identify the speaker by name when shown on screen, named in chyrons, named in the video title, or clearly addressed by another speaker. Otherwise return "Unknown speaker". Do not guess.
- Provide a start timestamp in MM:SS or H:MM:SS format.
- Tag each quote using only these topics: trades, free agency, team dynamics, player legacy, rumors, front office, coaching, playoffs, contracts.
- Add a one-sentence "why it matters" note framed for HoopsHype Rumors readers (NBA-savvy, want news value).
- Skip play-by-play recap, sponsor reads, intros, outros, generic opinions, and recycled talking points unless phrased forcefully.
- Do not fabricate. Do not exaggerate the speaker's tone. If fewer than 12 quotes meet the bar, return fewer.

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
    """Return the first existing artifact for this video_id, or None.

    Looks for <video_id>.md, <video_id>.SKIPPED-*, or <video_id>.FAILED.* in
    any subdirectory of output/. The presence of any of these means the video
    has already been seen and should not be re-processed.
    """
    if not OUTPUT_DIR.exists():
        return None
    for path in OUTPUT_DIR.rglob(f"{video_id}.*"):
        if path.is_file():
            return path
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
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day_dir = OUTPUT_DIR / today
    day_dir.mkdir(parents=True, exist_ok=True)
    md_path = day_dir / f"{video['video_id']}.md"
    json_path = day_dir / f"{video['video_id']}.json"
    md_path.write_text(to_markdown(video["url"], channel_name, data), encoding="utf-8")
    json_path.write_text(raw_text, encoding="utf-8")
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


# --------------------------------------------------------------------------- #
# Main pipeline
# --------------------------------------------------------------------------- #

def filter_video(video: dict, channel: dict, config: dict) -> str:
    """Return '' if the video should be processed, otherwise a reason string."""
    existing = find_existing_artifact(video["video_id"])
    if existing:
        return f"already processed ({existing.name})"

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


def process_video(client, video: dict, channel_name: str) -> str:
    """Run Gemini against one video, write outputs. Returns status string."""
    url = video["url"]
    video_id = video["video_id"]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day_dir = OUTPUT_DIR / today
    day_dir.mkdir(parents=True, exist_ok=True)

    attempts_429 = 0
    attempts_parse = 0
    last_raw = ""

    while True:
        try:
            raw_text, _usage = call_gemini(client, url)
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
                    return "failed-json"
                continue
            write_outputs(video, channel_name, data, raw_text)
            return "ok"
        except Exception as e:
            status = gemini_error_status(e)
            if status == 429:
                attempts_429 += 1
                if attempts_429 > 3:
                    log(f"  rate-limited 4x, giving up on {video_id}")
                    return "rate-limited"
                log(f"  429 rate-limit, sleeping 60s (attempt {attempts_429}/3)")
                time.sleep(60)
                continue
            if status == 400 or is_token_limit_error(e):
                log(f"  token-limit / 400 on {video_id}: {e}")
                (day_dir / f"{video_id}.SKIPPED-too-long").write_text("", encoding="utf-8")
                return "too-long"
            log(f"  unexpected Gemini error on {video_id}: {e}")
            (day_dir / f"{video_id}.FAILED.txt").write_text(str(e), encoding="utf-8")
            return "failed-other"


def main() -> int:
    gemini_key = os.getenv("GEMINI_API_KEY")
    yt_key = os.getenv("YOUTUBE_API_KEY")
    missing = [name for name, val in (("GEMINI_API_KEY", gemini_key), ("YOUTUBE_API_KEY", yt_key)) if not val]
    if missing:
        log(f"ERROR: missing env vars: {', '.join(missing)}. Copy .env.example to .env and fill them in.")
        return 1

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
        return 0

    if len(queued) > max_total:
        log(f"Capping queue from {len(queued)} to max_videos_per_run={max_total}")
        queued = queued[:max_total]

    # Step 4: process with Gemini.
    client = genai.Client(api_key=gemini_key)
    log(f"Processing {len(queued)} video(s) with Gemini...")
    summary = {"ok": 0, "too-long": 0, "rate-limited": 0, "failed-json": 0, "failed-other": 0}
    for channel, video in queued:
        name = channel.get("name", "?")
        log(f"  -> {video['video_id']} [{name}]: {video['title'][:80]}")
        status = process_video(client, video, name)
        summary[status] = summary.get(status, 0) + 1
        log(f"     status: {status}")

    regenerate_index()
    log(f"Done. {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

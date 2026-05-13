"""
Slack notifier for the daily quote-extractor run.

Posts either a digest (one message per run, with one block per processed
video) or a short "no new videos today" note. Reads SLACK_WEBHOOK_URL and
OUTPUT_BASE_URL from the environment. If SLACK_WEBHOOK_URL is missing,
posting is skipped — the markdown files in output/ are the source of truth.
"""

import os

import requests


def _log(msg: str) -> None:
    print(f"[slack] {msg}", flush=True)


def _webhook() -> str:
    return (os.getenv("SLACK_WEBHOOK_URL") or "").strip()


def _output_base_url() -> str:
    return (os.getenv("OUTPUT_BASE_URL") or "").strip().rstrip("/")


def _digest_link(date_str: str) -> str:
    base = _output_base_url()
    if not base:
        return f"output/{date_str}/digest.md"
    return f"{base}/{date_str}/digest.md"


CLOSING_LINE = (
    "CHECK OTHER YOUTUBE PODCASTS HERE: "
    "https://www.youtube.com/feed/subscriptions"
)


def _truncate_quote(text: str, n: int = 200) -> str:
    text = (text or "").strip().replace("\n", " ")
    if len(text) <= n:
        return text
    return text[:n].rstrip() + "..."


def _post(payload: dict) -> bool:
    url = _webhook()
    if not url:
        _log("SLACK_WEBHOOK_URL not set; skipping Slack post.")
        return False
    try:
        resp = requests.post(url, json=payload, timeout=15)
    except requests.RequestException as e:
        _log(f"Slack POST failed: {e}")
        return False
    if resp.status_code != 200:
        _log(f"Slack returned {resp.status_code}: {resp.text[:200]}")
        return False
    return True


def post_no_new_videos(date_str: str) -> bool:
    return _post({"text": f"HoopsHype YT Quotes — {date_str}: No new videos today."})


def post_digest(items: list, date_str: str) -> bool:
    """Post a digest to Slack.

    items is a list of dicts with keys: video_id, title, channel,
    top_quote, speaker, date.
    """
    if not items:
        return post_no_new_videos(date_str)

    n = len(items)
    plural = "s" if n != 1 else ""
    lines = [
        f"*HoopsHype YT Quotes — {date_str}*",
        f"Processed {n} video{plural}.",
        _digest_link(date_str),
    ]
    for it in items:
        title = it.get("title") or it["video_id"]
        channel = it.get("channel") or ""
        speaker_raw = (it.get("speaker") or "").strip()
        # Drop "Unknown speaker" and any "Unknown speaker (man with beard)"
        # variants — better to leave the attribution off than describe a hoodie.
        speaker = "" if speaker_raw.lower().startswith("unknown speaker") else speaker_raw
        quote = _truncate_quote(it.get("top_quote") or "")
        lines.append("")
        lines.append(f"📺 *{title}* ({channel})")
        if quote and speaker:
            lines.append(f'Top quote: "{quote}" — {speaker}')
        elif quote:
            lines.append(f'Top quote: "{quote}"')
    lines.append("")
    lines.append(CLOSING_LINE)
    return _post({"text": "\n".join(lines)})

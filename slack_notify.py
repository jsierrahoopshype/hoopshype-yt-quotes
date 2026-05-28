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


def _digest_link(date_str: str, filename: str = "digest.md") -> str:
    base = _output_base_url()
    if not base:
        return f"output/{date_str}/{filename}"
    return f"{base}/{date_str}/{filename}"


def _oneoff_digest_link(date_str: str, filename: str = "digest.md") -> str:
    base = _output_base_url()
    if not base:
        return f"output/oneoffs/{date_str}/{filename}"
    return f"{base}/oneoffs/{date_str}/{filename}"


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


def post_digest(
    items: list,
    date_str: str,
    digest_dates=None,
    oneoff_digest_dates=None,
    one_off_count: int = 0,
    run_slot: str | None = None,
    aborted_count: int = 0,
    deferred_count: int = 0,
) -> bool:
    """Post a digest to Slack.

    items is a list of dicts with keys: video_id, title, channel,
    top_quote, speaker, date (publish date), is_one_off.

    date_str is the RUN date used in the message header.

    digest_dates is an explicit list of rotation publish dates to render
    as digest URLs. When None, derived from items (rotation items only).

    oneoff_digest_dates is an explicit list of one-off publish dates to
    render as separate URLs under a "One-offs:" prefix. When None, derived
    from items (one-off items only).

    run_slot identifies which cron / manual trigger produced this run
    (e.g. "09utc", "14utc", "manual-1547"). When provided, the URLs
    target the per-run digest-<slot>.md instead of the aggregate
    digest.md, so the Slack link points at exactly this run's content.

    one_off_count is the number of items in this digest that came from the
    workflow_dispatch EXTRA_VIDEOS input. When > 0, the summary line breaks
    down the count.

    aborted_count is the number of videos skipped after a spending-cap
    abort. When > 0, the message gets a "ABORTED" banner at the top so
    readers can tell the run was cut short. Items that DID complete
    before the cap still render normally below the banner.

    deferred_count is the number of videos that hit a terminal Gemini
    503/500 after retries and were deferred to the next run. When > 0,
    a separate "deferred" banner explains these aren't real failures —
    they'll retry automatically on the next cron.
    """
    if not items and aborted_count == 0 and deferred_count == 0:
        return post_no_new_videos(date_str)

    if digest_dates is None:
        digest_dates = sorted({
            (it.get("date") or date_str)
            for it in items if not it.get("is_one_off")
        })
    if oneoff_digest_dates is None:
        oneoff_digest_dates = sorted({
            (it.get("date") or date_str)
            for it in items if it.get("is_one_off")
        })

    n = len(items)
    plural = "s" if n != 1 else ""
    if one_off_count > 0:
        rotation = n - one_off_count
        summary_line = (
            f"Processed {n} video{plural} "
            f"({rotation} from rotation, {one_off_count} one-off)."
        )
    else:
        summary_line = f"Processed {n} video{plural}."
    lines = [f"*HoopsHype YT Quotes — {date_str}*"]
    if aborted_count > 0:
        aborted_plural = "s" if aborted_count != 1 else ""
        lines.append(
            f":warning: ABORTED: Gemini spending cap hit; "
            f"{aborted_count} video{aborted_plural} in queue were not processed. "
            "Re-run this slot after the cap is raised."
        )
    if deferred_count > 0:
        deferred_plural = "s" if deferred_count != 1 else ""
        lines.append(
            f":hourglass_flowing_sand: {deferred_count} video{deferred_plural} "
            "deferred due to Gemini high-demand (503/500); will retry next run."
        )
    lines.append(summary_line)
    digest_filename = f"digest-{run_slot}.md" if run_slot else "digest.md"
    if oneoff_digest_dates:
        # Mixed run: label both sections so readers can tell editorial
        # rotation digests from off-rotation one-off digests at a glance.
        for d in digest_dates:
            lines.append(f"Rotation: {_digest_link(d, digest_filename)}")
        for d in oneoff_digest_dates:
            lines.append(f"One-offs: {_oneoff_digest_link(d, digest_filename)}")
    else:
        # Rotation-only run: keep the unlabeled URL list (current format).
        for d in digest_dates:
            lines.append(_digest_link(d, digest_filename))
    for it in items:
        title = it.get("title") or it["video_id"]
        channel = it.get("channel") or ""
        speaker_raw = (it.get("speaker") or "").strip()
        # Drop bare "Unknown", "Unknown speaker", and "Unknown speaker (man
        # with beard)" variants. Better to leave the attribution off than
        # describe a hoodie or invent a placeholder.
        speaker = "" if speaker_raw.lower().startswith("unknown") else speaker_raw
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

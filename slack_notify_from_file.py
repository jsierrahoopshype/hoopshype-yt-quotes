"""
Read a JSON payload written by quote_extractor.main() and post it to Slack.

Invoked from the workflow AFTER the git push step so the digest URLs the
Slack message links to are guaranteed to be on origin by the time anyone
clicks through. This decouples Slack-notification timing from script
liveness — abandoned background threads keeping the Python interpreter
alive (Gemini-side hung sockets that we time out from but can't actually
kill) can no longer delay the Slack post until the SIGALRM hard exit.

Payload shape:
  {"kind": "no_new_videos", "date_str": "YYYY-MM-DD"}
  {"kind": "digest", "date_str": "YYYY-MM-DD", "items": [...],
   "digest_dates": [...], "oneoff_digest_dates": [...],
   "one_off_count": N}

Exits 0 if the payload was missing or empty (nothing to do — not an
error). Exits 0 after a Slack POST regardless of HTTP outcome — Slack
errors are logged but never fail the workflow.
"""

import json
import sys
from pathlib import Path

import slack_notify


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: python slack_notify_from_file.py <payload.json>", file=sys.stderr)
        return 2
    path = Path(sys.argv[1])
    if not path.is_file():
        print(f"slack payload not found at {path}; nothing to post", file=sys.stderr)
        return 0
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        print(f"could not parse slack payload {path}: {e}", file=sys.stderr)
        return 0  # don't fail the workflow over a broken payload

    kind = (payload.get("kind") or "").strip()
    date_str = payload.get("date_str") or ""

    if kind == "no_new_videos":
        slack_notify.post_no_new_videos(date_str)
        return 0
    if kind == "digest":
        slack_notify.post_digest(
            payload.get("items") or [],
            date_str,
            digest_dates=payload.get("digest_dates") or [],
            oneoff_digest_dates=payload.get("oneoff_digest_dates") or [],
            one_off_count=int(payload.get("one_off_count") or 0),
        )
        return 0
    print(f"unknown slack payload kind: {kind!r}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
Tiny helper: given a YouTube channel URL or @handle, print the UC channel ID.

Usage:
    python add_channel.py https://www.youtube.com/@TheLowePost
    python add_channel.py @TheLowePost
    python add_channel.py https://www.youtube.com/channel/UCxxxxxxxxxxxxxxxxxxxxxx

Uses the YouTube Data API v3. Requires YOUTUBE_API_KEY in .env.
"""

import os
import re
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

YT_API_BASE = "https://www.googleapis.com/youtube/v3"


class QuotaExceeded(Exception):
    pass


def youtube_api_get(endpoint: str, params: dict, api_key: str) -> dict:
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


def resolve(arg: str, api_key: str) -> tuple[str, str]:
    """Return (channel_id, channel_title) for the given URL or handle."""
    arg = arg.strip()

    m = re.search(r"/channel/(UC[A-Za-z0-9_-]{22})", arg)
    if m:
        cid = m.group(1)
        data = youtube_api_get("channels", {"part": "snippet", "id": cid}, api_key)
        items = data.get("items", [])
        return cid, (items[0]["snippet"]["title"] if items else "")

    m = re.search(r"/user/([A-Za-z0-9_\-]+)", arg)
    if m:
        data = youtube_api_get(
            "channels", {"part": "id,snippet", "forUsername": m.group(1)}, api_key
        )
        items = data.get("items", [])
        if items:
            return items[0]["id"], items[0]["snippet"]["title"]
        return "", ""

    handle_match = re.search(r"@([A-Za-z0-9_.\-]+)", arg)
    if handle_match:
        handle = "@" + handle_match.group(1)
    elif arg.startswith("UC") and len(arg) == 24:
        data = youtube_api_get("channels", {"part": "snippet", "id": arg}, api_key)
        items = data.get("items", [])
        return arg, (items[0]["snippet"]["title"] if items else "")
    else:
        handle = "@" + arg.lstrip("@")

    data = youtube_api_get(
        "channels", {"part": "id,snippet", "forHandle": handle}, api_key
    )
    items = data.get("items", [])
    if items:
        return items[0]["id"], items[0]["snippet"]["title"]
    return "", ""


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python add_channel.py <youtube channel URL or @handle>")
        return 1

    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        print("ERROR: YOUTUBE_API_KEY is not set. Copy .env.example to .env and fill it in.")
        return 1

    arg = sys.argv[1]
    print(f"Looking up: {arg}")
    try:
        cid, name = resolve(arg, api_key)
    except QuotaExceeded as e:
        print(f"ERROR: YouTube Data API quota exceeded: {e}")
        return 2
    except requests.HTTPError as e:
        print(f"ERROR: API request failed: {e}")
        return 3

    if not cid:
        print("Could not resolve a channel ID for that input.")
        return 4

    print()
    print(f"Channel name: {name or '(unknown)'}")
    print(f"Channel ID:   {cid}")
    print()
    print("Snippet to paste into channels.json (inside the \"channels\" array):")
    print()
    print("{")
    print(f'  "name": "{name or "REPLACE_ME"}",')
    print(f'  "channel_id": "{cid}",')
    print('  "bypass_filters": false,')
    print('  "active": true')
    print("}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

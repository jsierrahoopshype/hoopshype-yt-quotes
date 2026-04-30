"""
Tiny helper: given a YouTube channel URL or @handle, print the UC channel ID.

Usage:
    python add_channel.py https://www.youtube.com/@TheLowePost
    python add_channel.py @TheLowePost
    python add_channel.py https://www.youtube.com/channel/UCxxxxxxxxxxxxxxxxxxxxxx

It prints the channel_id and a JSON snippet you can paste into channels.json.
"""

import re
import sys

import requests

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


def normalize(arg: str) -> str:
    arg = arg.strip()
    if arg.startswith("@"):
        return f"https://www.youtube.com/{arg}"
    if not arg.startswith("http"):
        return f"https://www.youtube.com/@{arg.lstrip('@')}"
    return arg


def extract_channel_id(url: str) -> tuple[str, str]:
    m = re.search(r"/channel/(UC[A-Za-z0-9_-]{22})", url)
    if m:
        return m.group(1), ""

    resp = requests.get(url, headers={"User-Agent": USER_AGENT, "Accept-Language": "en"}, timeout=20)
    resp.raise_for_status()
    html = resp.text

    cid = ""
    for pattern in (
        r'"channelId":"(UC[A-Za-z0-9_-]{22})"',
        r'"externalId":"(UC[A-Za-z0-9_-]{22})"',
        r'/channel/(UC[A-Za-z0-9_-]{22})',
    ):
        m = re.search(pattern, html)
        if m:
            cid = m.group(1)
            break

    name = ""
    m = re.search(r'<meta property="og:title" content="([^"]+)"', html)
    if m:
        name = m.group(1)
    return cid, name


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python add_channel.py <youtube channel URL or @handle>")
        return 1
    url = normalize(sys.argv[1])
    print(f"Looking up: {url}")
    cid, name = extract_channel_id(url)
    if not cid:
        print("Could not find a channel ID on that page.")
        return 2
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

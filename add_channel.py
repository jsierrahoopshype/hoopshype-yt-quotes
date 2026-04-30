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

    cid, name = _resolve_via_html(url)
    if cid:
        return cid, name

    print("HTML scrape didn't find a channel ID. Falling back to yt-dlp...")
    return _resolve_via_ytdlp(url)


def _resolve_via_html(url: str) -> tuple[str, str]:
    try:
        resp = requests.get(
            url, headers={"User-Agent": USER_AGENT, "Accept-Language": "en"}, timeout=20
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"HTML fetch failed: {e}")
        return "", ""
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


def _resolve_via_ytdlp(url: str) -> tuple[str, str]:
    try:
        import yt_dlp
    except ImportError:
        print("yt-dlp is not installed. Run: pip install -r requirements.txt")
        return "", ""

    videos_url = url.rstrip("/")
    if not videos_url.endswith("/videos"):
        videos_url = videos_url + "/videos"

    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": True,
        "playlistend": 1,
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(videos_url, download=False)
    except Exception as e:
        print(f"yt-dlp lookup failed: {e}")
        return "", ""

    if not info:
        return "", ""
    cid = info.get("channel_id") or info.get("uploader_id") or ""
    if not cid.startswith("UC"):
        cid = ""
    name = info.get("channel") or info.get("uploader") or info.get("title") or ""
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

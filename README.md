# HoopsHype YouTube quote extractor

Daily pipeline that polls NBA YouTube channels, sends each new editorial video
to Gemini 2.5 Flash, and saves the top 12 ranked quotes per video as markdown.

This README covers **phase 1**: running the extractor locally on Windows.
Slack distribution and GitHub Actions come in phases 2 and 3.

---

## What you need before you start

1. **Python 3.11 or newer** installed from <https://www.python.org/downloads/>.
   During install, tick the "Add python.exe to PATH" checkbox.
2. **Git for Windows** installed from <https://git-scm.com/download/win>.
3. A **paid Gemini API key**. The free tier rate limits are too tight for video
   processing. Get one at <https://aistudio.google.com/apikey> and enable
   billing on the project.

---

## One-time setup (PowerShell)

Open PowerShell in the repo folder, then run these commands one at a time.

### 1. Allow venv activation scripts to run

Windows blocks PowerShell scripts by default. Run this once per user account:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Answer `Y` when prompted. You only need to do this once, ever.

### 2. Create and activate a virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

After activation your prompt should start with `(.venv)`.

### 3. Install dependencies

```powershell
pip install -r requirements.txt
```

### 4. Create your `.env` file

```powershell
Copy-Item .env.example .env
notepad .env
```

Paste your Gemini API key after `GEMINI_API_KEY=` and save. Close Notepad.

**Optional but recommended on Windows**: also set `YTDLP_BROWSER=chrome` in
`.env`. This is only used when the RSS feed and HTML scrape can't determine a
video's duration. In that case the script falls back to `yt-dlp`, and YouTube
now blocks unauthenticated metadata lookups with "Sign in to confirm you're
not a bot." Pointing yt-dlp at your logged-in Chrome session bypasses this.
Other accepted values: `firefox`, `edge`, `brave`, `safari`, `vivaldi`. Leave
empty when running on GitHub Actions (we'll handle that in phase 3).

---

## Add channels

`channels.json` starts empty. To populate it:

```powershell
python add_channel.py https://www.youtube.com/@TheLowePost
```

That prints a JSON snippet. Open `channels.json` in Notepad, paste the snippet
into the `channels` array, separating multiple channels with commas. Example:

```json
{
  "channels": [
    {
      "name": "The Lowe Post",
      "channel_id": "UCxxxxxxxxxxxxxxxxxxxxxx",
      "bypass_filters": false,
      "active": true
    },
    {
      "name": "Another Show",
      "channel_id": "UCyyyyyyyyyyyyyyyyyyyyyy",
      "bypass_filters": true,
      "active": true
    }
  ],
  "min_duration_minutes": 15,
  ...
}
```

Set `bypass_filters: true` for channels where almost every upload is editorial
(podcasts, interviews). Those channels skip the title-keyword and duration
filters; only the duplicate check is applied.

Set `active: false` to temporarily pause a channel without deleting it.

---

## Run it

With the venv active and `.env` filled in:

```powershell
python quote_extractor.py
```

You should see logs like:

```
[09:00:01] Polling 2 channel(s)...
[09:00:02]   [The Lowe Post] skip dQw4w9WgXcQ (NBA Highlights Recap): title contains 'highlights'
[09:00:02]   [The Lowe Post] queued 1 video(s)
[09:00:02] Processing 1 video(s) with Gemini...
[09:00:02]   -> abc123XYZ12 [The Lowe Post]: Tatum trade chatter and Brunson...
[09:01:30]      status: ok
[09:01:30] Done. {'ok': 1, ...}
```

Output lands in `output/YYYY-MM-DD/<video_id>.md` with the raw Gemini JSON next
to it. A top-level `output/index.md` is regenerated every run.

Run it again and it should report nothing new — already-processed videos are
detected by the presence of any `<video_id>.*` file under `output/`.

---

## What gets filtered out

Before sending to Gemini, a video is skipped if **any** of these are true:

- A file matching `output/**/<video_id>.*` already exists.
- The URL is a `/shorts/` URL.
- The title contains any keyword from `skip_title_keywords` in `channels.json`
  ("highlights", "full game", "top 10", etc.).
- The video is shorter than `min_duration_minutes` (default 15). Duration is
  resolved in this order:
  1. The `media:content` `duration` attribute in the RSS feed (free, already
     fetched).
  2. A lightweight HTML scrape of the watch page for `lengthSeconds`.
  3. `yt-dlp` metadata extraction (no download). If `YTDLP_BROWSER` is set,
     yt-dlp uses cookies from that browser's logged-in session.
  4. Skip the video.

Channels marked `bypass_filters: true` skip everything except the duplicate
check.

---

## Failure handling

- **Token-limit / 400 from Gemini**: video is too long. An empty marker file
  `output/YYYY-MM-DD/<video_id>.SKIPPED-too-long` is written so it never gets
  retried.
- **429 rate-limit**: sleeps 60s and retries up to 3 times, then gives up.
- **Malformed JSON**: retries once. If still bad, the raw text is saved as
  `<video_id>.FAILED.txt`.
- **RSS fetch fails for a channel**: that channel is skipped for the run; the
  job continues with the others.

---

## Files in this repo

| File | What it is |
| --- | --- |
| `quote_extractor.py` | The main script. Run this. |
| `add_channel.py` | Helper that turns a YouTube URL or @handle into a UC channel ID. |
| `channels.json` | Editable list of channels and filter settings. |
| `requirements.txt` | Python dependencies. |
| `.env.example` | Template for your `.env` (which holds the API key). |
| `output/` | All processed videos. Markdown + raw JSON, organised by day. |

---

## Coming in later phases

- **Phase 2**: Slack digest posted at the end of every run.
- **Phase 3**: GitHub Actions cron to run this daily, plus GitHub Pages to
  serve the `output/` folder publicly.

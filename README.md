# HoopsHype YouTube quote extractor

Daily pipeline that polls NBA YouTube channels, sends each new editorial video
to Gemini 2.5 Flash, and saves the top 12 ranked quotes per video as markdown.

This README covers **phases 1 and 2**: running the extractor locally on
Windows and posting a daily digest to Slack. GitHub Actions cron and GitHub
Pages publishing come in phase 3.

---

## What you need before you start

1. **Python 3.11 or newer** installed from <https://www.python.org/downloads/>.
   During install, tick the "Add python.exe to PATH" checkbox.
2. **Git for Windows** installed from <https://git-scm.com/download/win>.
3. A **paid Gemini API key**. The free tier rate limits are too tight for video
   processing. Get one at <https://aistudio.google.com/apikey> and enable
   billing on the project.
4. A **YouTube Data API v3 key** for channel + video metadata. The free tier
   gives 10,000 units/day, which is roughly 200x what this pipeline needs.
   Enable the API and create a key here:
   <https://console.cloud.google.com/apis/library/youtube.googleapis.com>.
   Click "Enable", then go to "Credentials" → "Create credentials" → "API key".

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

Paste your Gemini key after `GEMINI_API_KEY=` and your YouTube Data API key
after `YOUTUBE_API_KEY=`. Save and close Notepad.

### 5. (Optional) Configure Slack

If you want a daily digest in Slack:

1. Create an incoming webhook in your workspace at
   <https://api.slack.com/messaging/webhooks>. Pick the channel where the
   digest should go and copy the webhook URL.
2. In `.env`, paste it after `SLACK_WEBHOOK_URL=`.
3. Set `OUTPUT_BASE_URL=` to the GitHub blob URL for the working branch, so
   the per-video links in the digest go somewhere readable. For this branch
   that's:

   ```
   OUTPUT_BASE_URL=https://github.com/jsierrahoopshype/hoopshype-yt-quotes/blob/claude/youtube-quote-extractor-pcYe7/output
   ```

   In phase 3 you'll swap this for the GitHub Pages URL.

If you leave `SLACK_WEBHOOK_URL` empty, the script just skips the Slack post
and continues normally — the markdown files are still the source of truth.

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
- The title contains any keyword from `skip_title_keywords` in `channels.json`
  ("highlights", "full game", "top 10", etc.).
- The video is shorter than `min_duration_minutes` (default 15). Duration
  comes from the YouTube Data API's `videos.list` `contentDetails.duration`
  field. Shorts get caught by the same filter since they're under a minute.

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
- **YouTube API fails for a channel**: that channel is skipped for the run;
  the job continues with the others.
- **YouTube API quota exceeded (403 quotaExceeded)**: the script logs the
  error and exits with status 2. The free quota resets daily at midnight
  Pacific Time.

---

## Files in this repo

| File | What it is |
| --- | --- |
| `quote_extractor.py` | The main script. Run this. |
| `slack_notify.py` | Posts the daily digest to the Slack webhook. |
| `add_channel.py` | Helper that turns a YouTube URL or @handle into a UC channel ID. |
| `channels.json` | Editable list of channels and filter settings. |
| `requirements.txt` | Python dependencies. |
| `.env.example` | Template for your `.env` (API keys, webhook, base URL). |
| `output/` | All processed videos. Markdown + raw JSON, organised by day. |

---

## Coming in later phases

- **Phase 3**: GitHub Actions cron to run this daily, plus GitHub Pages to
  serve the `output/` folder publicly.

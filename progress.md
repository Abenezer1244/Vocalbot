# Progress

## 2026-10-11

### Summary
Answered where the bot is hosted (Render) and how to reach this Claude Code remote-control session. Recorded two new untracked files that showed up in the repo during this session but were not written in it.

### Changes
- `reminder_bot.py` (untracked, not written in this session): a Telegram reminder bot. Reminders are stored in SQLite, can optionally be backed up to Google Sheets (`SHEETS_ID`), and only group admins can manage reminders in groups. Any user can set personal reminders in a DM.
- `test_reminder_bot.py` (untracked): offline tests for the parsing helpers.
- No changes to `vocal_bot.py`.

### Decisions
- No new files were committed.

### Next Steps
- Run `python -m pytest test_reminder_bot.py` (not run yet, so it is unconfirmed that the tests pass).
- Decide whether `reminder_bot.py` should be committed and deployed as its own Render service.

---

## 2026-10-11 (audit + group reminder bot)

### Summary
Audited `vocal_bot.py`, then wrote `reminder_bot.py` and `test_reminder_bot.py` in this session (this replaces the "not written in this session" note above). The user wants a bot that posts automatic reminders in the team group (no commands needed) and suggests songs it has learned from earlier song lists in the chat.

### Changes
- `reminder_bot.py` (new): runs in the group set by `GROUP_CHAT_ID`.
  - `SCHEDULE` list posts reminders automatically: Saturday 18:00 and Monday 09:00 "send all 4 songs by Monday" (each with 4 song suggestions), Thursday 17:00 "practice at 7 PM", and Saturday 15:00 "practice at 5 PM". Times are America/Los_Angeles, changeable with `TZ_NAME`.
  - Learns songs from numbered lists posted in the group (mixed titles and links). Link-only YouTube lines get their title from YouTube oEmbed. Saved lists get a 👍 reaction.
  - Suggestions are 4 random picks from the songs not sung in the longest time.
  - Commands: `/suggest`, `/songs`, `/chatid`, `/help`.
  - `python reminder_bot.py import result.json` learns past songs from a Telegram Desktop chat export.
  - Songs are stored in SQLite (`songs.db`) and mirrored to a `Songs` tab in Google Sheets when `SHEETS_ID` is set, so they survive Render redeploys.
  - The webhook uses `url_path="webhook"` with a `secret_token`, and the bot token is never logged.
- `test_reminder_bot.py` (new): offline asserts for parsing, merging duplicate songs, suggestions, the export import, and that each schedule entry fires on its named weekday. It passes (`python test_reminder_bot.py`, run with python-telegram-bot 21.4 through uv).
- `vocal_bot.py` is unchanged.

### Decisions
- Kept `vocal_bot.py` so the current bot keeps running until the user switches over.
- Days are numbered with 0=Sunday because PTB 21's `run_daily` uses that. The same check showed `vocal_bot.py` assumes 0=Monday, so all its scheduled jobs run one day early (not fixed there).
- Song matching uses exact normalized names, so "Waymaker" and "Way Maker" count as different songs.
- Only numbered lists (2 or more lines) count as song lists.
- Nothing committed (the user hasn't asked).

### Next Steps
- In BotFather, run `/setprivacy` → Disable, so the bot can read group messages.
- Add the bot to the group, get the ID with `/chatid`, and set `GROUP_CHAT_ID` on Render. Change the Render start command to `python reminder_bot.py`.
- Export the group's chat history to JSON and run the `import` command to learn past songs.
- The user plans to add more reminders. Add them as lines in `SCHEDULE`.
- Open audit items in `vocal_bot.py`: `.venv/` and `progress.db` are committed to the public repo, and `.venv/` should go in `.gitignore`.

---

## 2026-10-11 (reminders only)

### Summary
The user dropped the song-suggestion idea and asked for no commands. `reminder_bot.py` now only posts the scheduled reminders in the group.

### Changes
- `reminder_bot.py`: removed song learning and suggestions, all commands, SQLite, the Google Sheets backup, and the `import` command. What's left is the `SCHEDULE` list (Sat 18:00 and Mon 09:00 song-deadline reminders, Thu 17:00 and Sat 15:00 practice reminders) and a log line that prints the group's chat ID when the bot is added to a group.
- `test_reminder_bot.py`: now only checks that each schedule entry fires on its named weekday and time. It passes.

### Decisions
- No commands at all. The chat ID comes from the bot's log (the `my_chat_member` update) instead of `/chatid`.
- No storage is needed. The schedule lives in the code.
- BotFather privacy mode no longer matters, because the bot doesn't read messages.

### Next Steps
- Add the bot to the group, read the chat ID from the Render log, and set `GROUP_CHAT_ID`.
- Make sure the Render service doesn't go to sleep. A sleeping free Web Service won't fire the scheduled jobs.
- Add more reminders to `SCHEDULE` as the user remembers them.

---

## 2026-10-11 (reminders merged into vocal_bot.py)

### Summary
Hosting is moving to Railway (another session set it up in `.railway/railway.ts`, start `python vocal_bot.py`). Two bots polling with one BOT_TOKEN would conflict (409), so the user chose to keep one bot: the 4 group reminders now live in `vocal_bot.py`, and `reminder_bot.py` is deleted.

### Changes
- `vocal_bot.py`:
  - `GROUP_SCHEDULE` list (Sat 18:00 and Mon 09:00 "send all 4 songs by Monday", Thu 17:00 "practice at 7 PM", Sat 15:00 "practice at 5 PM", Pacific) replaces the old Monday/Saturday 9am posts, which referred to an outdated practice schedule.
  - New `ptb_days()` helper. PTB 21's `run_daily` counts days from 0=Sunday, but the code passed Python weekdays (0=Monday), so every scheduled job (group posts, Bible verse, DM nudges, weekly rollover, personal `/remind`) fired one day early. All `run_daily` calls now go through `ptb_days()`.
- `test_schedule.py` (new): checks every scheduled job fires on its intended weekday and time. Passes.
- Deleted `reminder_bot.py` and `test_reminder_bot.py`.

### Decisions
- One bot, one token. All old commands stay.
- The Bible verse (Sun 07:30) and DM nudges (Tue/Thu 10:00) are kept, now on the correct days.

### Next Steps
- Set the Railway variables (the other session is handling this) and deploy.
- Add more reminders as lines in `GROUP_SCHEDULE`.
- Remaining audit items in `vocal_bot.py` (token logged in webhook mode, register-as-anyone, XP farming via /undo, etc.) are still open.

---

## 2026-10-11 (audit fixes in vocal_bot.py)

### Summary
Fixed the audit bugs in `vocal_bot.py`. The user is dropping Google Sheets (relayed by the Railway session), so the Sheets-only bugs were left alone. The SQLite DB on the Railway volume is now the only store.

### Changes
- `/register`: a roster name already held by another Telegram account is refused, so nobody can impersonate someone else.
- `/undo`: takes back XP_PER_CHECKIN, plus XP_BONUS_FULL_WEEK when Day 3 is removed. This closes the check-in → undo → check-in XP farm.
- Check-in (`cb_day`): XP is only awarded when the insert succeeds. The full-week bonus no longer depends on GROUP_CHAT_ID. COMEBACK needs earlier activity, so it no longer goes to new users. The stored practice note is used once and then cleared. Notes are capped at 200 chars.
- Markdown: practice notes are escaped with `escape_markdown` in the confirmation, the reward DM and `/practices`. One `_` in a note used to break `/practices` for everyone.
- Streaks: new `full_week_streak()` is used by `/streaks` and the badges. An unfinished current week no longer resets the streak to 0, and empty weeks now break it.
- XP bar: capped at its width. At max level it used to exceed Telegram's 4096-character limit, and the reward DM silently failed.
- Webhook: path is `/webhook` with a `secret_token`, and the token is no longer in the URL or logs.
- Cleanup: removed the broken module-level archive-hydration block (undefined `c`), the unused `tokenize` import, the duplicate `dtime` import and the hardcoded Windows DB path (default is now next to the script).
- Repo: `.venv/` (1482 files) and `progress.db` are untracked; `.venv/` is added to `.gitignore`.
- `test_vocal_bot.py` (replaces `test_schedule.py`): covers the XP bar, register, streaks, undo XP, Markdown escaping in `/practices`, and that every scheduled job fires on the right weekday. Passes.

### Decisions
- Skipped the Sheets fixes (undo not removing the Sheets row, hydration missing local_date/notes, archive truncation) because Sheets is being dropped.
- Skipped moving the blocking Sheets calls to threads for the same reason.

### Next Steps
- Commit and deploy on Railway (the other session deploys after this).
- Optionally remove the now-unused Google Sheets code and the `gspread`/`google-auth` requirements.

---

## 2026-10-11 (moved hosting to Railway)

### Summary
Moved the bot's hosting from Render to Railway. The deploy succeeded; the bot runs in polling mode and Telegram's getMe and deleteWebhook calls both succeed.

### Changes
- `.railway/railway.ts` (Railway IaC): one service `vocalbot` that runs `python vocal_bot.py`, plus a 1GB volume `vocalbot-data` mounted at `/data` (region us-west2) with `DB_PATH=/data/progress.db`. `BOT_TOKEN`, `GROUP_CHAT_ID` and `ADMIN_IDS` are declared with `preserve()`, so the config never overwrites the real values set on Railway.
- `.railway/package.json` (Railway SDK) and `.railwayignore`. `.railway/node_modules/` is added to `.gitignore`.
- Google Sheets is dropped (no `SHEETS_ID`), so the volume is the only storage.
- Railway project: https://railway.com/project/6d174aa3-76d3-48e0-9803-350dc1162277

### Decisions
- Polling instead of a webhook, so no public domain is needed.
- Deployed with `railway up` from the local folder. Nothing is committed, and GitHub autodeploy is not connected.
- On Windows, `railway config plan/apply` has to be run from PowerShell with `%APPDATA%\npm\node_modules\@railway\cli\bin` first on PATH. Otherwise the SDK's CLI version check fails.

### Next Steps
- Suspend the Render service, or the group gets every broadcast twice and Render's webhook can take the bot back.
- `GROUP_CHAT_ID` on Railway is a positive number, but group IDs are negative. Run `/chatid` in the group and set the correct value.
- Optionally connect the GitHub repo in Railway for autodeploy after committing.

---

## 2026-10-11 (all commands removed)

### Summary
The user asked to remove every command, including videos, programs and Google Sheets. `vocal_bot.py` now only posts the 4 scheduled group reminders. It went from ~1,800 lines to 48.

### Changes
- `vocal_bot.py`: only `GROUP_SCHEDULE` (Sat 18:00 and Mon 09:00 song deadline, Thu 17:00 and Sat 15:00 practice, Pacific) plus polling. Removed check-ins, XP/badges, streaks, the roster, /remind, videos, programs, the Bible verse, DM nudges, SQLite and Sheets. The bot exits at startup unless GROUP_CHAT_ID is negative, i.e. a group ID; the Railway session found a positive (user) ID set there.
- `requirements.txt`: `python-telegram-bot[job-queue]==21.4`, `python-dotenv==1.0.1`.
- `test_vocal_bot.py`: checks each reminder fires on its named weekday and time. Passes against the trimmed requirements.
- `README.md`: rewritten for the reminder-only bot.

### Decisions
- Kept the filename `vocal_bot.py` so the Railway start command doesn't change.
- Polling only, no webhook. Railway keeps the process alive.
- The user chose to keep only the 4 song/practice reminders (not the Sunday Bible verse).

### Next Steps
- Set GROUP_CHAT_ID on Railway to the group's `-100…` ID (the Railway session asked the user for it), then deploy.
- The Railway /data volume and DB_PATH, ADMIN_IDS, SHEETS_ID and GOOGLE_* are no longer used. Removing the volume is the user's call.
- Commit (not done yet).

### Update (same day): reminders-only bot live
- The old bot @YesuWayBot was deleted in BotFather. A new bot, @Worshipjesus_bot, has its token set on Railway.
- `GROUP_CHAT_ID` is set to the "yesuway worship" group (read from the new bot's getUpdates once it was added).
- Deployed the 48-line reminders-only `vocal_bot.py`. The logs show "4 reminders scheduled" (SAT 18:00, MON 09:00, THU 17:00, SAT 15:00 Pacific), and polling works.
- The volume, `DB_PATH` and `ADMIN_IDS` are now unused. They're kept until the user decides to remove them.
- Nothing is committed yet.

---

## 2026-10-11 (schedule change, deploy blocked)

### Summary
Moved the "send all 4 songs by Monday" reminder from Saturday to Sunday at the user's request. Railway rejected the deploy because the account's trial has expired.

### Changes
- `vocal_bot.py`: `GROUP_SCHEDULE` entry `("SAT", "18:00", ...)` → `("SUN", "14:00", ...)` (user changed 18:00 to 2 PM). `test_vocal_bot.py` passes.
- The change is only local: not committed and not deployed.

### Decisions
- Sunday 14:00 Pacific, same message text.

### Next Steps
- The user needs to pick a Railway plan (Hobby, $5/mo). Then run `railway up --service vocalbot --detach`, check that the logs show `Added job "SUN 14:00"`, and commit and push.
- Until then the old deploy (Saturday 18:00) keeps running, and Railway may stop it at any time.

---

## 2026-10-11 (bot token leaked in logs)

### Summary
Railway logs showed the full bot token in every line. httpx logs each Telegram API request at INFO, and the request URL contains the token. Fixed in code, committed and pushed (`fb7cedd`). Not redeployed: the `railway up` attempt was blocked by the session's permission rules (production deploy), and the Railway trial has also expired (see the entry above).

### Changes
- `vocal_bot.py`: `logging.getLogger("httpx").setLevel(logging.WARNING)`. Verified that httpx INFO logging is suppressed; `test_vocal_bot.py` passes.
- Commit `fb7cedd` "Stop logging the bot token" is pushed to origin/main. It sits on top of `c627f79` (the Sunday 14:00 schedule change, committed by the other session).

### Decisions
- The leaked token must be replaced, not just hidden: it's in past Railway logs and in this session's transcript.
- Railway has no GitHub source (`source: null`), so pushing doesn't deploy; it needs `railway up`.

### Next Steps (user)
1. ~~Revoke the token~~: the user decided to keep the current token (2026-10-11). If the bot ever posts something unexpected, revoke it in @BotFather.
2. ~~Set the new token on Railway~~: not needed, since the token is kept.
3. Pick a Railway plan (trial expired), then deploy with `railway up --service vocalbot --detach`. Check that the logs show only the startup lines (no `getUpdates` URLs) and `Added job "SUN 14:00"`.

# Vocal Practice Telegram Bot

A Telegram bot for worship/vocal teams to track 20-minute practice sessions (3× per week), with check-ins, weekly table, leaderboard, personal DM reminders (Pacific Time), undo, streaks, history, and optional Google Sheets mirroring.

## Features
- `/register <Name>` — map your Telegram to a roster name
- `/checkin` → buttons for **Day 1 / Day 2 / Day 3** (20 min)
- `/week` — live weekly table
- `/leaderboard` — weekly standings (0–3)
- `/me` — your progress this week
- `/undo` — undo your last check-in this week
- `/streaks` — consecutive full weeks (3/3)
- `/history` — last 4 weeks summary
- **Personal reminders (Pacific time)**:  
  `/remind MON,WED,FRI 19:30`, `/myreminders`, `/stopreminders`
- `/timezone` — shows reminder timezone (America/Los_Angeles)
- Optional: mirror check-ins to Google Sheets tab **“Checkins”**

## Roster
Edit the `TEAM` list in `vocal_bot.py` to match your singers.

---

## Quick Start (Local)

1. **Bot token**
   - In Telegram → **@BotFather** → `/newbot` → copy `BOT_TOKEN`
   - `/setprivacy` → choose your bot → **Disable** (so it sees commands in groups)

2. **Env & install**
   ```bash
   python -m venv .venv
   source .venv/bin/activate           # Windows: .venv\Scripts\activate
   pip install -r requirements.txt

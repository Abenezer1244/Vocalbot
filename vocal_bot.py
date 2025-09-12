# vocal_bot.py
# Requirements:
#   pip install python-telegram-bot==21.4 python-dotenv==1.0.1
#   # Optional for Google Sheets mirroring:
#   pip install gspread google-auth
#
# .env variables:
#   BOT_TOKEN=YOUR_TELEGRAM_BOT_TOKEN
#   # Optional Google Sheets:
#   # SHEETS_ID=YOUR_GOOGLE_SHEET_ID
#   # GOOGLE_APPLICATION_CREDENTIALS=service_account.json

import os
import sqlite3
import datetime
import logging
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# ---------- Logging ----------
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("vocal-bot")

# ---------- Config & Env ----------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("ERROR: BOT_TOKEN missing in .env")

# Fixed roster (edit if your team changes)
TEAM = ["Isayas", "Sahara", "Zufan", "Mike", "Sami", "Barok", "Betty", "Ruth"]

DEFAULT_MINUTES = 20
DB_PATH = os.getenv("DB_PATH", "progress.db")

# Timezone: lock to Pacific
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    LOCAL_TZ = ZoneInfo("America/Los_Angeles")
    TZ_LABEL = "Pacific (America/Los_Angeles)"
except Exception as e:
    LOCAL_TZ = None
    TZ_LABEL = "Pacific"

# Optional Google Sheets (mirroring check-ins)
SHEETS_ID = os.getenv("SHEETS_ID", "").strip()
GS_ENABLED = False
gc = None
sheet = None
if SHEETS_ID:
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip() or "service_account.json"
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(SHEETS_ID).worksheet("Checkins")
        GS_ENABLED = True
        log.info("Google Sheets mirroring ENABLED.")
    except Exception as e:
        log.warning("Google Sheets not available/failed to init: %s", e)
        GS_ENABLED = False

def log_to_sheet(team_name: str, day: int, minutes: int, ts_iso: str, week_start_iso: str, telegram_id: int) -> None:
    if not GS_ENABLED:
        return
    try:
        sheet.append_row([team_name, f"Day {day}", minutes, ts_iso, week_start_iso, str(telegram_id)])
    except Exception as e:
        log.warning("Failed to append to sheet: %s", e)

# ---------- Database ----------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db() -> None:
    conn = db()
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS users(
      telegram_id INTEGER PRIMARY KEY,
      team_name TEXT NOT NULL
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS checkins(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      telegram_id INTEGER NOT NULL,
      team_name TEXT NOT NULL,
      week_start TEXT NOT NULL,   -- ISO date for Monday of the week
      day INTEGER NOT NULL CHECK(day IN (1,2,3)),
      minutes INTEGER NOT NULL,
      ts TEXT NOT NULL,           -- ISO timestamp
      UNIQUE(telegram_id, week_start, day)
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS reminders(
      telegram_id INTEGER PRIMARY KEY,
      days_csv TEXT NOT NULL,     -- e.g., "MON,WED,FRI"
      hour INTEGER NOT NULL,
      minute INTEGER NOT NULL
    )""")
    conn.commit()
    conn.close()

# ---------- Helpers ----------
WEEKDAY_MAP: Dict[str, int] = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}
REV_WEEKDAY_MAP: Dict[int, str] = {v: k for k, v in WEEKDAY_MAP.items()}

def week_start_iso(d: Optional[datetime.date] = None) -> str:
    d = d or datetime.date.today()
    start = d - datetime.timedelta(days=d.weekday())  # Monday
    return start.isoformat()

def week_end_iso(wk_start_iso: str) -> str:
    start = datetime.date.fromisoformat(wk_start_iso)
    end = start + datetime.timedelta(days=6)
    return end.isoformat()

def kb_days() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Day 1 ✅", callback_data="day:1"),
        InlineKeyboardButton("Day 2 ✅", callback_data="day:2"),
        InlineKeyboardButton("Day 3 ✅", callback_data="day:3"),
    ]])

def parse_days_csv(s: str) -> List[int]:
    days: List[int] = []
    for part in s.split(","):
        d = part.strip().upper()
        if d in WEEKDAY_MAP:
            days.append(WEEKDAY_MAP[d])
    return sorted(set(days))

def normalize_days_to_csv(days: List[int]) -> str:
    return ",".join(REV_WEEKDAY_MAP[d] for d in sorted(days))

def parse_time_hhmm(s: str) -> Optional[Tuple[int, int]]:
    try:
        hh, mm = s.strip().split(":")
        hh_i, mm_i = int(hh), int(mm)
        if 0 <= hh_i <= 23 and 0 <= mm_i <= 59:
            return hh_i, mm_i
    except Exception:
        pass
    return None

def parse_week_rows() -> Dict[str, Dict[int, str]]:
    """Return mapping: name -> {1:'✅'/'  ', 2:..., 3:...} for CURRENT week."""
    wk = week_start_iso()
    status: Dict[str, Dict[int, str]] = {n: {1: "  ", 2: "  ", 3: "  "} for n in TEAM}
    conn = db()
    c = conn.cursor()
    c.execute("SELECT team_name, day FROM checkins WHERE week_start=?", (wk,))
    for name, day in c.fetchall():
        if name in status and day in status[name]:
            status[name][day] = "✅"
    conn.close()
    return status

# ---------- Handlers ----------
async def help_cmd(update: Update, _: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Commands:\n"
        "/register <Name> — Map yourself to roster name\n"
        "/checkin — Log a 20min session (Day 1/2/3)\n"
        "/me — Your progress this week\n"
        "/undo — Remove your last check-in (this week)\n"
        "/week — Live weekly table\n"
        "/leaderboard — Weekly standings\n"
        "/streaks — Consecutive full weeks (3/3)\n"
        "/history — Last 4 weeks summary\n"
        "/remind <DAYS> <HH:MM> — Personal DM reminders in Pacific (e.g., /remind MON,WED,FRI 19:30)\n"
        "/myreminders — Show your reminder schedule\n"
        "/stopreminders — Turn off your personal reminders\n"
        "/timezone — Show the bot reminder timezone\n"
        "(All reminder times are interpreted in Pacific Time.)"
    )

async def start(update: Update, _: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Welcome! Use /register <Name> to join the tracker.\n"
        f"Roster: {', '.join(TEAM)}\n"
        "Then /checkin to log 20min for Day 1/2/3.\n"
        "Try /help to see all commands."
    )

async def timezone_cmd(update: Update, _: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"The bot’s reminder timezone is: {TZ_LABEL}")

async def register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /register <Name>  (e.g., /register Ruth)")
        return
    name = " ".join(context.args).strip()
    if name not in TEAM:
        await update.message.reply_text(f"Name must be one of: {', '.join(TEAM)}")
        return
    conn = db()
    c = conn.cursor()
    c.execute("REPLACE INTO users(telegram_id, team_name) VALUES(?,?)", (update.effective_user.id, name))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"Registered as {name}. Use /checkin to log!")

async def checkin(update: Update, _: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Tap the day you completed (20 min):", reply_markup=kb_days())

async def cb_day(update: Update, _: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    day = int(q.data.split(":")[1])
    user = q.from_user

    conn = db()
    c = conn.cursor()
    c.execute("SELECT team_name FROM users WHERE telegram_id=?", (user.id,))
    row = c.fetchone()
    if not row:
        conn.close()
        await q.edit_message_text("Not registered yet. Use /register <Name> first.")
        return
    team_name = row[0]
    wk = week_start_iso()
    ts = datetime.datetime.now(tz=LOCAL_TZ).isoformat(timespec="seconds") if LOCAL_TZ else \
         datetime.datetime.now().isoformat(timespec="seconds")

    try:
        c.execute(
            """INSERT INTO checkins(telegram_id, team_name, week_start, day, minutes, ts)
               VALUES (?,?,?,?,?,?)""",
            (user.id, team_name, wk, day, DEFAULT_MINUTES, ts),
        )
        conn.commit()
        msg = f"Logged {team_name}: Day {day} ✅ ({DEFAULT_MINUTES} min)"
        log_to_sheet(team_name, day, DEFAULT_MINUTES, ts, wk, user.id)
    except sqlite3.IntegrityError:
        msg = f"{team_name}: Day {day} already logged this week."
    finally:
        conn.close()

    await q.edit_message_text(msg)

async def week(update: Update, _: ContextTypes.DEFAULT_TYPE):
    wk = week_start_iso()
    end = week_end_iso(wk)
    status = parse_week_rows()
    lines = [
        f"*Week {wk} → {end} — Vocal Practice*",
        "_Goal: 20min × 3 sessions_",
        "",
        "`Name       | D1 | D2 | D3`",
        "`-----------+----+----+---`",
    ]
    for n in TEAM:
        d1, d2, d3 = status[n][1], status[n][2], status[n][3]
        lines.append(f"`{n:<10} | {d1} | {d2} | {d3}`")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def leaderboard(update: Update, _: ContextTypes.DEFAULT_TYPE):
    wk = week_start_iso()
    scores: Dict[str, int] = {n: 0 for n in TEAM}
    conn = db()
    c = conn.cursor()
    c.execute(
        """SELECT team_name, COUNT(DISTINCT day) FROM checkins
           WHERE week_start=? GROUP BY team_name""",
        (wk,),
    )
    for name, cnt in c.fetchall():
        if name in scores:
            scores[name] = min(3, cnt)
    conn.close()

    ordered = sorted(scores.items(), key=lambda x: (-x[1], x[0]))
    out = ["*Weekly Leaderboard*", "_3/3 = full week_", ""]
    for i, (n, s) in enumerate(ordered, 1):
        out.append(f"{i}. {n} — {s}/3")
    await update.message.reply_text("\n".join(out), parse_mode=ParseMode.MARKDOWN)

async def me(update: Update, _: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    wk = week_start_iso()
    conn = db()
    c = conn.cursor()
    c.execute("SELECT team_name FROM users WHERE telegram_id=?", (user.id,))
    row = c.fetchone()
    if not row:
        await update.message.reply_text("Not registered yet. Use /register <Name>.")
        conn.close()
        return
    name = row[0]
    c.execute("SELECT DISTINCT day FROM checkins WHERE telegram_id=? AND week_start=?", (user.id, wk))
    days = sorted([d for (d,) in c.fetchall()])
    conn.close()
    done = ", ".join([f"Day {d}" for d in days]) if days else "None yet"
    await update.message.reply_text(f"{name} progress this week: {len(days)}/3\nCompleted: {done}")

async def undo(update: Update, _: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    wk = week_start_iso()
    conn = db()
    c = conn.cursor()
    c.execute(
        """SELECT id, day, ts FROM checkins
           WHERE telegram_id=? AND week_start=?
           ORDER BY ts DESC LIMIT 1""",
        (user.id, wk),
    )
    row = c.fetchone()
    if not row:
        await update.message.reply_text("No check-ins to undo this week.")
        conn.close()
        return
    checkin_id, day, ts = row
    c.execute("DELETE FROM checkins WHERE id=?", (checkin_id,))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"Removed your last check-in: Day {day} ({ts})")

async def streaks(update: Update, _: ContextTypes.DEFAULT_TYPE):
    conn = db()
    c = conn.cursor()
    streak_map: Dict[str, int] = {}
    for name in TEAM:
        streak = 0
        c.execute(
            """SELECT week_start, COUNT(DISTINCT day)
               FROM checkins WHERE team_name=?
               GROUP BY week_start ORDER BY week_start DESC""",
            (name,),
        )
        for wk, cnt in c.fetchall():
            if cnt == 3:
                streak += 1
            else:
                break
        streak_map[name] = streak
    conn.close()

    lines = ["*Streaks (consecutive full weeks)*", ""]
    for n in TEAM:
        lines.append(f"{n}: {streak_map[n]} 🔥")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def history(update: Update, _: ContextTypes.DEFAULT_TYPE):
    N = 4
    conn = db()
    c = conn.cursor()
    c.execute("SELECT DISTINCT week_start FROM checkins ORDER BY week_start DESC LIMIT ?", (N,))
    weeks = [wk for (wk,) in c.fetchall()]
    lines: List[str] = [f"*History (last {N} weeks)*", ""]
    for wk in weeks:
        lines.append(f"Week {wk} → {week_end_iso(wk)}")
        for n in TEAM:
            c.execute(
                """SELECT COUNT(DISTINCT day) FROM checkins
                   WHERE team_name=? AND week_start=?""",
                (n, wk),
            )
            cnt = c.fetchone()[0]
            lines.append(f"  {n}: {cnt}/3")
        lines.append("")
    conn.close()
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

# ---------- Personal Reminders (DM) ----------
from datetime import time as dtime

async def personal_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    await context.bot.send_message(
        chat_id=chat_id,
        text="🎶 Friendly reminder: aim for 20 minutes today. Use /checkin when you finish!"
    )

def schedule_user_reminders(application: Application, telegram_id: int, days: List[int], hour: int, minute: int) -> None:
    # Remove existing jobs for this user
    for j in application.job_queue.get_jobs_by_name(f"rem-{telegram_id}"):
        j.schedule_removal()

    # Time with Pacific TZ
    t = dtime(hour=hour, minute=minute, tzinfo=LOCAL_TZ) if LOCAL_TZ else dtime(hour=hour, minute=minute)

    # Schedule daily jobs per weekday (DM)
    for wd in days:
        application.job_queue.run_daily(
            personal_reminder_job,
            time=t,
            days=(wd,),
            name=f"rem-{telegram_id}",
            chat_id=telegram_id,
        )

def restore_all_user_reminders(application: Application) -> None:
    conn = db()
    c = conn.cursor()
    c.execute("SELECT telegram_id, days_csv, hour, minute FROM reminders")
    rows = c.fetchall()
    conn.close()
    for telegram_id, days_csv, hour, minute in rows:
        days = parse_days_csv(days_csv)
        if days:
            schedule_user_reminders(application, int(telegram_id), int(hour), int(minute))

async def remind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /remind <DAYS> <HH:MM>
    Example: /remind MON,WED,FRI 19:30
    """
    user = update.effective_user
    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage: /remind <DAYS> <HH:MM>\n"
            "Example: /remind TUE,THU,SAT 19:00\n"
            f"Times are interpreted in {TZ_LABEL}."
        )
        return

    days_arg = context.args[0]
    time_arg = context.args[1]

    days = parse_days_csv(days_arg)
    if not days:
        await update.message.reply_text("Invalid DAYS. Use comma-separated weekdays like MON,WED,FRI.")
        return

    hm = parse_time_hhmm(time_arg)
    if not hm:
        await update.message.reply_text("Invalid time. Use 24h HH:MM, e.g., 19:30")
        return
    hour, minute = hm

    # Save to DB
    conn = db()
    c = conn.cursor()
    c.execute(
        """REPLACE INTO reminders(telegram_id, days_csv, hour, minute)
           VALUES (?,?,?,?)""",
        (user.id, normalize_days_to_csv(days), hour, minute),
    )
    conn.commit()
    conn.close()

    # Schedule jobs
    schedule_user_reminders(context.application, user.id, days, hour, minute)

    day_labels = ",".join(REV_WEEKDAY_MAP[d] for d in days)
    await update.message.reply_text(
        f"✅ Personal reminders set: {day_labels} at {hour:02d}:{minute:02d} {TZ_LABEL}. I’ll DM you on those days."
    )

async def myreminders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    conn = db()
    c = conn.cursor()
    c.execute("SELECT days_csv, hour, minute FROM reminders WHERE telegram_id=?", (user.id,))
    row = c.fetchone()
    conn.close()
    if not row:
        await update.message.reply_text("You don’t have personal reminders set. Use /remind <DAYS> <HH:MM>.")
        return
    days_csv, hour, minute = row
    await update.message.reply_text(
        f"Your reminders: {days_csv} at {hour:02d}:{minute:02d} {TZ_LABEL} (sent via DM)."
    )

async def stopreminders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    conn = db()
    c = conn.cursor()
    c.execute("DELETE FROM reminders WHERE telegram_id=?", (user.id,))
    conn.commit()
    conn.close()

    for j in context.application.job_queue.get_jobs_by_name(f"rem-{user.id}"):
        j.schedule_removal()

    await update.message.reply_text("🛑 Your personal reminders are turned off.")

# ---------- Main ----------
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("timezone", timezone_cmd))
    app.add_handler(CommandHandler("register", register))
    app.add_handler(CommandHandler("checkin", checkin))
    app.add_handler(CommandHandler("week", week))
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("me", me))
    app.add_handler(CommandHandler("undo", undo))
    app.add_handler(CommandHandler("streaks", streaks))
    app.add_handler(CommandHandler("history", history))
    app.add_handler(CommandHandler("remind", remind))
    app.add_handler(CommandHandler("myreminders", myreminders))
    app.add_handler(CommandHandler("stopreminders", stopreminders))
    app.add_handler(CallbackQueryHandler(cb_day, pattern=r"^day:\d$"))

    # Restore per-user reminder jobs from DB
    restore_all_user_reminders(app)

    log.info("Bot running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=["message", "callback_query", "chat_member", "my_chat_member"])

if __name__ == "__main__":
    main()

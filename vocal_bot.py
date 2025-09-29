# vocal_bot.py
import os
import sqlite3
import datetime
import logging
import math
import random
import asyncio



from tokenize import Name
from typing import Dict, List, Tuple, Optional

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    JobQueue,
)

# ---------------- Logging ----------------
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("vocal-bot")


# ----- XP / Levels -----
XP_PER_CHECKIN = int(os.getenv("XP_PER_CHECKIN", "10"))
XP_BONUS_FULL_WEEK = int(os.getenv("XP_BONUS_FULL_WEEK", "30"))
XP_BONUS_STREAK_4 = int(os.getenv("XP_BONUS_STREAK_4", "40"))  # bonus when hitting 4-week full streak

# Simple level curve: total XP thresholds for levels
LEVEL_THRESHOLDS = [0, 50, 120, 210, 320, 450, 600, 800, 1050]  # L1..L9

def level_for_xp(xp: int) -> int:
    lvl = 1
    for i, th in enumerate(LEVEL_THRESHOLDS, start=1):
        if xp >= th:
            lvl = i
    return lvl

def next_threshold(xp: int) -> int:
    for th in LEVEL_THRESHOLDS:
        if xp < th:
            return th
    return LEVEL_THRESHOLDS[-1]

def xp_progress_bar(xp: int, width: int = 12) -> str:
    cur_lvl = level_for_xp(xp)
    prev_th = 0 if cur_lvl <= 1 else LEVEL_THRESHOLDS[cur_lvl - 1 - 0]
    nxt_th = next_threshold(xp)
    span = max(1, nxt_th - prev_th)
    filled = int((xp - prev_th) / span * width)
    return "█" * filled + "░" * (width - filled)

# ----- Badges (codes + titles) -----
BADGE_TITLES = {
    "FIRST_FULL_WEEK": "First 3/3 ✅",
    "FOUR_WEEK_STREAK": "4-Week Streak 🔥",
    "EARLY_BIRD": "Early Bird (Monday start) 🌅",
    "COMEBACK": "Comeback (back after a zero week) 💪",
}




# ---------------- Env & Config ----------------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN missing. Set it in your environment or .env.")

# Team roster (edit to your singers)
TEAM = ["Isayas", "Sahara", "Zufan", "Mike", "Sami", "Barok", "Betty", "Ruth"]

# Minutes per session (override with env if desired)
DEFAULT_MINUTES = int(os.getenv("DEFAULT_MINUTES", "20"))


# ...
VIDEOS_PER_PAGE = int(os.getenv("VIDEOS_PER_PAGE", "8"))  # change 8 → 10/12 if you like


# Timezone (Pacific)
try:
    from zoneinfo import ZoneInfo  # py>=3.9
    LOCAL_TZ = ZoneInfo("America/Los_Angeles")
    TZ_LABEL = "Pacific (America/Los_Angeles)"
except Exception:
    LOCAL_TZ = None
    TZ_LABEL = "Pacific"

# ----- Admins (comma-separated Telegram user IDs in ENV ADMIN_IDS) -----
ADMIN_IDS = {
    int(x.strip())
    for x in (os.getenv("ADMIN_IDS", "").replace(";", ",").split(","))
    if x.strip().isdigit()
}
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# Quiet group mode: send /checkin buttons via DM, keep group tidy
GROUP_QUIET_MODE = True

# Local Windows default (adjust if you want); Render should override with DB_PATH
DB_DIR_LOCAL = r"C:\Users\Windows\OneDrive - Seattle Colleges\Desktop\Vocalbot"
os.makedirs(DB_DIR_LOCAL, exist_ok=True)
DB_PATH = os.getenv("DB_PATH", os.path.join(DB_DIR_LOCAL, "progress.db"))

# Ensure DB directory exists (works on Render and local)
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

# Detect Render Web Service (webhook path)
def on_render_web_service() -> bool:
    return bool(os.getenv("PORT") and (os.getenv("RENDER_EXTERNAL_URL") or os.getenv("WEBHOOK_BASE")))

# ---------------- Optional Google Sheets ----------------
SHEETS_ID = os.getenv("SHEETS_ID", "").strip()
GS_ENABLED = False
gc = None

if SHEETS_ID:
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
        if not creds_path:
            raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS env var is not set")
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        gc = gspread.authorize(creds)
        GS_ENABLED = True
        log.info("Google Sheets mirroring ENABLED.")
    except Exception as e:
        log.warning("Google Sheets not available/failed to init: %s", e)
        GS_ENABLED = False



GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "0") or "0")

# Group broadcast texts
MONDAY_9AM_MSG = (
    "🌅 *Happy Monday, team!* Let’s kick off the week strong.\n"
    "Aim for your first 20-min practice today. If you’re joining *tonight at 7:00 PM PT*, "
    "warm up with /videos and log it with /checkin ✅"
)

SATURDAY_9AM_MSG = (
    "🎯 *Saturday focus!* Set aside *5:00 PM PT* for a solid 20-min practice.\n"
    "Finish strong and grab that 3/3!  /checkin  |  /videos"
)

# Public-domain (KJV) verses focused on singing, praise, and encouragement
BIBLE_VERSES = [
    ("Psalm 96:1", "O sing unto the LORD a new song: sing unto the LORD, all the earth."),
    ("Psalm 95:1", "O come, let us sing unto the LORD: let us make a joyful noise to the rock of our salvation."),
    ("Psalm 33:3", "Sing unto him a new song; play skilfully with a loud noise."),
    ("Psalm 13:6", "I will sing unto the LORD, because he hath dealt bountifully with me."),
    ("Psalm 108:1", "O God, my heart is fixed; I will sing and give praise, even with my glory."),
    ("Psalm 57:7", "My heart is fixed, O God, my heart is fixed: I will sing and give praise."),
    ("Psalm 47:6", "Sing praises to God, sing praises: sing praises unto our King, sing praises."),
    ("Psalm 28:7", "The LORD is my strength and my shield; my heart trusted in him... and with my song will I praise him."),
    ("Psalm 42:8", "In the night his song shall be with me, and my prayer unto the God of my life."),
    ("Psalm 149:1", "Praise ye the LORD. Sing unto the LORD a new song, and his praise in the congregation of saints."),
    ("Psalm 100:1–2", "Make a joyful noise unto the LORD... Serve the LORD with gladness: come before his presence with singing."),
    ("1 Chr 16:23", "Sing unto the LORD, all the earth; shew forth from day to day his salvation."),
    ("Isaiah 40:31", "They that wait upon the LORD shall renew their strength; they shall mount up with wings as eagles."),
    ("Zephaniah 3:17", "The LORD thy God in the midst of thee is mighty; he will rejoice over thee with singing."),
    ("Ephesians 5:19", "Speaking to yourselves in psalms and hymns and spiritual songs, singing and making melody in your heart to the Lord."),
    ("Colossians 3:16", "Let the word of Christ dwell in you richly... singing with grace in your hearts to the Lord."),
    ("Philippians 4:13", "I can do all things through Christ which strengtheneth me."),
    ("Hebrews 13:15", "Let us offer the sacrifice of praise to God continually, that is, the fruit of our lips giving thanks to his name."),
    ("James 5:13", "Is any merry? let him sing psalms."),
    ("Psalm 118:24", "This is the day which the LORD hath made; we will rejoice and be glad in it."),
]





# --- Sheets helpers & worksheets (created on demand) ---
def _open_sheet():
    if not GS_ENABLED:
        return None
    return gc.open_by_key(SHEETS_ID)

def _get_ws(title: str):
    if not GS_ENABLED:
        return None
    sh = _open_sheet()
    try:
        return sh.worksheet(title)
    except Exception:
        try:
            return sh.add_worksheet(title=title, rows=2000, cols=12)
        except Exception:
            return None

def _append_row(ws, row: List):
    if ws:
        ws.append_row(row)

def _get_records(ws) -> List[Dict]:
    if not ws:
        return []
    try:
        return ws.get_all_records()
    except Exception:
        return []

WS_CHECKINS = None
WS_ARCHIVE = None
WS_USERS = None
WS_REMINDERS = None
WS_VIDEOS = None
WS_XP = None
WS_BADGES = None
WS_PROGRAMS = None
WS_PROG_ENROLL = None
if GS_ENABLED:
    WS_CHECKINS = _get_ws("Checkins")    # [team_name, "Day N", minutes, ts_iso, week_start_iso, telegram_id]
    WS_USERS = _get_ws("Users")          # [telegram_id, team_name]
    WS_REMINDERS = _get_ws("Reminders")  # [telegram_id, days_csv, hour, minute]
    WS_VIDEOS = _get_ws("Videos")        # [title, url, tags, duration]
    WS_ARCHIVE = _get_ws("CheckinsArchive")
    WS_XP = _get_ws("XP")
    WS_BADGES = _get_ws("Badges")
    WS_PROGRAMS = _get_ws("Programs")
    WS_PROG_ENROLL = _get_ws("ProgramEnrollments")

    # Archived Checkins (keep history even if Checkins is cleaned)
    if GS_ENABLED and WS_ARCHIVE:
        for r in _get_records(WS_ARCHIVE):
            try:
                name = str(r.get("team_name", "")).strip()
                day_s = str(r.get("day", "")).strip().replace("Day", "").strip()
                day = int(day_s) if day_s.isdigit() else None
                minutes = int(str(r.get("minutes", "20")).strip() or "20")
                ts = str(r.get("ts", "")).strip() or str(r.get("timestamp", "")).strip()
                wk = str(r.get("week_start", "")).strip()
                tg = int(str(r.get("telegram_id", "")).strip()) if r.get("telegram_id") else None
                if name and day in (1, 2, 3) and wk:
                    c.execute("""INSERT OR IGNORE INTO checkins
                                 (telegram_id, team_name, week_start, day, minutes, ts)
                                 VALUES (?,?,?,?,?,?)""",
                              (tg, name, wk, day, minutes, ts or ""))
            except Exception:
                pass



def ensure_xp_header():
    if GS_ENABLED and WS_XP:
        vals = WS_XP.get_all_values()
        if not vals:
            WS_XP.update("A1:E1", [["telegram_id","team_name","xp","level","last_badge"]])

def ensure_badges_header():
    if GS_ENABLED and WS_BADGES:
        vals = WS_BADGES.get_all_values()
        if not vals:
            WS_BADGES.update("A1:F1", [["telegram_id","team_name","badge_code","badge_title","awarded_ts","week_start"]])

def ensure_program_headers():
    if GS_ENABLED and WS_PROGRAMS:
        vals = WS_PROGRAMS.get_all_values()
        if not vals:
            WS_PROGRAMS.update("A1:F1", [["program","step","title","url","tags","duration"]])
    if GS_ENABLED and WS_PROG_ENROLL:
        vals2 = WS_PROG_ENROLL.get_all_values()
        if not vals2:
            WS_PROG_ENROLL.update("A1:C1", [["telegram_id","program","step_index"]])


def ensure_videos_header():
    if not GS_ENABLED or not WS_VIDEOS:
        return
    try:
        vals = WS_VIDEOS.get_all_values()
        if not vals:
            WS_VIDEOS.update("A1:D1", [["title","url","tags","duration"]])
    except Exception:
        pass

def load_videos() -> List[Dict]:
    if not GS_ENABLED or not WS_VIDEOS:
        return []
    try:
        rows = _get_records(WS_VIDEOS)
        out = []
        for r in rows:
            title = str(r.get("title","")).strip()
            url = str(r.get("url","")).strip()
            tags_raw = str(r.get("tags","")).strip()
            duration = str(r.get("duration","")).strip()
            if title and url:
                tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
                out.append({"title": title, "url": url, "tags": tags, "duration": duration})
        return out
    except Exception:
        return []

def log_to_sheet(team_name: str, day: int, minutes: int, ts_iso: str, week_start_iso: str, telegram_id: int):
    if not GS_ENABLED:
        return
    _append_row(WS_CHECKINS, [team_name, f"Day {day}", minutes, ts_iso, week_start_iso, str(telegram_id)])

# ---------------- Database ----------------
def db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    conn = db()
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS xp(
      telegram_id INTEGER PRIMARY KEY,
      team_name TEXT NOT NULL,
      xp INTEGER NOT NULL DEFAULT 0,
      level INTEGER NOT NULL DEFAULT 1,
      last_badge TEXT
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS badges(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      telegram_id INTEGER NOT NULL,
      team_name TEXT NOT NULL,
      badge_code TEXT NOT NULL,
      badge_title TEXT NOT NULL,
      awarded_ts TEXT NOT NULL,
      week_start TEXT
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS program_enrollments(
      telegram_id INTEGER PRIMARY KEY,
      program TEXT NOT NULL,
      step_index INTEGER NOT NULL
    )""")

    conn.commit()

    # Ensure 'local_date' column exists (older DBs might not have it)
    cols = {row[1] for row in c.execute("PRAGMA table_info(checkins)").fetchall()}
    if "local_date" not in cols:
        c.execute("ALTER TABLE checkins ADD COLUMN local_date TEXT")
        conn.commit()

    # Backfill local_date where missing, using Pacific time if available
    rows = c.execute("SELECT id, ts FROM checkins WHERE local_date IS NULL OR local_date=''").fetchall()
    for cid, ts in rows:
        local_date = None
        try:
            dt = datetime.datetime.fromisoformat(ts)
            if LOCAL_TZ:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=LOCAL_TZ)
                else:
                    dt = dt.astimezone(LOCAL_TZ)
            local_date = dt.date().isoformat()
        except Exception:
            # fallback: best-effort date parse
            local_date = ts.split("T")[0] if "T" in ts else ts[:10]
        c.execute("UPDATE checkins SET local_date=? WHERE id=?", (local_date, cid))
    conn.commit()

    # Create a UNIQUE daily index so a user can log at most once per calendar day
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_checkins_daily ON checkins(telegram_id, local_date)")
        conn.commit()
    except Exception as e:
        # If historic duplicates exist, index creation can fail; the code-level guard still prevents new ones.
        log.warning("Could not create daily unique index (existing duplicates?). Guard will be enforced in code. %s", e)

    conn.close()

# ---------------- Helpers ----------------
WEEKDAY_MAP: Dict[str, int] = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}
REV_WEEKDAY_MAP: Dict[int, str] = {v: k for k, v in WEEKDAY_MAP.items()}

def week_start_iso(d: Optional[datetime.date] = None) -> str:
    d = d or datetime.date.today()
    start = d - datetime.timedelta(days=d.weekday())  # Monday
    return start.isoformat()

def week_end_iso(wk: str) -> str:
    start = datetime.date.fromisoformat(wk)
    return (start + datetime.timedelta(days=6)).isoformat()

def kb_days() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Day 1 ✅", callback_data="day:1"),
        InlineKeyboardButton("Day 2 ✅", callback_data="day:2"),
        InlineKeyboardButton("Day 3 ✅", callback_data="day:3"),
    ]])

def _compact_filter_token(q: str) -> str:
    """Pack a short, callback-safe token from the filter (<= ~40 chars)."""
    token = q.lower().strip().replace(" ", "+")
    token = "".join(ch for ch in token if ch.isalnum() or ch in "+-_")
    return token[:40]

def _expand_filter_token(t: str) -> str:
    return (t or "").replace("+", " ").strip()

def _filter_videos_by_query(all_vids: List[Dict], q: str) -> List[Dict]:
    q = (q or "").lower().strip()
    if not q:
        return all_vids
    return [
        v for v in all_vids
        if q in v["title"].lower() or any(q in t.lower() for t in v["tags"])
    ]

def _archive_and_clear_week(week_iso: str):
    """Copy last week's rows from Checkins -> CheckinsArchive, then clear them (and DB)."""
    # Sheets side
    if GS_ENABLED and WS_CHECKINS:
        try:
            # Ensure archive sheet exists
            arch = WS_ARCHIVE or _get_ws("CheckinsArchive")
            raw = WS_CHECKINS.get_all_values()  # includes header
            rows_to_delete = []
            payload = []
            for idx, r in enumerate(raw[1:], start=2):
                if len(r) >= 5 and r[4].strip() == week_iso:
                    # r: [team_name, day, minutes, ts, week_start, telegram_id?]
                    # Normalize length to 6 columns
                    row = r + [""] * max(0, 6 - len(r))
                    payload.append(row[:6])
                    rows_to_delete.append(idx)
            # Append to archive
            if payload:
                arch.append_rows(payload)
            # Delete from bottom up to keep indices valid
            for i in reversed(rows_to_delete):
                WS_CHECKINS.delete_rows(i)
        except Exception as e:
            log.warning(f"Archive step failed: {e}")

    # DB side (optional cleanup)
    try:
        conn = db(); c = conn.cursor()
        c.execute("DELETE FROM checkins WHERE week_start=?", (week_iso,))
        conn.commit(); conn.close()
    except Exception as e:
        log.warning(f"DB cleanup for {week_iso} failed: {e}")


def get_or_create_xp(telegram_id: int, team_name: str) -> Tuple[int,int,str]:
    conn = db(); c = conn.cursor()
    c.execute("SELECT xp, level, last_badge FROM xp WHERE telegram_id=?", (telegram_id,))
    row = c.fetchone()
    if not row:
        c.execute("INSERT INTO xp(telegram_id,team_name,xp,level,last_badge) VALUES (?,?,?,?,?)",
                  (telegram_id, team_name, 0, 1, None))
        conn.commit()
        xp, lvl, lb = 0, 1, None
    else:
        xp, lvl, lb = row
    conn.close()
    return xp, lvl, lb

def set_xp(telegram_id: int, team_name: str, xp: int, level: int, last_badge: Optional[str]):
    conn = db(); c = conn.cursor()
    c.execute("""REPLACE INTO xp(telegram_id,team_name,xp,level,last_badge)
                 VALUES (?,?,?,?,?)""", (telegram_id, team_name, xp, level, last_badge))
    conn.commit(); conn.close()
    # mirror to Sheets
    if GS_ENABLED and WS_XP:
        ensure_xp_header()
        rows = _get_records(WS_XP)
        found = False
        for idx, r in enumerate(rows, start=2):
            if str(r.get("telegram_id","")).strip() == str(telegram_id):
                WS_XP.update(f"A{idx}:E{idx}", [[str(telegram_id), team_name, xp, level, last_badge or ""]])
                found = True; break
        if not found:
            WS_XP.append_row([str(telegram_id), team_name, xp, level, last_badge or ""])

def award_badge(telegram_id: int, team_name: str, code: str, wk: Optional[str] = None) -> bool:
    """Return True if newly awarded (i.e., not duplicate)."""
    title = BADGE_TITLES.get(code, code)
    ts = datetime.datetime.now(tz=LOCAL_TZ).isoformat(timespec="seconds") if LOCAL_TZ else datetime.datetime.now().isoformat(timespec="seconds")
    conn = db(); c = conn.cursor()
    c.execute("SELECT 1 FROM badges WHERE telegram_id=? AND badge_code=?", (telegram_id, code))
    if c.fetchone():
        conn.close()
        return False
    c.execute("""INSERT INTO badges(telegram_id,team_name,badge_code,badge_title,awarded_ts,week_start)
                 VALUES (?,?,?,?,?,?)""",(telegram_id, team_name, code, title, ts, wk))
    conn.commit(); conn.close()
    # mirror
    if GS_ENABLED and WS_BADGES:
        ensure_badges_header()
        WS_BADGES.append_row([str(telegram_id), team_name, code, title, ts, wk or ""])
    # store last_badge in xp table
    xp, lvl, _ = get_or_create_xp(telegram_id, team_name)
    set_xp(telegram_id, team_name, xp, lvl, code)
    return True

def maybe_award_condition_badges(telegram_id: int, team_name: str, wk: str, done_days_for_week: List[int], weekly_streak_full: int, was_zero_last_week: bool, monday_logged: bool) -> List[str]:
    earned = []
    if set(done_days_for_week) == {1,2,3}:
        if award_badge(telegram_id, team_name, "FIRST_FULL_WEEK", wk):
            earned.append("FIRST_FULL_WEEK")
    if weekly_streak_full >= 4:
        if award_badge(telegram_id, team_name, "FOUR_WEEK_STREAK", wk):
            earned.append("FOUR_WEEK_STREAK")
    if monday_logged:
        if award_badge(telegram_id, team_name, "EARLY_BIRD", wk):
            earned.append("EARLY_BIRD")
    if was_zero_last_week and set(done_days_for_week) == {1,2,3}:
        if award_badge(telegram_id, team_name, "COMEBACK", wk):
            earned.append("COMEBACK")
    return earned


def load_programs() -> Dict[str, List[Dict]]:
    """Return {program_name: [steps...]}; each step is a dict with title,url,tags,duration."""
    if not GS_ENABLED or not WS_PROGRAMS:
        return {}
    ensure_program_headers()
    rows = _get_records(WS_PROGRAMS)
    prog: Dict[str, List[Dict]] = {}
    for r in rows:
        name = str(r.get("program","")).strip()
        try:
            step = int(str(r.get("step","")).strip())
        except Exception:
            continue
        title = str(r.get("title","")).strip()
        url = str(r.get("url","")).strip()
        tags = str(r.get("tags","")).strip()
        duration = str(r.get("duration","")).strip()
        if name and step and url:
            prog.setdefault(name, []).append({"step": step, "title": title, "url": url, "tags": tags, "duration": duration})
    for k in list(prog.keys()):
        prog[k] = sorted(prog[k], key=lambda x: x["step"])
    return prog

def get_enrollment(telegram_id: int) -> Optional[Tuple[str,int]]:
    conn = db(); c = conn.cursor()
    c.execute("SELECT program, step_index FROM program_enrollments WHERE telegram_id=?", (telegram_id,))
    row = c.fetchone(); conn.close()
    return (row[0], row[1]) if row else None

def set_enrollment(telegram_id: int, program: str, step_index: int):
    conn = db(); c = conn.cursor()
    c.execute("""REPLACE INTO program_enrollments(telegram_id,program,step_index)
                 VALUES (?,?,?)""",(telegram_id, program, step_index))
    conn.commit(); conn.close()
    if GS_ENABLED and WS_PROG_ENROLL:
        ensure_program_headers()
        rows = _get_records(WS_PROG_ENROLL)
        found = False
        for idx, r in enumerate(rows, start=2):
            if str(r.get("telegram_id","")).strip() == str(telegram_id):
                WS_PROG_ENROLL.update(f"A{idx}:C{idx}", [[str(telegram_id), program, step_index]])
                found = True; break
        if not found:
            WS_PROG_ENROLL.append_row([str(telegram_id), program, step_index])

async def programs_cmd(update: Update, _: ContextTypes.DEFAULT_TYPE):
    progs = load_programs()
    if not progs:
        await update.message.reply_text("No programs yet. Ask a leader to add rows in the *Programs* sheet.")
        return
    names = sorted(progs.keys())
    lines = ["*Available Programs*", ""]
    for n in names:
        lines.append(f"• {n}  ({len(progs[n])} steps)")
    lines += ["", "Start one:", "`/program_start <ProgramName>`"]
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def program_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /program_start <ProgramName>")
        return
    name = " ".join(context.args).strip()
    progs = load_programs()
    if name not in progs:
        await update.message.reply_text("Program not found. See `/programs`.")
        return
    set_enrollment(update.effective_user.id, name, 1)
    step = progs[name][0]
    label = step["title"] or f"Step 1"
    await update.message.reply_text(
        f"🎓 *Program started:* {name}\n*Step 1:* {label}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"▶️ {label}", url=step["url"])]])
    )

async def program_next(update: Update, _: ContextTypes.DEFAULT_TYPE):
    enroll = get_enrollment(update.effective_user.id)
    if not enroll:
        await update.message.reply_text("You’re not enrolled in a program. Use `/programs` then `/program_start <Name>`.")
        return
    name, idx = enroll
    progs = load_programs()
    steps = progs.get(name, [])
    if not steps:
        await update.message.reply_text("This program has no steps. Ask a leader to fill the *Programs* sheet.")
        return
    if idx >= len(steps):
        await update.message.reply_text(f"🎉 You’ve finished *{name}*! Use `/programs` to pick another.")
        return
    step = steps[idx]
    set_enrollment(update.effective_user.id, name, idx+1)
    label = step["title"] or f"Step {idx+1}"
    await update.message.reply_text(
        f"*{name} — Step {idx+1}:* {label}",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"▶️ {label}", url=step['url'])]])
    )

async def program_status(update: Update, _: ContextTypes.DEFAULT_TYPE):
    enroll = get_enrollment(update.effective_user.id)
    if not enroll:
        await update.message.reply_text("No current program. Use `/programs` to see options.")
        return
    name, idx = enroll
    progs = load_programs()
    total = len(progs.get(name, []))
    await update.message.reply_text(f"🎓 Program: *{name}*  —  Step {idx}/{total}", parse_mode=ParseMode.MARKDOWN)

async def program_stop(update: Update, _: ContextTypes.DEFAULT_TYPE):
    conn = db(); c = conn.cursor()
    c.execute("DELETE FROM program_enrollments WHERE telegram_id=?", (update.effective_user.id,))
    conn.commit(); conn.close()
    if GS_ENABLED and WS_PROG_ENROLL:
        rows = _get_records(WS_PROG_ENROLL)
        for idx, r in enumerate(rows, start=2):
            if str(r.get("telegram_id","")).strip() == str(update.effective_user.id):
                WS_PROG_ENROLL.delete_rows(idx); break
    await update.message.reply_text("Stopped your current program. Use `/programs` to start another.")


def _clear_week_no_archive(week_iso: str):
    """Delete last week's rows from Checkins and DB without archiving (not recommended)."""
    if GS_ENABLED and WS_CHECKINS:
        try:
            raw = WS_CHECKINS.get_all_values()
            rows_to_delete = []
            for idx, r in enumerate(raw[1:], start=2):
                if len(r) >= 5 and r[4].strip() == week_iso:
                    rows_to_delete.append(idx)
            for i in reversed(rows_to_delete):
                WS_CHECKINS.delete_rows(i)
        except Exception as e:
            log.warning(f"Sheet clear failed: {e}")
    try:
        conn = db(); c = conn.cursor()
        c.execute("DELETE FROM checkins WHERE week_start=?", (week_iso,))
        conn.commit(); conn.close()
    except Exception as e:
        log.warning(f"DB clear failed: {e}")


from datetime import time as dtime

def schedule_week_rollover(application: Application):
    """Run every Monday 00:05 Pacific: archive/clear the *previous* week."""
    def _job_selector(context: ContextTypes.DEFAULT_TYPE):
        today = datetime.date.today()
        this_week_start = today - datetime.timedelta(days=today.weekday())  # Monday
        last_week_start = (this_week_start - datetime.timedelta(days=7)).isoformat()
        if AUTO_ARCHIVE_PREV_WEEK:
            _archive_and_clear_week(last_week_start)
        elif AUTO_CLEAR_PREV_WEEK:
            _clear_week_no_archive(last_week_start)
        else:
            # neither enabled: do nothing
            pass

    jq = ensure_job_queue(application)
    jq.run_daily(
        _job_selector,
        time=dtime(hour=0, minute=5, tzinfo=LOCAL_TZ),  # 00:05 Pacific, cushion for midnight transitions
        days=(0,),  # Monday
        name="weekly-rollover",
    )


def _build_videos_page(vids: List[Dict], page: int, q_token: str) -> Tuple[str, InlineKeyboardMarkup]:
    total = len(vids)
    pages = max(1, math.ceil(total / VIDEOS_PER_PAGE))
    page = max(0, min(page, pages - 1))
    start, end = page * VIDEOS_PER_PAGE, min(total, (page + 1) * VIDEOS_PER_PAGE)

    rows: List[List[InlineKeyboardButton]] = []
    for v in vids[start:end]:
        label = v["title"]
        if v.get("duration"):
            label = f"{label} ({v['duration']})"
        rows.append([InlineKeyboardButton(text=f"▶️ {label}", url=v["url"])])

    # nav buttons
    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton("« Prev", callback_data=f"vidpg:{page-1}:{q_token}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Next »", callback_data=f"vidpg:{page+1}:{q_token}"))
    if nav:
        rows.append(nav)

    # header text
    info = f"Total: {total} • Page {page+1}/{pages}"
    return info, InlineKeyboardMarkup(rows)



def parse_days_csv(s: str) -> List[int]:
    out: List[int] = []
    for part in s.split(","):
        d = part.strip().upper()
        if d in WEEKDAY_MAP:
            out.append(WEEKDAY_MAP[d])
    return sorted(set(out))

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
    wk = week_start_iso()
    status: Dict[str, Dict[int, str]] = {n: {1: "  ", 2: "  ", 3: "  "} for n in TEAM}
    conn = db(); c = conn.cursor()
    c.execute("SELECT team_name, day FROM checkins WHERE week_start=?", (wk,))
    for name, day in c.fetchall():
        if name in status and day in status[name]:
            status[name][day] = "✅"
    conn.close()
    return status

# ---------------- Hydration from Sheets ----------------
def hydrate_from_sheets():
    if not GS_ENABLED:
        return
    conn = db(); c = conn.cursor()

    # Users
    for r in _get_records(WS_USERS):
        try:
            tg = int(str(r.get("telegram_id", "")).strip())
            name = str(r.get("team_name", "")).strip()
            if tg and name:
                c.execute("REPLACE INTO users(telegram_id, team_name) VALUES(?,?)", (tg, name))
        except Exception:
            pass

    # Checkins
    for r in _get_records(WS_CHECKINS):
        try:
            name = str(r.get("team_name", "")).strip()
            day_s = str(r.get("day", "")).strip().replace("Day", "").strip()
            day = int(day_s) if day_s.isdigit() else None
            minutes = int(str(r.get("minutes", "20")).strip() or "20")
            ts = str(r.get("ts", "")).strip() or str(r.get("timestamp", "")).strip()
            wk = str(r.get("week_start", "")).strip()
            tg = int(str(r.get("telegram_id", "")).strip()) if r.get("telegram_id") else None
            if name and day in (1, 2, 3) and wk:
                c.execute("""INSERT OR IGNORE INTO checkins
                             (telegram_id, team_name, week_start, day, minutes, ts)
                             VALUES (?,?,?,?,?,?)""",
                          (tg, name, wk, day, minutes, ts or ""))
        except Exception:
            pass

    # Reminders
    for r in _get_records(WS_REMINDERS):
        try:
            tg = int(str(r.get("telegram_id", "")).strip())
            days_csv = str(r.get("days_csv", "")).strip()
            hour = int(str(r.get("hour", "19")).strip() or "19")
            minute = int(str(r.get("minute", "0")).strip() or "0")
            if tg and days_csv:
                c.execute("""REPLACE INTO reminders(telegram_id, days_csv, hour, minute)
                             VALUES (?,?,?,?)""", (tg, days_csv, hour, minute))
        except Exception:
            pass

        # XP
    if GS_ENABLED and WS_XP:
        ensure_xp_header()
        for r in _get_records(WS_XP):
            try:
                tg = int(str(r.get("telegram_id","")).strip())
                name = str(r.get("team_name","")).strip()
                xp = int(str(r.get("xp","0")).strip() or "0")
                lvl = int(str(r.get("level","1")).strip() or "1")
                last_badge = str(r.get("last_badge","")).strip() or None
                if tg and name:
                    c.execute("""REPLACE INTO xp(telegram_id,team_name,xp,level,last_badge)
                                 VALUES (?,?,?,?,?)""",(tg,name,xp,lvl,last_badge))
            except Exception:
                pass

    # Badges (history)
    if GS_ENABLED and WS_BADGES:
        ensure_badges_header()
        for r in _get_records(WS_BADGES):
            try:
                tg = int(str(r.get("telegram_id","")).strip())
                name = str(r.get("team_name","")).strip()
                code = str(r.get("badge_code","")).strip()
                title = str(r.get("badge_title","")).strip() or BADGE_TITLES.get(code, code)
                ts = str(r.get("awarded_ts","")).strip()
                wk = str(r.get("week_start","")).strip()
                if tg and name and code and ts:
                    c.execute("""INSERT OR IGNORE INTO badges(telegram_id,team_name,badge_code,badge_title,awarded_ts,week_start)
                                 VALUES (?,?,?,?,?,?)""",(tg,name,code,title,ts,wk))
            except Exception:
                pass

    # Program enrollments
    if GS_ENABLED and WS_PROG_ENROLL:
        ensure_program_headers()
        for r in _get_records(WS_PROG_ENROLL):
            try:
                tg = int(str(r.get("telegram_id","")).strip())
                program = str(r.get("program","")).strip()
                step_index = int(str(r.get("step_index","1")).strip() or "1")
                if tg and program:
                    c.execute("""REPLACE INTO program_enrollments(telegram_id,program,step_index)
                                 VALUES (?,?,?)""",(tg,program,step_index))
            except Exception:
                pass


    conn.commit(); conn.close()
    log.info("Hydrated local state from Google Sheets.")

async def chatid(update: Update, _: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Chat ID: {update.effective_chat.id}")



async def _send_group_text(context: ContextTypes.DEFAULT_TYPE):
    text = context.job.data.get("text", "")
    if GROUP_CHAT_ID:
        try:
            await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
        except Exception:
            pass

async def _send_bible_verse(context: ContextTypes.DEFAULT_TYPE):
    if not GROUP_CHAT_ID or not BIBLE_VERSES:
        return
    # Pick by ISO week number so it rotates predictably week to week
    try:
        wk = datetime.date.today().isocalendar()[1]  # 1..53
    except Exception:
        wk = int(datetime.date.today().strftime("%U"))  # fallback
    ref, verse = BIBLE_VERSES[wk % len(BIBLE_VERSES)]
    text = f"📖 *Weekly encouragement*\n_{ref}_\n“{verse}”"
    try:
        await context.bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text=text,
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        pass


def schedule_group_broadcasts(application: Application):
    jq = ensure_job_queue(application)
    if not GROUP_CHAT_ID:
        log.warning("GROUP_CHAT_ID not set; group broadcasts disabled.")
        return

    # Monday 09:00 PT (heads-up for 7pm)
    jq.run_daily(
        _send_group_text,
        time=dtime(hour=9, minute=0, tzinfo=LOCAL_TZ),
        days=(0,),  # Monday
        name="grp-mon-9am",
        data={"text": MONDAY_9AM_MSG},
    )

    # Saturday 09:00 PT (heads-up for 5pm)
    jq.run_daily(
        _send_group_text,
        time=dtime(hour=9, minute=0, tzinfo=LOCAL_TZ),
        days=(5,),  # Saturday
        name="grp-sat-9am",
        data={"text": SATURDAY_9AM_MSG},
    )

    # Weekly Bible verse — Sunday 07:30 PT
    jq.run_daily(
        _send_bible_verse,
        time=dtime(hour=7, minute=30, tzinfo=LOCAL_TZ),
        days=(6,),  # Sunday
        name="grp-sun-verse",
    )


async def _dm_nudge(context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "👋 *Quick practice nudge*: Aim for a 20-minute session today or tomorrow.\n"
        "Log it with /checkin and browse ideas with /videos. You’ve got this! 🎶"
    )
    conn = db(); c = conn.cursor()
    c.execute("SELECT telegram_id, team_name FROM users")
    rows = c.fetchall(); conn.close()
    for uid, _name in rows:
        try:
            await context.bot.send_message(chat_id=uid, text=msg, parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(0.05)  # be gentle with rate limits
        except Exception:
            # user may not have /start'd the bot yet
            pass

def schedule_individual_nudges(application: Application):
    jq = ensure_job_queue(application)
    # Tue & Thu at 10:00 PT (adjust if you prefer)
    jq.run_daily(_dm_nudge, time=dtime(hour=10, minute=0, tzinfo=LOCAL_TZ), days=(1,), name="dm-tue")
    jq.run_daily(_dm_nudge, time=dtime(hour=10, minute=0, tzinfo=LOCAL_TZ), days=(3,), name="dm-thu")



# ---------------- Handlers ----------------
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
        "/remind <DAYS> <HH:MM> — Personal DM reminders (Pacific)\n"
        "/myreminders — Show your reminder schedule\n"
        "/stopreminders — Turn off your reminders\n"
        "/timezone — Show reminder timezone\n"
        "/videos [filter] — Browse practice videos (e.g., /videos warmup)\n"
        "/addvideo <title> | <url> | [tags] | [duration]  (admin)\n"
        "/delvideo <url>  or  /delvideo --all <url>      (admin)\n"
        "/whoami — Show your Telegram ID\n" 
        "/roster — who is registered (names)\n" 
        "/programs — list programs\n" 
        "/program_start <Name> — enroll & get Step 1\n" 
        "/program_next — next step\n" 
        "/program_status — where you are\n" 
        "/program_stop — leave the program\n" 
        "/roster_ids — registered names + IDs (admin)\n"
        "/nocheckins — who hasn’t checked in this week\n"
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

async def whoami(update: Update, _: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(f"Your Telegram ID: {u.id}\nName: {u.full_name}")

async def register(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /register <Name>  (e.g., /register Ruth)")
        return
    name = " ".join(context.args).strip()
    if name not in TEAM:
        await update.message.reply_text(f"Name must be one of: {', '.join(TEAM)}")
        return
    conn = db(); c = conn.cursor()
    c.execute("REPLACE INTO users(telegram_id, team_name) VALUES(?,?)", (update.effective_user.id, name))
    conn.commit(); conn.close()
    await update.message.reply_text(f"Registered as {name}. Use /checkin to log!")

    # Sync to Sheets
    if GS_ENABLED and WS_USERS:
        tg = update.effective_user.id
        rows = _get_records(WS_USERS)
        found = False
        for idx, r in enumerate(rows, start=2):
            if str(r.get("telegram_id", "")).strip() == str(tg):
                WS_USERS.update(f"A{idx}:B{idx}", [[str(tg), name]])
                found = True
                break
        if not found:
            _append_row(WS_USERS, [str(tg), name])

# --- Quiet-mode helpers for group check-ins ---
async def _delete_after(context):
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    msg_id = data.get("msg_id")
    try:
        await context.bot.delete_message(chat_id, msg_id)
    except Exception:
        pass

async def checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat

    # Always try to send buttons via DM (quiet)
    try:
        await context.bot.send_message(
            chat_id=user.id,
            text="Tap the day you completed (20 min):",
            reply_markup=kb_days(),
            disable_notification=True,
            allow_sending_without_reply=True,
        )
    except Exception:
        await update.message.reply_text(
            "Please DM me first with /start, then try /checkin again.",
            disable_notification=True,
        )
        return

    # If triggered from a group, keep it tidy
    if chat.type in ("group", "supergroup") and GROUP_QUIET_MODE:
        try:
            await update.message.delete()
        except Exception:
            pass
        try:
            m = await context.bot.send_message(
                chat_id=chat.id,
                text=f"📩 Sent a check-in to {user.first_name} via DM.",
                disable_notification=True,
            )
            context.job_queue.run_once(_delete_after, when=5, data={"chat_id": chat.id, "msg_id": m.message_id})
        except Exception:
            pass

async def cb_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    day = int(q.data.split(":")[1])
    user = q.from_user

    conn = db(); c = conn.cursor()
    c.execute("SELECT team_name FROM users WHERE telegram_id=?", (user.id,))
    row = c.fetchone()
    if not row:
        conn.close()
        await q.edit_message_text("Not registered yet. Use /register <Name> first.")
        return
    team_name = row[0]
    wk = week_start_iso()

    # Timestamp & Pacific local date
    now = datetime.datetime.now(tz=LOCAL_TZ) if LOCAL_TZ else datetime.datetime.now()
    ts = now.isoformat(timespec="seconds")
    local_date = now.date().isoformat()

    # Daily guard
    c.execute("SELECT 1 FROM checkins WHERE telegram_id=? AND local_date=?", (user.id, local_date))
    if c.fetchone():
        conn.close()
        await q.edit_message_text(f"You’ve already checked in today ({local_date}). Please come back tomorrow.")
        return

    # Consecutive order guard
    c.execute("SELECT DISTINCT day FROM checkins WHERE telegram_id=? AND week_start=?", (user.id, wk))
    done_days = sorted([d for (d,) in c.fetchall()])
    next_needed = (max(done_days) + 1) if done_days else 1
    if day != next_needed:
        conn.close()
        await q.edit_message_text(
            f"Let’s go in order this week. Next up is *Day {next_needed}*.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # Insert
    try:
        c.execute("""INSERT INTO checkins(telegram_id, team_name, week_start, day, minutes, ts, local_date)
                     VALUES (?,?,?,?,?,?,?)""",(user.id, team_name, wk, day, DEFAULT_MINUTES, ts, local_date))
        conn.commit()
        logged_ok = True
    except sqlite3.IntegrityError:
        logged_ok = False

    # Count this week after insert
    c.execute("SELECT COUNT(DISTINCT day) FROM checkins WHERE telegram_id=? AND week_start=?", (user.id, wk))
    cnt = c.fetchone()[0]

    # Compute streak & monday flag & zero-last-week for badges
    # Weekly full streak
    streak_full = 0
    c.execute("""SELECT week_start, COUNT(DISTINCT day) FROM checkins
                 WHERE team_name=? GROUP BY week_start ORDER BY week_start DESC""", (team_name,))
    for wk_i, n_days in c.fetchall():
        if n_days == 3:
            streak_full += 1
        else:
            break
    # Was last week zero?
    today = now.date()
    this_week_start = today - datetime.timedelta(days=today.weekday())
    last_week_start = (this_week_start - datetime.timedelta(days=7)).isoformat()
    c.execute("""SELECT COUNT(*) FROM checkins WHERE team_name=? AND week_start=?""", (team_name, last_week_start))
    was_zero_last_week = (c.fetchone()[0] == 0)
    # Monday logged?
    monday_logged = (now.weekday() == 0)

    conn.close()

    # Log to Sheets
    if logged_ok:
        log_to_sheet(team_name, day, DEFAULT_MINUTES, ts, wk, user.id)

    # --- XP & badges ---
    # Base XP for each check-in
    xp, lvl, _lb = get_or_create_xp(user.id, team_name)
    before_lvl = lvl
    xp += XP_PER_CHECKIN
    lvl = level_for_xp(xp)
    set_xp(user.id, team_name, xp, lvl, None)

    # Maybe award badges based on conditions
    earned_codes = maybe_award_condition_badges(
        user.id, team_name, wk, sorted(done_days + ([day] if logged_ok else [])),
        weekly_streak_full=streak_full, was_zero_last_week=was_zero_last_week, monday_logged=monday_logged
    )

    # DM the reward card
    bar = xp_progress_bar(xp)
    nxt = next_threshold(xp)
    dm_lines = [
        f"🏅 *Great job, {team_name}!* You logged *Day {day}*.",
        f"+{XP_PER_CHECKIN} XP  |  Total: *{xp} XP*  |  Level: *{lvl}*",
        f"`{bar}`  _Next level at {nxt} XP_",
    ]
    if earned_codes:
        badges_txt = ", ".join([BADGE_TITLES.get(c, c) for c in earned_codes])
        dm_lines.append(f"🎖️ *Badge unlocked:* {badges_txt}")
    try:
        await context.bot.send_message(chat_id=user.id, text="\n".join(dm_lines), parse_mode=ParseMode.MARKDOWN)
    except Exception:
        pass

    # Edit the original button message
    msg = f"Logged {team_name}: Day {day} ✅ ({DEFAULT_MINUTES} min)" if logged_ok else f"{team_name}: Day {day} already logged this week."
    await q.edit_message_text(msg)

    # Group celebration: finishing 3/3
    if cnt == 3 and GROUP_CHAT_ID:
        try:
            party = random.choice(["🎉", "🙌", "✨", "🎊", "🔥"])
            extra = ""
            if "FIRST_FULL_WEEK" in earned_codes:
                extra = " — *First full week!* 🥇"
            text = f"{party} *{team_name}* just completed all 3 practices this week! {extra}"
            await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=text, parse_mode=ParseMode.MARKDOWN)
            # XP bonus for full week
            xp2, lvl2, _ = get_or_create_xp(user.id, team_name)
            xp2 += XP_BONUS_FULL_WEEK
            new_lvl = level_for_xp(xp2)
            set_xp(user.id, team_name, xp2, new_lvl, None)
            if new_lvl > lvl2:
                try:
                    await context.bot.send_message(chat_id=user.id,
                        text=f"✨ *Weekly bonus:* +{XP_BONUS_FULL_WEEK} XP\nLevel up! → *Level {new_lvl}*", parse_mode=ParseMode.MARKDOWN)
                except Exception:
                    pass
        except Exception:
            pass


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

async def roster(update: Update, _: ContextTypes.DEFAULT_TYPE):
    """Show who is registered (names only)."""
    conn = db(); c = conn.cursor()
    c.execute("SELECT team_name FROM users")
    names = sorted([row[0] for row in c.fetchall()])
    conn.close()

    registered = set(names)
    missing = [n for n in TEAM if n not in registered]

    lines = [f"*Registered* ({len(names)}/{len(TEAM)})"]
    lines.append(", ".join(names) if names else "_none_")
    if missing:
        lines += ["", "*Not yet registered*", ", ".join(missing)]
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def roster_ids(update: Update, _: ContextTypes.DEFAULT_TYPE):
    """Admins-only: show registered names with Telegram IDs."""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Sorry, admins only.")
        return

    conn = db(); c = conn.cursor()
    c.execute("SELECT team_name, telegram_id FROM users ORDER BY team_name")
    rows = c.fetchall()
    conn.close()

    lines = [f"*Registered IDs* ({len(rows)}/{len(TEAM)})", ""]
    if not rows:
        lines.append("_none_")
    else:
        for name, tid in rows:
            lines.append(f"{name}: `{tid}`")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def nocheckins(update: Update, _: ContextTypes.DEFAULT_TYPE):
    """Who hasn't logged anything *this week*."""
    wk = week_start_iso()
    conn = db(); c = conn.cursor()
    c.execute("SELECT DISTINCT team_name FROM checkins WHERE week_start=?", (wk,))
    done = {name for (name,) in c.fetchall()}
    conn.close()

    missing = [n for n in TEAM if n not in done]
    if missing:
        await update.message.reply_text("Not yet checked in this week: " + ", ".join(missing))
    else:
        await update.message.reply_text("Everyone has checked in this week 🎉")



async def leaderboard(update: Update, _: ContextTypes.DEFAULT_TYPE):
    wk = week_start_iso()
    scores: Dict[str, int] = {n: 0 for n in TEAM}
    conn = db(); c = conn.cursor()
    c.execute("""SELECT team_name, COUNT(DISTINCT day) FROM checkins
                 WHERE week_start=? GROUP BY team_name""", (wk,))
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
    conn = db(); c = conn.cursor()
    c.execute("SELECT team_name FROM users WHERE telegram_id=?", (user.id,))
    row = c.fetchone()
    if not row:
        conn.close()
        await update.message.reply_text("Not registered yet. Use /register <Name>.")
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
    conn = db(); c = conn.cursor()
    c.execute("""SELECT id, day, ts FROM checkins
                 WHERE telegram_id=? AND week_start=?
                 ORDER BY ts DESC LIMIT 1""", (user.id, wk))
    row = c.fetchone()
    if not row:
        conn.close()
        await update.message.reply_text("No check-ins to undo this week.")
        return
    checkin_id, day, ts = row
    c.execute("DELETE FROM checkins WHERE id=?", (checkin_id,))
    conn.commit(); conn.close()
    await update.message.reply_text(f"Removed your last check-in: Day {day} ({ts})")

async def streaks(update: Update, _: ContextTypes.DEFAULT_TYPE):
    conn = db(); c = conn.cursor()
    streak_map: Dict[str, int] = {}
    for name in TEAM:
        streak = 0
        c.execute("""SELECT week_start, COUNT(DISTINCT day)
                     FROM checkins WHERE team_name=?
                     GROUP BY week_start ORDER BY week_start DESC""", (name,))
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
    conn = db(); c = conn.cursor()
    c.execute("SELECT DISTINCT week_start FROM checkins ORDER BY week_start DESC LIMIT ?", (N,))
    weeks = [wk for (wk,) in c.fetchall()]
    lines: List[str] = [f"*History (last {N} weeks)*", ""]
    for wk in weeks:
        lines.append(f"Week {wk} → {week_end_iso(wk)}")
        for n in TEAM:
            c.execute("""SELECT COUNT(DISTINCT day) FROM checkins
                         WHERE team_name=? AND week_start=?""", (n, wk))
            cnt = c.fetchone()[0]
            lines.append(f"  {n}: {cnt}/3")
        lines.append("")
    conn.close()
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

# ---------------- Personal Reminders ----------------
from datetime import time as dtime

async def personal_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    await context.bot.send_message(
        chat_id=chat_id,
        text="🎶 Friendly reminder: aim for 20 minutes today. Use /checkin when you finish!"
    )

def ensure_job_queue(application: Application) -> JobQueue:
    jq = application.job_queue
    if jq is None:
        jq = JobQueue()
        jq.set_application(application)
        jq.start()
        application.job_queue = jq
    return jq

def schedule_user_reminders(application: Application, telegram_id: int, days: List[int], hour: int, minute: int):
    jq = ensure_job_queue(application)

    # Clear old jobs for this user
    for j in jq.get_jobs_by_name(f"rem-{telegram_id}"):
        j.schedule_removal()

    t = dtime(hour=hour, minute=minute, tzinfo=LOCAL_TZ) if LOCAL_TZ else dtime(hour=hour, minute=minute)

    for wd in days:
        jq.run_daily(
            personal_reminder_job,
            time=t,
            days=(wd,),
            name=f"rem-{telegram_id}",
            chat_id=telegram_id,
        )

def restore_all_user_reminders(application: Application):
    conn = db(); c = conn.cursor()
    c.execute("SELECT telegram_id, days_csv, hour, minute FROM reminders")
    rows = c.fetchall(); conn.close()
    for telegram_id, days_csv, hour, minute in rows:
        days = parse_days_csv(days_csv)
        if days:
            schedule_user_reminders(application, int(telegram_id), days, int(hour), int(minute))

async def remind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage: /remind <DAYS> <HH:MM>\nExample: /remind MON,WED,FRI 19:30\nTimes are interpreted in Pacific."
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

    conn = db(); c = conn.cursor()
    c.execute("""REPLACE INTO reminders(telegram_id, days_csv, hour, minute)
                 VALUES (?,?,?,?)""", (user.id, normalize_days_to_csv(days), hour, minute))
    conn.commit(); conn.close()

    schedule_user_reminders(context.application, user.id, days, hour, minute)

    # Sync to Sheets
    if GS_ENABLED and WS_REMINDERS:
        tg = user.id
        rows = _get_records(WS_REMINDERS)
        payload = [str(tg), normalize_days_to_csv(days), hour, minute]
        found = False
        for idx, r in enumerate(rows, start=2):
            if str(r.get("telegram_id", "")).strip() == str(tg):
                WS_REMINDERS.update(f"A{idx}:D{idx}", [payload])
                found = True
                break
        if not found:
            _append_row(WS_REMINDERS, payload)

    day_labels = ",".join(REV_WEEKDAY_MAP[d] for d in days)
    await update.message.reply_text(
        f"✅ Personal reminders set: {day_labels} at {hour:02d}:{minute:02d} {TZ_LABEL}. I’ll DM you on those days."
    )

async def myreminders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    conn = db(); c = conn.cursor()
    c.execute("SELECT days_csv, hour, minute FROM reminders WHERE telegram_id=?", (user.id,))
    row = c.fetchone(); conn.close()
    if not row:
        await update.message.reply_text("You don’t have personal reminders set. Use /remind <DAYS> <HH:MM>.")
        return
    days_csv, hour, minute = row
    await update.message.reply_text(f"Your reminders: {days_csv} at {hour:02d}:{minute:02d} {TZ_LABEL} (sent via DM).")

async def stopreminders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    conn = db(); c = conn.cursor()
    c.execute("DELETE FROM reminders WHERE telegram_id=?", (user.id,))
    conn.commit(); conn.close()

    jq = ensure_job_queue(context.application)
    for j in jq.get_jobs_by_name(f"rem-{user.id}"):
        j.schedule_removal()

    # Sync to Sheets
    if GS_ENABLED and WS_REMINDERS:
        tg = user.id
        rows = _get_records(WS_REMINDERS)
        for idx, r in enumerate(rows, start=2):
            if str(r.get("telegram_id", "")).strip() == str(tg):
                WS_REMINDERS.delete_rows(idx)
                break

    await update.message.reply_text("🛑 Your personal reminders are turned off.")

# ---------------- Videos ----------------
async def videos_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not GS_ENABLED or not WS_VIDEOS:
        await update.message.reply_text(
            "🎬 Practice videos aren’t set up yet.\n"
            "Ask your leader to enable Google Sheets & add a 'Videos' tab."
        )
        return

    ensure_videos_header()
    all_vids = load_videos()

    q = " ".join(context.args).strip() if context.args else ""
    vids = _filter_videos_by_query(all_vids, q)

    if not vids:
        await update.message.reply_text("No videos found. Try a different filter or ask your leader to add some.")
        return

    q_token = _compact_filter_token(q)
    info, kb = _build_videos_page(vids, page=0, q_token=q_token)
    heading = "🎵 Practice Videos" if not q else f"🎵 Practice Videos (filter: {q})"
    await update.message.reply_text(
        f"{heading}\n{info}\nTip: try `/videos warmup` or `/videos breath`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb,
        disable_web_page_preview=False
    )

async def videos_page_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    try:
        _, page_str, token = (q.data.split(":", 2) + ["", ""])[:3]
        page = int(page_str)
    except Exception:
        await q.answer()
        return

    # Re-load & re-filter each time (stateless & safe)
    all_vids = load_videos()
    q_text = _expand_filter_token(token)
    vids = _filter_videos_by_query(all_vids, q_text)

    if not vids:
        await q.edit_message_text("No videos found.")
        await q.answer()
        return

    info, kb = _build_videos_page(vids, page=page, q_token=token)
    heading = "🎵 Practice Videos" if not q_text else f"🎵 Practice Videos (filter: {q_text})"
    await q.edit_message_text(
        f"{heading}\n{info}\nTip: try `/videos warmup` or `/videos breath`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb,
        disable_web_page_preview=False
    )
    await q.answer()


async def addvideo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Sorry, admins only. Ask your leader to grant access.")
        return
    if not GS_ENABLED or not WS_VIDEOS:
        await update.message.reply_text("Videos list requires Google Sheets enabled with a 'Videos' tab.")
        return

    ensure_videos_header()
    text = update.message.text or ""
    payload = text.partition(" ")[2].strip()
    if not payload:
        await update.message.reply_text(
            "Usage:\n/addvideo <title> | <url> | [tags] | [duration]\n"
            "Example:\n/addvideo 5-min Lip Trills | https://youtu.be/abc123 | warmup, lip | 5:12"
        )
        return

    parts = [p.strip() for p in payload.split("|")]
    if len(parts) < 2:
        await update.message.reply_text("Please include at least: <title> | <url>")
        return

    title = parts[0]
    url = parts[1]
    tags = parts[2] if len(parts) >= 3 else ""
    duration = parts[3] if len(parts) >= 4 else ""

    if not (url.startswith("http://") or url.startswith("https://")):
        await update.message.reply_text("URL must start with http:// or https://")
        return

    try:
        WS_VIDEOS.append_row([title, url, tags, duration])
        await update.message.reply_text(f"✅ Added video:\n• {title}\n• {url}\n• tags: {tags or '(none)'}\n• duration: {duration or '(n/a)'}")
    except Exception as e:
        await update.message.reply_text(f"Could not add video (Sheets error): {e}")

async def delvideo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("Sorry, admins only.")
        return
    if not GS_ENABLED or not WS_VIDEOS:
        await update.message.reply_text("Videos list requires Google Sheets enabled with a 'Videos' tab.")
        return

    ensure_videos_header()
    args = (context.args or [])
    if not args:
        await update.message.reply_text(
            "Usage:\n"
            "/delvideo <url>\n"
            "or remove every match:\n"
            "/delvideo --all <url>"
        )
        return

    delete_all = False
    if args[0] == "--all":
        delete_all = True
        args = args[1:]

    url = " ".join(args).strip()
    if not url:
        await update.message.reply_text("Please provide the exact URL to delete.")
        return

    try:
        raw = WS_VIDEOS.get_all_values()  # includes header row
        to_delete = []
        for idx, row in enumerate(raw[1:], start=2):  # start=2 because header is row 1
            if len(row) >= 2 and row[1].strip() == url:
                to_delete.append(idx)
                if not delete_all:
                    break
        if not to_delete:
            await update.message.reply_text("No row found with that URL.")
            return
        for r in reversed(to_delete):
            WS_VIDEOS.delete_rows(r)
        if delete_all and len(to_delete) > 1:
            await update.message.reply_text(f"🗑️ Deleted {len(to_delete)} videos with that URL.")
        else:
            await update.message.reply_text("🗑️ Deleted 1 video.")
    except Exception as e:
        await update.message.reply_text(f"Could not delete (Sheets error): {e}")

# ---------------- Main ----------------
def main():
    init_db()
    hydrate_from_sheets()  # rebuild local state from Sheets (no disk needed)

    app = Application.builder().token(BOT_TOKEN).build()

    # Ensure JobQueue exists (needed in webhook mode on Render)
    ensure_job_queue(app)

    # Broadcasts & nudges
    schedule_group_broadcasts(app)
    schedule_individual_nudges(app)

    # Schedule weekly rollover (archive+clear or clear-only)
    schedule_week_rollover(app)


    # Handlers
    app.add_handler(CommandHandler("chatid", chatid))
    app.add_handler(CommandHandler("programs", programs_cmd))
    app.add_handler(CommandHandler("program_start", program_start))
    app.add_handler(CommandHandler("program_next", program_next))
    app.add_handler(CommandHandler("program_status", program_status))
    app.add_handler(CommandHandler("program_stop", program_stop))

    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("timezone", timezone_cmd))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("register", register))
    app.add_handler(CommandHandler("checkin", checkin))
    app.add_handler(CallbackQueryHandler(cb_day, pattern=r"^day:\d$"))
    app.add_handler(CommandHandler("week", week))
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("me", me))
    app.add_handler(CommandHandler("undo", undo))
    app.add_handler(CommandHandler("streaks", streaks))
    app.add_handler(CommandHandler("history", history))
    app.add_handler(CommandHandler("remind", remind))
    app.add_handler(CommandHandler("myreminders", myreminders))
    app.add_handler(CommandHandler("stopreminders", stopreminders))
    app.add_handler(CommandHandler("videos", videos_cmd))
    app.add_handler(CommandHandler("addvideo", addvideo))
    app.add_handler(CommandHandler("delvideo", delvideo))
    app.add_handler(CallbackQueryHandler(videos_page_cb, pattern=r"^vidpg:"))
    app.add_handler(CommandHandler("roster", roster))
    app.add_handler(CommandHandler("roster_ids", roster_ids))
    app.add_handler(CommandHandler("nocheckins", nocheckins))

    # Restore scheduled reminders from DB
    restore_all_user_reminders(app)

    log.info("Bot starting...")

    if on_render_web_service():
        port = int(os.getenv("PORT", "10000"))
        base = os.getenv("WEBHOOK_BASE") or os.getenv("RENDER_EXTERNAL_URL")
        if not base:
            raise SystemExit("WEBHOOK_BASE or RENDER_EXTERNAL_URL not set")
        webhook_url = f"{base.rstrip('/')}/{BOT_TOKEN}"
        log.info(f"Using webhook URL: {webhook_url}")

        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=BOT_TOKEN,      # Telegram will POST to /<token>
            webhook_url=webhook_url, # Public https URL
            allowed_updates=["message", "callback_query", "chat_member", "my_chat_member"],
            drop_pending_updates=True,
        )
    else:
        app.run_polling(
            allowed_updates=["message", "callback_query", "chat_member", "my_chat_member"],
            drop_pending_updates=True,
        )

if __name__ == "__main__":
    main()

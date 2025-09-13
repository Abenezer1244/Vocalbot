# vocal_bot.py
import os
import sqlite3
import datetime
import logging
import math
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
if GS_ENABLED:
    WS_CHECKINS = _get_ws("Checkins")    # [team_name, "Day N", minutes, ts_iso, week_start_iso, telegram_id]
    WS_USERS = _get_ws("Users")          # [telegram_id, team_name]
    WS_REMINDERS = _get_ws("Reminders")  # [telegram_id, days_csv, hour, minute]
    WS_VIDEOS = _get_ws("Videos")        # [title, url, tags, duration]
    WS_ARCHIVE = _get_ws("CheckinsArchive")


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
    CREATE TABLE IF NOT EXISTS users(
      telegram_id INTEGER PRIMARY KEY,
      team_name TEXT NOT NULL
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS checkins(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      telegram_id INTEGER,
      team_name TEXT NOT NULL,
      week_start TEXT NOT NULL,
      day INTEGER NOT NULL CHECK(day IN (1,2,3)),
      minutes INTEGER NOT NULL,
      ts TEXT NOT NULL,
      local_date TEXT
      -- NOTE: uniqueness is enforced via index created below
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS reminders(
      telegram_id INTEGER PRIMARY KEY,
      days_csv TEXT NOT NULL,
      hour INTEGER NOT NULL,
      minute INTEGER NOT NULL
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

    conn.commit(); conn.close()
    log.info("Hydrated local state from Google Sheets.")

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
        "/whoami — Show your Telegram ID\n" \
        "/roster — who is registered (names)\n"
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

async def cb_day(update: Update, _: ContextTypes.DEFAULT_TYPE):
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

    # Current timestamp & local Pacific date
    now = datetime.datetime.now(tz=LOCAL_TZ) if LOCAL_TZ else datetime.datetime.now()
    ts = now.isoformat(timespec="seconds")
    local_date = now.date().isoformat()

    # NEW: block multiple check-ins on the same calendar day
    c.execute("SELECT 1 FROM checkins WHERE telegram_id=? AND local_date=?", (user.id, local_date))
    if c.fetchone():
        conn.close()
        await q.edit_message_text(
            f"You’ve already checked in today ({local_date}). "
            f"Please come back tomorrow to log Day {day}."
        )
        return

    try:
        c.execute(
            """INSERT INTO checkins(telegram_id, team_name, week_start, day, minutes, ts, local_date)
               VALUES (?,?,?,?,?,?,?)""",
            (user.id, team_name, wk, day, DEFAULT_MINUTES, ts, local_date),
        )
        conn.commit()
        msg = f"Logged {team_name}: Day {day} ✅ ({DEFAULT_MINUTES} min)"
        log_to_sheet(team_name, day, DEFAULT_MINUTES, ts, wk, user.id)
    except sqlite3.IntegrityError:
        # This still covers the per-week 'same Day twice' case
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

    # Schedule weekly rollover (archive+clear or clear-only)
    schedule_week_rollover(app)


    # Handlers
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

# vocal_bot.py — posts the team's weekly reminders in the Telegram group automatically. No commands.
import os
import logging
import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telegram.ext import Application, ContextTypes

logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s | %(message)s", level=logging.INFO)
log = logging.getLogger("vocal-bot")

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN missing. Set it in your environment or .env.")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "0") or "0")
if GROUP_CHAT_ID >= 0:  # group/supergroup IDs are negative (e.g. -100…); a positive ID is a person
    raise SystemExit("GROUP_CHAT_ID must be the team group's ID (starts with -100…).")

TZ = ZoneInfo("America/Los_Angeles")

# Automatic group reminders: (day, "HH:MM" Pacific, message). Add a line to add a reminder.
GROUP_SCHEDULE = [
    ("SUN", "14:00", "🎵 Heads-up: please send all 4 songs for this week by Monday!"),
    ("MON", "09:00", "🎵 Songs are due today — please send all 4 songs!"),
    ("THU", "17:00", "🎤 Practice tonight at 7:00 PM!"),
    ("SAT", "15:00", "🎤 Practice today at 5:00 PM!"),
]
DAYS = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]  # PTB run_daily numbers days 0=Sunday

async def send_reminder(context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(GROUP_CHAT_ID, context.job.data)

def schedule_reminders(app: Application):
    for day, hhmm, text in GROUP_SCHEDULE:
        h, m = map(int, hhmm.split(":"))
        app.job_queue.run_daily(send_reminder, time=datetime.time(h, m, tzinfo=TZ),
                                days=(DAYS.index(day),), data=text, name=f"{day} {hhmm}")

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    schedule_reminders(app)
    log.info("Bot starting: %d reminders scheduled for chat %s.", len(GROUP_SCHEDULE), GROUP_CHAT_ID)
    app.run_polling()  # keeps the process alive for the scheduler; there are no handlers

if __name__ == "__main__":
    main()

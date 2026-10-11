# Run: python test_vocal_bot.py   (no network)
# PTB 21 run_daily numbers days 0=Sunday; every reminder must fire on its named weekday and time.
import os, datetime
os.environ.update(BOT_TOKEN="123:fake", GROUP_CHAT_ID="-100123")
import vocal_bot as vb
from telegram.ext import Application

app = Application.builder().token("123:fake").build()
vb.schedule_reminders(app)
jobs = app.job_queue.jobs()
assert len(jobs) == len(vb.GROUP_SCHEDULE)
now = datetime.datetime.now(vb.TZ)
for job in jobs:
    day, hhmm = job.name.split()
    nxt = job.job.trigger.get_next_fire_time(None, now)
    assert (nxt.strftime("%a").upper(), nxt.strftime("%H:%M")) == (day, hhmm), (job.name, nxt)
print("OK")

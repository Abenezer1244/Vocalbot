# Vocal Team Reminder Bot

Posts the team's weekly reminders in the Telegram group automatically. No commands.

| When (Pacific) | Message |
|---|---|
| Sunday 2:00 PM | Send all 4 songs by Monday |
| Monday 9:00 AM | Songs are due today |
| Wednesday 9:00 PM | Leader: have you prepared a word to share? |
| Thursday 5:00 PM | Practice tonight at 7:00 PM |
| Saturday 3:00 PM | Practice today at 5:00 PM |

To add or change a reminder, edit `GROUP_SCHEDULE` at the top of `vocal_bot.py`.

## Settings
- `BOT_TOKEN`: from @BotFather
- `GROUP_CHAT_ID`: the team group's ID (negative, starts with `-100…`)

## Run
```bash
pip install -r requirements.txt
python vocal_bot.py
```
Hosted on Railway (`.railway/railway.ts`), start command `python vocal_bot.py`.

## Test
```bash
python test_vocal_bot.py
```

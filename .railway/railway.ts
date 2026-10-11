import { defineRailway, preserve, project, service } from "railway/iac";

export default defineRailway(() => {
  const vocalbot = service("vocalbot", {
    start: "python vocal_bot.py",
    env: {
      // Secrets are set with `railway variables --set`, never committed here
      BOT_TOKEN: preserve(),
      GROUP_CHAT_ID: preserve(),
    },
  });

  return project("vocalbot", { resources: [vocalbot] });
});

---
name: thefeed-hermes-bridge
description: Use when connecting Hermes to a thefeed DNS chat.
---

# thefeed DNS chat bridge

Lets the owner chat with this Hermes over thefeed (DNS-only messenger) when the internet is cut.
Do the whole setup yourself, then message the owner over thefeed.

## Ask the owner (2 things only)
1. Their `thefeed://` config URI. It MUST contain `sk=` (server key), or chat cannot work.
2. Their own thefeed chat address (20 chars, from the app: Chat -> my address).

**Prefer the owner's own thefeed server.** A bridge polls around the clock, so it should run on a
server the owner (or someone they trust) hosts, not on the shared default servers built into the
app. If they have no server of their own, a built-in default config works too. Tell them it is
shared, and keep polling to what this skill does.

## Setup
```bash
H=${HERMES_HOME:-~/.hermes}; mkdir -p $H/thefeed $H/scripts
V=$(curl -s https://api.github.com/repos/sartoopjj/thefeed/releases/latest | python3 -c 'import sys,json;print(json.load(sys.stdin)["tag_name"])')
case $(uname -m) in x86_64|amd64) A=amd64;; aarch64|arm64) A=arm64;; *) echo "unsupported arch"; exit 1;; esac
curl -L -o $H/thefeed/thefeed-client https://github.com/sartoopjj/thefeed/releases/download/$V/thefeed-client-$V-linux-$A
chmod +x $H/thefeed/thefeed-client
cp <this skill>/scripts/thefeed_bridge.py <this skill>/scripts/thefeed_watchdog.py $H/scripts/
python3 $H/scripts/thefeed_bridge.py setup '<URI>' <OWNER_ADDR>   # prints MY address
```
Verify the binary's SHA-256 against the release page `digest` before running it.
The local client listens on `127.0.0.1:8090`; set `THEFEED_BRIDGE_PORT` if that port is taken.

Cron jobs (names matter: the watchdog finds the bridge by name):
- `thefeed bridge`: schedule `every 1m`, script `thefeed_bridge.py`, continuity=true, deliver local,
  toolsets terminal/file/web/session_search/memory (+cronjob if `cron.allow_agent_scheduling: true`),
  prompt = the block below.
- `thefeed watchdog`: `every 2m`, no_agent=true, script `thefeed_watchdog.py`.

Then: tell the owner your address on the normal channel, and send a first message over thefeed
(`write_file` text -> `python3 $H/scripts/thefeed_bridge.py send <file>`). Ask them to reply; confirm
the reply arrives within ~1 min.

## Bridge prompt
> You are Hermes, talking to your owner over thefeed (DNS-only). They may have NO internet: this is
> their only channel, and nobody can press approval buttons. Output above = your notes + last 100
> messages (OWNER/YOU), new ones marked <== NEW. Continue the conversation; use session_search for
> context from other chats. Get tasks done with the tools you have. If a command is blocked, do not
> try to work around the block: tell the owner what was blocked and why, so they can decide. Never
> say "later when internet is back". To reply: write_file the text to `thefeed/out.txt`, then run
> `thefeed_bridge.py send <that file>` (never put reply text on the command line). Respect the byte
> cap shown above; split long answers. Keep `thefeed/notes.md` (<3KB) for task state. You may compact
> memory with the memory tool only, never delete a fact stored nowhere else, then say what changed.
> Never touch `thefeed_bridge.py`, its cron jobs, or config.yaml.

## Changing the config
If the owner asks (on any channel) to move to another config, re-run `setup` with the new URI and
the same owner address. Your chat address stays the same, and messages already handled are not
replayed. `setup` makes the new config the client's active one: the client only builds chat
servers when the active config changes, so without that the new server would stay unusable.
Replies follow the owner: they go out on the server the owner last wrote from (the new config's
server until they write). A different owner address starts a fresh conversation.

## Edge cases (each one has bitten)
- **Owner only.** The config may be public: strangers can message you. The script reads only the
  owner's thread; others are never opened, logged, answered, or shown to the model. No passphrase
  unlock. New owner address = re-run `setup` by hand on the normal channel.
- **Cursor by `seq` per chat server, not `ts`.** ts has 1-second resolution, so same-second
  messages get dropped. And seq restarts on each chat server, so one global seq would drop every
  message after a config or server change.
- **Lost `state.json`** = everything already in the thread counts as seen. Old messages and old
  `!` commands are not replayed.
- **No approvals in cron** (`approvals.cron_mode: deny` is the default): flagged commands are blocked
  instantly, not queued. Reply text goes through a file because words like `rm -rf` in a reply
  trip the scanner and block the send.
- **Each cron run is a fresh session.** The script feeds the last 100 messages + `notes.md`, and
  continuity=true adds the previous run's output. Without this the chat has no memory.
- **Empty checks cost nothing**: the script prints `{"wakeAgent": false}`, so the model only runs
  when a message arrived.
- **The cron ticker is 60s.** fastpoll (started by the watchdog) polls every 10s for 10 min after a
  message and triggers `hermes cron run` for faster replies.
- **Memory safety.** The watchdog snapshots MEMORY.md / USER.md (20 good + 5 broken copies, never an
  empty file). The owner sends `!restore-memory` (one step back per send) or `!status`; `!` messages
  are handled by the script and never reach the model.
- **Byte cap is set by the server owner** (`/api/chat/availability` -> limits.maxMsgBytes). Never
  hardcode it; Persian is ~2 bytes/char.
- **The client dies on container recreate.** Every check/watchdog restarts it. If the whole Hermes
  container is down, nothing here helps: it must be restarted on the host.
- **Testing:** use a second client (`-port 8091 -data-dir /tmp/tf2`) as the owner. Kill it by PID
  (check `/proc/PID/cmdline`); `pkill -f` can match your own shell.

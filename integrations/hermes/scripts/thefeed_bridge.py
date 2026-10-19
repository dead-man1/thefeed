#!/usr/bin/env python3
"""thefeed DNS chat <-> Hermes bridge. Stdlib only.

  thefeed_bridge.py setup URI OWNER_ADDR  -> import config, create identity, print own address
  thefeed_bridge.py check                 -> cron gate (cron runs it with no args)
  thefeed_bridge.py send FILE             -> send FILE's text to the owner
  thefeed_bridge.py watchdog              -> keep client/fastpoll/bridge alive + memory backups
  thefeed_bridge.py fastpoll              -> long-running: 10s polling for 10 min after a message

Security: ONLY the single OWNER address is ever read. Other threads are not opened, not
logged, not answered, never shown to the model (a public config means strangers can write).
"""
import glob, hashlib, json, os, shutil, socket, subprocess, sys, time, urllib.parse, urllib.request

HOME = os.environ.get("HERMES_HOME") or os.path.expanduser("~/.hermes")
D = f"{HOME}/thefeed"
BIN, PORT = f"{D}/thefeed-client", int(os.environ.get("THEFEED_BRIDGE_PORT", "8090"))
BASE = f"http://127.0.0.1:{PORT}"
CFG, STATE, NOTES = f"{D}/bridge.json", f"{D}/state.json", f"{D}/notes.md"
MEM, BK = f"{HOME}/memories", f"{D}/backups"
HERMES = shutil.which("hermes") or "hermes"
HIST, KEEP, FAST_S, IDLE_S, FAST_WIN = 100, 20, 10, 60, 600


def rjson(p):
    with open(p) as f:
        return json.load(f)


def wjson(p, obj):
    with open(p, "w") as f:
        json.dump(obj, f)


def rtext(p, mode="r"):
    with open(p, mode, **({} if "b" in mode else {"encoding": "utf-8"})) as f:
        return f.read()


def wtext(p, text):
    with open(p, "w", encoding="utf-8") as f:
        f.write(text)


def cfg():
    return rjson(CFG)


def call(method, path, body=None, timeout=120):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode()
    if "data:" in raw:  # /api/chat/send streams SSE: keep the last JSON event
        raw = ([l[5:].strip() for l in raw.splitlines() if l.startswith("data:")] or ["{}"])[-1]
    return json.loads(raw) if raw.strip() else {}


def ensure_client():
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=2).close()
        return
    except OSError:
        pass
    with open(f"{D}/client.log", "a") as log:  # the child keeps its own copy of the fd
        subprocess.Popen([BIN, "-data-dir", f"{D}/data", "-host", "127.0.0.1", "-port", str(PORT), "-open=false"],
                         stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    for _ in range(30):
        time.sleep(1)
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=2).close()
            return
        except OSError:
            pass
    raise SystemExit("thefeed client did not start; see client.log")


def parse_uri(uri):
    """Same fields as the app's importer: domain/key path, sk=, d= (extra domains), r=."""
    body = uri.strip()[len("thefeed://"):]
    path, _, q = body.partition("?")
    dom, key = (urllib.parse.unquote(x) for x in path.split("/")[:2])
    p = dict(kv.split("=", 1) for kv in q.split("&") if "=" in kv)
    split = lambda v: [x.strip() for x in urllib.parse.unquote(v).split(",") if x.strip()]
    return dom, key, p.get("sk", "").strip(), split(p.get("d", "")), split(p.get("r", ""))


# ---------- cursors ----------
# seq is per chat server, so the cursor is a map server -> last seq. A single global seq
# would silently drop every message after a server change (the new server starts at 1).

def srv_of(m, default):
    return m.get("server") or default


def tops(msgs, default):
    t = {}
    for m in msgs:
        if m["dir"] == "in":
            s = srv_of(m, default)
            t[s] = max(t.get(s, 0), m["seq"])
    return t


def load_state(default):
    """None when there is no state (first run after losing it). Old {"seq": n} files migrate."""
    if not os.path.exists(STATE):
        return None
    st = rjson(STATE)
    if "cursors" not in st:
        st = {"cursors": {default: max(st.get("seq", 0), st.get("rescued", 0))}}
    return st


def save_state(st):
    wjson(STATE, st)


def setup(uri, owner):
    dom, key, sk, extra, resolvers = parse_uri(uri)
    if not sk:
        raise SystemExit("URI has no sk= server key: chat cannot work without it")
    os.makedirs(D, exist_ok=True)
    ensure_client()
    call("POST", "/api/profiles", {"action": "create", "profile": {"id": "", "nickname": dom, "config": {
        "domain": dom, "key": key, "serverKey": sk, "extraDomains": extra, "queryMode": "single",
        "rateLimit": 6, "resolvers": resolvers}}})
    me = call("POST", "/api/chat/enable", {"action": "create"})["address"]  # keeps an existing identity
    # The client builds chat servers only when the active config changes, so a config added
    # later stays unusable until we switch to it.
    pl = call("GET", "/api/profiles")
    pid = [p["id"] for p in pl.get("profiles", []) if p["config"]["domain"] == dom][-1]
    if pl.get("active") != pid:
        call("POST", "/api/profiles/switch", {"id": pid})
    if not chat_server_up(dom):
        raise SystemExit(f"chat server {dom} did not come up; see {D}/client.log")
    call("POST", "/api/chat/enable", {"action": "server", "server": dom, "on": True})
    # Re-running setup with a new config keeps cursors. A new owner starts from 0: seqs count
    # per sender, so the old owner's cursors would hide the new owner's first messages.
    # Migrate an old single-seq state under the PREVIOUS server, before cfg is overwritten.
    prev = cfg() if os.path.exists(CFG) else None
    st = None
    if prev and prev["owner"] == owner.strip():
        st = load_state(prev["server"])
    st = st or {"cursors": {}}
    st["reply"] = dom  # until the owner writes again, answer on the config just set up
    wjson(CFG, {"server": dom, "owner": owner.strip(), "me": me})
    os.chmod(CFG, 0o600)
    save_state(st)
    print("my address:", me)


def chat_server_up(dom, wait=90):
    key = dom.strip().rstrip(".").lower()
    for _ in range(wait):
        try:
            if any(x.get("key") == key for x in call("GET", "/api/chat/info").get("servers") or []):
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def owner_thread():
    c = cfg()
    return call("GET", f"/api/chat/messages?peer={urllib.parse.quote(c['owner'])}&markRead=1")


def max_bytes():
    """Server-set cap (owner can change it). A fresh client returns no limits until a forced
    probe (?retry=1) finishes, so retry once and cache 1h; fall back to 500 only if both fail."""
    cache = f"{D}/limits.json"
    try:
        c = rjson(cache)
        if time.time() - c["at"] < 3600:
            return c["max"]
    except (OSError, ValueError, KeyError):
        pass
    for q in ("", "?retry=1"):
        try:
            n = int(call("GET", "/api/chat/availability" + q, timeout=120)["limits"]["maxMsgBytes"])
            wjson(cache, {"at": time.time(), "max": n})
            return n
        except Exception:
            continue
    return 500


def check():
    ensure_client()
    call("POST", "/api/chat/poll", {}, timeout=600)
    c = cfg()
    msgs = owner_thread().get("msgs", [])
    st = load_state(c["server"])
    if st is None:
        # Lost state: take everything as seen. Replaying old messages would re-run old
        # !restore-memory commands and re-answer old questions.
        ins = [m for m in msgs if m["dir"] == "in"]
        save_state({"cursors": tops(msgs, c["server"]), "reply": srv_of(ins[-1], c["server"]) if ins else c["server"]})
        print(json.dumps({"wakeAgent": False}))
        return
    cur = st["cursors"]
    fresh = [m for m in msgs if m["dir"] == "in" and m["seq"] > cur.get(srv_of(m, c["server"]), 0)]
    for s, n in tops(fresh, c["server"]).items():
        cur[s] = max(cur.get(s, 0), n)
    if fresh:
        st["reply"] = srv_of(fresh[-1], c["server"])  # answer where the owner last wrote from
    save_state(st)  # before acting: a crash must not re-run a command or re-wake the model
    rescue([m["text"].strip() for m in fresh if m["text"].startswith("!")])
    new = [m for m in fresh if not m["text"].startswith("!")]
    if not new:
        print(json.dumps({"wakeAgent": False}))  # no model, no tokens
        return
    print(f"thefeed DNS chat. Reply cap {max_bytes()} bytes per message.")
    if os.path.exists(NOTES):
        print("--- your notes ---\n" + rtext(NOTES)[-3000:])
    ids = {id(m) for m in new}
    print(f"--- conversation (last {HIST}, oldest first) ---")
    for m in msgs[-HIST:]:
        if m["dir"] == "in" and m["text"].startswith("!"):
            continue  # bridge commands never reach the model
        who = "OWNER" if m["dir"] == "in" else "YOU"
        print(f"{who}: {m['text']}" + ("  <== NEW" if id(m) in ids else ""))


def send(path, quiet=False):
    text = rtext(path).strip()
    cap = max_bytes()
    if len(text.encode()) > cap:
        raise SystemExit(f"refused: {len(text.encode())} bytes > {cap}; split it")
    c = cfg(); ensure_client()
    # Reply on the server the owner last wrote from. Not the thread's `server` field: that is
    # the conversation's binding and does not follow the owner to a new server.
    server = (load_state(c["server"]) or {}).get("reply") or c["server"]
    r = call("POST", "/api/chat/send", {"peer": c["owner"], "text": text, "server": server}, timeout=300)
    if not quiet:  # quiet inside check(): stdout there is the cron gate, must stay clean
        print(json.dumps(r, ensure_ascii=False))


# ---------- watchdog: no model ----------
def h(p):
    return hashlib.sha256(rtext(p, "rb")).hexdigest() if os.path.exists(p) else ""


def snaps(name):
    return sorted(s for s in glob.glob(f"{BK}/{name}.*") if ".broken-" not in s)


def stamp():
    return f"{time.time_ns():020d}"  # strictly increasing; '%Y%m%d-%H%M%S' alone collides in 1s


def backup():
    os.makedirs(BK, exist_ok=True)
    for name in ("MEMORY.md", "USER.md"):
        src = f"{MEM}/{name}"
        if not os.path.exists(src) or os.path.getsize(src) == 0:
            continue  # never snapshot an emptied file
        if not snaps(name) or h(snaps(name)[-1]) != h(src):
            shutil.copy2(src, f"{BK}/{name}.{stamp()}")
        for old in snaps(name)[:-KEEP] + sorted(glob.glob(f"{BK}/{name}.broken-*"))[:-5]:
            os.remove(old)


def restore():
    """One step back per call, never forward (avoids ping-pong between good and wrecked copies)."""
    done = []
    for name in ("MEMORY.md", "USER.md"):
        dst, s = f"{MEM}/{name}", snaps(name)
        cur = h(dst)
        idx = max([i for i, p in enumerate(s) if h(p) == cur], default=len(s))
        cands = [p for p in s[:idx] if h(p) != cur]
        if cands:
            if os.path.exists(dst):
                shutil.copy2(dst, f"{BK}/{name}.broken-{stamp()}")
            shutil.copy2(cands[-1], dst)
            done.append(name)
    return done


def rescue(cmds):
    for c in cmds:
        if c == "!restore-memory":
            r = restore(); msg = f"restored: {', '.join(r)}" if r else "no older backup"
        elif c == "!status":
            msg = f"ok, backups={len(glob.glob(BK + '/*.md.*'))}"
        else:
            msg = "commands: !status  !restore-memory"
        f = f"{D}/rescue.txt"; wtext(f, msg); send(f, quiet=True)


def pid_alive(pidf):
    try:
        pid = int(rtext(pidf))
    except (OSError, ValueError):
        return False
    if os.path.exists(f"/proc/{pid}/cmdline"):  # Linux: also guards against a reused pid
        return "thefeed_bridge.py" in rtext(f"/proc/{pid}/cmdline", "rb").decode(errors="replace")
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def watchdog():
    backup()
    ensure_client()
    jobs = rjson(f"{HOME}/cron/jobs.json")["jobs"]
    bridge = next((j for j in jobs if j.get("name") == "thefeed bridge"), None)
    if bridge and not bridge.get("enabled", True):
        subprocess.run([HERMES, "cron", "resume", bridge["id"]], capture_output=True, timeout=60)
    if not pid_alive(f"{D}/fastpoll.pid"):
        with open(f"{D}/fastpoll.log", "a") as log:
            subprocess.Popen([sys.executable, __file__, "fastpoll"], stdout=subprocess.DEVNULL,
                             stderr=log, start_new_session=True)


def arrived(before, after):
    """True if any server's newest incoming seq went up (per server: seqs restart on a new server)."""
    return before is not None and any(n > before.get(s, 0) for s, n in after.items())


def due(done, now, fired):
    """Wake when the owner has messages the bridge hasn't handled (vs its saved cursors), once
    per new state. Comparing with the cursors, not a startup snapshot, also catches a message
    that landed while fastpoll was starting."""
    return arrived(done, now) and now != fired


def fastpoll():
    wtext(f"{D}/fastpoll.pid", str(os.getpid()))
    fired, fast_until = None, 0.0
    while True:
        try:
            ensure_client()
            call("POST", "/api/chat/poll", {}, timeout=120)
            c = cfg()
            now = tops(owner_thread().get("msgs", []), c["server"])
            st = load_state(c["server"])
            if st and due(st["cursors"], now, fired):  # no state yet: check() baselines first
                fired, fast_until = now, time.time() + FAST_WIN
                subprocess.run([HERMES, "cron", "run", "thefeed bridge"], capture_output=True, timeout=60)
        except Exception:
            pass  # a DNS hiccup must never kill the loop
        time.sleep(FAST_S if time.time() < fast_until else IDLE_S)


if __name__ == "__main__":
    a = sys.argv[1:] or ["check"]
    {"setup": lambda: setup(a[1], a[2]), "check": check, "send": lambda: send(a[1]),
     "watchdog": watchdog, "fastpoll": fastpoll}[a[0]]()

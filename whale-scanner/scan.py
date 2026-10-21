#!/usr/bin/env python3
"""WhaleYay holder scanner: who holds a perp position on each coin (main dex + HIP-3 sub-dexes).

Hyperliquid has no "who holds coin X" endpoint, so this scans wallet states (clearinghouseState, weight 2) for a large address
set, within a strict time / rate budget, and writes static JSON per coin. Meant for a GitHub Actions cron (every 1-2 h).

Address sources: stats-data leaderboard (all ~48k addresses, with account value), users of recent trades on every market
(recentTrades + a short websocket listen, incl. sub-dex markets), and wallets that held positions in earlier runs.

Usage: python scan.py [--minutes 45] [--out out] [--state state] [--rate 1000] [--ws-seconds 180]
Only standard library, plus the optional `websockets` package for the trade-stream listen.
"""
import argparse, gzip, json, os, random, sys, threading, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

API = "https://api.hyperliquid.xyz/info"
LB_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
WS_URL = "wss://api.hyperliquid.xyz/ws"

ap = argparse.ArgumentParser()
ap.add_argument("--minutes", type=float, default=40, help="time budget for the wallet scan phase")
ap.add_argument("--out", default="out", help="output folder (published: index.json + coins/*.json)")
ap.add_argument("--state", default="state", help="memory carried between runs (state.json.gz)")
ap.add_argument("--rate", type=int, default=1000, help="info API weight per minute (Hyperliquid limit: 1200 per IP)")
ap.add_argument("--ws-seconds", type=int, default=180, help="listen to the live trade stream this long (0 = off)")
ap.add_argument("--conc", type=int, default=8)
args = ap.parse_args()

T0 = time.time(); NOW = int(T0 * 1000)
MIN_AV = 1000            # skip leaderboard accounts below $1k (see README: at <= 40x such accounts hold < $40k)
BIG_TRADE = 10_000       # a trade of >= $10k makes its two users "active" -> scanned this run
KEEP_MIN = 10_000        # per coin file: every position >= $10k ...
KEEP_TOP = 300           # ... plus the 300 largest; the rest is only counted
KEEP_MAX = 1500
STALE_MS = 24 * 3600e3   # holders not re-scanned for 24 h are dropped from the output
DEX_KEEP_MS = 14 * 864e5 # remember a sub-dex for an address for 14 days after the last position / balance on it
FULLDEX_EVERY_MS = 7 * 864e5   # every address gets a full sub-dex sweep at least every 7 days (rotating)
# trader cohorts (documented in README + the site's (i) notes)
# all-time PnL (leaderboard), 7 groups (ids 0..6): >= $1M | $100K-1M | $10K-100K | -$10K..$10K | -$100K..-$10K | -$1M..-$100K | <= -$1M
# (the old scan.py had 6 groups, the last one <= -$100K; the site detects the scheme from "edges"/"pnlGroups" in every file)
PNL_EDGES = [1e6, 1e5, 1e4, -1e4, -1e5, -1e6]
SIZE_EDGES = [1e4, 1e5, 1e6, 1e7]         # account value: < $10K | $10K-100K | $100K-1M | $1M-10M | >= $10M
EDGES = {"p": PNL_EDGES, "s": SIZE_EDGES}   # written into every file that carries cohort ids (pnlCohort / sizeCohort)
def pnl_coh(pl):
    if pl is None: return -1
    for i, e in enumerate(PNL_EDGES[:3]):
        if pl >= e: return i
    for i in range(3, len(PNL_EDGES)):
        if pl > PNL_EDGES[i]: return i
    return len(PNL_EDGES)
def size_coh(av):
    for i, e in enumerate(SIZE_EDGES):
        if av < e: return i
    return len(SIZE_EDGES)
CH_MIN = 1000              # position changes: ignore changes worth < $1k
CH_KEEP_MS = 24 * 3600e3   # keep 24 h of changes in the state
CH_SPAN_MAX = 26 * 3600e3  # a change found more than 26 h after the previous scan of that wallet is not reported (too vague)
stats = {"calls": 0, "weight": 0, "r429": 0, "errors": 0}

# ---------------- rate-limited HTTP ----------------
class Bucket:
    """token bucket on request weight (per minute), shared by all threads; pauses everyone after a 429"""
    def __init__(self, per_min):
        self.rate = per_min / 60.0; self.cap = per_min / 6.0; self.tokens = self.cap / 2; self.t = time.time(); self.lock = threading.Lock(); self.pause_until = 0
    def take(self, w):
        while True:
            with self.lock:
                now = time.time()
                if now < self.pause_until: wait = self.pause_until - now
                else:
                    self.tokens = min(self.cap, self.tokens + (now - self.t) * self.rate); self.t = now
                    if self.tokens >= w: self.tokens -= w; return
                    wait = (w - self.tokens) / self.rate
            time.sleep(min(wait, 2))
    def backoff(self, s):
        with self.lock: self.pause_until = max(self.pause_until, time.time() + s); self.tokens = 0
bucket = Bucket(args.rate)

def post(body, w, tries=5):
    for i in range(tries):
        bucket.take(w); stats["calls"] += 1; stats["weight"] += w
        try:
            req = urllib.request.Request(API, data=json.dumps(body).encode(), headers={"Content-Type": "application/json", "User-Agent": "whaleyay-scanner"})
            with urllib.request.urlopen(req, timeout=25) as r: return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                stats["r429"] += 1; bucket.backoff(min(60, 5 * 2 ** i)); continue
            if e.code >= 500: time.sleep(1 + i); continue
            stats["errors"] += 1; return None
        except Exception:
            time.sleep(1 + i)
    stats["errors"] += 1; return None

def get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "whaleyay-scanner", "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=120) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip": raw = gzip.decompress(raw)
        return json.loads(raw)

def log(*a): print(f"[{time.time()-T0:6.0f}s]", *a, flush=True)

# ---------------- state between runs ----------------
os.makedirs(args.state, exist_ok=True)
SP = os.path.join(args.state, "state.json.gz")
try: state = json.loads(gzip.decompress(open(SP, "rb").read()))
except Exception: state = {}
state.setdefault("v", 1)
A = state.setdefault("a", {})     # addr -> {av, ts (last main scan), dx:{dex:lastSeen}, fd (last full sub-dex sweep), p:{coin:[...]}, pts:{dex:ts}, fs:{coin|side:firstSeen}, src}
state.setdefault("known_dexes", [])
EV = [e for e in state.get("ev", []) if NOW - e[2] * 1000 < CH_KEEP_MS]   # changes: [addr, coin, t(s), tPrev(s), sziBefore, sziAfter, px]
state["ev"] = EV
RUNS = [t for t in state.get("runs", []) if NOW - t * 1000 < 7 * 864e5] + [NOW // 1000]; state["runs"] = RUNS[-100:]
def rec(a): return A.setdefault(a, {"av": 0, "ts": 0, "dx": {}, "fd": 0, "p": {}, "pts": {}, "fs": {}})

# ---------------- 1. markets ----------------
dex_list = [d["name"] for d in (post({"type": "perpDexs"}, 20) or []) if d and d.get("name")]
new_dexes = [d for d in dex_list if d not in state["known_dexes"]] if state["known_dexes"] else []
markets = []   # (coin, dex, dayNtlVlm)
PX = {}        # coin -> mark price (values closed positions in the changes list)
for dex in [""] + dex_list:
    r = post({"type": "metaAndAssetCtxs", **({"dex": dex} if dex else {})}, 20)
    if not r: continue
    for u, c in zip(r[0]["universe"], r[1]):
        if u.get("isDelisted"): continue
        markets.append((u["name"], dex, float(c.get("dayNtlVlm") or 0)))
        try: PX[u["name"]] = float(c.get("markPx") or 0)
        except Exception: pass
log(f"markets: {len(markets)} on {1+len(dex_list)} dexes; new dexes since last run: {new_dexes or '-'}")

# ---------------- 2. addresses ----------------
lb = get_json(LB_URL)["leaderboardRows"]
lb_av = {r["ethAddress"].lower(): float(r["accountValue"] or 0) for r in lb}
def _alltime(r):
    for w in r.get("windowPerformances") or []:
        if w and w[0] == "allTime":
            try: return float(w[1].get("pnl"))
            except Exception: return None
    return None
lb_pnl = {r["ethAddress"].lower(): _alltime(r) for r in lb}
lb_rank = {a: i + 1 for i, a in enumerate(sorted(lb_av, key=lambda a: -lb_av[a]))}
for a, v in lb_av.items():
    if v >= MIN_AV or a in A:
        r_ = rec(a); r_["av"] = v
        if lb_pnl.get(a) is not None: r_["pl"] = round(lb_pnl[a])
log(f"leaderboard: {len(lb_av)} addresses, {sum(1 for v in lb_av.values() if v >= MIN_AV)} with >= ${MIN_AV}")

active = {}    # addr -> set(dex) seen trading big this run (dex '' = main)
def note_trade(coin, px, sz, users):
    try: ntl = float(px) * float(sz)
    except Exception: return
    if ntl < BIG_TRADE: return
    dex = coin.split(":")[0] if ":" in coin else ""
    for u in users or []: active.setdefault(u.lower(), set()).add(dex)

# recent trades on the most traded markets of every dex (weight 20 each)
by_dex = {}
for m in markets: by_dex.setdefault(m[1], []).append(m)
rt = []
for dex, ms in by_dex.items():
    ms.sort(key=lambda m: -m[2]); rt += ms[: (12 if dex == "" else 2)]
with ThreadPoolExecutor(4) as ex:
    for coin, res in zip([m[0] for m in rt], ex.map(lambda m: post({"type": "recentTrades", "coin": m[0]}, 20), rt)):
        for t in res or []: note_trade(coin, t["px"], t["sz"], t.get("users"))
log(f"recentTrades on {len(rt)} markets -> {len(active)} active addresses (big trades)")

# a short listen to the live trade stream (all markets, no info weight)
if args.ws_seconds > 0:
    try:
        import asyncio, websockets
        async def listen():
            async with websockets.connect(WS_URL, max_size=2**24, ping_interval=20) as ws:
                for coin, _, _ in markets:
                    await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}))
                end = time.time() + args.ws_seconds
                while time.time() < end:
                    try: msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=max(0.1, end - time.time())))
                    except asyncio.TimeoutError: break
                    if msg.get("channel") == "trades":
                        for t in msg["data"]: note_trade(t["coin"], t["px"], t["sz"], t.get("users"))
        n0 = len(active); asyncio.run(listen()); log(f"websocket {args.ws_seconds}s -> +{len(active)-n0} active addresses")
    except ImportError: log("websocket listen skipped (pip install websockets)")
    except Exception as e: log("websocket listen failed:", repr(e)[:120])
for a in active: rec(a)

# ---------------- 3. plan: which (address, dex) calls this run ----------------
def interval_ms(a, r):
    held = sum(abs(p[2]) for p in r["p"].values()) if r["p"] else 0
    if a in active or held >= 50_000 or r["av"] >= 1e6: return 3600e3          # every run
    if r["av"] >= 1e5 or held > 0: return 2 * 3600e3
    if r["av"] >= 1e4: return 6 * 3600e3
    return 12 * 3600e3

tasks = []   # (score, addr, dex)
for a, r in A.items():
    if r["av"] < MIN_AV and a not in active and not r["p"]: continue                      # dust, never active, holds nothing
    # never scanned: just above "due now", so it cannot starve the hourly re-scans of big holders (which feed the changes list)
    due = (NOW - r["ts"]) / interval_ms(a, r) if r["ts"] else 1.5
    if a in active: due += 10
    tasks.append((due, a, ""))
    dexes = set(d for d, t in r["dx"].items() if NOW - t < DEX_KEEP_MS) | (active.get(a, set()) - {""})
    for d in dexes: tasks.append((due + 0.01, a, d))
tasks.sort(key=lambda t: -t[0])
# budget: ~97% of rate/2 calls per minute; 20% is reserved for full sub-dex sweeps (address x every sub-dex), most overdue big
# accounts first, so sub-dex usage is discovered over the runs even for wallets that never showed up in the trade stream
budget_calls = int(args.minutes * args.rate / 2 * 0.97)
sweep_n = int(budget_calls * 0.20) // max(1, len(dex_list))
cand = sorted((a for a, r in A.items() if r["av"] >= 1e4 or r["p"] or a in active),
              key=lambda a: (A[a]["fd"], -A[a]["av"]))
sweep = [a for a in cand if NOW - A[a]["fd"] >= FULLDEX_EVERY_MS or new_dexes][:sweep_n]
sweep_set = set(sweep)
seen, sw, rest = set(), [], []
for a in sweep:
    for d in dex_list: seen.add((a, d)); sw.append((0, a, d))
for t in tasks:
    if (t[1], t[2]) not in seen: seen.add((t[1], t[2])); rest.append(t)
# interleave: sweeps share the budget with the due list instead of running first
order, i, j = [], 0, 0
while i < len(rest) or j < len(sw):
    for _ in range(4):
        if i < len(rest): order.append(rest[i]); i += 1
    if j < len(sw): order.append(sw[j]); j += 1
log(f"plan: {len(rest)} due calls ({sum(1 for p in rest if p[2]=='')} main) + {len(sw)} sweep calls ({len(sweep)} addresses x {len(dex_list)} sub-dexes); budget ~{budget_calls} calls in {args.minutes} min")

# ---------------- 4. scan ----------------
deadline = time.time() + args.minutes * 60; scan_t0 = time.time(); stats['calls'] = 0
done_main, done_dex, found_new_dex = set(), 0, 0
lock = threading.Lock()
def scan(t):
    _, a, d = t
    if time.time() > deadline: return
    s = post({"type": "clearinghouseState", "user": a, **({"dex": d} if d else {})}, 2)
    if s is None: return
    r = A[a]; ts = int(time.time() * 1000)
    av = float(s.get("marginSummary", {}).get("accountValue") or 0)
    pos = {}
    for ap_ in s.get("assetPositions") or []:
        p = ap_["position"]; szi = float(p["szi"])
        if szi == 0: continue
        lev = p.get("leverage") or {}
        pos[p["coin"]] = [1 if szi > 0 else -1, szi, float(p.get("positionValue") or 0), float(p.get("entryPx") or 0), float(p.get("unrealizedPnl") or 0),
                          lev.get("value"), float(p["liquidationPx"]) if p.get("liquidationPx") else None, "i" if lev.get("type") == "isolated" else "c"]
    with lock:
        global done_dex, found_new_dex
        # position changes since the previous scan of this (address, dex)
        prev = r["pts"].get(d)
        if prev and ts - prev <= CH_SPAN_MAX:
            old = {c: v for c, v in r["p"].items() if (c.split(":")[0] if ":" in c else "") == d}
            for c in set(old) | set(pos):
                b = old[c][1] if c in old else 0.0; a_ = pos[c][1] if c in pos else 0.0
                if b == a_: continue
                px = pos[c][2] / abs(a_) if c in pos and a_ else (PX.get(c) or (old[c][3] if c in old else 0))
                if abs(a_ - b) * px < CH_MIN: continue
                EV.append([a, c, ts // 1000, prev // 1000, float(f"{b:.6g}"), float(f"{a_:.6g}"), float(f"{px:.6g}")])
        # replace this dex's positions
        for c in [c for c in r["p"] if (c.split(":")[0] if ":" in c else "") == d]: del r["p"][c]
        for c, v in pos.items():
            k = c + "|" + str(v[0])
            if k not in r["fs"]: r["fs"][k] = ts if r["pts"].get(d) else 0      # 0 = was already open when we first looked
            r["p"][c] = v
        for k in list(r["fs"]):
            c, sd = k.split("|")
            if (c.split(":")[0] if ":" in c else "") == d and (c not in pos or str(pos[c][0]) != sd): del r["fs"][k]
        r["pts"][d] = ts
        if d == "": r["ts"] = ts; r["av"] = av if av else r["av"]; done_main.add(a)
        else:
            done_dex += 1
            if pos or av > 1:
                if d not in r["dx"]: found_new_dex += 1
                r["dx"][d] = ts
    return True
with ThreadPoolExecutor(args.conc) as ex:
    n = 0
    for ok in ex.map(scan, order):
        if not ok: continue          # deadline reached (or failed call): the rest waits for the next run, most overdue first
        n += 1
        if n % 2000 == 0: log(f"scanned {n}/{len(order)} calls, {stats['weight']} weight, 429s {stats['r429']}")
log(f"deadline: {len(order) - n} planned calls left for the next run" if n < len(order) else "all planned calls done")
for a in sweep_set:
    if all(A[a]["pts"].get(d, 0) >= NOW for d in dex_list): A[a]["fd"] = NOW
state["known_dexes"] = dex_list
scan_s = time.time() - T0
log(f"throughput: {stats['calls']/max(1,(time.time()-scan_t0))*60:.0f} calls/min during the scan")
log(f"scan done: {len(done_main)} addresses (main), {done_dex} sub-dex calls, {found_new_dex} new address/dex pairs; {stats['calls']} calls, {stats['weight']} weight, 429s {stats['r429']}, errors {stats['errors']}")

# ---------------- 5. output ----------------
coins = {}
for a, r in A.items():
    for c, v in r["p"].items():
        d = c.split(":")[0] if ":" in c else ""
        pts = r["pts"].get(d, 0)
        if NOW - pts > STALE_MS: continue
        coins.setdefault(c, []).append((a, v, r, pts))
for sub in ("coins", "changes"):
    os.makedirs(os.path.join(args.out, sub), exist_ok=True)
    for f in os.listdir(os.path.join(args.out, sub)): os.remove(os.path.join(args.out, sub, f))
coh = lambda a, r: (pnl_coh(r.get("pl")), size_coh(r["av"]))
index = {}
r6 = lambda x: float(f"{x:.6g}") if isinstance(x, float) else x
for c, hs in coins.items():
    hs.sort(key=lambda h: -abs(h[1][2]))
    keep = [h for i, h in enumerate(hs) if abs(h[1][2]) >= KEEP_MIN or i < KEEP_TOP][:KEEP_MAX]
    om = hs[len(keep):]
    L = [h for h in hs if h[1][0] > 0]; S = [h for h in hs if h[1][0] < 0]
    rows = []
    for a, v, r, pts in keep:
        fs = r["fs"].get(c + "|" + str(v[0]), 0)
        rows.append([a, v[0], r6(v[1]), round(v[2]), r6(v[3]), round(v[4]), v[5], r6(v[6]) if v[6] else None, v[7], round(r["av"]), fs // 1000 if fs else 0, pts // 1000, lb_rank.get(a, 0), *coh(a, r)])
    fn = c.replace(":", "_").replace("/", "-") + ".json"
    body = {"coin": c, "generatedAt": NOW // 1000, "edges": EDGES,
            "cols": ["addr", "side", "szi", "ntl", "entry", "upnl", "lev", "liq", "mode", "av", "firstSeen", "scannedAt", "lbRank", "pnlCohort", "sizeCohort"],
            "rows": rows, "omitted": {"n": len(om), "ntl": round(sum(abs(h[1][2]) for h in om)), "max": round(abs(om[0][1][2])) if om else 0}}
    with open(os.path.join(args.out, "coins", fn), "w") as f: json.dump(body, f, separators=(",", ":"))
    index[c] = {"f": fn, "n": len(hs), "nL": len(L), "nS": len(S), "vL": round(sum(h[1][2] for h in L)), "vS": round(sum(h[1][2] for h in S))}
# ---- cohorts: long/short by all-time-PnL group and by account-size group, per asset + all assets ----
def cells(hs, k):
    out = [[0, 0, 0] for _ in range(len(PNL_EDGES) + 1 if k == 0 else len(SIZE_EDGES) + 1)]
    for a, v, r, pts in hs:
        i = coh(a, r)[k]
        if i < 0: continue
        o = out[i]; o[0] += 1; o[1 if v[0] > 0 else 2] += abs(v[2])
    return [[n, round(l), round(s_)] for n, l, s_ in out]
coh_coins = {}
for c, hs in coins.items():
    if len(hs) < 10: continue
    coh_coins[c] = {"n": len(hs), "p": cells(hs, 0), "s": cells(hs, 1), "u": sum(1 for a, v, r, pts in hs if r.get("pl") is None)}
tot = lambda c: index[c]["vL"] + index[c]["vS"]
main = [c for c in ("BTC", "ETH", "HYPE", "SOL") if c in coh_coins]
xyz = sorted((c for c in coh_coins if c.startswith("xyz:")), key=lambda c: -tot(c))[:4]
other = sorted((c for c in coh_coins if ":" not in c and c not in main), key=lambda c: -tot(c))[:4]
allw = {}   # all assets: unique wallets per cohort, summed long/short value
def all_cells(k):
    out = [[set(), 0, 0] for _ in range(len(PNL_EDGES) + 1 if k == 0 else len(SIZE_EDGES) + 1)]
    for c, hs in coins.items():
        for a, v, r, pts in hs:
            i = coh(a, r)[k]
            if i < 0: continue
            o = out[i]; o[0].add(a); o[1 if v[0] > 0 else 2] += abs(v[2])
    return [[len(w), round(l), round(s_)] for w, l, s_ in out]
cohorts = {"v": 2, "pnlGroups": len(PNL_EDGES) + 1, "generatedAt": NOW // 1000, "edges": EDGES, "rows": main + xyz + other,
           "all": {"p": all_cells(0), "s": all_cells(1), "n": len({a for hs in coins.values() for a, *_ in hs})}, "coins": coh_coins}
with open(os.path.join(args.out, "cohorts.json"), "w") as f: json.dump(cohorts, f, separators=(",", ":"))

# ---- position changes (last 24 h), per asset; a wallet's events are kept or dropped together ----
by_coin = {}
for e in EV: by_coin.setdefault(e[1], {}).setdefault(e[0], []).append(e)
n_ch = 0
for c, per in by_coin.items():
    ws_ = sorted(per.items(), key=lambda kv: -max(abs(e[5] - e[4]) * e[6] for e in kv[1]))
    keep = [kv for i, kv in enumerate(ws_) if max(abs(e[5] - e[4]) * e[6] for e in kv[1]) >= KEEP_MIN or i < KEEP_TOP][:KEEP_MAX]
    om = ws_[len(keep):]
    rows = []
    for a, evs in keep:
        r = A.get(a) or {"av": 0}; pc, zc = coh(a, r)
        for e in sorted(evs, key=lambda e: e[2]): rows.append([a, e[4], e[5], e[6], e[2], e[3], round(r["av"]), lb_rank.get(a, 0), pc, zc])
    fn = c.replace(":", "_").replace("/", "-") + ".json"
    omv = [max(abs(e[5] - e[4]) * e[6] for e in evs) for a, evs in om]
    with open(os.path.join(args.out, "changes", fn), "w") as f:
        json.dump({"coin": c, "generatedAt": NOW // 1000, "edges": EDGES, "cols": ["addr", "sziBefore", "sziAfter", "px", "t", "tPrev", "av", "lbRank", "pnlCohort", "sizeCohort"],
                   "rows": rows, "omitted": {"n": len(om), "ntl": round(sum(omv)), "max": round(max(omv)) if omv else 0}}, f, separators=(",", ":"))
    ix_ = index.setdefault(c, {"f": "", "n": 0, "nL": 0, "nS": 0, "vL": 0, "vS": 0}); ix_["cf"] = fn; ix_["ch"] = len(per)
    n_ch += len(per)
# ---- big position changes across ALL assets: since the previous scan (= found in this run), 6 h, 24 h; merged per wallet+asset ----
TOP_N = 300
def merged(since_ms):
    g = {}
    for e in sorted(EV, key=lambda e: e[2]):
        if e[2] * 1000 < since_ms: continue
        k = (e[0], e[1]); m = g.get(k)
        if m is None: g[k] = [e[1], e[0], e[4], e[5], e[6], e[2], e[3]]
        else: m[3] = e[5]; m[4] = e[6]; m[5] = e[2]
    out = []
    for m in g.values():
        b_, a_, px = m[2], m[3], m[4]
        if b_ == a_: continue
        flip = b_ and a_ and (b_ > 0) != (a_ > 0)
        d = (abs(a_) + abs(b_)) * px if flip else abs(abs(a_) - abs(b_)) * px
        r = A.get(m[1]) or {"av": 0}; pc, zc = coh(m[1], r)
        out.append((d, m + [round(r["av"]), lb_rank.get(m[1], 0), pc, zc]))
    out.sort(key=lambda x: -x[0])
    return {"rows": [x[1] for x in out[:TOP_N]], "omitted": {"n": max(0, len(out) - TOP_N), "max": round(out[TOP_N][0]) if len(out) > TOP_N else 0}, "total": len(out)}
top = {"v": 2, "generatedAt": NOW // 1000, "edges": EDGES, "runStart": NOW // 1000, "prevRun": RUNS[-2] if len(RUNS) > 1 else 0,
       "cols": ["coin", "addr", "sziBefore", "sziAfter", "px", "t", "tPrev", "av", "lbRank", "pnlCohort", "sizeCohort"],
       "windows": {"last": merged(NOW), "6": merged(NOW - 6 * 3600e3), "24": merged(NOW - 24 * 3600e3)}}
with open(os.path.join(args.out, "changes", "top.json"), "w") as f: json.dump(top, f, separators=(",", ":"))
log(f"changes: {len(EV)} events in 24 h, {n_ch} wallet/asset pairs on {len(by_coin)} assets; cohorts on {len(coh_coins)} assets")
# every listed market gets an index entry (n=0, no file when nobody tracked holds it), so the site can offer ALL coins
for c, d, vol in markets:
    if c not in index: index[c] = {"f": "", "n": 0, "nL": 0, "nS": 0, "vL": 0, "vS": 0}
for c, d, vol in markets: index[c]["dv"] = round(vol)
tot_scanned_24h = sum(1 for r in A.values() if NOW - r["ts"] < STALE_MS)
cov = {"lbAddresses": len(lb_av), "lbAtLeastMinAv": sum(1 for v in lb_av.values() if v >= MIN_AV), "minAv": MIN_AV,
       "skippedDust": sum(1 for v in lb_av.values() if v < MIN_AV), "activeFromTrades": len(active),
       "scannedThisRun": len(done_main), "subdexCallsThisRun": done_dex, "scanned24h": tot_scanned_24h,
       "addressesKnown": len(A), "addressesWithSubdex": sum(1 for r in A.values() if r["dx"]),
       "calls": stats["calls"], "weight": stats["weight"], "r429": stats["r429"], "errors": stats["errors"],
       "durationSec": round(time.time() - T0), "ratePerMin": args.rate, "dexes": [""] + dex_list}
with open(os.path.join(args.out, "index.json"), "w") as f:
    json.dump({"v": 1, "generatedAt": NOW // 1000, "coverage": cov, "coins": index, "edges": EDGES,
               "changes": {"runs": len(RUNS), "firstRun": RUNS[0], "events": len(EV)}}, f, separators=(",", ":"))
# state: drop addresses that are dust, inactive and empty for a long time
for a in [a for a, r in A.items() if r["av"] < MIN_AV and not r["p"] and not r["dx"] and NOW - r["ts"] > 7 * 864e5]: del A[a]
open(SP, "wb").write(gzip.compress(json.dumps(state, separators=(",", ":")).encode(), 6))
log(f"wrote {len(coins)} coin files + index.json ({len(index)} markets); state {os.path.getsize(SP)//1024} KB; total {time.time()-T0:.0f}s")

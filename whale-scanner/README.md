# WhaleYay holder scanner

Hyperliquid has no "who holds coin X" API. This job checks the open perp positions of a large set of wallets on a
schedule and publishes one small static JSON file per coin. The "Tìm theo coin / Find by coin" tab of the site reads those files.

## What it scans (each run)
1. **Addresses**
   - **Leaderboard:** every address on the public leaderboard (`stats-data.hyperliquid.xyz/Mainnet/leaderboard`, about 48k addresses with account value, free CDN).
   - **Big traders:** addresses that just made a big trade (≥ $10k). They come from `recentTrades` on the 12 most-traded main markets plus 2 per sub-dex, and from a 3-minute listen to the live trade stream on all ~340 markets (main and HIP-3 sub-dexes).
   - **Earlier holders:** every address that held a position in an earlier run.
2. **Skipped:** leaderboard accounts under **$1,000** are skipped unless they show up in the big-trade stream or held a position before.
   - Why: even at 40x leverage a $1k account holds at most about $40k, which is below what the coin pages focus on.
   - Effect: this halves the work (about 24k of 48k addresses are under $1k).
   - Small accounts that open a big leveraged trade still get found, through the trade stream.
3. **Positions:** `clearinghouseState` (weight 2) on the main dex for every address that is due.
   - **Due interval:** big accounts (≥ $1M), recent big traders and holders ≥ $50k every run; ≥ $100k or any holder every 2 h; ≥ $10k every 6 h; the rest every 12 h.
   - **Order:** the most overdue go first.
4. **Sub-dexes** (xyz, flx, vntl, hyna, km, abcd, cash, para, mkts, io; the list is read from `perpDexs`).
   - **Remembered dexes:** a remembered dex is scanned together with its address. A dex is remembered for 14 days after the last position or balance on it.
   - **Rotating full sweep:** 20% of each run's budget goes to sweeping every sub-dex for the largest accounts, rotating so each account is covered at least every 7 days.
   - **New dexes:** a dex that appears in `perpDexs` gets swept for everyone.
   - **Traders:** an address seen trading on a sub-dex is scanned there in the same run.
5. **Rate limit**
   - **Pacing:** a token bucket on request weight, 1000/min by default. Hyperliquid's limit is 1200/min per IP.
   - **On HTTP 429:** every thread pauses with exponential backoff.
   - **Measured on this box:** ~475 calls/min (950 weight/min) with 0 errors.

## Output (`out/`, published to the `data` branch)
- `index.json`:
  - `generatedAt`
  - `coverage`: leaderboard size, how many were scanned this run and in the last 24 h, how many were skipped as dust, active traders, calls, weight, 429s, duration
  - `coins`: `{coin: {f: file, n, nL, nS, vL, vS, dv}}` for **every listed market** (main dex + all HIP-3 sub-dexes; `dv` = 24 h volume). A market nobody tracked holds has `n: 0` and `f: ""` (no file); the site still lets you open it and fills it from the live trade stream.
- `coins/<coin>.json`:
  - Names: `BTC.json`, `xyz_TSLA.json`.
  - Columns: address, side, size, value, entry, uPnL, leverage, liq price, margin mode, account value, first-seen, scanned-at, leaderboard rank.
  - Kept per coin: every position ≥ $10k plus the 300 largest (max 1500). The rest is only counted, in `omitted`.
- `state/state.json.gz`: the memory carried between runs (per address: dexes used, last scan time, positions, first-seen).
- **First-seen:** a position not there in the previous scan of that address gets the scan time. A position already open at the first scan gets 0 ("unknown").
- **Stale holders:** holders not re-scanned for 24 h are dropped.

## Run locally
    pip install websockets          # optional, for the trade-stream listen
    python scan.py --minutes 40 --out out --state state

Options: `--minutes` (scan time budget), `--rate` (weight/min), `--ws-seconds` (0 = no stream), `--conc`.

## Go live on GitHub (free)
1. Create a free GitHub account and a **public** repository, e.g. `whaleyay`. Actions minutes are free and unlimited for public repos.
2. Upload the contents of `WhaleYay-scanner.zip` (the `whale-scanner/` folder and the `.github/workflows/whale-scanner.yml` file) to the repo root, keeping the folder layout.
3. In the repo, go to **Settings → Actions → General → Workflow permissions** and choose **Read and write**.
4. Open the **Actions** tab, enable workflows, then run "whale-scanner" once with **Run workflow**. After about 50 min a `data` branch appears.
5. In the site's `assets/features.js`, set `DATA_URL = "https://raw.githubusercontent.com/<user>/<repo>/data/"`, then redeploy the site.
6. GitHub pauses scheduled workflows in a repo with no activity for 60 days. If that happens, press "Enable workflow" in the Actions tab, or push any commit.

## Limitations
- **Coverage:** only the tracked wallets (the leaderboard, big traders, earlier holders), not every Hyperliquid account. A brand-new wallet that never made a ≥ $10k trade while the stream was listening, and isn't on the leaderboard, is not found. The site merges live big traders while the page is open: every big trade it sees (≥ $10k on big coins, ≥ $1k on small / sub-dex coins) is remembered per coin, and those wallets are checked and added as "new" (or re-checked if already listed).
- **Data age:** big accounts are re-checked every run (about 2 h with the 2-hourly cron), small ones every 6–12 h. The site re-checks the 20 largest holders live.
- **Sub-dexes:** positions on a dex we don't know an address uses are found only by the rotating sweep (≤ 7 days) or by the trade stream.
- **Spot:** spot balances are not included (perps only).

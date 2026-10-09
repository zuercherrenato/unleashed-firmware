# AI Fund Office — module contract

This file is the single source of truth for how the modules talk to each other.
Every module must follow it exactly. UI copy is **European Portuguese (pt-PT)**;
code, identifiers and comments are English.

## Principles (non-negotiable)

1. **Nothing on screen is fake state.** Every agent status, movement, screen, light
   and number is driven by an engine event or engine state. `Math.random()` is
   allowed ONLY for purely cosmetic micro-motion (breathing phase, LED flicker
   phase) and inside the seeded simulated market (which uses its own seeded RNG,
   never `Math.random`).
2. **Risk and Compliance are deterministic code.** An LLM may only *propose*.
   Sizing, limits, vetoes and the kill switch are plain rules.
3. **No real money.** Paper trading only. No keys, no order endpoints, ever.
4. **Zero install.** The app is a static page opened from disk (`file://`) or
   published as a claude.ai Artifact. Classic `<script>` tags only (no ES
   modules, no bundler, no `import`). No network except the public market-data
   WebSockets/REST listed in "Market".
5. **Never `innerHTML` with data.** Build DOM with `createElement` +
   `textContent`. Static markup in `index.html` is fine.

## File layout (all paths relative to `ai-fund-office/`)

| File | Owner | Purpose |
|---|---|---|
| `index.html` | ui | Full HTML document, loads scripts in the order below |
| `styles.css` | ui | All page CSS |
| `vendor/three.min.js` | — | three.js r149 UMD build (global `THREE`), MIT, already present |
| `js/util.js` | engine | `AFO.createBus`, `AFO.rng`, `AFO.hash64`, formatting helpers |
| `js/indicators.js` | engine | EMA, RSI, ATR, crossover helpers |
| `js/engine.js` | engine | Portfolio, agents pipeline, risk, compliance, execution, audit |
| `js/market.js` | market | Live Binance / Coinbase adapters + seeded simulated market + hub |
| `js/office3d.js` | office | three.js scene, characters, choreography |
| `js/claude.js` | ui | Optional Claude analysis via the Artifact `sample` capability |
| `js/ui.js` | ui | DOM panels, controls, modal, feed |
| `js/main.js` | ui | Wiring and the main loop |
| `tests/*.test.js` | engine/market | `node tests/<name>.test.js`, no dependencies, exit code ≠ 0 on failure |

Script order in `index.html`:
`vendor/three.min.js, js/util.js, js/indicators.js, js/engine.js, js/market.js, js/office3d.js, js/claude.js, js/ui.js, js/main.js`.

### Module wrapper (every `js/*.js` file)

```js
(function (root, factory) {
  var AFO = (root.AFO = root.AFO || {});
  var api = factory(AFO, root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  Object.assign(AFO, api);
})(typeof self !== 'undefined' ? self : globalThis, function (AFO, root) {
  'use strict';
  // In node tests, dependencies are loaded with require() BEFORE this file,
  // so AFO.createBus etc. already exist on globalThis.AFO.
  return { /* exported members */ };
});
```

Node tests load files in order with `require('../js/util.js')` etc.; each require
populates `globalThis.AFO`. `util.js`, `indicators.js`, `engine.js` and the
simulated part of `market.js` must run in Node 22 without DOM.

## util.js

- `AFO.createBus()` → `{ on(topic, fn) → off(), emit(topic, payload), once(topic, fn) }`.
  Synchronous, in-order delivery. A throwing listener is caught and logged with
  `console.error`; it never breaks other listeners.
- `AFO.rng(seed)` → function returning floats in [0,1) (mulberry32). Also
  `AFO.gaussian(rand)` → standard normal (Box–Muller).
- `AFO.hash64(str)` → 16-char lowercase hex string (cyrb53-style, two 32-bit
  lanes). NOT cryptographic; the UI must say so ("hash não criptográfico").
- Formatting: `AFO.fmtMoney(n)` (2 decimals, thin-space thousands, suffix ` USDT`),
  `AFO.fmtPct(n, digits=2)` (sign + `%`), `AFO.fmtPrice(n)` (adaptive decimals:
  ≥1000 → 2, ≥1 → 3, else 5), `AFO.fmtTime(ms)` (HH:MM:SS, pt-PT).

## indicators.js

Pure functions on arrays of numbers (oldest first):
`AFO.ema(values, period)` → array (same length, `NaN` until seeded),
`AFO.rsi(closes, period=14)` → array (Wilder),
`AFO.atr(candles, period=14)` → array (Wilder, candles `{h,l,c}`),
`AFO.crossed(prevA, prevB, a, b)` → `'up' | 'down' | null`.

## Market data shapes

Symbols (fixed order): `['BTC','ETH','SOL','LINK','AVAX']`.

Bus topic `market:candle`, payload:
```js
{ symbol: 'BTC', t: 1760000000000 /* candle open time, ms */, o, h, l, c, v,
  closed: true|false, source: 'binance'|'coinbase'|'sim' }
```
- `closed:false` = intrabar update of the forming candle (mark-to-market only).
- `closed:true` = the candle is final (strategy runs on these only).
- Candles for one symbol arrive in non-decreasing `t`. A late duplicate closed
  candle (same `t`) must be ignored by the engine.

Bus topic `market:status`, payload:
```js
{ source: 'binance'|'coinbase'|'sim', state: 'connecting'|'live'|'fallback'|'error',
  detail: 'texto pt-PT para o utilizador', candleMs: 60000, speed: 1 }
```

Bus topic `market:history` (once per source switch, before live candles):
`{ source, candles: { BTC: [candle...], ... } }` — closed historical candles for
indicator warm-up (oldest first, ≥ 60 per symbol when available).

## market.js

`AFO.createMarketHub(bus, opts)` → `{ start(), stop(), setSpeed(n), getStatus(), source }`.
`opts = { prefer: 'auto'|'sim', seed: 42, simCandleMs: 4000, liveTimeoutMs: 7000 }`.
For tests, `opts` may also inject `WebSocketImpl`, `fetchImpl`, `setTimeout`/`clearTimeout`/`setInterval`/`clearInterval`
and `now()`; defaults are the globals.

- `auto`: try Binance (REST warm-up `GET https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=120`
  + WebSocket kline_1m streams), then Coinbase (public `ticker` channel,
  aggregated locally into 1-minute candles), then the simulated market. Any
  exception, CSP block, close, or no message within `liveTimeoutMs` moves to the
  next source. Exact endpoints and message formats must be checked against the
  exchanges' official public docs; if REST warm-up fails (e.g. CORS) the live
  source still works and the engine warms up from live candles.
- Simulated market: seeded (`AFO.rng`), **zero drift** geometric Brownian motion
  with a shared market factor (correlation ≈ 0.6) and Markov volatility regimes
  (calm / normal / stormy). Illustrative start prices only. Emits 120 historical
  candles via `market:history`, then a closed candle every `simCandleMs / speed`
  per symbol and intrabar `closed:false` updates every ~500 ms / speed.
  Simulated candle time advances 60 000 ms per candle (one simulated minute).
  `setSpeed(n)` only affects the sim (allowed: 1, 4, 16).
- Emits `market:status` on every state change. Never uses `Math.random`.

## engine.js

`AFO.createEngine(bus, config?)` → engine object.

Default config (all overridable):
```js
{ startingNav: 100000, symbols: [...], maxGrossExposurePct: 35, maxPositionPct: 12,
  riskPerTradePct: 1.0, maxDrawdownPct: 5, maxOpenPositions: 4, maxOrdersPerHour: 12,
  feePct: 0.10, slippagePct: 0.05, cooldownCandles: 3, warmupCandles: 30,
  emaFast: 9, emaSlow: 21, rsiPeriod: 14, atrPeriod: 14, stopAtr: 2, takeProfitAtr: 3,
  minNotionalPct: 0.5, minStopPct: 0.15, maxStopPct: 8 }
```

Methods: `onCandle(candle)`, `onHistory(payload)`, `submitExternalProposal({symbol, side, reason, source:'claude'})`
→ result object, `kill(by='user', reason)`, `pause()`, `resume()`, `reset()`,
`getState()` → snapshot (below), `verifyAudit()` → `{ok, brokenAt|null, length}`.
`main.js` wires `bus.on('market:candle', engine.onCandle)` and `market:history`.

Accounting (linear, no leverage cost, no funding):
- Long P/L = `qty × (mark − entry)`; Short P/L = `qty × (entry − mark)`.
- Entry fill = mark × (1 + slip) for LONG, × (1 − slip) for SHORT. Exit is the
  reverse. Fee = `feePct%` of notional on entry AND on exit, deducted from cash.
- `cash` changes only by realized P/L and fees. `nav = cash + Σ unrealized P/L`.
- `grossExposurePct = Σ(qty × mark) / nav × 100`.
- `peakNav = max(peakNav, nav)` on every mark; `drawdownPct = (peakNav − nav)/peakNav × 100`.

Pipeline on each closed candle of symbol S (in this order, all synchronous):
1. Update marks; check stop / take-profit of S's open position using the candle's
   `l`/`h` (stop has priority if both hit) → close at the stop/TP level with
   slippage + fee.
2. Drawdown check on every mark update (closed or not): if `drawdownPct ≥ maxDrawdownPct`
   → automatic kill (`by:'risk'`) closing everything at mark.
3. Research (rules): after `warmupCandles`, EMA fast/slow cross on closes, RSI
   filter (LONG if cross up and 50 ≤ RSI ≤ 70; SHORT if cross down and 30 ≤ RSI ≤ 50).
   Opposite cross on a symbol with an open position → close it (`reason:'signal'`).
   Emits `research.scan` for every closed candle after warm-up.
4. Proposal → Risk → Compliance → Execution (each emits events, see below).

Risk checks (all must pass; sizing may shrink qty): not killed/paused; drawdown below
limit; open positions < max; no open position in S; stop distance within
[`minStopPct`, `maxStopPct`] % of price; qty = (nav × riskPerTradePct%) / |entry − stop|;
notional capped to maxPositionPct% of nav and to the remaining gross-exposure room;
reject if resulting notional < minNotionalPct% of nav.
Compliance checks: symbol whitelisted; fills in the last 60 simulated minutes
(by candle time) < maxOrdersPerHour; cooldown of `cooldownCandles` closed candles
after a close on S; audit chain intact; proposal fields finite and stop/TP on the
correct side of entry.

Agent ids: `research` (NOVA), `execution` (VECTOR), `risk` (AEGIS), `compliance` (ORACLE).

Bus topic `agent:event`, payload:
```js
{ id: 17 /* monotonically increasing */, t /* candle time ms */, type, agent,
  symbol /* or null */, text /* pt-PT sentence for the feed */, data: {...} }
```
Types and `data`:
- `research.scan` `{indicators:{emaFast, emaSlow, rsi, atr, close}, warm:true}`
- `research.signal` `{side, rsi, reason}`
- `proposal.created` `{proposalId, side, entry, stop, takeProfit, source:'rules'|'claude', reason}`
- `risk.review` `{proposalId}`
- `risk.approved` `{proposalId, qty, notional, riskAmount, checks:[{name, ok, detail}]}`
- `risk.rejected` `{proposalId, reason, checks}`
- `compliance.review` `{proposalId}`
- `compliance.approved` `{proposalId, checks}`
- `compliance.vetoed` `{proposalId, reason, checks}`
- `execution.filled` `{positionId, side, qty, price, fee, notional}`
- `execution.closed` `{positionId, side, qty, exitPrice, pnl, fee, reason:'stop'|'takeProfit'|'signal'|'kill'}`
- `risk.alert` `{kind:'drawdown'|'exposure', value, limit}` (drawdown alert at ≥ 60% of limit, once per crossing)
- `system.kill` `{by:'user'|'risk', reason}`; `system.pause`; `system.resume`; `system.reset`
- `audit.append` `{seq, hash, prevHash, entryType}` (one per decision-bearing event)
- `claude.request` / `claude.response` `{summary, ideas}` / `claude.error` `{code}` — emitted by `claude.js`, agent `research`

`getState()` snapshot (plain data, safe to read every frame):
```js
{ nav, cash, peakNav, drawdownPct, grossExposurePct, realizedPnl, unrealizedPnl, feesPaid,
  startingNav, killed, killReason, paused, config,
  marks: {BTC: 0,...}, warmup: {BTC: {have, need}},
  positions: [{id, symbol, side, qty, entry, mark, stop, takeProfit, notional, pnl, pnlPct, openedAt, source}],
  agents: { research:{status, task, last}, risk:{...}, compliance:{...}, execution:{...} },
  stats: {proposals, approved, rejectedByRisk, vetoedByCompliance, fills, closes, wins, losses},
  audit: {length, head, last: [ ...up to 20 entries ]}, lastEventId }
```
Agent `status` ∈ `'idle'|'analyzing'|'reviewing'|'executing'|'monitoring'|'alert'|'stopped'`;
`task` = short pt-PT text; `last` = last decision text.

## office3d.js

`AFO.createOffice(container, bus, getState, opts)` → `{ start(), stop(), resize(), focusAgent(id), setReducedMotion(bool), onAgentClick(fn) }`.
- Owns its `<canvas>` and an HTML overlay layer for name tags / speech bubbles
  (inside `container`). Depends only on `THREE`, `bus`, `getState`.
- Listens to `agent:event`, `market:candle`, `market:status`.
- Animation queue per agent; when the visual backlog exceeds ~8 s, it collapses
  to the latest state (agents return to their desks, screens refresh) instead of
  falling behind.

## ui.js / main.js / claude.js

- `AFO.createUI(root, bus, engine, hub, office)`; reads `engine.getState()` at
  ≤ 5 Hz for numbers, appends feed items on `agent:event`.
- `claude.js`: `AFO.initClaude(bus, engine, ui)`. Uses
  `window.claude && window.claude.use ? await window.claude.use('sample') : null`.
  If `null` → the Claude button is hidden with a short note. Calls happen ONLY on
  an explicit click (never on load, timer or loop), with `sample.json(prompt, {modelTier:'default', effort:'low', cache:false})`.
  The answer can only create proposals via `engine.submitExternalProposal`; Risk
  and Compliance still decide. Errors are branched on `e.code`.

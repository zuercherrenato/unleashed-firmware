'use strict';
// node tests/market.test.js — no dependencies; exit code 1 on failure.

const assert = require('assert');
const fs = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..');
const UTIL_PATH = path.join(ROOT, 'js', 'util.js');
const MARKET_PATH = path.join(ROOT, 'js', 'market.js');

// util.js is optional here: market.js falls back to its own seeded RNG.
if (fs.existsSync(UTIL_PATH)) require(UTIL_PATH);
require(MARKET_PATH);
const AFO = globalThis.AFO;
const M = AFO.marketInternals;
const SYMBOLS = ['BTC', 'ETH', 'SOL', 'LINK', 'AVAX'];
const MINUTE = 60000;

// ------------------------------------------------------------------ harness

const tests = [];
function test(name, fn) { tests.push({ name, fn }); }

function createBus() {
  if (typeof AFO.createBus === 'function') return AFO.createBus();
  const handlers = {};
  return {
    on(topic, fn) { (handlers[topic] = handlers[topic] || []).push(fn); return () => {}; },
    emit(topic, payload) { (handlers[topic] || []).slice().forEach((fn) => fn(payload)); }
  };
}

function recorder() {
  const bus = createBus();
  const events = [];
  ['market:status', 'market:history', 'market:candle'].forEach((topic) => {
    bus.on(topic, (payload) => events.push({ topic, payload: JSON.parse(JSON.stringify(payload)) }));
  });
  return {
    bus,
    events,
    of: (topic) => events.filter((e) => e.topic === topic).map((e) => e.payload),
    statuses: () => events.filter((e) => e.topic === 'market:status').map((e) => e.payload),
    clear: () => { events.length = 0; }
  };
}

function createClock(start) {
  let now = start;
  let seq = 0;
  const timers = new Map();
  function add(fn, ms, every) {
    const id = ++seq;
    const delay = Math.max(0, Number(ms) || 0);
    timers.set(id, { at: now + delay, fn, every: every ? Math.max(1, delay) : 0, id });
    return id;
  }
  return {
    now: () => now,
    setTimeout: (fn, ms) => add(fn, ms, false),
    clearTimeout: (id) => { timers.delete(id); },
    setInterval: (fn, ms) => add(fn, ms, true),
    clearInterval: (id) => { timers.delete(id); },
    pending: () => timers.size,
    advance(ms) {
      const target = now + ms;
      for (;;) {
        let next = null;
        for (const t of timers.values()) {
          if (t.at <= target && (!next || t.at < next.at || (t.at === next.at && t.id < next.id))) next = t;
        }
        if (!next) break;
        now = next.at;
        if (next.every) next.at += next.every;
        else timers.delete(next.id);
        next.fn();
      }
      now = target;
    }
  };
}

function createFakeWebSocketClass(options = {}) {
  const instances = [];
  class FakeWebSocket {
    constructor(url) {
      this.url = url;
      this.sent = [];
      this.closed = false;
      this.onopen = this.onmessage = this.onerror = this.onclose = null;
      instances.push(this);
      if (options.throwFor && options.throwFor(url)) throw new Error('SecurityError: blocked by CSP');
    }
    send(data) { this.sent.push(data); }
    close() { this.closed = true; }
    open() { if (this.onopen) this.onopen({}); }
    receive(payload) {
      if (this.onmessage) this.onmessage({ data: typeof payload === 'string' ? payload : JSON.stringify(payload) });
    }
    drop() { if (this.onclose) this.onclose({ code: 1006 }); }
    fail() { if (this.onerror) this.onerror({}); if (this.onclose) this.onclose({ code: 1006 }); }
  }
  FakeWebSocket.instances = instances;
  FakeWebSocket.last = () => instances[instances.length - 1];
  return FakeWebSocket;
}

const flush = () => new Promise((resolve) => setImmediate(resolve));
async function settle() { for (let i = 0; i < 5; i++) await flush(); }

function hubWith(rec, clock, extra) {
  return AFO.createMarketHub(rec.bus, Object.assign({
    seed: 42, simCandleMs: 4000, liveTimeoutMs: 7000,
    now: clock.now, setTimeout: clock.setTimeout, clearTimeout: clock.clearTimeout,
    setInterval: clock.setInterval, clearInterval: clock.clearInterval,
    fetchImpl: () => new Promise(() => {}),
    WebSocketImpl: createFakeWebSocketClass()
  }, extra));
}

function assertOhlc(c, where) {
  const label = where + ' ' + JSON.stringify(c);
  ['o', 'h', 'l', 'c', 'v', 't'].forEach((k) => assert.ok(Number.isFinite(c[k]), 'finite ' + k + ' ' + label));
  assert.ok(c.l > 0, 'positive low ' + label);
  assert.ok(c.l <= Math.min(c.o, c.c), 'l <= min(o,c) ' + label);
  assert.ok(c.h >= Math.max(c.o, c.c), 'h >= max(o,c) ' + label);
  assert.ok(c.v >= 0, 'volume >= 0 ' + label);
  assert.ok(SYMBOLS.includes(c.symbol), 'known symbol ' + label);
  assert.strictEqual(typeof c.closed, 'boolean', 'closed flag ' + label);
}

function meanAndSe(xs) {
  const n = xs.length;
  const mean = xs.reduce((a, b) => a + b, 0) / n;
  const variance = xs.reduce((a, b) => a + (b - mean) * (b - mean), 0) / (n - 1);
  return { mean, se: Math.sqrt(variance / n), sd: Math.sqrt(variance) };
}

function binanceKline(symbol, t, o, h, l, c, v, closed) {
  return {
    e: 'kline', E: t + 1234, s: symbol + 'USDT',
    k: {
      t, T: t + MINUTE - 1, s: symbol + 'USDT', i: '1m', f: 100, L: 200,
      o: String(o), c: String(c), h: String(h), l: String(l), v: String(v),
      n: 100, x: closed, q: '1.0', V: '0.5', Q: '0.5', B: '0'
    }
  };
}

function wrapped(symbol, payload) {
  return { stream: symbol.toLowerCase() + 'usdt@kline_1m', data: payload };
}

function restRows(symbol, nowMs, count) {
  const currentMinute = Math.floor(nowMs / MINUTE) * MINUTE;
  const rows = [];
  for (let i = count - 1; i >= 0; i--) {
    const t = currentMinute - i * MINUTE; // last row is the forming candle
    const base = 100 + i;
    rows.push([t, String(base), String(base + 2), String(base - 2), String(base + 1), '12.5', t + MINUTE - 1, '1', 10, '1', '1', '0']);
  }
  return rows;
}

// ------------------------------------------------------------- simulation

test('source never calls Math.random', () => {
  const src = fs.readFileSync(MARKET_PATH, 'utf8');
  assert.ok(!/Math\s*\.\s*random/.test(src), 'market.js must not reference Math.random');
});

test('sim generator is deterministic by seed', () => {
  const run = (seed) => {
    const g = M.createSimGenerator(seed, { startT: 0 });
    const h = g.history(30);
    const next = [g.nextCandle(), g.nextCandle()];
    return JSON.stringify({ h, next });
  };
  assert.strictEqual(run(7), run(7));
  assert.notStrictEqual(run(7), run(8));
});

test('sim hub: same seed and clock give identical event streams; different seed differs', () => {
  const runHub = (seed) => {
    const rec = recorder();
    const clock = createClock(1760000000000);
    const hub = hubWith(rec, clock, { prefer: 'sim', seed });
    hub.start();
    clock.advance(20000);
    hub.stop();
    return JSON.stringify(rec.events);
  };
  assert.strictEqual(runHub(42), runHub(42));
  assert.notStrictEqual(runHub(42), runHub(43));
});

test('sim history: 120 contiguous closed candles per symbol, then live candles continue the minute grid', () => {
  const rec = recorder();
  const clock = createClock(1760000012345);
  const hub = hubWith(rec, clock, { prefer: 'sim' });
  hub.start();
  const kinds = rec.events.map((e) => e.topic);
  assert.deepStrictEqual(kinds.slice(0, 2), ['market:status', 'market:history']);
  const status = rec.statuses()[0];
  assert.strictEqual(status.source, 'sim');
  assert.strictEqual(status.state, 'live');
  assert.ok(/simulado/.test(status.detail) && /ilustrativos/.test(status.detail), status.detail);
  const history = rec.of('market:history')[0];
  assert.strictEqual(history.source, 'sim');
  const currentMinute = Math.floor(clock.now() / MINUTE) * MINUTE;
  SYMBOLS.forEach((s) => {
    const list = history.candles[s];
    assert.strictEqual(list.length, 120, s);
    list.forEach((c, i) => {
      assertOhlc(c, 'history');
      assert.strictEqual(c.closed, true);
      assert.strictEqual(c.source, 'sim');
      assert.strictEqual(c.t, currentMinute - (120 - i) * MINUTE);
    });
  });
  clock.advance(4000);
  const candles = rec.of('market:candle');
  SYMBOLS.forEach((s) => {
    const mine = candles.filter((c) => c.symbol === s);
    assert.strictEqual(mine.length, 8, 'eight updates per candle for ' + s);
    assert.deepStrictEqual(mine.map((c) => c.closed), [false, false, false, false, false, false, false, true]);
    mine.forEach((c) => assert.strictEqual(c.t, currentMinute));
    assert.strictEqual(mine[0].o, history.candles[s][119].c, 'new candle opens at previous close');
  });
  hub.stop();
});

test('sim OHLC invariants hold for history, intrabar and closed candles', () => {
  const g = M.createSimGenerator(123, { startT: 0 });
  const h = g.history(120);
  SYMBOLS.forEach((s) => h[s].forEach((c) => assertOhlc(c, 'history')));
  for (let n = 0; n < 400; n++) {
    const candle = g.nextCandle();
    SYMBOLS.forEach((s) => {
      const frames = candle.frames[s];
      frames.forEach((f, k) => {
        assertOhlc(Object.assign({ symbol: s, t: candle.t, closed: k === frames.length - 1 }, f), 'frame');
        if (k > 0) {
          assert.strictEqual(f.o, frames[k - 1].o, 'open is fixed within the candle');
          assert.ok(f.h >= frames[k - 1].h && f.l <= frames[k - 1].l, 'range only widens');
          assert.ok(f.v >= frames[k - 1].v, 'volume accumulates');
        }
      });
    });
  }
});

test('sim has zero drift: E[P_T/P_0] = 1 and mean log-return ≈ 0 across 300 seeds (±4.5 standard errors)', () => {
  const SEEDS = 300;
  const CANDLES = 200;
  const gross = {};
  const logs = {};
  SYMBOLS.forEach((s) => { gross[s] = []; logs[s] = []; });
  for (let seed = 1; seed <= SEEDS; seed++) {
    const g = M.createSimGenerator(seed * 7919, { startT: 0 });
    const start = {};
    SYMBOLS.forEach((s) => { start[s] = g.getPrice(s); });
    for (let n = 0; n < CANDLES; n++) g.nextCandle();
    SYMBOLS.forEach((s) => {
      const ratio = g.getPrice(s) / start[s];
      gross[s].push(ratio);
      logs[s].push(Math.log(ratio));
    });
  }
  SYMBOLS.forEach((s) => {
    const G = meanAndSe(gross[s]);
    const L = meanAndSe(logs[s]);
    // Under the null (martingale prices) each statistic is ~N(0, se); 4.5 se keeps the
    // family-wise false-alarm rate below 1e-4 while a drift of ~1e-4 per candle
    // (≥ 0.02 over 200 candles, i.e. > 10 se) would fail.
    assert.ok(Math.abs(G.mean - 1) < 4.5 * G.se, s + ' gross mean ' + G.mean + ' se ' + G.se);
    assert.ok(Math.abs(L.mean) < 4.5 * L.se, s + ' log mean ' + L.mean + ' se ' + L.se);
    assert.ok(L.sd > 0.005 && L.sd < 0.1, s + ' path dispersion is plausible: ' + L.sd);
  });
});

test('sim: factor correlation ≈ 0.6, BTC lowest vol, AVAX highest, all regimes visited', () => {
  const g = M.createSimGenerator(2024, { startT: 0 });
  const returns = {};
  SYMBOLS.forEach((s) => { returns[s] = []; });
  const regimes = new Set();
  let prev = {};
  SYMBOLS.forEach((s) => { prev[s] = g.getPrice(s); });
  for (let n = 0; n < 6000; n++) {
    const candle = g.nextCandle();
    regimes.add(candle.regime);
    SYMBOLS.forEach((s) => {
      const p = g.getPrice(s);
      returns[s].push(Math.log(p / prev[s]));
      prev[s] = p;
    });
  }
  const sd = (xs) => meanAndSe(xs).sd;
  const corr = (a, b) => {
    const ma = meanAndSe(a).mean;
    const mb = meanAndSe(b).mean;
    let num = 0, da = 0, db = 0;
    for (let i = 0; i < a.length; i++) {
      num += (a[i] - ma) * (b[i] - mb);
      da += (a[i] - ma) ** 2;
      db += (b[i] - mb) ** 2;
    }
    return num / Math.sqrt(da * db);
  };
  const rho = corr(returns.BTC, returns.ETH);
  assert.ok(rho > 0.52 && rho < 0.68, 'BTC/ETH correlation ' + rho);
  const rho2 = corr(returns.SOL, returns.AVAX);
  assert.ok(rho2 > 0.52 && rho2 < 0.68, 'SOL/AVAX correlation ' + rho2);
  const vols = SYMBOLS.map((s) => sd(returns[s]));
  assert.strictEqual(Math.min(...vols), vols[0], 'BTC has the lowest volatility ' + vols);
  assert.strictEqual(Math.max(...vols), vols[4], 'AVAX has the highest volatility ' + vols);
  ['calm', 'normal', 'stormy'].forEach((r) => assert.ok(regimes.has(r), 'regime visited: ' + r));
});

test('sim speed: closed candles every simCandleMs/speed, invalid speeds ignored', () => {
  const rec = recorder();
  const clock = createClock(1760000000000);
  const hub = hubWith(rec, clock, { prefer: 'sim' });
  hub.start();
  const closedCount = () => rec.of('market:candle').filter((c) => c.closed && c.symbol === 'BTC').length;

  clock.advance(40000);
  assert.strictEqual(closedCount(), 10, 'speed 1: 10 candles in 40 s');

  assert.strictEqual(hub.setSpeed(4), 4);
  let st = rec.statuses().pop();
  assert.strictEqual(st.speed, 4);
  assert.strictEqual(st.candleMs, 1000);
  clock.advance(10000);
  assert.strictEqual(closedCount(), 20, 'speed 4: 10 candles in 10 s');

  assert.strictEqual(hub.setSpeed(16), 16);
  clock.advance(2500);
  assert.strictEqual(closedCount(), 30, 'speed 16: 10 candles in 2.5 s');

  const before = rec.statuses().length;
  assert.strictEqual(hub.setSpeed(3), 16, 'speed 3 is not allowed');
  assert.strictEqual(hub.setSpeed('fast'), 16);
  assert.strictEqual(rec.statuses().length, before, 'no status for ignored speed');

  hub.setSpeed(1);
  st = hub.getStatus();
  assert.strictEqual(st.speed, 1);
  assert.strictEqual(st.candleMs, 4000);
  const btc = rec.of('market:candle').filter((c) => c.symbol === 'BTC' && c.closed);
  btc.forEach((c, i) => { if (i) assert.strictEqual(c.t - btc[i - 1].t, MINUTE, 'sim time advances one minute per candle'); });
  hub.stop();
  const total = rec.events.length;
  clock.advance(60000);
  assert.strictEqual(rec.events.length, total, 'no events after stop()');
});

// ---------------------------------------------------------------- Binance

test('binance parser: combined wrapper, raw payload, closed vs intrabar, malformed ignored', () => {
  const t = 1760000040000 - (1760000040000 % MINUTE);
  const intrabar = M.parseBinanceMessage(JSON.stringify(wrapped('BTC', binanceKline('BTC', t, 100, 105, 99, 103, 7.5, false))));
  assert.deepStrictEqual(intrabar, { symbol: 'BTC', t, o: 100, h: 105, l: 99, c: 103, v: 7.5, closed: false, source: 'binance' });
  const closed = M.parseBinanceMessage(JSON.stringify(binanceKline('AVAX', t, 20, 21, 19, 20.5, 3, true)));
  assert.strictEqual(closed.symbol, 'AVAX');
  assert.strictEqual(closed.closed, true);
  const truthyString = binanceKline('ETH', t, 1, 2, 0.5, 1.5, 1, 'true');
  assert.strictEqual(M.parseBinanceMessage(JSON.stringify(truthyString)).closed, false, 'only boolean true closes');

  const bad = [
    'not json', '', 'null', '42', '{}',
    JSON.stringify({ stream: 'btcusdt@trade', data: { e: 'trade', s: 'BTCUSDT', p: '1' } }),
    JSON.stringify(wrapped('BTC', binanceKline('DOGE', t, 1, 1, 1, 1, 1, true))),
    JSON.stringify(Object.assign(binanceKline('BTC', t, 1, 1, 1, 1, 1, true), { k: Object.assign(binanceKline('BTC', t, 1, 1, 1, 1, 1, true).k, { i: '5m' }) })),
    JSON.stringify(binanceKline('BTC', t, 'abc', 1, 1, 1, 1, true)),
    JSON.stringify(binanceKline('BTC', t, -1, 1, 1, 1, 1, true)),
    JSON.stringify(binanceKline('BTC', t + 5, 1, 1, 1, 1, 1, true)),
    JSON.stringify({ result: null, id: 1 })
  ];
  bad.forEach((raw) => assert.strictEqual(M.parseBinanceMessage(raw), null, 'ignored: ' + raw.slice(0, 80)));
});

test('binance REST parser keeps closed klines only, oldest first, deduplicated', () => {
  const now = 1760000030000;
  const rows = restRows('BTC', now, 120);
  rows.push(rows[5].slice()); // duplicate
  rows.push(['x', 'y']); // malformed
  rows.push([rows[0][0] - MINUTE, 'NaN', '1', '1', '1', '1', rows[0][0] - 1]);
  const list = M.parseBinanceRestKlines(rows.reverse(), 'BTC', now);
  assert.strictEqual(list.length, 119, 'forming candle dropped');
  list.forEach((c, i) => {
    assertOhlc(c, 'rest');
    assert.strictEqual(c.closed, true);
    if (i) assert.strictEqual(c.t - list[i - 1].t, MINUTE);
  });
  assert.strictEqual(list[list.length - 1].t, Math.floor(now / MINUTE) * MINUTE - MINUTE);
  assert.deepStrictEqual(M.parseBinanceRestKlines({ code: -1 }, 'BTC', now), []);
});

test('binance live: REST warm-up history precedes live candles; duplicates and malformed ignored', async () => {
  const rec = recorder();
  const clock = createClock(1760000030000);
  const WS = createFakeWebSocketClass();
  const urls = [];
  const fetchImpl = (url) => {
    urls.push(url);
    const symbol = /symbol=([A-Z]+)USDT/.exec(url)[1];
    return Promise.resolve({ ok: true, json: () => Promise.resolve(restRows(symbol, clock.now(), 120)) });
  };
  const hub = hubWith(rec, clock, { WebSocketImpl: WS, fetchImpl });
  hub.start();
  assert.deepStrictEqual(urls, SYMBOLS.map((s) => 'https://data-api.binance.vision/api/v3/klines?symbol=' + s + 'USDT&interval=1m&limit=120'));
  assert.strictEqual(WS.instances.length, 1);
  assert.strictEqual(WS.last().url, 'wss://data-stream.binance.vision/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m/solusdt@kline_1m/linkusdt@kline_1m/avaxusdt@kline_1m');
  let st = rec.statuses();
  assert.strictEqual(st.length, 1);
  assert.deepStrictEqual([st[0].source, st[0].state], ['binance', 'connecting']);
  assert.ok(/Binance/.test(st[0].detail));

  await settle();
  assert.strictEqual(rec.of('market:history').length, 0, 'no history before the stream proves live');
  const ws = WS.last();
  ws.open();
  ws.receive('garbage');
  assert.strictEqual(rec.statuses().length, 1, 'malformed message does not make the source live');

  const minute = Math.floor(clock.now() / MINUTE) * MINUTE;
  ws.receive(wrapped('BTC', binanceKline('BTC', minute, 101, 104, 100, 103, 2, false)));
  const kinds = rec.events.map((e) => e.topic);
  assert.deepStrictEqual(kinds, ['market:status', 'market:status', 'market:history', 'market:candle']);
  st = rec.statuses()[1];
  assert.deepStrictEqual([st.source, st.state, st.candleMs, st.speed], ['binance', 'live', 60000, 1]);
  assert.ok(/em direto/.test(st.detail) && /Histórico recente carregado/.test(st.detail), st.detail);
  const history = rec.of('market:history')[0];
  assert.strictEqual(history.source, 'binance');
  SYMBOLS.forEach((s) => {
    assert.ok(history.candles[s].length >= 60, s + ' warm-up length');
    assert.strictEqual(history.candles[s][history.candles[s].length - 1].t, minute - MINUTE);
  });
  assert.strictEqual(rec.of('market:candle')[0].closed, false);

  clock.advance(30000);
  ws.receive(wrapped('BTC', binanceKline('BTC', minute, 101, 106, 99, 105, 9, true)));
  ws.receive(wrapped('BTC', binanceKline('BTC', minute, 101, 106, 99, 105, 9, true))); // late duplicate
  ws.receive(wrapped('BTC', binanceKline('BTC', minute, 101, 106, 99, 104, 9, false))); // stale intrabar
  ws.receive(JSON.stringify({ stream: 'x', data: { e: 'kline', k: { s: 'BTCUSDT', o: 'oops' } } }));
  ws.receive(wrapped('ETH', binanceKline('ETH', minute - MINUTE, 1, 1, 1, 1, 1, true))); // already in history
  ws.receive(wrapped('BTC', binanceKline('BTC', minute + MINUTE, 105, 105, 105, 105, 0.1, false)));
  const btc = rec.of('market:candle').filter((c) => c.symbol === 'BTC');
  assert.deepStrictEqual(btc.map((c) => [c.t - minute, c.closed]), [[0, false], [0, true], [MINUTE, false]]);
  assert.strictEqual(rec.of('market:candle').filter((c) => c.symbol === 'ETH').length, 0);
  assert.strictEqual(hub.source, 'binance');
  hub.stop();
  assert.ok(ws.closed, 'socket closed on stop');
});

test('binance: REST failure is non-fatal (history empty, live candles still flow)', async () => {
  const rec = recorder();
  const clock = createClock(1760000030000);
  const WS = createFakeWebSocketClass();
  const hub = hubWith(rec, clock, { WebSocketImpl: WS, fetchImpl: () => Promise.reject(new TypeError('Failed to fetch (CORS)')) });
  hub.start();
  await settle();
  const minute = Math.floor(clock.now() / MINUTE) * MINUTE;
  WS.last().receive(wrapped('SOL', binanceKline('SOL', minute, 150, 151, 149, 150.5, 4, false)));
  const st = rec.statuses().pop();
  assert.strictEqual(st.state, 'live');
  assert.ok(/Histórico indisponível/.test(st.detail), st.detail);
  const history = rec.of('market:history')[0];
  SYMBOLS.forEach((s) => assert.deepStrictEqual(history.candles[s], []));
  assert.strictEqual(rec.of('market:candle').length, 1);
  hub.stop();
});

test('binance: a hanging REST warm-up delays go-live by at most liveTimeoutMs', async () => {
  const rec = recorder();
  const clock = createClock(1760000030000);
  const WS = createFakeWebSocketClass();
  const hub = hubWith(rec, clock, { WebSocketImpl: WS, fetchImpl: () => new Promise(() => {}) });
  hub.start();
  const minute = Math.floor(clock.now() / MINUTE) * MINUTE;
  clock.advance(1000);
  WS.last().receive(wrapped('BTC', binanceKline('BTC', minute, 1, 1, 1, 1, 1, false)));
  clock.advance(2000);
  WS.last().receive(wrapped('BTC', binanceKline('BTC', minute, 1, 2, 1, 2, 1, false)));
  assert.strictEqual(rec.statuses().length, 1, 'still waiting for warm-up');
  clock.advance(4000); // REST timeout at 7 s; first-message timer was already cleared
  await settle();
  const st = rec.statuses();
  assert.deepStrictEqual(st.map((s) => s.state), ['connecting', 'live']);
  assert.deepStrictEqual(rec.events.map((e) => e.topic), ['market:status', 'market:status', 'market:history', 'market:candle', 'market:candle']);
  hub.stop();
});

// --------------------------------------------------------------- Coinbase

function ticker(product, price, time, size) {
  const msg = { type: 'ticker', sequence: 1, product_id: product, price: String(price), open_24h: '1', volume_24h: '1',
    low_24h: '1', high_24h: '1', volume_30d: '1', best_bid: '1', best_bid_size: '1', best_ask: '1', best_ask_size: '1',
    side: 'buy', time: new Date(time).toISOString(), trade_id: 1 };
  if (size !== undefined) msg.last_size = String(size);
  return msg;
}

test('coinbase aggregator: 1-minute OHLCV, partial first minute, rollover, throttle, flush, malformed ignored', () => {
  const clock = createClock(1760000000000);
  const agg = M.createCoinbaseAggregator({ now: clock.now, intrabarMs: 1000, graceMs: 1500 });
  const m0 = Math.floor(clock.now() / MINUTE) * MINUTE;
  const m1 = m0 + MINUTE;
  const m2 = m1 + MINUTE;

  let out = agg.ingest(ticker('BTC-USD', 100, m0 + 30000, 0.5));
  assert.strictEqual(out.length, 1, 'first tick shows intrabar');
  assert.strictEqual(out[0].closed, false);

  out = agg.ingest(JSON.stringify(ticker('BTC-USD', 101, m1 + 1000, 0.25)));
  assert.deepStrictEqual(out.map((c) => [c.t, c.closed]), [[m1, false]], 'partial minute m0 is never emitted as closed');

  clock.advance(200);
  assert.deepStrictEqual(agg.ingest(ticker('BTC-USD', 107, m1 + 2000, 1)), [], 'intrabar throttled to 1/s');
  assert.deepStrictEqual(agg.ingest(ticker('BTC-USD', 98, m1 + 3000)), [], 'missing last_size counts as 0');
  clock.advance(900);
  out = agg.ingest(ticker('BTC-USD', 103, m1 + 50000, 0.75));
  assert.deepStrictEqual(out, [{ symbol: 'BTC', t: m1, o: 101, h: 107, l: 98, c: 103, v: 2, closed: false, source: 'coinbase' }]);

  out = agg.ingest(ticker('BTC-USD', 104, m2 + 500, 0.1));
  assert.deepStrictEqual(out.map((c) => [c.t, c.closed]), [[m1, true], [m2, false]]);
  assert.deepStrictEqual(out[0], { symbol: 'BTC', t: m1, o: 101, h: 107, l: 98, c: 103, v: 2, closed: true, source: 'coinbase' });

  assert.deepStrictEqual(agg.ingest(ticker('BTC-USD', 1, m1 + 59000, 1)), [], 'late tick for a closed minute ignored');

  [null, 'nope', { type: 'subscriptions', channels: [] }, { type: 'heartbeat' },
    ticker('DOGE-USD', 1, m2, 1), ticker('BTC-USD', 'NaN', m2, 1), Object.assign(ticker('BTC-USD', 1, m2, 1), { price: undefined }),
    ticker('BTC-USD', -5, m2, 1)].forEach((msg) => assert.strictEqual(agg.ingest(msg), null, 'ignored ' + JSON.stringify(msg)));

  clock.advance((m2 + MINUTE + 1499) - clock.now());
  assert.deepStrictEqual(agg.flush(), [], 'grace period not over');
  clock.advance(1);
  out = agg.flush();
  assert.deepStrictEqual(out.map((c) => [c.symbol, c.t, c.closed, c.o, c.c]), [['BTC', m2, true, 104, 104]]);
});

test('coinbase via hub: subscribe message, live status, history (empty) before candles', () => {
  const rec = recorder();
  const clock = createClock(1760000000000);
  const WS = createFakeWebSocketClass({ throwFor: (url) => url.includes('binance') });
  const hub = hubWith(rec, clock, { WebSocketImpl: WS });
  hub.start();
  const st = rec.statuses();
  assert.deepStrictEqual(st.map((s) => [s.source, s.state]), [['binance', 'connecting'], ['binance', 'error'], ['coinbase', 'connecting']]);
  assert.ok(/bloqueou a ligação à Binance/.test(st[1].detail) && /Coinbase/.test(st[1].detail), st[1].detail);
  const ws = WS.last();
  assert.strictEqual(ws.url, 'wss://ws-feed.exchange.coinbase.com');
  ws.open();
  assert.deepStrictEqual(JSON.parse(ws.sent[0]), {
    type: 'subscribe', product_ids: ['BTC-USD', 'ETH-USD', 'SOL-USD', 'LINK-USD', 'AVAX-USD'], channels: ['ticker']
  });
  ws.receive({ type: 'subscriptions', channels: [{ name: 'ticker', product_ids: ['BTC-USD'] }] });
  assert.strictEqual(rec.statuses().length, 3, 'subscription ack is not market data');
  const m0 = Math.floor(clock.now() / MINUTE) * MINUTE;
  ws.receive(ticker('ETH-USD', 3000, m0 + 1000, 0.1));
  const after = rec.events.slice(3).map((e) => e.topic);
  assert.deepStrictEqual(after, ['market:status', 'market:history', 'market:candle']);
  const live = rec.statuses().pop();
  assert.deepStrictEqual([live.source, live.state], ['coinbase', 'live']);
  assert.ok(/agregados localmente/.test(live.detail), live.detail);
  SYMBOLS.forEach((s) => assert.deepStrictEqual(rec.of('market:history')[0].candles[s], []));

  // ETH trades once in minute m1 and then goes quiet; BTC keeps the socket busy.
  // The ETH bar for m1 is closed by the local-clock flush 1.5 s after m1 ends.
  const m1 = m0 + MINUTE;
  const m2 = m1 + MINUTE;
  const pump = (until) => {
    while (clock.now() + 10000 <= until) {
      clock.advance(10000);
      ws.receive(ticker('BTC-USD', 64000, clock.now(), 0.01));
    }
    clock.advance(until - clock.now());
  };
  pump(m1 + 100);
  ws.receive(ticker('ETH-USD', 3010, m1 + 100, 0.2));
  pump(m2 + 1499);
  const closedOf = (s) => rec.of('market:candle').filter((c) => c.closed && c.symbol === s);
  assert.deepStrictEqual(closedOf('ETH'), [], 'not closed before the grace period');
  assert.deepStrictEqual(closedOf('BTC').map((c) => c.t), [m1], 'BTC m1 closed by the first m2 trade');
  clock.advance(1001); // the flush runs on the hub's 1 s pulse
  assert.deepStrictEqual(closedOf('ETH').map((c) => [c.t, c.o, c.c, c.v]), [[m1, 3010, 3010, 0.2]]);
  assert.strictEqual(rec.statuses().pop().state, 'live');
  hub.stop();
  assert.strictEqual(clock.pending(), 0, 'all timers cleared on stop');
});

test('coinbase error message before going live falls through to the simulated market', () => {
  const rec = recorder();
  const clock = createClock(1760000000000);
  const WS = createFakeWebSocketClass({ throwFor: (url) => url.includes('binance') });
  const hub = hubWith(rec, clock, { WebSocketImpl: WS });
  hub.start();
  WS.last().open();
  WS.last().receive({ type: 'error', message: 'Failed to subscribe', reason: 'x' });
  const st = rec.statuses();
  assert.deepStrictEqual(st.slice(-2).map((s) => [s.source, s.state]), [['coinbase', 'error'], ['sim', 'fallback']]);
  assert.ok(/recusou a subscrição/.test(st[st.length - 2].detail));
  hub.stop();
});

// --------------------------------------------------------------- fallback

test('fallback chain timing: binance timeout -> coinbase timeout -> sim, with pt-PT details', async () => {
  const rec = recorder();
  const clock = createClock(1760000000000);
  const WS = createFakeWebSocketClass();
  const hub = hubWith(rec, clock, { WebSocketImpl: WS, liveTimeoutMs: 7000 });
  hub.start();
  assert.strictEqual(WS.instances.length, 1);
  WS.last().open();
  clock.advance(6999);
  assert.strictEqual(rec.statuses().length, 1, 'still connecting to Binance at 6.999 s');
  clock.advance(1);
  let st = rec.statuses();
  assert.deepStrictEqual(st.map((s) => [s.source, s.state]), [['binance', 'connecting'], ['binance', 'error'], ['coinbase', 'connecting']]);
  assert.strictEqual(st[1].detail, 'A Binance não enviou dados em 7 s. A tentar a Coinbase…');
  assert.ok(WS.instances[0].closed, 'binance socket closed');
  assert.strictEqual(WS.instances.length, 2);
  assert.strictEqual(WS.instances[1].url, 'wss://ws-feed.exchange.coinbase.com');
  assert.strictEqual(hub.source, 'coinbase');

  WS.instances[1].open();
  clock.advance(6999);
  assert.strictEqual(rec.statuses().length, 3, 'still connecting to Coinbase');
  clock.advance(1);
  st = rec.statuses();
  assert.deepStrictEqual(st.slice(3).map((s) => [s.source, s.state]), [['coinbase', 'error'], ['sim', 'fallback']]);
  assert.strictEqual(st[3].detail, 'A Coinbase não enviou dados em 7 s. A mudar para o mercado simulado.');
  assert.ok(/^Binance e Coinbase indisponíveis\. Mercado simulado \(semente 42\)/.test(st[4].detail), st[4].detail);
  assert.strictEqual(st[4].candleMs, 4000);
  assert.ok(WS.instances[1].closed, 'coinbase socket closed');
  const history = rec.of('market:history');
  assert.strictEqual(history.length, 1);
  assert.strictEqual(history[0].source, 'sim');
  SYMBOLS.forEach((s) => assert.strictEqual(history[0].candles[s].length, 120));
  assert.strictEqual(hub.source, 'sim');

  clock.advance(4000);
  const closed = rec.of('market:candle').filter((c) => c.closed);
  assert.strictEqual(closed.length, 5);
  closed.forEach((c) => assert.strictEqual(c.source, 'sim'));
  await settle(); // pending REST warm-up resolves into a dead run: nothing changes
  assert.strictEqual(rec.of('market:history').length, 1);
  assert.strictEqual(WS.instances.length, 2, 'no more sockets once on sim');
  hub.stop();
});

test('fallback: socket closing before any data and missing WebSocket both advance the chain', () => {
  const rec = recorder();
  const clock = createClock(1760000000000);
  const WS = createFakeWebSocketClass();
  const hub = hubWith(rec, clock, { WebSocketImpl: WS });
  hub.start();
  WS.last().fail();
  assert.deepStrictEqual(rec.statuses().map((s) => [s.source, s.state]), [['binance', 'connecting'], ['binance', 'error'], ['coinbase', 'connecting']]);
  assert.ok(/Erro de rede na ligação à Binance/.test(rec.statuses()[1].detail));
  WS.last().drop();
  assert.deepStrictEqual(rec.statuses().slice(3).map((s) => [s.source, s.state]), [['coinbase', 'error'], ['sim', 'fallback']]);
  assert.ok(/A ligação à Coinbase foi fechada/.test(rec.statuses()[3].detail));
  hub.stop();

  const rec2 = recorder();
  const hub2 = hubWith(rec2, createClock(1760000000000), { WebSocketImpl: null });
  hub2.start();
  const st = rec2.statuses();
  assert.deepStrictEqual(st.map((s) => s.state), ['connecting', 'error', 'connecting', 'error', 'fallback']);
  assert.ok(/não suporta WebSocket/.test(st[1].detail));
  hub2.stop();
});

test('live drop: one reconnect succeeds; a second quick drop falls back to sim anchored at the last real prices', () => {
  const rec = recorder();
  const clock = createClock(1760000030000);
  const WS = createFakeWebSocketClass();
  const hub = hubWith(rec, clock, { WebSocketImpl: WS, fetchImpl: () => Promise.reject(new Error('offline')) });
  hub.start();
  return settle().then(() => {
    const minute = Math.floor(clock.now() / MINUTE) * MINUTE;
    WS.last().receive(wrapped('BTC', binanceKline('BTC', minute, 61000, 61300, 60900, 61234.5, 3, false)));
    assert.strictEqual(rec.statuses().pop().state, 'live');

    WS.last().drop();
    let st = rec.statuses().pop();
    assert.deepStrictEqual([st.source, st.state], ['binance', 'connecting']);
    assert.strictEqual(st.detail, 'A ligação à Binance foi fechada. A tentar voltar a ligar à Binance…');
    assert.strictEqual(WS.instances.length, 1, 'reconnect waits a moment');
    clock.advance(1000);
    assert.strictEqual(WS.instances.length, 2);
    assert.strictEqual(WS.last().url, WS.instances[0].url);
    WS.last().receive(wrapped('BTC', binanceKline('BTC', minute, 61000, 61300, 60900, 61234.5, 4, false)));
    st = rec.statuses().pop();
    assert.deepStrictEqual([st.source, st.state], ['binance', 'live']);
    assert.ok(/restabelecida/.test(st.detail));
    assert.strictEqual(rec.of('market:history').length, 1, 'a reconnect is not a source switch');

    clock.advance(5000);
    WS.last().drop();
    const tail = rec.statuses().slice(-2);
    assert.deepStrictEqual(tail.map((s) => [s.source, s.state]), [['binance', 'error'], ['sim', 'fallback']]);
    assert.ok(/^Ligação à Binance perdida\. Mercado simulado \(semente 42\), a partir dos últimos preços reais/.test(tail[1].detail), tail[1].detail);
    const history = rec.of('market:history').pop();
    assert.strictEqual(history.source, 'sim');
    const lastBtc = history.candles.BTC[119];
    assert.strictEqual(lastBtc.c, 61234.5, 'sim continues from the last real BTC price');
    assertOhlc(lastBtc, 'anchored');
    assert.ok(Math.abs(history.candles.ETH[119].c - M.SIM_PROFILES.ETH.start) / M.SIM_PROFILES.ETH.start < 0.5, 'symbols without a real price keep illustrative levels');
    assert.strictEqual(WS.instances.length, 2, 'no further reconnect');
    hub.stop();
  });
});

test('live drop: failed reconnect attempt (no data in time) falls back to sim', async () => {
  const rec = recorder();
  const clock = createClock(1760000030000);
  const WS = createFakeWebSocketClass();
  const hub = hubWith(rec, clock, { WebSocketImpl: WS, fetchImpl: () => Promise.reject(new Error('offline')) });
  hub.start();
  await settle();
  const minute = Math.floor(clock.now() / MINUTE) * MINUTE;
  WS.last().receive(wrapped('ETH', binanceKline('ETH', minute, 3000, 3001, 2999, 3000.5, 1, false)));
  WS.last().fail();
  clock.advance(1000);
  assert.strictEqual(WS.instances.length, 2);
  clock.advance(6999);
  assert.strictEqual(rec.statuses().pop().state, 'connecting');
  clock.advance(1);
  const tail = rec.statuses().slice(-2);
  assert.deepStrictEqual(tail.map((s) => [s.source, s.state]), [['binance', 'error'], ['sim', 'fallback']]);
  assert.strictEqual(tail[0].detail, 'A Binance não enviou dados em 7 s. Não foi possível manter a ligação à Binance.');
  assert.strictEqual(rec.of('market:history').pop().candles.ETH[119].c, 3000.5);
  hub.stop();
});

test('live silence watchdog treats a mute socket as a drop', async () => {
  const rec = recorder();
  const clock = createClock(1760000030000);
  const WS = createFakeWebSocketClass();
  const hub = hubWith(rec, clock, { WebSocketImpl: WS, fetchImpl: () => Promise.reject(new Error('offline')) });
  hub.start();
  await settle();
  const minute = Math.floor(clock.now() / MINUTE) * MINUTE;
  WS.last().receive(wrapped('BTC', binanceKline('BTC', minute, 1, 1, 1, 1, 1, false)));
  clock.advance(30000);
  assert.strictEqual(rec.statuses().pop().state, 'live');
  clock.advance(1000);
  const st = rec.statuses().pop();
  assert.strictEqual(st.state, 'connecting');
  assert.ok(/deixou de enviar dados há mais de 30 s/.test(st.detail), st.detail);
  assert.ok(WS.instances[0].closed);
  hub.stop();
});

test('prefer sim touches no network; start is idempotent; stop then start restarts cleanly', () => {
  const rec = recorder();
  const clock = createClock(1760000000000);
  const WS = createFakeWebSocketClass();
  let fetches = 0;
  const hub = hubWith(rec, clock, { prefer: 'sim', WebSocketImpl: WS, fetchImpl: () => { fetches++; return new Promise(() => {}); } });
  hub.start();
  hub.start();
  assert.strictEqual(rec.of('market:history').length, 1);
  clock.advance(8000);
  assert.strictEqual(WS.instances.length, 0);
  assert.strictEqual(fetches, 0);
  hub.stop();
  assert.strictEqual(clock.pending(), 0, 'all timers cleared');
  hub.start();
  assert.strictEqual(rec.of('market:history').length, 2);
  assert.deepStrictEqual(rec.of('market:history')[1].candles.BTC.map((c) => c.c), rec.of('market:history')[0].candles.BTC.map((c) => c.c));
  hub.stop();
});

test('a status listener that stops the hub halts the fallback chain', () => {
  const rec = recorder();
  const clock = createClock(1760000000000);
  const WS = createFakeWebSocketClass({ throwFor: () => true });
  const hub = hubWith(rec, clock, { WebSocketImpl: WS });
  rec.bus.on('market:status', (s) => { if (s.state === 'error') hub.stop(); });
  hub.start();
  assert.deepStrictEqual(rec.statuses().map((s) => s.state), ['connecting', 'error']);
  assert.strictEqual(clock.pending(), 0);
});

// ------------------------------------------------------------------ runner

(async () => {
  let failed = 0;
  for (const t of tests) {
    try {
      await t.fn();
      console.log('ok   ' + t.name);
    } catch (err) {
      failed++;
      console.error('FAIL ' + t.name + '\n     ' + (err && err.stack ? err.stack : err));
    }
  }
  console.log('\n' + (tests.length - failed) + '/' + tests.length + ' passed');
  if (failed) process.exitCode = 1;
})();

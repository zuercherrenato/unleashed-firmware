/*
 * AI Fund Office — market data hub (paper trading only; public market data, no keys).
 *
 * Source chain for prefer === 'auto': Binance -> Coinbase -> seeded simulated market.
 *
 * Endpoints and message formats were checked against the official public docs:
 *   Binance (official spot API docs repository, mirrored at developers.binance.com):
 *     https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
 *       - wss://data-stream.binance.vision serves market data only
 *       - combined streams: /stream?streams=<a>/<b>, events wrapped as {"stream": ..., "data": ...}
 *       - stream names are lowercase: <symbol>@kline_<interval>; kline updates every 2000 ms
 *       - payload {e:'kline', E, s, k:{t, T, s, i, f, L, o, c, h, l, v, n, x, q, V, Q, B}};
 *         prices/volumes are strings, k.x is a boolean "is this kline closed?"
 *     https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md
 *       - GET /api/v3/klines?symbol&interval&limit (default 500, max 1000); rows are
 *         [openTime, "open", "high", "low", "close", "volume", closeTime, ...]
 *     https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md
 *       - data-api.binance.vision serves GET /api/v3/klines
 *   Coinbase Exchange:
 *     https://docs.cdp.coinbase.com/exchange/websocket-feed/overview
 *       - wss://ws-feed.exchange.coinbase.com, market data without authentication,
 *         a subscribe message must be sent within 5 s of connecting
 *     https://docs.cdp.coinbase.com/exchange/websocket-feed/channels
 *       - subscribe {"type":"subscribe","product_ids":[...],"channels":["ticker"]}
 *       - ticker message {type:'ticker', product_id, price, time (ISO 8601), trade_id,
 *         last_size, best_bid, best_ask, ...}; numeric fields are strings
 *   Parsers below are defensive: anything that does not match is ignored.
 */
(function (root, factory) {
  var AFO = (root.AFO = root.AFO || {});
  var api = factory(AFO, root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  Object.assign(AFO, api);
})(typeof self !== 'undefined' ? self : globalThis, function (AFO, root) {
  'use strict';

  var SYMBOLS = ['BTC', 'ETH', 'SOL', 'LINK', 'AVAX'];
  var MINUTE_MS = 60000;
  var HISTORY_CANDLES = 120;
  var ALLOWED_SPEEDS = [1, 4, 16];

  var BINANCE_REST_URL = 'https://data-api.binance.vision/api/v3/klines';
  var BINANCE_WS_URL = 'wss://data-stream.binance.vision/stream';
  var COINBASE_WS_URL = 'wss://ws-feed.exchange.coinbase.com';

  var BINANCE_PAIRS = { BTC: 'BTCUSDT', ETH: 'ETHUSDT', SOL: 'SOLUSDT', LINK: 'LINKUSDT', AVAX: 'AVAXUSDT' };
  var COINBASE_PRODUCTS = { BTC: 'BTC-USD', ETH: 'ETH-USD', SOL: 'SOL-USD', LINK: 'LINK-USD', AVAX: 'AVAX-USD' };
  var SYMBOL_BY_BINANCE_PAIR = invert(BINANCE_PAIRS);
  var SYMBOL_BY_COINBASE_PRODUCT = invert(COINBASE_PRODUCTS);

  var RECONNECT_DELAY_MS = 1000;
  var STABLE_MS = 120000; // a live link this old earns a fresh reconnect attempt
  var PULSE_MS = 1000;
  var MAX_BUFFER = 1000;
  var COINBASE_INTRABAR_MS = 1000;
  var COINBASE_CLOSE_GRACE_MS = 1500;

  // Simulated market. Start prices and volumes are illustrative, not quotes.
  // vol = standard deviation of the 1-minute log-return in the 'normal' regime.
  var SIM_PROFILES = {
    BTC: { start: 64000, vol: 0.0008, volume: 30 },
    ETH: { start: 3200, vol: 0.0011, volume: 420 },
    SOL: { start: 150, vol: 0.0016, volume: 6500 },
    LINK: { start: 14.5, vol: 0.0015, volume: 32000 },
    AVAX: { start: 28, vol: 0.0019, volume: 16000 }
  };
  // Markov chain over market-wide volatility regimes, one transition per candle.
  var SIM_REGIMES = [
    { name: 'calm', volMult: 0.6, next: [0.97, 0.03, 0] },
    { name: 'normal', volMult: 1.0, next: [0.02, 0.96, 0.02] },
    { name: 'stormy', volMult: 1.9, next: [0, 0.08, 0.92] }
  ];
  var SIM_START_REGIME = 1;
  var SIM_FACTOR_CORR = 0.6; // pairwise correlation of returns through the shared factor
  var SIM_VISIBLE_STEPS = 8; // intrabar updates per candle (the last one closes it)
  var SIM_MICRO_STEPS = 4; // path resolution between visible steps (for realistic wicks)

  function invert(map) {
    var out = {};
    Object.keys(map).forEach(function (k) { out[map[k]] = k; });
    return out;
  }

  function toNumber(value) {
    if (typeof value === 'number') return value;
    if (typeof value === 'string' && value.trim() !== '') return Number(value);
    return NaN;
  }

  function isPositive(n) {
    return typeof n === 'number' && isFinite(n) && n > 0;
  }

  function positiveOr(value, fallback) {
    return isPositive(value) ? value : fallback;
  }

  function secondsText(ms) {
    return String(Math.round(ms / 100) / 10).replace('.', ',') + ' s';
  }

  // Normalised candle, or null when any price is unusable. High/low are widened to
  // contain open and close so l <= min(o, c) and h >= max(o, c) always hold.
  function makeCandle(symbol, t, o, h, l, c, v, closed, source) {
    if (typeof t !== 'number' || !isFinite(t)) return null;
    if (!isPositive(o) || !isPositive(h) || !isPositive(l) || !isPositive(c)) return null;
    return {
      symbol: symbol,
      t: t,
      o: o,
      h: Math.max(h, o, c),
      l: Math.min(l, o, c),
      c: c,
      v: typeof v === 'number' && isFinite(v) && v >= 0 ? v : 0,
      closed: closed === true,
      source: source
    };
  }

  function parseJson(raw) {
    if (raw && typeof raw === 'object') return raw;
    if (typeof raw !== 'string') return null;
    try {
      var parsed = JSON.parse(raw);
      return parsed && typeof parsed === 'object' ? parsed : null;
    } catch (e) {
      return null;
    }
  }

  // ---------------------------------------------------------------- Binance

  function binanceStreamUrl() {
    var streams = SYMBOLS.map(function (s) { return BINANCE_PAIRS[s].toLowerCase() + '@kline_1m'; });
    return BINANCE_WS_URL + '?streams=' + streams.join('/');
  }

  function binanceKlinesUrl(symbol) {
    return BINANCE_REST_URL + '?symbol=' + BINANCE_PAIRS[symbol] + '&interval=1m&limit=' + HISTORY_CANDLES;
  }

  // Accepts a combined-stream event ({stream, data}) or a raw kline event.
  function parseBinanceMessage(raw) {
    var msg = parseJson(raw);
    if (!msg) return null;
    var payload = typeof msg.stream === 'string' && msg.data && typeof msg.data === 'object' ? msg.data : msg;
    if (payload.e !== 'kline' || !payload.k || typeof payload.k !== 'object') return null;
    var k = payload.k;
    if (k.i !== undefined && k.i !== '1m') return null;
    var symbol = SYMBOL_BY_BINANCE_PAIR[String(k.s || payload.s || '').toUpperCase()];
    if (!symbol) return null;
    var t = toNumber(k.t);
    if (!isFinite(t) || t % MINUTE_MS !== 0) return null;
    return makeCandle(symbol, t, toNumber(k.o), toNumber(k.h), toNumber(k.l), toNumber(k.c),
      toNumber(k.v), k.x === true, 'binance');
  }

  // Keeps only klines whose close time has passed (the endpoint also returns the
  // candle that is still forming). Oldest first, unique open times.
  function parseBinanceRestKlines(rows, symbol, nowMs) {
    if (!Array.isArray(rows) || !BINANCE_PAIRS[symbol]) return [];
    var out = [];
    rows.forEach(function (row) {
      if (!Array.isArray(row) || row.length < 6) return;
      var t = toNumber(row[0]);
      if (!isFinite(t) || t % MINUTE_MS !== 0) return;
      var closeTime = toNumber(row[6]);
      if (!isFinite(closeTime)) closeTime = t + MINUTE_MS - 1;
      if (closeTime >= nowMs) return;
      var candle = makeCandle(symbol, t, toNumber(row[1]), toNumber(row[2]), toNumber(row[3]),
        toNumber(row[4]), toNumber(row[5]), true, 'binance');
      if (candle) out.push(candle);
    });
    out.sort(function (a, b) { return a.t - b.t; });
    return out.filter(function (c, i) { return i === 0 || c.t !== out[i - 1].t; });
  }

  function createBinanceAdapter() {
    return {
      url: binanceStreamUrl(),
      onOpen: function () { return null; },
      handle: function (data, out) {
        var candle = parseBinanceMessage(data);
        if (!candle) return false;
        out.push(candle);
        return true;
      }
    };
  }

  // --------------------------------------------------------------- Coinbase

  // Aggregates Coinbase 'ticker' messages into 1-minute OHLCV candles keyed by
  // minute start (from the message time). The first bar per symbol after a
  // (re)connect is partial, so it is shown intrabar but never emitted as closed.
  function createCoinbaseAggregator(options) {
    options = options || {};
    var now = typeof options.now === 'function' ? options.now : function () { return Date.now(); };
    var intrabarMs = isFinite(options.intrabarMs) && options.intrabarMs >= 0 ? options.intrabarMs : COINBASE_INTRABAR_MS;
    var graceMs = isFinite(options.graceMs) && options.graceMs >= 0 ? options.graceMs : COINBASE_CLOSE_GRACE_MS;
    var bars = {};
    var lastClosedT = {};
    var startedSinceReset = {};

    function toCandle(bar, symbol, closed) {
      return makeCandle(symbol, bar.t, bar.o, bar.h, bar.l, bar.c, bar.v, closed, 'coinbase');
    }

    function closeBar(symbol, out) {
      var bar = bars[symbol];
      bars[symbol] = null;
      if (!bar) return;
      if (bar.partial) return;
      lastClosedT[symbol] = bar.t;
      var candle = toCandle(bar, symbol, true);
      if (candle) out.push(candle);
    }

    // Returns null for messages that are not usable tickers, else the candles to emit.
    function ingest(raw) {
      var msg = parseJson(raw);
      if (!msg || msg.type !== 'ticker') return null;
      var symbol = SYMBOL_BY_COINBASE_PRODUCT[msg.product_id];
      var price = toNumber(msg.price);
      if (!symbol || !isPositive(price)) return null;
      var ms = typeof msg.time === 'string' ? Date.parse(msg.time) : NaN;
      if (!isFinite(ms)) ms = now();
      var minute = Math.floor(ms / MINUTE_MS) * MINUTE_MS;
      var size = toNumber(msg.last_size);
      if (!(isFinite(size) && size >= 0)) size = 0;

      var out = [];
      if (lastClosedT[symbol] !== undefined && minute <= lastClosedT[symbol]) return out;
      var bar = bars[symbol];
      if (bar && minute < bar.t) return out;
      if (bar && minute > bar.t) {
        closeBar(symbol, out);
        bar = null;
      }
      if (!bar) {
        bar = bars[symbol] = {
          t: minute, o: price, h: price, l: price, c: price, v: size,
          partial: !startedSinceReset[symbol], lastEmit: -Infinity
        };
        startedSinceReset[symbol] = true;
      } else {
        if (price > bar.h) bar.h = price;
        if (price < bar.l) bar.l = price;
        bar.c = price;
        bar.v += size;
      }
      var at = now();
      if (at - bar.lastEmit >= intrabarMs) {
        bar.lastEmit = at;
        var candle = toCandle(bar, symbol, false);
        if (candle) out.push(candle);
      }
      return out;
    }

    // Closes bars whose minute ended (local clock) more than graceMs ago.
    function flush() {
      var out = [];
      var at = now();
      SYMBOLS.forEach(function (symbol) {
        var bar = bars[symbol];
        if (bar && at >= bar.t + MINUTE_MS + graceMs) closeBar(symbol, out);
      });
      return out;
    }

    function reset() {
      bars = {};
      startedSinceReset = {};
    }

    return { ingest: ingest, flush: flush, reset: reset };
  }

  function coinbaseSubscribeMessage() {
    return {
      type: 'subscribe',
      product_ids: SYMBOLS.map(function (s) { return COINBASE_PRODUCTS[s]; }),
      channels: ['ticker']
    };
  }

  function createCoinbaseAdapter(now) {
    var aggregator = createCoinbaseAggregator({ now: now });
    function push(candles, out) {
      for (var i = 0; i < candles.length; i++) out.push(candles[i]);
    }
    return {
      url: COINBASE_WS_URL,
      onOpen: function (socket) {
        try {
          socket.send(JSON.stringify(coinbaseSubscribeMessage()));
          return null;
        } catch (e) {
          return 'error';
        }
      },
      handle: function (data, out) {
        var msg = parseJson(data);
        if (!msg) return false;
        if (msg.type === 'error') return 'rejected';
        var candles = aggregator.ingest(msg);
        if (!candles) return false;
        push(candles, out);
        return true;
      },
      tick: function (out) { push(aggregator.flush(), out); },
      reset: function () { aggregator.reset(); }
    };
  }

  // -------------------------------------------------------------- Simulated

  function mulberry32(seed) {
    var a = seed >>> 0;
    return function () {
      a = (a + 0x6D2B79F5) >>> 0;
      var t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  function makeRng(seed) {
    return typeof AFO.rng === 'function' ? AFO.rng(seed) : mulberry32(seed);
  }

  // Box–Muller with the spare value cached per generator (keeps runs reproducible).
  function makeGaussian(rand) {
    var spare = null;
    return function () {
      if (spare !== null) {
        var s = spare;
        spare = null;
        return s;
      }
      var u = 1 - rand();
      var v = rand();
      var r = Math.sqrt(-2 * Math.log(u));
      spare = r * Math.sin(2 * Math.PI * v);
      return r * Math.cos(2 * Math.PI * v);
    };
  }

  function toSeed(value) {
    var n = Number(value);
    return isFinite(n) ? Math.floor(Math.abs(n)) >>> 0 : 42;
  }

  // Deterministic candle generator: zero-drift geometric Brownian motion. Each
  // micro-step log-return is -σ²/2 + σ·ε (Itô correction, so E[price] stays flat),
  // with ε = √ρ·z_market + √(1−ρ)·z_symbol and σ scaled by the current regime.
  function createSimGenerator(seed, options) {
    options = options || {};
    var rand = makeRng(toSeed(seed));
    var gauss = makeGaussian(rand);
    var steps = SIM_VISIBLE_STEPS * SIM_MICRO_STEPS;
    var factorWeight = Math.sqrt(SIM_FACTOR_CORR);
    var ownWeight = Math.sqrt(1 - SIM_FACTOR_CORR);
    var prices = {};
    SYMBOLS.forEach(function (s) { prices[s] = SIM_PROFILES[s].start; });
    var regime = SIM_START_REGIME;
    var t = isFinite(options.startT) ? options.startT : 0;

    function stepRegime() {
      var row = SIM_REGIMES[regime].next;
      var u = rand();
      var acc = 0;
      for (var i = 0; i < row.length; i++) {
        acc += row[i];
        if (u < acc) {
          regime = i;
          return;
        }
      }
    }

    function nextCandle() {
      stepRegime();
      var mult = SIM_REGIMES[regime].volMult;
      var running = {};
      var frames = {};
      var sigmas = SYMBOLS.map(function (s) { return SIM_PROFILES[s].vol * mult / Math.sqrt(steps); });
      SYMBOLS.forEach(function (s) {
        var p = prices[s];
        running[s] = { o: p, h: p, l: p, c: p, v: 0 };
        frames[s] = [];
      });
      for (var k = 0; k < SIM_VISIBLE_STEPS; k++) {
        for (var m = 0; m < SIM_MICRO_STEPS; m++) {
          var market = gauss();
          for (var i = 0; i < SYMBOLS.length; i++) {
            var st = running[SYMBOLS[i]];
            var sigma = sigmas[i];
            var eps = factorWeight * market + ownWeight * gauss();
            st.c *= Math.exp(-0.5 * sigma * sigma + sigma * eps);
            if (st.c > st.h) st.h = st.c;
            if (st.c < st.l) st.l = st.c;
          }
        }
        for (var j = 0; j < SYMBOLS.length; j++) {
          var sym = SYMBOLS[j];
          var r = running[sym];
          // Lognormal noise with mean 1; busier tape in stormier regimes.
          r.v += SIM_PROFILES[sym].volume / SIM_VISIBLE_STEPS * (0.5 + 0.5 * mult) * Math.exp(0.4 * gauss() - 0.08);
          frames[sym].push({ o: r.o, h: r.h, l: r.l, c: r.c, v: r.v });
        }
      }
      SYMBOLS.forEach(function (s) { prices[s] = running[s].c; });
      var candleT = t;
      t += MINUTE_MS;
      return { t: candleT, regime: SIM_REGIMES[regime].name, frames: frames };
    }

    // Closed historical candles. With anchorPrices, each symbol's path is rescaled
    // (returns unchanged) so the last close equals the anchor and later candles continue from it.
    function history(count) {
      var out = {};
      SYMBOLS.forEach(function (s) { out[s] = []; });
      for (var n = 0; n < count; n++) {
        var candle = nextCandle();
        SYMBOLS.forEach(function (s) { out[s].push(frameCandle(candle, s, SIM_VISIBLE_STEPS - 1, true)); });
      }
      var anchors = options.anchorPrices || {};
      SYMBOLS.forEach(function (s) {
        if (!isPositive(anchors[s])) return;
        var f = anchors[s] / prices[s];
        prices[s] = anchors[s];
        out[s].forEach(function (c) {
          c.o *= f;
          c.h *= f;
          c.l *= f;
          c.c *= f;
        });
        var last = out[s][out[s].length - 1];
        if (last) {
          last.c = anchors[s];
          last.h = Math.max(last.h, last.o, last.c);
          last.l = Math.min(last.l, last.o, last.c);
        }
      });
      return out;
    }

    return { nextCandle: nextCandle, history: history, getPrice: function (s) { return prices[s]; } };
  }

  function frameCandle(candle, symbol, step, closed) {
    var f = candle.frames[symbol][step];
    return { symbol: symbol, t: candle.t, o: f.o, h: f.h, l: f.l, c: f.c, v: f.v, closed: closed, source: 'sim' };
  }

  // -------------------------------------------------------------------- Hub

  var SOURCE_NAMES = { binance: 'Binance', coinbase: 'Coinbase' };

  function failureText(reason, name, liveTimeoutMs, silenceMs) {
    switch (reason) {
      case 'unsupported': return 'Este navegador não suporta WebSocket.';
      case 'blocked': return 'O navegador ou a página bloqueou a ligação à ' + name + '.';
      case 'timeout': return 'A ' + name + ' não enviou dados em ' + secondsText(liveTimeoutMs) + '.';
      case 'closed': return 'A ligação à ' + name + ' foi fechada.';
      case 'silence': return 'A ' + name + ' deixou de enviar dados há mais de ' + secondsText(silenceMs) + '.';
      case 'rejected': return 'A ' + name + ' recusou a subscrição.';
      default: return 'Erro de rede na ligação à ' + name + '.';
    }
  }

  function createGate() {
    var lastT = {};
    var lastClosedT = {};
    return {
      seed: function (symbol, t) {
        lastT[symbol] = t;
        lastClosedT[symbol] = t;
      },
      pass: function (c) {
        if (lastT[c.symbol] !== undefined && c.t < lastT[c.symbol]) return false;
        if (lastClosedT[c.symbol] !== undefined && c.t <= lastClosedT[c.symbol]) return false;
        lastT[c.symbol] = c.t;
        if (c.closed) lastClosedT[c.symbol] = c.t;
        return true;
      }
    };
  }

  function createMarketHub(bus, opts) {
    if (!bus || typeof bus.emit !== 'function') throw new TypeError('createMarketHub: a bus with emit() is required');
    opts = opts || {};
    var prefer = opts.prefer === 'sim' ? 'sim' : 'auto';
    var seed = toSeed(opts.seed === undefined ? 42 : opts.seed);
    var simCandleMs = positiveOr(opts.simCandleMs, 4000);
    var liveTimeoutMs = positiveOr(opts.liveTimeoutMs, 7000);
    var silenceMs = Math.max(30000, liveTimeoutMs * 3);
    var WebSocketImpl = opts.WebSocketImpl !== undefined ? opts.WebSocketImpl
      : (typeof root.WebSocket === 'function' ? root.WebSocket : null);
    var fetchImpl = opts.fetchImpl !== undefined ? opts.fetchImpl
      : (typeof root.fetch === 'function' ? function (url, init) { return root.fetch(url, init); } : null);
    var clock = {
      setTimeout: opts.setTimeout || function (fn, ms) { return root.setTimeout(fn, ms); },
      clearTimeout: opts.clearTimeout || function (id) { root.clearTimeout(id); },
      setInterval: opts.setInterval || function (fn, ms) { return root.setInterval(fn, ms); },
      clearInterval: opts.clearInterval || function (id) { root.clearInterval(id); },
      now: typeof opts.now === 'function' ? opts.now : function () { return Date.now(); }
    };

    var generation = 0;
    var running = false;
    var speed = 1;
    var run = null;
    var status = {
      source: prefer === 'sim' ? 'sim' : 'binance',
      state: 'connecting',
      detail: 'Mercado ainda não iniciado.',
      candleMs: prefer === 'sim' ? simCandleMs : MINUTE_MS,
      speed: 1
    };

    function setStatus(source, state, detail) {
      var isSim = source === 'sim';
      status = {
        source: source,
        state: state,
        detail: detail,
        candleMs: isSim ? simCandleMs / speed : MINUTE_MS,
        speed: isSim ? speed : 1
      };
      bus.emit('market:status', Object.assign({}, status));
    }

    function simDetail(prefix, anchored) {
      return (prefix ? prefix + ' ' : '') + 'Mercado simulado (semente ' + seed + ')' +
        (anchored ? ', a partir dos últimos preços reais; os movimentos seguintes são ilustrativos.'
          : ': preços ilustrativos, não são cotações reais.');
    }

    // ---- live source (binance | coinbase)
    function startLive(kind) {
      var gen = generation;
      var name = SOURCE_NAMES[kind];
      var adapter = kind === 'binance' ? createBinanceAdapter() : createCoinbaseAdapter(clock.now);
      var ws = null;
      var firstTimer = null;
      var retryTimer = null;
      var pulseTimer = null;
      var dead = false;
      var awaitingFirst = true;
      var wentLive = false;
      var reconnectsLeft = 1;
      var liveSince = 0;
      var lastMessageAt = 0;
      var buffer = [];
      var gate = createGate();
      var lastPrices = {};
      var history = null;
      var historyReady = kind !== 'binance';
      var warmUpCancels = [];

      function alive() { return !dead && gen === generation; }

      run = { kind: kind, dispose: dispose, applySpeed: function () {} };
      setStatus(kind, 'connecting', kind === 'binance'
        ? 'A ligar à Binance (dados públicos de mercado, sem chaves)…'
        : 'A ligar à Coinbase (canal público «ticker», sem chaves)…');
      if (!alive()) return;

      if (kind === 'binance') {
        warmUpBinance(warmUpCancels).then(function (result) {
          if (!alive()) return;
          history = result;
          historyReady = true;
          if (!awaitingFirst && !wentLive) goLive();
        });
      }
      pulseTimer = clock.setInterval(pulse, PULSE_MS);
      connect();

      function connect() {
        awaitingFirst = true;
        if (typeof WebSocketImpl !== 'function') {
          fail('unsupported');
          return;
        }
        var socket;
        try {
          socket = new WebSocketImpl(adapter.url);
        } catch (e) {
          fail('blocked');
          return;
        }
        ws = socket;
        firstTimer = clock.setTimeout(function () {
          firstTimer = null;
          if (alive() && ws === socket && awaitingFirst) fail('timeout');
        }, liveTimeoutMs);
        socket.onopen = function () {
          if (!alive() || ws !== socket) return;
          var problem = adapter.onOpen(socket);
          if (problem) fail(problem);
        };
        socket.onmessage = function (event) {
          if (alive() && ws === socket) onMessage(event ? event.data : undefined);
        };
        socket.onerror = function () {
          if (alive() && ws === socket) fail('error');
        };
        socket.onclose = function () {
          if (alive() && ws === socket) fail('closed');
        };
      }

      function onMessage(data) {
        var out = [];
        var result = adapter.handle(data, out);
        if (typeof result === 'string') {
          fail(result);
          return;
        }
        if (!result) return;
        var at = clock.now();
        lastMessageAt = at;
        if (awaitingFirst) {
          awaitingFirst = false;
          if (firstTimer !== null) {
            clock.clearTimeout(firstTimer);
            firstTimer = null;
          }
          if (wentLive) {
            liveSince = at;
            setStatus(kind, 'live', 'Ligação à ' + name + ' restabelecida: dados em direto retomados.');
            if (!alive()) return;
          } else {
            out.forEach(deliver);
            if (historyReady) goLive();
            return;
          }
        } else if (wentLive && reconnectsLeft === 0 && at - liveSince >= STABLE_MS) {
          reconnectsLeft = 1;
        }
        for (var i = 0; i < out.length && alive(); i++) deliver(out[i]);
      }

      function deliver(candle) {
        if (!wentLive) {
          if (buffer.length >= MAX_BUFFER) buffer.shift();
          buffer.push(candle);
          return;
        }
        if (!gate.pass(candle)) return;
        lastPrices[candle.symbol] = candle.c;
        bus.emit('market:candle', candle);
      }

      function goLive() {
        wentLive = true;
        liveSince = clock.now();
        var firstLiveT = {};
        buffer.forEach(function (c) {
          if (firstLiveT[c.symbol] === undefined) firstLiveT[c.symbol] = c.t;
        });
        var candles = {};
        var loaded = 0;
        SYMBOLS.forEach(function (s) {
          var list = history && Array.isArray(history[s]) ? history[s] : [];
          if (firstLiveT[s] !== undefined) list = list.filter(function (c) { return c.t < firstLiveT[s]; });
          candles[s] = list.slice(-HISTORY_CANDLES);
          if (list.length) loaded++;
        });
        var detail;
        if (kind === 'binance') {
          detail = 'Binance em direto: velas de 1 minuto de BTC, ETH, SOL, LINK e AVAX (pares USDT).' +
            (loaded === SYMBOLS.length ? ' Histórico recente carregado.'
              : loaded > 0 ? ' Histórico parcial; o motor aquece com as velas em direto.'
                : ' Histórico indisponível; o motor aquece com as velas em direto.');
        } else {
          detail = 'Coinbase em direto: preços do canal «ticker» (USD) agregados localmente em velas de 1 minuto.' +
            ' Sem histórico; o motor aquece com as velas em direto.';
        }
        setStatus(kind, 'live', detail);
        if (!alive()) return;
        SYMBOLS.forEach(function (s) {
          var list = candles[s];
          if (!list.length) return;
          gate.seed(s, list[list.length - 1].t);
          lastPrices[s] = list[list.length - 1].c;
        });
        bus.emit('market:history', { source: kind, candles: candles });
        var pending = buffer;
        buffer = [];
        for (var i = 0; i < pending.length && alive(); i++) deliver(pending[i]);
      }

      function pulse() {
        if (!alive()) return;
        if (adapter.tick) {
          var out = [];
          adapter.tick(out);
          for (var i = 0; i < out.length && alive(); i++) deliver(out[i]);
        }
        if (alive() && ws && !awaitingFirst && clock.now() - lastMessageAt > silenceMs) fail('silence');
      }

      function fail(reason) {
        if (!alive()) return;
        var wasAwaiting = awaitingFirst;
        dropSocket();
        var reasonText = failureText(reason, name, liveTimeoutMs, silenceMs);
        if (!wentLive) {
          dispose();
          var next = kind === 'binance' ? 'coinbase' : 'sim';
          setStatus(kind, 'error', reasonText + (next === 'coinbase' ? ' A tentar a Coinbase…' : ' A mudar para o mercado simulado.'));
          if (gen !== generation) return;
          if (next === 'coinbase') startLive('coinbase');
          else startSim('fallback', simDetail('Binance e Coinbase indisponíveis.', false), null);
          return;
        }
        if (!wasAwaiting && reconnectsLeft > 0) {
          reconnectsLeft--;
          if (adapter.reset) adapter.reset();
          setStatus(kind, 'connecting', reasonText + ' A tentar voltar a ligar à ' + name + '…');
          if (!alive()) return;
          retryTimer = clock.setTimeout(function () {
            retryTimer = null;
            if (alive()) connect();
          }, RECONNECT_DELAY_MS);
          return;
        }
        dispose();
        setStatus(kind, 'error', reasonText + ' Não foi possível manter a ligação à ' + name + '.');
        if (gen !== generation) return;
        startSim('fallback', simDetail('Ligação à ' + name + ' perdida.', Object.keys(lastPrices).length > 0),
          Object.assign({}, lastPrices));
      }

      function dropSocket() {
        if (firstTimer !== null) {
          clock.clearTimeout(firstTimer);
          firstTimer = null;
        }
        if (!ws) return;
        var socket = ws;
        ws = null;
        socket.onopen = socket.onmessage = socket.onerror = socket.onclose = null;
        try {
          socket.close();
        } catch (e) { /* already closed */ }
      }

      function dispose() {
        if (dead) return;
        dead = true;
        dropSocket();
        if (retryTimer !== null) clock.clearTimeout(retryTimer);
        if (pulseTimer !== null) clock.clearInterval(pulseTimer);
        retryTimer = pulseTimer = null;
        warmUpCancels.splice(0).forEach(function (cancel) { cancel(); });
      }
    }

    // Resolves to {BTC: [candles], ...} or null; never rejects, bounded by liveTimeoutMs.
    // Each request registers a canceller in `cancels` so a disposed run leaves nothing behind.
    function warmUpBinance(cancels) {
      if (typeof fetchImpl !== 'function') return Promise.resolve(null);
      return Promise.all(SYMBOLS.map(function (s) { return fetchKlines(s, cancels); })).then(function (lists) {
        var candles = {};
        var any = false;
        SYMBOLS.forEach(function (s, i) {
          candles[s] = lists[i] || [];
          if (candles[s].length) any = true;
        });
        return any ? candles : null;
      });
    }

    function fetchKlines(symbol, cancels) {
      return new Promise(function (resolve) {
        var settled = false;
        var controller = typeof root.AbortController === 'function' ? new root.AbortController() : null;
        var timer = clock.setTimeout(abort, liveTimeoutMs);
        cancels.push(abort);
        function abort() {
          if (settled) return;
          if (controller) {
            try { controller.abort(); } catch (e) { /* ignore */ }
          }
          finish(null);
        }
        function finish(value) {
          if (settled) return;
          settled = true;
          clock.clearTimeout(timer);
          resolve(value);
        }
        var request;
        try {
          request = fetchImpl(binanceKlinesUrl(symbol), controller ? { signal: controller.signal } : undefined);
        } catch (e) {
          finish(null);
          return;
        }
        Promise.resolve(request)
          .then(function (res) { return res && res.ok ? res.json() : null; })
          .then(function (rows) { finish(rows ? parseBinanceRestKlines(rows, symbol, clock.now()) : null); },
            function () { finish(null); });
      });
    }

    // ---- simulated source
    function startSim(state, detail, anchorPrices) {
      var gen = generation;
      var dead = false;
      var timer = null;
      var startT = Math.floor(clock.now() / MINUTE_MS) * MINUTE_MS - HISTORY_CANDLES * MINUTE_MS;
      var generator = createSimGenerator(seed, { startT: startT, anchorPrices: anchorPrices });
      var candle = null;
      var step = 0;

      function alive() { return !dead && gen === generation; }

      run = {
        kind: 'sim',
        dispose: function () {
          dead = true;
          if (timer !== null) clock.clearInterval(timer);
          timer = null;
        },
        applySpeed: function () {
          if (!alive()) return;
          schedule();
          setStatus('sim', status.state, status.detail);
        }
      };

      setStatus('sim', state, detail);
      if (!alive()) return;
      bus.emit('market:history', { source: 'sim', candles: generator.history(HISTORY_CANDLES) });
      if (!alive()) return;
      candle = generator.nextCandle();
      schedule();

      function schedule() {
        if (timer !== null) clock.clearInterval(timer);
        timer = clock.setInterval(tick, simCandleMs / SIM_VISIBLE_STEPS / speed);
      }

      function tick() {
        if (!alive()) return;
        var closed = step === SIM_VISIBLE_STEPS - 1;
        for (var i = 0; i < SYMBOLS.length; i++) {
          if (!alive()) return;
          bus.emit('market:candle', frameCandle(candle, SYMBOLS[i], step, closed));
        }
        if (closed) {
          candle = generator.nextCandle();
          step = 0;
        } else {
          step++;
        }
      }
    }

    var hub = {
      start: function () {
        if (running) return;
        running = true;
        generation++;
        if (prefer === 'sim') startSim('live', simDetail('', false), null);
        else startLive('binance');
      },
      stop: function () {
        if (!running) return;
        running = false;
        generation++;
        if (run) run.dispose();
        run = null;
      },
      setSpeed: function (n) {
        var value = Number(n);
        if (ALLOWED_SPEEDS.indexOf(value) === -1) return speed;
        if (value === speed) return speed;
        speed = value;
        if (running && run && run.kind === 'sim') run.applySpeed();
        return speed;
      },
      getStatus: function () {
        return Object.assign({}, status);
      }
    };
    Object.defineProperty(hub, 'source', {
      enumerable: true,
      get: function () { return status.source; }
    });
    return hub;
  }

  return {
    createMarketHub: createMarketHub,
    marketInternals: {
      SYMBOLS: SYMBOLS.slice(),
      SIM_PROFILES: SIM_PROFILES,
      SIM_REGIMES: SIM_REGIMES,
      SIM_FACTOR_CORR: SIM_FACTOR_CORR,
      SIM_VISIBLE_STEPS: SIM_VISIBLE_STEPS,
      HISTORY_CANDLES: HISTORY_CANDLES,
      BINANCE_PAIRS: BINANCE_PAIRS,
      COINBASE_PRODUCTS: COINBASE_PRODUCTS,
      binanceStreamUrl: binanceStreamUrl,
      binanceKlinesUrl: binanceKlinesUrl,
      coinbaseSubscribeMessage: coinbaseSubscribeMessage,
      COINBASE_WS_URL: COINBASE_WS_URL,
      parseBinanceMessage: parseBinanceMessage,
      parseBinanceRestKlines: parseBinanceRestKlines,
      createCoinbaseAggregator: createCoinbaseAggregator,
      createSimGenerator: createSimGenerator
    }
  };
});

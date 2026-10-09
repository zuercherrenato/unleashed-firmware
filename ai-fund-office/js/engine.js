/*
 * AI Fund Office — deterministic paper-trading engine.
 *
 * Pipeline per closed candle: exits (stop/take-profit) → drawdown guard →
 * research rules → proposal → risk (AEGIS) → compliance (ORACLE) →
 * execution (VECTOR). Every decision is published on the bus as an
 * `agent:event` and appended to a hash-chained audit log.
 *
 * Determinism: decisions depend only on the candles received and on user
 * actions. Time comes from candle timestamps, never from the wall clock.
 */
(function (root, factory) {
  var AFO = (root.AFO = root.AFO || {});
  var api = factory(AFO, root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  Object.assign(AFO, api);
})(typeof self !== 'undefined' ? self : globalThis, function (AFO, root) {
  'use strict';

  var HOUR_MS = 3600000;
  var GENESIS_HASH = '0000000000000000';
  var AUDIT_TAIL = 20;
  var EPS = 1e-9;

  var AGENT_NAMES = { research: 'NOVA', execution: 'VECTOR', risk: 'AEGIS', compliance: 'ORACLE' };

  var DEFAULT_CONFIG = {
    startingNav: 100000,
    symbols: ['BTC', 'ETH', 'SOL', 'LINK', 'AVAX'],
    maxGrossExposurePct: 35,
    maxPositionPct: 12,
    riskPerTradePct: 1.0,
    maxDrawdownPct: 5,
    maxOpenPositions: 4,
    maxOrdersPerHour: 12,
    feePct: 0.1,
    slippagePct: 0.05,
    cooldownCandles: 3,
    warmupCandles: 30,
    emaFast: 9,
    emaSlow: 21,
    rsiPeriod: 14,
    atrPeriod: 14,
    stopAtr: 2,
    takeProfitAtr: 3,
    minNotionalPct: 0.5,
    minStopPct: 0.15,
    maxStopPct: 8,
    // Extension: symbols ORACLE may trade. null → same as `symbols`.
    whitelist: null
  };

  var CONFIG_RULES = {
    startingNav: { min: 0, open: true },
    maxGrossExposurePct: { min: 0, open: true },
    maxPositionPct: { min: 0, open: true },
    riskPerTradePct: { min: 0, open: true, max: 100 },
    maxDrawdownPct: { min: 0, open: true, max: 100 },
    maxOpenPositions: { min: 1, int: true },
    maxOrdersPerHour: { min: 0, int: true },
    feePct: { min: 0, max: 10 },
    slippagePct: { min: 0, max: 10 },
    cooldownCandles: { min: 0, int: true },
    warmupCandles: { min: 0, int: true },
    emaFast: { min: 1, int: true },
    emaSlow: { min: 2, int: true },
    rsiPeriod: { min: 1, int: true },
    atrPeriod: { min: 1, int: true },
    stopAtr: { min: 0, open: true },
    takeProfitAtr: { min: 0, open: true },
    minNotionalPct: { min: 0 },
    minStopPct: { min: 0 },
    maxStopPct: { min: 0, open: true }
  };

  // Event types recorded in the audit chain (decision-bearing events).
  var AUDIT_LABELS = {
    'proposal.created': 'proposta',
    'risk.approved': 'aprovação do AEGIS',
    'risk.rejected': 'rejeição do AEGIS',
    'compliance.approved': 'aprovação do ORACLE',
    'compliance.vetoed': 'veto do ORACLE',
    'execution.filled': 'execução',
    'execution.closed': 'fecho de posição',
    'risk.alert': 'alerta de risco',
    'system.kill': 'kill switch',
    'system.pause': 'pausa',
    'system.resume': 'retoma',
    'system.reset': 'reposição',
    'system.source': 'mudança de fonte de dados',
    'claude.response': 'resposta do Claude'
  };

  var CLOSE_REASON_TEXT = {
    stop: 'no stop',
    takeProfit: 'no alvo',
    signal: 'por sinal contrário',
    kill: 'pelo kill switch',
    source: 'por mudança da fonte de dados'
  };

  var SOURCE_LABELS = { binance: 'Binance', coinbase: 'Coinbase', sim: 'mercado simulado' };

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  function requireDeps() {
    var needed = ['hash64', 'stableStringify', 'fmtNum', 'fmtMoney', 'fmtPrice', 'fmtQty', 'ema', 'rsi', 'atr', 'crossed'];
    for (var i = 0; i < needed.length; i++) {
      if (typeof AFO[needed[i]] !== 'function') {
        throw new Error('engine.js: AFO.' + needed[i] + ' is missing (load util.js and indicators.js first)');
      }
    }
  }

  function isNum(x) {
    return typeof x === 'number' && isFinite(x);
  }

  function pad4(n) {
    var s = String(n);
    return s.length >= 4 ? s : '0000'.slice(s.length) + s;
  }

  function dirOf(side) {
    return side === 'LONG' ? 1 : -1;
  }

  function cloneData(v) {
    if (v === null || typeof v !== 'object') return v;
    if (Array.isArray(v)) return v.map(cloneData);
    var out = {};
    var keys = Object.keys(v);
    for (var i = 0; i < keys.length; i++) {
      var x = v[keys[i]];
      if (x === undefined || typeof x === 'function') continue;
      out[keys[i]] = cloneData(x);
    }
    return out;
  }

  function deepFreeze(v) {
    if (v === null || typeof v !== 'object' || Object.isFrozen(v)) return v;
    Object.freeze(v);
    var keys = Object.keys(v);
    for (var i = 0; i < keys.length; i++) deepFreeze(v[keys[i]]);
    return v;
  }

  function sanitizeText(value, maxLen) {
    if (value == null) return '';
    var s = String(value)
      .replace(/[\u0000-\u001f\u007f]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    return s.length > maxLen ? s.slice(0, maxLen - 1) + '…' : s;
  }

  function normalizeSide(side) {
    var s = typeof side === 'string' ? side.trim().toUpperCase() : '';
    if (s === 'LONG' || s === 'BUY') return 'LONG';
    if (s === 'SHORT' || s === 'SELL') return 'SHORT';
    return null;
  }

  function plural(n, one, many) {
    return n === 1 ? one : many;
  }

  function buildConfig(overrides) {
    var cfg = {};
    var o = overrides && typeof overrides === 'object' ? overrides : {};
    Object.keys(DEFAULT_CONFIG).forEach(function (key) {
      cfg[key] = o[key] !== undefined ? o[key] : DEFAULT_CONFIG[key];
    });
    Object.keys(CONFIG_RULES).forEach(function (key) {
      var rule = CONFIG_RULES[key];
      var v = cfg[key];
      var bad =
        !isNum(v) ||
        (rule.open ? v <= rule.min : v < rule.min) ||
        (rule.max != null && v > rule.max) ||
        (rule.int && Math.floor(v) !== v);
      if (bad) throw new RangeError('createEngine: invalid config.' + key + ' = ' + String(v));
    });
    if (cfg.emaFast >= cfg.emaSlow) throw new RangeError('createEngine: emaFast must be < emaSlow');
    if (cfg.minStopPct > cfg.maxStopPct) throw new RangeError('createEngine: minStopPct must be ≤ maxStopPct');
    cfg.symbols = normalizeSymbolList(cfg.symbols, 'symbols');
    if (cfg.symbols.length === 0) throw new RangeError('createEngine: symbols must not be empty');
    cfg.whitelist = cfg.whitelist == null ? null : normalizeSymbolList(cfg.whitelist, 'whitelist');
    return cfg;
  }

  function normalizeSymbolList(list, name) {
    if (!Array.isArray(list)) throw new TypeError('createEngine: config.' + name + ' must be an array');
    var out = [];
    list.forEach(function (s) {
      if (typeof s !== 'string' || !s.trim()) throw new TypeError('createEngine: config.' + name + ' entries must be strings');
      var sym = s.trim().toUpperCase();
      if (out.indexOf(sym) < 0) out.push(sym);
    });
    return out;
  }

  // ---------------------------------------------------------------------------
  // Audit chain
  // ---------------------------------------------------------------------------

  function auditBody(e) {
    return {
      seq: e.seq,
      t: e.t,
      type: e.type,
      agent: e.agent,
      symbol: e.symbol,
      text: e.text,
      data: e.data,
      eventId: e.eventId,
      prevHash: e.prevHash
    };
  }

  function computeEntryHash(e) {
    return AFO.hash64(AFO.stableStringify(auditBody(e)));
  }

  /**
   * Verifies a hash chain of audit entries. `isTrusted(entry)` may vouch for
   * deep-frozen entries created by the engine, skipping their rehash.
   */
  function verifyChain(entries, isTrusted) {
    var prev = GENESIS_HASH;
    var n = entries ? entries.length : 0;
    for (var i = 0; i < n; i++) {
      var e = entries[i];
      var ok =
        e !== null &&
        typeof e === 'object' &&
        e.seq === i + 1 &&
        e.prevHash === prev &&
        typeof e.hash === 'string' &&
        ((isTrusted && isTrusted(e)) || computeEntryHash(e) === e.hash);
      if (!ok) return { ok: false, brokenAt: i + 1, length: n };
      prev = e.hash;
    }
    return { ok: true, brokenAt: null, length: n };
  }

  /** Full recomputation of a chain (e.g. an exported copy). */
  function verifyAuditChain(entries) {
    return verifyChain(Array.isArray(entries) ? entries : [], null);
  }

  // ---------------------------------------------------------------------------
  // Engine
  // ---------------------------------------------------------------------------

  function createEngine(bus, userConfig) {
    requireDeps();
    if (!bus || typeof bus.emit !== 'function') throw new TypeError('createEngine: a bus with emit() is required');

    var cfg = buildConfig(userConfig);
    var publicConfig = deepFreeze(cloneData(cfg));
    var historyMax = Math.max(250, cfg.warmupCandles + 2, cfg.emaSlow * 8, cfg.rsiPeriod * 12, cfg.atrPeriod * 12);
    var slip = cfg.slippagePct / 100;
    var feeRate = cfg.feePct / 100;

    var fmtNum = AFO.fmtNum;
    var fmtMoney = AFO.fmtMoney;
    var fmtPrice = AFO.fmtPrice;
    var fmtQty = AFO.fmtQty;
    function lim(x) {
      return fmtNum(x, 2, true);
    }
    function signedMoney(x) {
      return (x > 0 ? '+' : '') + fmtMoney(x);
    }

    // Session-wide state (survives reset()).
    var eventSeq = 0;
    var proposalSeq = 0;
    var positionSeq = 0;
    var audit = [];
    var trustedEntries = typeof WeakSet === 'function' ? new WeakSet() : null;
    var auditBrokenAt = null;
    var source = null;
    var clock = 0;
    var series = {};
    var marks = {};
    var ignoredCandles = 0;

    // Trading state (restored by reset()).
    var portfolio;
    var flags;
    var stats;
    var alerts;
    var fillTimes;
    var lastCloseCount;
    var agents;

    function resetSeries() {
      cfg.symbols.forEach(function (sym) {
        series[sym] = {
          candles: [],
          lastClosedT: -Infinity,
          markT: -Infinity,
          closedCount: 0,
          ind: null,
          indCount: -1
        };
        if (!(sym in marks)) marks[sym] = 0;
      });
    }

    function resetTrading() {
      portfolio = {
        cash: cfg.startingNav,
        realizedPnl: 0,
        feesPaid: 0,
        peakNav: cfg.startingNav,
        positions: []
      };
      flags = { killed: false, killedBy: null, killReason: null, paused: false };
      stats = {
        proposals: 0,
        approved: 0,
        rejectedByRisk: 0,
        vetoedByCompliance: 0,
        fills: 0,
        closes: 0,
        wins: 0,
        losses: 0
      };
      alerts = { drawdown: false, exposure: false };
      fillTimes = [];
      lastCloseCount = {};
      agents = {
        research: { status: 'idle', task: 'À espera de dados de mercado', last: '' },
        risk: { status: 'idle', task: '', last: '' },
        compliance: { status: 'idle', task: '', last: '' },
        execution: { status: 'idle', task: 'À espera de ordens aprovadas', last: '' }
      };
    }

    resetSeries();
    resetTrading();

    // ----- events & audit ---------------------------------------------------

    function appendAudit(ev) {
      var prevHash = audit.length ? audit[audit.length - 1].hash : GENESIS_HASH;
      var entry = {
        seq: audit.length + 1,
        t: ev.t,
        type: ev.type,
        agent: ev.agent,
        symbol: ev.symbol,
        text: ev.text,
        data: cloneData(ev.data),
        eventId: ev.id,
        prevHash: prevHash
      };
      entry.hash = computeEntryHash(entry);
      deepFreeze(entry);
      audit.push(entry);
      if (trustedEntries) trustedEntries.add(entry);
      return entry;
    }

    function emitEvent(type, agent, symbol, text, data) {
      var ev = {
        id: ++eventSeq,
        t: clock,
        type: type,
        agent: agent,
        symbol: symbol || null,
        text: text,
        data: data || {}
      };
      var entry = AUDIT_LABELS[type] ? appendAudit(ev) : null;
      if (agents[agent] && type.indexOf('.review') < 0 && type !== 'research.scan') agents[agent].last = text;
      bus.emit('agent:event', ev);
      if (entry) {
        bus.emit('agent:event', {
          id: ++eventSeq,
          t: clock,
          type: 'audit.append',
          agent: 'compliance',
          symbol: ev.symbol,
          text:
            'ORACLE registou #' + entry.seq + ' na auditoria (' + AUDIT_LABELS[type] + ') · hash ' + entry.hash + '.',
          data: { seq: entry.seq, hash: entry.hash, prevHash: entry.prevHash, entryType: type }
        });
      }
      return ev;
    }

    function isTrusted(entry) {
      return !!trustedEntries && trustedEntries.has(entry);
    }

    function verifyAudit(options) {
      var full = !!(options && options.full);
      var res = verifyChain(audit, full ? null : isTrusted);
      auditBrokenAt = res.ok ? null : res.brokenAt;
      return res;
    }

    function setAgent(id, status, task) {
      agents[id].status = status;
      if (task != null) agents[id].task = task;
    }

    // ----- portfolio math ---------------------------------------------------

    function findPosition(sym) {
      var list = portfolio.positions;
      for (var i = 0; i < list.length; i++) if (list[i].symbol === sym) return list[i];
      return null;
    }

    function unrealized(p) {
      return dirOf(p.side) * p.qty * (marks[p.symbol] - p.entry);
    }

    function currentNav() {
      var nav = portfolio.cash;
      var list = portfolio.positions;
      for (var i = 0; i < list.length; i++) nav += unrealized(list[i]);
      return nav;
    }

    function grossNotional() {
      var sum = 0;
      var list = portfolio.positions;
      for (var i = 0; i < list.length; i++) sum += list[i].qty * marks[list[i].symbol];
      return sum;
    }

    function exposurePct(gross, nav) {
      if (gross === 0) return 0;
      return nav > 0 ? (gross / nav) * 100 : Infinity;
    }

    function drawdownOf(nav) {
      var peak = portfolio.peakNav;
      return peak > 0 ? Math.max(0, ((peak - nav) / peak) * 100) : 0;
    }

    function markToMarket() {
      var nav = currentNav();
      if (nav > portfolio.peakNav) portfolio.peakNav = nav;
      return nav;
    }

    /** Re-marks the book, raises alerts and fires the automatic drawdown kill. */
    function afterChange() {
      var nav = markToMarket();
      if (flags.killed) return;
      var dd = drawdownOf(nav);
      var gross = exposurePct(grossNotional(), nav);

      if (!alerts.drawdown && dd >= cfg.maxDrawdownPct * 0.6) {
        alerts.drawdown = true;
        setAgent('risk', 'alert');
        emitEvent(
          'risk.alert',
          'risk',
          null,
          'AEGIS em alerta: drawdown de ' + fmtNum(dd, 2) + '% (limite ' + lim(cfg.maxDrawdownPct) + '%).',
          { kind: 'drawdown', value: dd, limit: cfg.maxDrawdownPct }
        );
      } else if (alerts.drawdown && dd < cfg.maxDrawdownPct * 0.5) {
        alerts.drawdown = false;
      }

      if (!alerts.exposure && gross > cfg.maxGrossExposurePct + EPS) {
        alerts.exposure = true;
        setAgent('risk', 'alert');
        emitEvent(
          'risk.alert',
          'risk',
          null,
          'AEGIS em alerta: exposição bruta subiu para ' +
            fmtNum(gross, 1) +
            '% com o movimento dos preços (limite ' +
            lim(cfg.maxGrossExposurePct) +
            '% para novas ordens).',
          { kind: 'exposure', value: gross, limit: cfg.maxGrossExposurePct }
        );
      } else if (alerts.exposure && gross <= cfg.maxGrossExposurePct * 0.95) {
        alerts.exposure = false;
      }

      if (dd >= cfg.maxDrawdownPct) {
        kill(
          'risk',
          'drawdown de ' + fmtNum(dd, 2) + '% atingiu o limite de ' + lim(cfg.maxDrawdownPct) + '%'
        );
      }
    }

    // ----- execution ----------------------------------------------------------

    function openPosition(p, sizing) {
      var dir = dirOf(p.side);
      var price = p.entry * (1 + dir * slip);
      var qty = sizing.qty;
      var fee = feeRate * qty * price;
      var s = series[p.symbol];
      var intrabar = s.markT > s.lastClosedT;
      portfolio.cash -= fee;
      portfolio.feesPaid += fee;
      var pos = {
        id: 'POS-' + pad4(++positionSeq),
        symbol: p.symbol,
        side: p.side,
        qty: qty,
        entry: price,
        stop: p.stop,
        takeProfit: p.takeProfit,
        openedAt: clock,
        source: p.source,
        proposalId: p.proposalId,
        entryFee: fee,
        riskAmount: sizing.riskAmount,
        // Bar in which the fill happened: its pre-fill range must not trigger exits.
        entryBarT: intrabar ? s.markT : s.lastClosedT,
        watchLow: p.entry,
        watchHigh: p.entry
      };
      portfolio.positions.push(pos);
      fillTimes.push(clock);
      stats.fills++;
      markToMarket();
      setAgent('execution', 'executing');
      var notional = qty * price;
      var verb = p.side === 'LONG' ? 'comprou ' : 'vendeu a descoberto ';
      emitEvent(
        'execution.filled',
        'execution',
        p.symbol,
        'VECTOR ' +
          verb +
          fmtQty(qty) +
          ' ' +
          p.symbol +
          ' a ' +
          fmtPrice(price) +
          ' (' +
          p.side +
          ', ' +
          fmtMoney(notional) +
          ') · comissão ' +
          fmtMoney(fee) +
          '.',
        {
          positionId: pos.id,
          proposalId: p.proposalId,
          side: p.side,
          qty: qty,
          price: price,
          fee: fee,
          notional: notional,
          stop: p.stop,
          takeProfit: p.takeProfit
        }
      );
      return pos;
    }

    function closePosition(pos, level, reason, note) {
      var list = portfolio.positions;
      var idx = list.indexOf(pos);
      if (idx < 0) return null;
      var dir = dirOf(pos.side);
      var exitPrice = level * (1 - dir * slip);
      var gross = dir * pos.qty * (exitPrice - pos.entry);
      var fee = feeRate * pos.qty * exitPrice;
      portfolio.cash += gross - fee;
      portfolio.realizedPnl += gross;
      portfolio.feesPaid += fee;
      list.splice(idx, 1);
      var net = gross - pos.entryFee - fee;
      stats.closes++;
      if (net > 0) stats.wins++;
      else stats.losses++;
      lastCloseCount[pos.symbol] = series[pos.symbol].closedCount;
      markToMarket();
      setAgent('execution', 'executing');
      emitEvent(
        'execution.closed',
        'execution',
        pos.symbol,
        'VECTOR fechou ' +
          pos.side +
          ' ' +
          pos.symbol +
          ' ' +
          CLOSE_REASON_TEXT[reason] +
          ' a ' +
          fmtPrice(exitPrice) +
          ': ' +
          signedMoney(net) +
          ' líquidos de comissões' +
          (note ? ' (' + note + ')' : '') +
          '.',
        {
          positionId: pos.id,
          side: pos.side,
          qty: pos.qty,
          entry: pos.entry,
          exitPrice: exitPrice,
          pnl: net,
          grossPnl: gross,
          fee: fee,
          reason: reason
        }
      );
      return net;
    }

    /** Stop / take-profit on a closed candle. Stop wins when both are touched. */
    function checkExits(c) {
      var pos = findPosition(c.symbol);
      if (!pos || c.t < pos.entryBarT) return;
      var low;
      var high;
      var open = null;
      if (c.t === pos.entryBarT) {
        // Only prices observed after the fill count for the entry bar.
        low = Math.min(pos.watchLow, c.c);
        high = Math.max(pos.watchHigh, c.c);
      } else {
        low = c.l;
        high = c.h;
        open = c.o;
      }
      var isLong = pos.side === 'LONG';
      var stopHit = isLong ? low <= pos.stop : high >= pos.stop;
      var tpHit = isLong ? high >= pos.takeProfit : low <= pos.takeProfit;
      if (stopHit) {
        var gapped = open !== null && (isLong ? open < pos.stop : open > pos.stop);
        closePosition(pos, gapped ? open : pos.stop, 'stop', gapped ? 'abertura em gap para lá do stop' : null);
      } else if (tpHit) {
        closePosition(pos, pos.takeProfit, 'takeProfit', null);
      }
    }

    // ----- research -----------------------------------------------------------

    function indicatorsFor(sym) {
      var s = series[sym];
      if (!s) return null;
      if (s.indCount === s.closedCount) return s.ind;
      var candles = s.candles;
      var n = candles.length;
      var ind = null;
      if (n >= 2) {
        var closes = new Array(n);
        for (var i = 0; i < n; i++) closes[i] = candles[i].c;
        var fast = AFO.ema(closes, cfg.emaFast);
        var slow = AFO.ema(closes, cfg.emaSlow);
        var rsi = AFO.rsi(closes, cfg.rsiPeriod);
        var atr = AFO.atr(candles, cfg.atrPeriod);
        ind = {
          emaFast: fast[n - 1],
          emaSlow: slow[n - 1],
          prevFast: fast[n - 2],
          prevSlow: slow[n - 2],
          rsi: rsi[n - 1],
          atr: atr[n - 1],
          close: closes[n - 1]
        };
      }
      s.ind = ind;
      s.indCount = s.closedCount;
      return ind;
    }

    function isWarm(sym) {
      var s = series[sym];
      if (!s || s.closedCount < cfg.warmupCandles) return false;
      var ind = indicatorsFor(sym);
      return (
        !!ind &&
        isNum(ind.emaFast) &&
        isNum(ind.emaSlow) &&
        isNum(ind.prevFast) &&
        isNum(ind.prevSlow) &&
        isNum(ind.rsi) &&
        isNum(ind.atr) &&
        ind.atr > 0
      );
    }

    function runResearch(c) {
      var sym = c.symbol;
      if (!isWarm(sym)) return;
      var ind = indicatorsFor(sym);
      var cross = AFO.crossed(ind.prevFast, ind.prevSlow, ind.emaFast, ind.emaSlow);
      var rsi = ind.rsi;
      var side = null;
      var note = '';
      if (cross === 'up') {
        if (rsi >= 50 && rsi <= 70) side = 'LONG';
        else note = ' · cruzamento ascendente ignorado (RSI fora de 50–70)';
      } else if (cross === 'down') {
        if (rsi >= 30 && rsi <= 50) side = 'SHORT';
        else note = ' · cruzamento descendente ignorado (RSI fora de 30–50)';
      }
      if (side && flags.paused) note = ' · sinal ' + side + ' não proposto: sistema em pausa';
      else if (side) note = ' · sinal ' + side;

      setAgent('research', 'analyzing');
      emitEvent(
        'research.scan',
        'research',
        sym,
        'NOVA analisou ' +
          sym +
          ': fecho ' +
          fmtPrice(ind.close) +
          ' · EMA ' +
          cfg.emaFast +
          '/' +
          cfg.emaSlow +
          ' ' +
          fmtPrice(ind.emaFast) +
          ' / ' +
          fmtPrice(ind.emaSlow) +
          ' · RSI ' +
          fmtNum(rsi, 1) +
          ' · ATR ' +
          fmtPrice(ind.atr) +
          note +
          '.',
        {
          indicators: { emaFast: ind.emaFast, emaSlow: ind.emaSlow, rsi: rsi, atr: ind.atr, close: ind.close },
          warm: true,
          cross: cross
        }
      );

      var pos = findPosition(sym);
      if (pos && ((cross === 'up' && pos.side === 'SHORT') || (cross === 'down' && pos.side === 'LONG'))) {
        closePosition(pos, marks[sym], 'signal', null);
        afterChange();
        if (flags.killed) return;
      }
      if (!side || flags.paused) return;

      var reason =
        'EMA ' +
        cfg.emaFast +
        ' cruzou ' +
        (cross === 'up' ? 'acima' : 'abaixo') +
        ' da EMA ' +
        cfg.emaSlow +
        ' com RSI ' +
        fmtNum(rsi, 1);
      emitEvent('research.signal', 'research', sym, 'NOVA detetou sinal ' + side + ' em ' + sym + ': ' + reason + '.', {
        side: side,
        rsi: rsi,
        reason: reason
      });
      processProposal(buildProposal(sym, side, 'rules', reason));
    }

    // ----- proposal → risk → compliance → execution -----------------------------

    function buildProposal(sym, side, src, reason) {
      var ind = indicatorsFor(sym);
      var atr = ind ? ind.atr : NaN;
      var entry = marks[sym];
      var dir = dirOf(side);
      return {
        proposalId: 'P-' + pad4(++proposalSeq),
        symbol: sym,
        side: side,
        entry: entry,
        stop: entry - dir * cfg.stopAtr * atr,
        takeProfit: entry + dir * cfg.takeProfitAtr * atr,
        atr: atr,
        source: src,
        reason: reason
      };
    }

    function proposalFieldsOk(p) {
      var finite = isNum(p.entry) && isNum(p.stop) && isNum(p.takeProfit) && p.entry > 0 && p.stop > 0 && p.takeProfit > 0;
      if (!finite) return false;
      return p.side === 'LONG'
        ? p.stop < p.entry && p.entry < p.takeProfit
        : p.side === 'SHORT' && p.takeProfit < p.entry && p.entry < p.stop;
    }

    function riskReview(p) {
      var checks = [];
      function check(name, ok, detail) {
        checks.push({ name: name, ok: !!ok, detail: detail });
      }
      var dir = dirOf(p.side);
      var nav = currentNav();
      var peak = portfolio.peakNav;
      var open = portfolio.positions.length;
      var existing = findPosition(p.symbol);
      var gross = grossNotional();
      var grossNowPct = exposurePct(gross, nav);
      var stopDistPct = (Math.abs(p.entry - p.stop) / p.entry) * 100;
      var stopSideOk = isNum(stopDistPct) && dir * (p.entry - p.stop) > 0;
      var stopOk = stopSideOk && stopDistPct >= cfg.minStopPct && stopDistPct <= cfg.maxStopPct;

      // All-in sizing: loss at the stop includes slippage and fees on both legs.
      var budget = (nav * cfg.riskPerTradePct) / 100;
      var entryFill = p.entry * (1 + dir * slip);
      var stopFill = p.stop * (1 - dir * slip);
      var lossPerUnit = dir * (entryFill - stopFill) + feeRate * (entryFill + stopFill);
      // NAV given up per unit of mark notional the moment the order fills.
      var costRate = slip + feeRate * (1 + dir * slip);
      var posCap = cfg.maxPositionPct / 100;
      var grossCap = cfg.maxGrossExposurePct / 100;
      var room = grossCap * nav - gross;

      var sized = stopOk && nav > 0 && lossPerUnit > 0;
      var qty = 0;
      var limitedBy = 'risk';
      if (sized) {
        var qRisk = budget / lossPerUnit;
        var qPosition = (posCap * nav) / (p.entry * (1 + posCap * costRate));
        var qGross = room > 0 ? room / (p.entry * (1 + grossCap * costRate)) : 0;
        qty = qRisk;
        if (qPosition < qty) {
          qty = qPosition;
          limitedBy = 'position';
        }
        if (qGross < qty) {
          qty = qGross;
          limitedBy = 'gross';
        }
        qty *= 1 - 1e-12; // keep float rounding on the safe side of every cap
      }
      var notional = qty * p.entry;
      var riskAmount = qty * lossPerUnit;
      var postNav = nav - notional * costRate;
      var ddAfter = peak > 0 ? Math.max(0, ((peak - postNav) / peak) * 100) : 0;
      var positionPct = postNav > 0 ? (notional / postNav) * 100 : Infinity;
      var grossAfterPct = exposurePct(gross + notional, postNav);
      var minNotional = (nav * cfg.minNotionalPct) / 100;

      check(
        'Sistema ativo',
        !flags.killed && !flags.paused,
        flags.killed ? 'kill switch ativo — é preciso repor o sistema' : flags.paused ? 'sistema em pausa' : 'a operar'
      );
      var ddShown = sized ? ddAfter : drawdownOf(nav);
      check(
        'Drawdown',
        ddShown < cfg.maxDrawdownPct,
        'drawdown ' + (sized ? 'após a ordem ' : '') + fmtNum(ddShown, 2) + '% (limite ' + lim(cfg.maxDrawdownPct) + '%)'
      );
      check(
        'Posições abertas',
        open < cfg.maxOpenPositions,
        open + '/' + cfg.maxOpenPositions + ' ' + plural(cfg.maxOpenPositions, 'posição aberta', 'posições abertas')
      );
      check(
        'Uma posição por ativo',
        !existing,
        existing ? 'já existe ' + existing.side + ' ' + p.symbol + ' aberta' : 'sem posição em ' + p.symbol
      );
      check(
        'Distância do stop',
        stopOk,
        !stopSideOk
          ? 'stop inválido ou do lado errado da entrada'
          : 'stop a ' +
              fmtNum(stopDistPct, 2) +
              '% da entrada (intervalo ' +
              lim(cfg.minStopPct) +
              '–' +
              lim(cfg.maxStopPct) +
              '%)'
      );
      if (sized) {
        check(
          'Risco por operação',
          riskAmount <= budget * (1 + EPS),
          'perda no stop ' +
            fmtMoney(riskAmount) +
            ' = ' +
            fmtNum((riskAmount / nav) * 100, 2) +
            '% do NAV (limite ' +
            lim(cfg.riskPerTradePct) +
            '%, com comissões e slippage)'
        );
        check(
          'Limite por posição',
          positionPct <= cfg.maxPositionPct + EPS,
          fmtNum(positionPct, 1) +
            '% do NAV (limite ' +
            lim(cfg.maxPositionPct) +
            '%)' +
            (limitedBy === 'position' ? ' — tamanho reduzido' : '')
        );
        check(
          'Exposição bruta',
          room > 0 && grossAfterPct <= cfg.maxGrossExposurePct + EPS,
          room > 0
            ? fmtNum(grossNowPct, 1) +
                '% → ' +
                fmtNum(grossAfterPct, 1) +
                '% (limite ' +
                lim(cfg.maxGrossExposurePct) +
                '%)' +
                (limitedBy === 'gross' ? ' — tamanho reduzido' : '')
            : 'exposição bruta já em ' + fmtNum(grossNowPct, 1) + '% (limite ' + lim(cfg.maxGrossExposurePct) + '%)'
        );
        if (room > 0) {
          check(
            'Notional mínimo',
            notional >= minNotional,
            (notional >= minNotional
              ? fmtMoney(notional)
              : (limitedBy === 'gross' ? 'margem de exposição só permite ' : 'notional de ') + fmtMoney(notional)) +
              ' (' +
              fmtNum((notional / nav) * 100, 2) +
              '% do NAV; mínimo ' +
              lim(cfg.minNotionalPct) +
              '%)'
          );
        }
      }

      var failed = null;
      for (var i = 0; i < checks.length; i++) {
        if (!checks[i].ok) {
          failed = checks[i];
          break;
        }
      }
      return {
        ok: !failed && sized && qty > 0,
        reason: failed ? failed.detail : sized ? '' : 'não foi possível dimensionar a ordem',
        checks: checks,
        qty: qty,
        notional: notional,
        riskAmount: riskAmount,
        riskBudget: budget,
        nav: nav,
        limitedBy: limitedBy
      };
    }

    function recentFills() {
      var since = clock - HOUR_MS;
      while (fillTimes.length && fillTimes[0] <= since) fillTimes.shift();
      var n = 0;
      for (var i = 0; i < fillTimes.length; i++) if (fillTimes[i] > since && fillTimes[i] <= clock) n++;
      return n;
    }

    function complianceReview(p, sizing) {
      var checks = [];
      function check(name, ok, detail) {
        checks.push({ name: name, ok: !!ok, detail: detail });
      }
      var whitelist = cfg.whitelist || cfg.symbols;
      check(
        'Lista autorizada',
        whitelist.indexOf(p.symbol) >= 0,
        whitelist.indexOf(p.symbol) >= 0
          ? p.symbol + ' autorizado'
          : p.symbol + ' fora da lista autorizada (' + whitelist.join(', ') + ')'
      );
      var recent = recentFills();
      check(
        'Ordens por hora',
        recent < cfg.maxOrdersPerHour,
        recent + '/' + cfg.maxOrdersPerHour + ' execuções nos últimos 60 minutos'
      );
      var lastClose = lastCloseCount[p.symbol];
      var since = lastClose == null ? Infinity : series[p.symbol].closedCount - lastClose;
      check(
        'Cooldown',
        since >= cfg.cooldownCandles,
        lastClose == null
          ? 'sem fechos recentes em ' + p.symbol
          : since + '/' + cfg.cooldownCandles + ' velas desde o último fecho em ' + p.symbol
      );
      var chain = verifyAudit();
      check(
        'Cadeia de auditoria',
        chain.ok,
        chain.ok ? chain.length + ' registos íntegros' : 'cadeia de auditoria quebrada no registo #' + chain.brokenAt
      );
      var fieldsOk = proposalFieldsOk(p) && isNum(sizing.qty) && sizing.qty > 0;
      check(
        'Campos da proposta',
        fieldsOk,
        fieldsOk ? 'preços finitos; stop e alvo do lado correto' : 'proposta malformada (preços, lados ou quantidade)'
      );
      var failed = null;
      for (var i = 0; i < checks.length; i++) {
        if (!checks[i].ok) {
          failed = checks[i];
          break;
        }
      }
      return { ok: !failed, reason: failed ? failed.detail : '', checks: checks, recent: recent };
    }

    function processProposal(p) {
      var label = p.side + ' ' + p.symbol;
      stats.proposals++;
      setAgent('research', 'analyzing');
      emitEvent(
        'proposal.created',
        'research',
        p.symbol,
        'NOVA propôs ' +
          label +
          ' a ' +
          fmtPrice(p.entry) +
          ' · stop ' +
          fmtPrice(p.stop) +
          ' · alvo ' +
          fmtPrice(p.takeProfit) +
          ' (' +
          (p.source === 'claude' ? 'ideia do Claude' : 'regras EMA/RSI') +
          ').',
        {
          proposalId: p.proposalId,
          side: p.side,
          entry: p.entry,
          stop: p.stop,
          takeProfit: p.takeProfit,
          source: p.source,
          reason: p.reason,
          atr: p.atr
        }
      );

      setAgent('risk', 'reviewing');
      emitEvent('risk.review', 'risk', p.symbol, 'AEGIS a rever ' + p.proposalId + ': ' + label + '.', {
        proposalId: p.proposalId
      });
      var risk = riskReview(p);
      if (!risk.ok) {
        stats.rejectedByRisk++;
        emitEvent('risk.rejected', 'risk', p.symbol, 'AEGIS rejeitou ' + label + ': ' + risk.reason + '.', {
          proposalId: p.proposalId,
          reason: risk.reason,
          checks: risk.checks
        });
        return { ok: false, stage: 'risk', proposalId: p.proposalId, reason: risk.reason, checks: risk.checks };
      }
      var navPct = (risk.notional / risk.nav) * 100;
      var limitNote =
        risk.limitedBy === 'position'
          ? ' Tamanho reduzido pelo limite por posição (' + lim(cfg.maxPositionPct) + '% do NAV).'
          : risk.limitedBy === 'gross'
            ? ' Tamanho reduzido pelo limite de exposição bruta (' + lim(cfg.maxGrossExposurePct) + '%).'
            : '';
      emitEvent(
        'risk.approved',
        'risk',
        p.symbol,
        'AEGIS aprovou ' +
          label +
          ': ' +
          fmtQty(risk.qty) +
          ' ' +
          p.symbol +
          ' ≈ ' +
          fmtMoney(risk.notional) +
          ' (' +
          fmtNum(navPct, 1) +
          '% do NAV), perda máxima no stop ' +
          fmtMoney(risk.riskAmount) +
          ' (' +
          fmtNum((risk.riskAmount / risk.nav) * 100, 2) +
          '% do NAV).' +
          limitNote,
        {
          proposalId: p.proposalId,
          qty: risk.qty,
          notional: risk.notional,
          riskAmount: risk.riskAmount,
          riskBudget: risk.riskBudget,
          nav: risk.nav,
          limitedBy: risk.limitedBy,
          checks: risk.checks
        }
      );

      setAgent('compliance', 'reviewing');
      emitEvent('compliance.review', 'compliance', p.symbol, 'ORACLE a verificar ' + p.proposalId + ': ' + label + '.', {
        proposalId: p.proposalId
      });
      var comp = complianceReview(p, risk);
      if (!comp.ok) {
        stats.vetoedByCompliance++;
        emitEvent('compliance.vetoed', 'compliance', p.symbol, 'ORACLE vetou ' + label + ': ' + comp.reason + '.', {
          proposalId: p.proposalId,
          reason: comp.reason,
          checks: comp.checks
        });
        return { ok: false, stage: 'compliance', proposalId: p.proposalId, reason: comp.reason, checks: comp.checks };
      }
      emitEvent(
        'compliance.approved',
        'compliance',
        p.symbol,
        'ORACLE aprovou ' +
          p.proposalId +
          ' (' +
          label +
          '): ativo autorizado, ' +
          comp.recent +
          '/' +
          cfg.maxOrdersPerHour +
          ' ordens na última hora, sem cooldown, auditoria íntegra.',
        { proposalId: p.proposalId, checks: comp.checks }
      );
      stats.approved++;

      var pos = openPosition(p, risk);
      afterChange();
      return {
        ok: true,
        stage: 'filled',
        proposalId: p.proposalId,
        positionId: pos.id,
        reason: '',
        qty: pos.qty,
        price: pos.entry,
        notional: pos.qty * pos.entry,
        checks: risk.checks.concat(comp.checks)
      };
    }

    // ----- agents' steady state -----------------------------------------------

    function refreshAgents() {
      var n = portfolio.positions.length;
      var nav = currentNav();
      var dd = drawdownOf(nav);
      var gross = exposurePct(grossNotional(), nav);
      var posText = n + ' ' + plural(n, 'posição', 'posições');

      if (flags.killed) {
        setAgent('research', 'stopped', 'Parado: kill switch ativo');
      } else if (flags.paused) {
        setAgent('research', 'idle', 'Em pausa: sem novas propostas');
      } else {
        var withData = 0;
        var warm = 0;
        var minHave = Infinity;
        cfg.symbols.forEach(function (sym) {
          var s = series[sym];
          if (s.closedCount > 0 || marks[sym] > 0) withData++;
          if (isWarm(sym)) warm++;
          else minHave = Math.min(minHave, s.closedCount);
        });
        if (withData === 0) {
          setAgent('research', 'idle', 'À espera de dados de mercado');
        } else if (warm < cfg.symbols.length) {
          setAgent(
            'research',
            'analyzing',
            'Aquecimento dos indicadores: ' + Math.min(minHave, cfg.warmupCandles) + '/' + cfg.warmupCandles + ' velas'
          );
        } else {
          setAgent(
            'research',
            'analyzing',
            'A analisar ' +
              warm +
              ' mercados (EMA ' +
              cfg.emaFast +
              '/' +
              cfg.emaSlow +
              ' · RSI ' +
              cfg.rsiPeriod +
              ')'
          );
        }
      }

      if (flags.killed) {
        setAgent('risk', flags.killedBy === 'risk' ? 'alert' : 'stopped', 'Kill switch ativo: ordens bloqueadas até repor');
      } else if (alerts.drawdown || alerts.exposure) {
        setAgent(
          'risk',
          'alert',
          'Drawdown ' +
            fmtNum(dd, 2) +
            '% (limite ' +
            lim(cfg.maxDrawdownPct) +
            '%) · exposição ' +
            fmtNum(gross, 1) +
            '% (limite ' +
            lim(cfg.maxGrossExposurePct) +
            '%)'
        );
      } else if (n > 0) {
        setAgent(
          'risk',
          'monitoring',
          'A vigiar ' + posText + ' · exposição ' + fmtNum(gross, 1) + '% · drawdown ' + fmtNum(dd, 2) + '%'
        );
      } else {
        setAgent(
          'risk',
          'idle',
          'Limites ativos: exposição ' +
            lim(cfg.maxGrossExposurePct) +
            '% · drawdown ' +
            lim(cfg.maxDrawdownPct) +
            '% · risco ' +
            lim(cfg.riskPerTradePct) +
            '% por operação'
        );
      }

      var auditText = 'Auditoria: ' + audit.length + ' ' + plural(audit.length, 'registo', 'registos');
      if (auditBrokenAt !== null) {
        setAgent('compliance', 'alert', 'Cadeia de auditoria quebrada no registo #' + auditBrokenAt);
      } else if (flags.killed) {
        setAgent('compliance', 'monitoring', auditText + ' · kill switch registado');
      } else {
        setAgent(
          'compliance',
          n > 0 ? 'monitoring' : 'idle',
          auditText + ' · ' + recentFills() + '/' + cfg.maxOrdersPerHour + ' ordens na última hora'
        );
      }

      if (flags.killed) setAgent('execution', 'stopped', 'Parado: kill switch ativo');
      else if (n > 0) setAgent('execution', 'monitoring', 'A gerir ' + posText + ' com stop e alvo');
      else setAgent('execution', 'idle', 'À espera de ordens aprovadas');
    }

    // ----- market data ---------------------------------------------------------

    function normalizeCandle(raw, forced) {
      if (!raw || typeof raw !== 'object') return null;
      var symbol = forced && forced.symbol ? forced.symbol : raw.symbol;
      symbol = typeof symbol === 'string' ? symbol.trim().toUpperCase() : '';
      if (!series[symbol]) return null;
      var t = Number(raw.t);
      var c = Number(raw.c);
      if (!isFinite(t) || !(c > 0) || !isFinite(c)) return null;
      var o = Number(raw.o);
      var h = Number(raw.h);
      var l = Number(raw.l);
      if (!(o > 0) || !isFinite(o)) o = c;
      if (!(h > 0) || !isFinite(h)) h = c;
      if (!(l > 0) || !isFinite(l)) l = c;
      var v = Number(raw.v);
      var src = forced && 'source' in forced ? forced.source : raw.source;
      return {
        symbol: symbol,
        t: t,
        o: o,
        h: Math.max(h, o, c, l),
        l: Math.min(l, o, c, h),
        c: c,
        v: isFinite(v) && v >= 0 ? v : 0,
        closed: forced && 'closed' in forced ? forced.closed : raw.closed === true,
        source: typeof src === 'string' && src ? src : null
      };
    }

    function appendClosed(s, c) {
      s.candles.push({ t: c.t, o: c.o, h: c.h, l: c.l, c: c.c, v: c.v });
      if (s.candles.length > historyMax) s.candles.splice(0, s.candles.length - historyMax);
      s.lastClosedT = c.t;
      s.closedCount++;
    }

    function updateMark(c) {
      var s = series[c.symbol];
      if (c.t > clock) clock = c.t;
      if (c.t >= s.markT) {
        marks[c.symbol] = c.c;
        s.markT = c.t;
      }
      var pos = findPosition(c.symbol);
      if (pos && c.t === pos.entryBarT && !c.closed) {
        if (c.c < pos.watchLow) pos.watchLow = c.c;
        if (c.c > pos.watchHigh) pos.watchHigh = c.c;
      }
    }

    /** Different market source: positions from the old price universe are closed. */
    function switchSource(next) {
      var prev = source;
      source = next;
      if (prev === null) return;
      var open = portfolio.positions.slice();
      emitEvent(
        'system.source',
        'risk',
        null,
        'Fonte de dados mudou de ' +
          (SOURCE_LABELS[prev] || prev) +
          ' para ' +
          (SOURCE_LABELS[next] || next) +
          ': indicadores reiniciados' +
          (open.length
            ? ' e ' + open.length + ' ' + plural(open.length, 'posição fechada', 'posições fechadas') + ' ao último preço conhecido'
            : '') +
          '.',
        { from: prev, to: next, closedPositions: open.map(function (p) { return p.id; }) }
      );
      open.forEach(function (p) {
        closePosition(p, marks[p.symbol], 'source', null);
      });
      afterChange();
      // New time base and price universe: per-source state starts over.
      fillTimes = [];
      lastCloseCount = {};
      clock = 0;
      cfg.symbols.forEach(function (sym) {
        var s = series[sym];
        s.candles = [];
        s.lastClosedT = -Infinity;
        s.markT = -Infinity;
        s.closedCount = 0;
        s.ind = null;
        s.indCount = -1;
      });
    }

    function onCandle(raw) {
      var c = normalizeCandle(raw, null);
      if (!c) {
        ignoredCandles++;
        return false;
      }
      if (c.source && c.source !== source) switchSource(c.source);
      var s = series[c.symbol];
      if (c.t <= s.lastClosedT) {
        // Duplicate or late candle for a bar that is already final.
        ignoredCandles++;
        return false;
      }
      updateMark(c);
      if (!c.closed) {
        afterChange();
        refreshAgents();
        return true;
      }
      appendClosed(s, c);
      checkExits(c);
      afterChange();
      if (!flags.killed) runResearch(c);
      refreshAgents();
      return true;
    }

    function onHistory(payload) {
      if (!payload || typeof payload !== 'object') return 0;
      var src = typeof payload.source === 'string' && payload.source ? payload.source : null;
      if (src && src !== source) switchSource(src);
      var bySymbol = payload.candles && typeof payload.candles === 'object' ? payload.candles : {};
      var accepted = 0;
      cfg.symbols.forEach(function (sym) {
        var list = bySymbol[sym];
        if (!Array.isArray(list)) return;
        var candles = [];
        for (var i = 0; i < list.length; i++) {
          var c = normalizeCandle(list[i], { symbol: sym, closed: true, source: src });
          if (c) candles.push(c);
        }
        candles.sort(function (a, b) {
          return a.t - b.t;
        });
        var s = series[sym];
        for (var j = 0; j < candles.length; j++) {
          if (candles[j].t <= s.lastClosedT) continue;
          updateMark(candles[j]);
          appendClosed(s, candles[j]);
          // History never opens trades, but it must still honour open stops.
          checkExits(candles[j]);
          accepted++;
        }
      });
      afterChange();
      refreshAgents();
      return accepted;
    }

    // ----- external proposals -------------------------------------------------

    function submitExternalProposal(input) {
      var req = input && typeof input === 'object' ? input : {};
      var sym = typeof req.symbol === 'string' ? req.symbol.trim().toUpperCase() : '';
      var side = normalizeSide(req.side);
      var reason = sanitizeText(req.reason, 280) || 'sem justificação';
      function inputError(msg) {
        return { ok: false, stage: 'input', proposalId: null, reason: msg, checks: [] };
      }
      if (!series[sym]) {
        return inputError((sym || 'Símbolo vazio') + ' não faz parte do universo (' + cfg.symbols.join(', ') + ').');
      }
      if (!side) return inputError('Lado inválido: use LONG ou SHORT.');
      if (!(marks[sym] > 0)) return inputError('Ainda não há cotação para ' + sym + '.');
      if (!isWarm(sym)) {
        return inputError(
          sym +
            ' ainda em aquecimento (' +
            Math.min(series[sym].closedCount, cfg.warmupCandles) +
            '/' +
            cfg.warmupCandles +
            ' velas).'
        );
      }
      var result = processProposal(buildProposal(sym, side, 'claude', reason));
      refreshAgents();
      return result;
    }

    /** Lets claude.js publish claude.* events with engine ids and candle time. */
    function emitExternalEvent(ev) {
      var e = ev && typeof ev === 'object' ? ev : {};
      var type = String(e.type || '');
      if (!/^claude\.(request|response|error)$/.test(type)) return null;
      var symbol = typeof e.symbol === 'string' && series[e.symbol.toUpperCase()] ? e.symbol.toUpperCase() : null;
      return emitEvent(type, 'research', symbol, sanitizeText(e.text, 400), cloneData(e.data || {}));
    }

    // ----- user controls -------------------------------------------------------

    function kill(by, reason) {
      if (flags.killed) return false;
      var who = by === 'risk' ? 'risk' : 'user';
      var why = sanitizeText(reason, 200) || (who === 'user' ? 'acionado pelo utilizador' : 'limite de risco atingido');
      var open = portfolio.positions.slice();
      flags.killed = true;
      flags.killedBy = who;
      flags.killReason = why;
      flags.paused = false;
      setAgent('risk', who === 'risk' ? 'alert' : 'stopped');
      var closing = open.length
        ? open.length + ' ' + plural(open.length, 'posição fechada', 'posições fechadas') + ' ao preço de mercado'
        : 'sem posições abertas';
      emitEvent(
        'system.kill',
        'risk',
        null,
        (who === 'risk' ? 'KILL SWITCH automático (AEGIS): ' : 'KILL SWITCH acionado pelo utilizador: ') +
          why +
          '. ' +
          closing.charAt(0).toUpperCase() +
          closing.slice(1) +
          '; novas ordens bloqueadas.',
        { by: who, reason: why }
      );
      open.forEach(function (p) {
        closePosition(p, marks[p.symbol], 'kill', null);
      });
      markToMarket();
      refreshAgents();
      return true;
    }

    function pause() {
      if (flags.killed || flags.paused) return false;
      flags.paused = true;
      emitEvent(
        'system.pause',
        'risk',
        null,
        'Sistema em pausa pelo utilizador: NOVA deixa de propor operações; stops, alvos e o kill switch automático continuam ativos.',
        { by: 'user' }
      );
      refreshAgents();
      return true;
    }

    function resume() {
      if (flags.killed || !flags.paused) return false;
      flags.paused = false;
      emitEvent('system.resume', 'risk', null, 'Sistema retomado pelo utilizador: NOVA volta a propor operações.', {
        by: 'user'
      });
      refreshAgents();
      return true;
    }

    function reset() {
      var discarded = portfolio.positions.map(function (p) {
        return p.id;
      });
      resetTrading();
      emitEvent(
        'system.reset',
        'risk',
        null,
        'Sistema reposto: NAV ' +
          fmtMoney(cfg.startingNav) +
          ', sem posições e estatísticas a zero' +
          (discarded.length
            ? ' (' + discarded.length + ' ' + plural(discarded.length, 'posição descartada', 'posições descartadas') + ' sem execução)'
            : '') +
          '. O registo de auditoria continua.',
        { by: 'user', startingNav: cfg.startingNav, discardedPositions: discarded }
      );
      refreshAgents();
      return true;
    }

    // ----- snapshot -------------------------------------------------------------

    function getState() {
      var nav = currentNav();
      var unreal = 0;
      var positions = portfolio.positions.map(function (p) {
        var mark = marks[p.symbol];
        var pnl = unrealized(p);
        unreal += pnl;
        return {
          id: p.id,
          symbol: p.symbol,
          side: p.side,
          qty: p.qty,
          entry: p.entry,
          mark: mark,
          stop: p.stop,
          takeProfit: p.takeProfit,
          notional: p.qty * mark,
          pnl: pnl,
          pnlPct: (pnl / (p.qty * p.entry)) * 100,
          openedAt: p.openedAt,
          source: p.source
        };
      });
      var markCopy = {};
      var warmup = {};
      cfg.symbols.forEach(function (sym) {
        markCopy[sym] = marks[sym];
        warmup[sym] = { have: Math.min(series[sym].closedCount, cfg.warmupCandles), need: cfg.warmupCandles };
      });
      var agentCopy = {};
      Object.keys(agents).forEach(function (id) {
        agentCopy[id] = { status: agents[id].status, task: agents[id].task, last: agents[id].last };
      });
      return {
        nav: nav,
        cash: portfolio.cash,
        peakNav: portfolio.peakNav,
        drawdownPct: drawdownOf(nav),
        grossExposurePct: exposurePct(grossNotional(), nav),
        realizedPnl: portfolio.realizedPnl,
        unrealizedPnl: unreal,
        feesPaid: portfolio.feesPaid,
        startingNav: cfg.startingNav,
        killed: flags.killed,
        killReason: flags.killReason,
        paused: flags.paused,
        config: publicConfig,
        marks: markCopy,
        warmup: warmup,
        positions: positions,
        agents: agentCopy,
        stats: {
          proposals: stats.proposals,
          approved: stats.approved,
          rejectedByRisk: stats.rejectedByRisk,
          vetoedByCompliance: stats.vetoedByCompliance,
          fills: stats.fills,
          closes: stats.closes,
          wins: stats.wins,
          losses: stats.losses
        },
        audit: {
          length: audit.length,
          head: audit.length ? audit[audit.length - 1].hash : GENESIS_HASH,
          last: audit.slice(-AUDIT_TAIL)
        },
        lastEventId: eventSeq
      };
    }

    refreshAgents();

    var engine = {
      onCandle: onCandle,
      onHistory: onHistory,
      submitExternalProposal: submitExternalProposal,
      emitExternalEvent: emitExternalEvent,
      kill: function (by, reason) {
        return kill(by === undefined ? 'user' : by, reason);
      },
      pause: pause,
      resume: resume,
      reset: reset,
      getState: getState,
      verifyAudit: verifyAudit,
      /** Deep-frozen copy-free view of the full audit chain (entries are immutable). */
      getAuditLog: function () {
        return audit.slice();
      }
    };
    // Test hook: live internals. Not part of the UI contract.
    Object.defineProperty(engine, '__internals', {
      enumerable: false,
      value: function () {
        return { audit: audit, positions: portfolio.positions, series: series, ignoredCandles: ignoredCandles };
      }
    });
    return engine;
  }

  return {
    createEngine: createEngine,
    verifyAuditChain: verifyAuditChain,
    ENGINE_DEFAULTS: deepFreeze(cloneData(DEFAULT_CONFIG)),
    AGENT_NAMES: Object.freeze(Object.assign({}, AGENT_NAMES))
  };
});

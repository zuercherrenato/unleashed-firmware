/*
 * AI Fund Office — shared utilities.
 * Event bus, seeded RNG, non-cryptographic hashing and pt-PT formatters.
 */
(function (root, factory) {
  var AFO = (root.AFO = root.AFO || {});
  var api = factory(AFO, root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  Object.assign(AFO, api);
})(typeof self !== 'undefined' ? self : globalThis, function (AFO, root) {
  'use strict';

  // ---------------------------------------------------------------------------
  // Event bus
  // ---------------------------------------------------------------------------

  /**
   * Synchronous publish/subscribe bus. Listeners of a topic run in subscription
   * order; a listener that throws is logged and never stops the others.
   * Unsubscribing during an emit takes effect immediately.
   */
  function createBus() {
    var topics = Object.create(null);

    function on(topic, fn) {
      if (typeof fn !== 'function') throw new TypeError('bus.on: listener must be a function');
      var record = { fn: fn, active: true };
      (topics[topic] || (topics[topic] = [])).push(record);
      return function off() {
        if (!record.active) return;
        record.active = false;
        var list = topics[topic];
        if (!list) return;
        var i = list.indexOf(record);
        if (i >= 0) list.splice(i, 1);
      };
    }

    function once(topic, fn) {
      if (typeof fn !== 'function') throw new TypeError('bus.once: listener must be a function');
      var off = on(topic, function (payload) {
        off();
        fn(payload);
      });
      return off;
    }

    function emit(topic, payload) {
      var list = topics[topic];
      if (!list || list.length === 0) return;
      var snapshot = list.slice();
      for (var i = 0; i < snapshot.length; i++) {
        var record = snapshot[i];
        if (!record.active) continue;
        try {
          record.fn(payload);
        } catch (err) {
          console.error('[AFO bus] listener for "' + topic + '" threw:', err);
        }
      }
    }

    return { on: on, emit: emit, once: once };
  }

  // ---------------------------------------------------------------------------
  // Seeded randomness and hashing
  // ---------------------------------------------------------------------------

  /** cyrb53-style hash: two independent 32-bit lanes. NOT cryptographic. */
  function hashLanes(str, seed) {
    var s = seed >>> 0;
    var h1 = 0xdeadbeef ^ s;
    var h2 = 0x41c6ce57 ^ s;
    for (var i = 0; i < str.length; i++) {
      var ch = str.charCodeAt(i);
      h1 = Math.imul(h1 ^ ch, 2654435761);
      h2 = Math.imul(h2 ^ ch, 1597334677);
    }
    h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507);
    h1 ^= Math.imul(h2 ^ (h2 >>> 13), 3266489909);
    h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507);
    h2 ^= Math.imul(h1 ^ (h1 >>> 13), 3266489909);
    return [h2 >>> 0, h1 >>> 0];
  }

  function hex8(n) {
    var s = (n >>> 0).toString(16);
    return '00000000'.slice(s.length) + s;
  }

  /** 64-bit (16 lowercase hex chars) non-cryptographic hash of a string. */
  function hash64(str) {
    var lanes = hashLanes(String(str), 0);
    return hex8(lanes[0]) + hex8(lanes[1]);
  }

  /**
   * mulberry32 PRNG. Returns a function producing floats in [0, 1).
   * A string seed is hashed first so any label can seed a stream.
   */
  function rng(seed) {
    var a = typeof seed === 'string' ? hashLanes(seed, 0)[1] : Number(seed) >>> 0;
    return function () {
      a = (a + 0x6d2b79f5) >>> 0;
      var t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }

  /** Standard normal sample via Box–Muller from a [0,1) generator. */
  function gaussian(rand) {
    var u1 = 1 - rand(); // (0, 1], so log() is finite
    var u2 = rand();
    return Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
  }

  /** JSON with recursively sorted object keys (stable input for hashing). */
  function stableStringify(value) {
    if (value === null || typeof value !== 'object') {
      var json = JSON.stringify(value);
      return json === undefined ? 'null' : json;
    }
    if (Array.isArray(value)) {
      var items = [];
      for (var i = 0; i < value.length; i++) items.push(stableStringify(value[i]));
      return '[' + items.join(',') + ']';
    }
    var keys = Object.keys(value).sort();
    var parts = [];
    for (var k = 0; k < keys.length; k++) {
      var v = value[keys[k]];
      if (v === undefined || typeof v === 'function') continue;
      parts.push(JSON.stringify(keys[k]) + ':' + stableStringify(v));
    }
    return '{' + parts.join(',') + '}';
  }

  // ---------------------------------------------------------------------------
  // pt-PT formatting
  // ---------------------------------------------------------------------------

  var GROUP_SEPARATOR = ' '; // narrow no-break (thin) space
  var MINUS = '−';
  var DASH = '—';

  function groupThousands(intDigits) {
    return intDigits.replace(/\B(?=(\d{3})+(?!\d))/g, GROUP_SEPARATOR);
  }

  /**
   * Formats a number pt-PT style: thin-space thousands, decimal comma.
   * `trim` drops trailing zeros (e.g. limits: 35 → "35", 0.5 → "0,5").
   */
  function fmtNum(n, digits, trim) {
    n = Number(n);
    if (!isFinite(n)) return DASH;
    var d = digits == null ? 2 : Math.max(0, Math.min(10, digits | 0));
    var fixed = Math.abs(n).toFixed(d);
    var dot = fixed.indexOf('.');
    var intPart = dot < 0 ? fixed : fixed.slice(0, dot);
    var fracPart = dot < 0 ? '' : fixed.slice(dot + 1);
    if (trim) fracPart = fracPart.replace(/0+$/, '');
    var isZero = Number(fixed) === 0;
    return (n < 0 && !isZero ? MINUS : '') + groupThousands(intPart) + (fracPart ? ',' + fracPart : '');
  }

  /** Money in USDT, 2 decimals: "1 234,50 USDT". */
  function fmtMoney(n) {
    return fmtNum(n, 2) + ' USDT';
  }

  /** Signed percentage: "+1,25%", "−0,40%", "0,00%". */
  function fmtPct(n, digits) {
    n = Number(n);
    var d = digits == null ? 2 : digits;
    if (!isFinite(n)) return DASH;
    var body = fmtNum(Math.abs(n), d);
    if (Number(Math.abs(n).toFixed(d)) === 0) return body + '%';
    return (n > 0 ? '+' : MINUS) + body + '%';
  }

  /** Price with adaptive decimals: ≥1000 → 2, ≥1 → 3, otherwise 5. */
  function fmtPrice(n) {
    n = Number(n);
    if (!isFinite(n)) return DASH;
    var a = Math.abs(n);
    return fmtNum(n, a >= 1000 ? 2 : a >= 1 ? 3 : 5);
  }

  /** Quantity with adaptive decimals: ≥1000 → 2, ≥1 → 4, otherwise 6. */
  function fmtQty(n) {
    n = Number(n);
    if (!isFinite(n)) return DASH;
    var a = Math.abs(n);
    return fmtNum(n, a >= 1000 ? 2 : a >= 1 ? 4 : 6);
  }

  function pad2(n) {
    return n < 10 ? '0' + n : String(n);
  }

  /** Local wall-clock time "HH:MM:SS" (24 h, pt-PT). */
  function fmtTime(ms) {
    var d = new Date(Number(ms));
    if (!isFinite(d.getTime())) return '--:--:--';
    return pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds());
  }

  return {
    createBus: createBus,
    rng: rng,
    gaussian: gaussian,
    hash64: hash64,
    stableStringify: stableStringify,
    fmtNum: fmtNum,
    fmtMoney: fmtMoney,
    fmtPct: fmtPct,
    fmtPrice: fmtPrice,
    fmtQty: fmtQty,
    fmtTime: fmtTime
  };
});

/*
 * AI Fund Office — technical indicators.
 * Pure functions over arrays ordered oldest → newest. Outputs have the same
 * length as the input and hold NaN until the indicator is seeded.
 */
(function (root, factory) {
  var AFO = (root.AFO = root.AFO || {});
  var api = factory(AFO, root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  Object.assign(AFO, api);
})(typeof self !== 'undefined' ? self : globalThis, function (AFO, root) {
  'use strict';

  function validPeriod(period, name) {
    var p = Number(period);
    if (!(p >= 1) || Math.floor(p) !== p) throw new RangeError(name + ': period must be a positive integer');
    return p;
  }

  function filledNaN(n) {
    var out = new Array(n);
    for (var i = 0; i < n; i++) out[i] = NaN;
    return out;
  }

  /** Exponential moving average, seeded with the SMA of the first `period` values. */
  function ema(values, period) {
    var p = validPeriod(period, 'ema');
    var n = values.length;
    var out = filledNaN(n);
    if (n < p) return out;
    var k = 2 / (p + 1);
    var sum = 0;
    for (var i = 0; i < p; i++) sum += values[i];
    var prev = sum / p;
    out[p - 1] = prev;
    for (var j = p; j < n; j++) {
      prev = prev + k * (values[j] - prev);
      out[j] = prev;
    }
    return out;
  }

  function rsiFromAverages(avgGain, avgLoss) {
    if (avgLoss === 0) return avgGain === 0 ? 50 : 100;
    return 100 - 100 / (1 + avgGain / avgLoss);
  }

  /** Relative Strength Index with Wilder smoothing. First value at index `period`. */
  function rsi(closes, period) {
    var p = validPeriod(period == null ? 14 : period, 'rsi');
    var n = closes.length;
    var out = filledNaN(n);
    if (n <= p) return out;
    var gain = 0;
    var loss = 0;
    for (var i = 1; i <= p; i++) {
      var ch = closes[i] - closes[i - 1];
      if (ch > 0) gain += ch;
      else loss -= ch;
    }
    var avgGain = gain / p;
    var avgLoss = loss / p;
    out[p] = rsiFromAverages(avgGain, avgLoss);
    for (var j = p + 1; j < n; j++) {
      var d = closes[j] - closes[j - 1];
      avgGain = (avgGain * (p - 1) + (d > 0 ? d : 0)) / p;
      avgLoss = (avgLoss * (p - 1) + (d < 0 ? -d : 0)) / p;
      out[j] = rsiFromAverages(avgGain, avgLoss);
    }
    return out;
  }

  /**
   * Average True Range with Wilder smoothing over candles `{h, l, c}`.
   * TR of the first candle is h − l; the first ATR (index period − 1) is the
   * simple mean of the first `period` true ranges.
   */
  function atr(candles, period) {
    var p = validPeriod(period == null ? 14 : period, 'atr');
    var n = candles.length;
    var out = filledNaN(n);
    if (n < p) return out;
    var trSum = 0;
    var prevAtr = NaN;
    for (var i = 0; i < n; i++) {
      var c = candles[i];
      var tr = c.h - c.l;
      if (i > 0) {
        var pc = candles[i - 1].c;
        tr = Math.max(tr, Math.abs(c.h - pc), Math.abs(c.l - pc));
      }
      if (i < p) {
        trSum += tr;
        if (i === p - 1) {
          prevAtr = trSum / p;
          out[i] = prevAtr;
        }
      } else {
        prevAtr = (prevAtr * (p - 1) + tr) / p;
        out[i] = prevAtr;
      }
    }
    return out;
  }

  /** Crossover of series A over B between two consecutive points. */
  function crossed(prevA, prevB, a, b) {
    if (!isFinite(prevA) || !isFinite(prevB) || !isFinite(a) || !isFinite(b)) return null;
    if (prevA <= prevB && a > b) return 'up';
    if (prevA >= prevB && a < b) return 'down';
    return null;
  }

  return { ema: ema, rsi: rsi, atr: atr, crossed: crossed };
});

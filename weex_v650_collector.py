#!/usr/bin/env python3
"""WEEX V6.50: standalone public-market-data collector + causal signal guard.

NO ORDER EXECUTION. No API key. Not the original V6.35 proprietary strategy engine.
Install: pip install requests
Usage: python weex_v650_collector.py --top 150 --download-ready
Or: python weex_v650_collector.py --self-test

Causality:
- Every network response records exchange payload time and receive time (exchange-calibrated).
- Klines must have completed BEFORE they were received, not just before the run ends.
- Funding history vs forecast are separate; funding-history timestamp alone is NOT
  proof of when the published record became available; offline history is not backtestable
  without observation logs.
- Historical signals without TTL are quarantined. New LIMIT events require TTL at capture.
- Trade gate is disabled by default, even for scored watchlist instruments.
"""
from __future__ import annotations
import argparse
import csv
from decimal import Decimal, ROUND_DOWN, ROUND_UP, ROUND_HALF_UP
import hashlib
import json
import math
import os
import random
import shutil
import sys
import threading
import time
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit('Brak biblioteki requests; uruchom: pip install requests')

VERSION = 'WEEX V6.50 TWO-STAGE PRE-RR FILTER + VALIDATED ABCD CRYPTO150 TRADFI50 COLLECTOR'
API_BASE = 'https://api-contract.weex.com'
TF = {'1m': 60_000, '5m': 300_000, '15m': 900_000, '1h': 3600_000,
      '4h': 14400_000, '1d': 86400_000}
DEFAULT_TFS = ('5m', '15m', '1h')
EXTRA_TFS = ('4h', '1d')
CANDLE_LIMITS = {'5m': 160, '15m': 110, '1h': 110, '4h': 100, '1d': 90}
MAX_STAGE2 = 30  # max selected setups to refresh deeply
THREAD_LOCAL = threading.local()


def now_ms():
    return time.time_ns() // 1_000_000


def utc(ms=None):
    return datetime.fromtimestamp((now_ms() if ms is None else ms) / 1000, timezone.utc).isoformat()


def safe_float(value):
    try:
        f = float(value)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError, OverflowError):
        return None


def safe_ms(value):
    v = safe_float(value)
    return int(v) if v is not None and 1_400_000_000_000 <= v <= 4_100_000_000_000 else None


def csv_write(path, rows, fields):
    with open(path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


def atomic_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, allow_nan=False)
    os.replace(tmp, path)


class HTTPError(Exception):
    pass


class Weex:
    def __init__(self, delay=0.18, timeout=15, retries=3):
        self.delay, self.timeout, self.retries = delay, timeout, retries
        self.lock = threading.Lock()
        self.next_at = 0.0
        self.call_count = 0
        self.errors = []
        self.clock_offset_ms = 0
        self.clock_quality = 'NOT_CALIBRATED'

    def request(self, path, params=None):
        if not path.startswith('/capi/v3/market/'):
            raise ValueError('Only documented read-only /capi/v3/market/* endpoints allowed')
        if not hasattr(THREAD_LOCAL, 'session'):
            THREAD_LOCAL.session = requests.Session()
            THREAD_LOCAL.session.headers.update({'User-Agent': 'WEEX-V650-safe-public-collector/1.0',
                                                 'Accept': 'application/json'})
        failure = None
        for attempt in range(self.retries):
            with self.lock:
                pause = max(0.0, self.next_at - time.monotonic())
                if pause:
                    time.sleep(pause)
                self.next_at = time.monotonic() + self.delay
                self.call_count += 1
            send_ms = now_ms()
            try:
                res = THREAD_LOCAL.session.get(API_BASE + path, params=params,
                                               timeout=self.timeout)
                recv_ms = now_ms()
                if res.status_code in (403, 451):
                    raise HTTPError(f'{res.status_code}: API zablokowane w tej lokalizacji; bez fallbacku innej giełdy')
                if res.status_code in (418, 429, 500, 502, 503, 504):
                    failure = f'HTTP {res.status_code} {path}'
                    retry_after = safe_float(res.headers.get('Retry-After'))
                    if attempt + 1 < self.retries:
                        time.sleep(min(30.0, max(0.8 * (2 ** attempt), retry_after or 0)))
                        continue
                res.raise_for_status()
                obj = res.json()
                if isinstance(obj, dict) and obj.get('code') not in (None, 0, '0', '00000') and 'data' in obj:
                    raise HTTPError(f'Unexpected WEEX envelope: {str(obj)[:300]}')
                return obj, {'sent_local_ms': send_ms, 'received_local_ms': recv_ms,
                             'received_server_est_ms': recv_ms + self.clock_offset_ms,
                             'rtt_ms': recv_ms - send_ms, 'path': path, 'params': params or {}}
            except (requests.RequestException, ValueError, HTTPError) as exc:
                failure = f'{type(exc).__name__}: {str(exc)[:260]}'
                # 4xx bad-request / invalid instrument is not transient.
                if isinstance(exc, requests.HTTPError) and exc.response is not None \
                        and exc.response.status_code in (400, 404, 422):
                    break
                if ('403:' in failure or '451:' in failure):
                    break
                if attempt + 1 < self.retries:
                    time.sleep(0.8 * (2 ** attempt) + random.uniform(0, 0.2))
        with self.lock:
            self.errors.append({'path': path, 'params': params, 'error': failure})
        raise HTTPError(f'{path}: {failure}')

    def calibrate(self):
        try:
            obj, meta = self.request('/capi/v3/market/time')
            server = safe_ms(obj.get('serverTime')) if isinstance(obj, dict) else None
            if server is None:
                raise ValueError('No serverTime')
            midpoint = (meta['sent_local_ms'] + meta['received_local_ms']) // 2
            self.clock_offset_ms = server - midpoint
            self.clock_quality = 'OK' if meta['rtt_ms'] < 4000 else 'HIGH_RTT'
            return {'server_time_ms': server, 'offset_ms': self.clock_offset_ms,
                    'rtt_ms': meta['rtt_ms'], 'clock_quality': self.clock_quality}
        except Exception as exc:
            self.clock_quality = 'UNVERIFIED_USING_LOCAL_CLOCK'
            return {'clock_quality': self.clock_quality, 'error': str(exc)}


def clean_klines(raw, tf, received_server_ms, max_age_bars=3):
    """Accept documented closeTime at interval edge OR interval end minus 1ms.
    Require BOTH the semantic end and the explicit closeTime to have passed before receipt.
    No lookahead via run-start/run-end anchor. Duplicate entries invalidated.
    """
    duration = TF[tf]
    counts = Counter()
    keyed = defaultdict(list)
    if not isinstance(raw, list):
        return [], {'invalid_rows': 1, 'reason': 'WRONG_RESPONSE_TYPE'}
    for row in raw:
        if not isinstance(row, (list, tuple)) or len(row) < 7:
            counts['malformed'] += 1
            continue
        t0, tclose = safe_ms(row[0]), safe_ms(row[6])
        o,h,l,c,v = (safe_float(row[i]) for i in range(1,6))
        if (t0 is None or tclose is None or None in (o,h,l,c,v)
            or o <= 0 or h <= 0 or l <= 0 or c <= 0 or v < 0
            or h < max(o,c,l) or l > min(o,c,h)
            or t0 % duration or tclose not in (t0 + duration - 1, t0 + duration)):
            counts['invalid_rows'] += 1
            continue
        if t0 + duration > received_server_ms or tclose > received_server_ms:
            counts['open_or_future'] += 1
            continue
        keyed[t0].append(list(row))
    out = []
    for t0, same in sorted(keyed.items()):
        if len(same) != 1:
            counts['duplicate_or_ambiguous'] += len(same)
            continue
        out.append(same[0])
    gaps = sum(int(out[i][0]) - int(out[i-1][0]) != duration for i in range(1,len(out)))
    age = received_server_ms - (int(out[-1][0]) + duration) if out else None
    stale = age is None or age > duration * max_age_bars
    return out, {'raw': len(raw), 'closed': len(out), 'last_closed_at_ms': int(out[-1][0]) + duration if out else None,
                 'last_closed_age_ms': age, 'stale': stale, 'gaps': gaps,
                 **dict(counts)}


def select_symbols(exchange_data, tickers, top, *, contract_type='PERPETUAL', volume_required=True):
    """Select contract type by exchange metadata, never by guessed ticker suffix.

    Crypto: rank by non-zero USDT quote volume and select top N.
    TradFi: rank by 24h quote volume, select TOP N; missing values sort last.
    """
    raw = exchange_data.get('symbols', []) if isinstance(exchange_data, dict) else []
    lookup = {}
    for item in raw:
        if (isinstance(item, dict) and item.get('symbol')
                and item.get('contractType') == contract_type
                and item.get('quoteAsset') == 'USDT'
                and item.get('marginAsset') == 'USDT'):
            lookup[item['symbol']] = item
    ticklist = tickers if isinstance(tickers,list) else ([tickers] if isinstance(tickers,dict) else [])
    tmap = {t['symbol']:t for t in ticklist if isinstance(t,dict) and t.get('symbol') in lookup}
    ranked = []
    for symbol, instrument in lookup.items():
        ticker=tmap.get(symbol)
        quote_vol=safe_float(ticker.get('quoteVolume')) if ticker else None
        price=safe_float(ticker.get('lastPrice')) if ticker else None
        if volume_required and not (quote_vol is not None and quote_vol > 0 and price is not None and price > 0):
            continue
        ranked.append((symbol,instrument,ticker or {'symbol':symbol},quote_vol))
    ranked.sort(key=lambda row:(row[3] is not None,row[3] or 0),reverse=True)
    return [(sym,meta,ticker) for sym,meta,ticker,_ in ranked[:top if top>0 else None]]


def unwrap_symbol(resp, symbol):
    if isinstance(resp, list):
        matches = [r for r in resp if isinstance(r,dict) and r.get('symbol') == symbol]
        return matches[0] if len(matches) == 1 else None
    if isinstance(resp,dict) and resp.get('symbol') == symbol:
        return resp
    return None


def collect_symbol(client, symbol, meta, ticker, ticker_meta, tfs, fetch_depth=False, market_type='CRYPTO', preflight_bar=None):
    result = {'symbol': symbol, 'market': market_type, 'meta': meta, 'ticker': ticker,
              'ticker_observed': ticker_meta, 'klines': {}, 'errors': {}, 'open_interest': None,
              'premium': None, 'funding_history': [], 'depth': None, 'feature_status': {}}
    for tf in tfs:
        try:
            if tf == '5m' and preflight_bar is not None:
                result['klines'][tf] = preflight_bar
                continue
            obj, stamp = client.request('/capi/v3/market/klines',
                                        {'symbol': symbol, 'interval': tf, 'limit': CANDLE_LIMITS.get(tf,150)})
            arr, qc = clean_klines(obj, tf, stamp['received_server_est_ms'])
            result['klines'][tf] = {'data': arr, 'quality': qc, 'observed': stamp,
                                     'source': 'WEEX_LAST_TRADE_FUTURES', 'only_closed': True}
        except Exception as exc:
            result['errors']['kline_'+tf] = str(exc)
    for label, path in [('open_interest','/capi/v3/market/openInterest'),
                        ('premium','/capi/v3/market/premiumIndex'),
                        ('funding_history','/capi/v3/market/fundingRate')]:
        if label == 'funding_history': continue  # Stage 2 only
        try:
            params = {'symbol': symbol}
            if label == 'funding_history': params['limit'] = 20
            obj, stamp = client.request(path, params)
            if label == 'funding_history':
                if not isinstance(obj,list): raise ValueError('funding history not a list')
                # Settlement timestamp is not necessarily a publication timestamp!
                result['funding_history'] = [v for v in obj if isinstance(v,dict)
                    and v.get('symbol') == symbol and safe_ms(v.get('fundingTime')) is not None
                    and safe_ms(v.get('fundingTime')) <= stamp['received_server_est_ms']]
                result['funding_history_observed'] = stamp
                continue
            r = unwrap_symbol(obj, symbol)
            if r is None: raise ValueError('Missing/mismatched symbol')
            source_ms = safe_ms(r.get('time'))
            age_ms = stamp['received_server_est_ms'] - source_ms if source_ms else None
            result[label] = {'data': r, 'observed': stamp, 'source_time_ms': source_ms,
                             'source_age_ms': age_ms,
                             'source_time_valid': bool(source_ms and -3000 <= age_ms <= 15 * 60_000),
                             'units': 'contracts' if label == 'open_interest' else 'fraction_per_funding_period'}
        except Exception as exc:
            result['errors'][label] = str(exc)
    if fetch_depth:
        try:
            obj, stamp = client.request('/capi/v3/market/depth',{'symbol':symbol,'limit':15})
            result['depth'] = {'data':obj,'observed':stamp}
        except Exception as exc:
            result['errors']['depth'] = str(exc)
    return result


def basic_features(coin):
    """WATCHLIST ONLY; no 8/10 claim, no readiness to execute."""
    t = coin['ticker']
    sym = coin['symbol']
    out = {'symbol':sym, 'market':coin.get('market', 'CRYPTO'), 'price':safe_float(t.get('lastPrice')),
           'quote_volume_24h_usdt':safe_float(t.get('quoteVolume')),
           'change_24h_raw':safe_float(t.get('priceChangePercent')),
           'oi_contracts':None,'oi_source_age_ms':None, 'funding_forecast':None,
           'funding_latest_settled':None,'closed_5m':0,'closed_15m':0,'closed_1h':0,
           'gap_5m':None,'rvol_5m':None,'trend_15m':None,'trend_1h':None,
           'data_ok':False,'watch_direction':'NONE','trade_allowed':False,
           'warnings':[],'ticker_window_close_age_ms':None}
    warnings = out['warnings']
    # closeTime belongs to the 24h ticker window, NOT a reliable per-symbol
    # last-trade freshness clock (WEEX may round all tickers to the same hour).
    window_close_ms=safe_ms(t.get('closeTime'))
    ticker_observed_ms=safe_ms(coin.get('ticker_observed',{}).get('received_server_est_ms'))
    if window_close_ms is not None and ticker_observed_ms is not None:
        out['ticker_window_close_age_ms']=ticker_observed_ms-window_close_ms
        if window_close_ms>ticker_observed_ms+3000:
            warnings.append('TICKER_WINDOW_IN_FUTURE')
    else:
        warnings.append('TICKER_TIMESTAMP_MISSING')
    if not coin.get('ticker_refresh_ok',False):
        warnings.append('TICKER_REFRESH_NOT_CONFIRMED')
    if out['price'] is None or out['price']<=0:
        warnings.append('TICKER_PRICE_INVALID')
    oi=coin.get('open_interest')
    if oi:
        out['oi_contracts'] = safe_float(oi['data'].get('openInterest'))
        out['oi_source_age_ms']=oi['source_age_ms']
        if not oi['source_time_valid'] or out['oi_contracts'] is None or out['oi_contracts'] < 0:
            warnings.append('OI_TIME_OR_VALUE_INVALID')
    else: warnings.append('OI_MISSING')
    p=coin.get('premium')
    if p:
        out['funding_forecast'] = safe_float(p['data'].get('forecastFundingRate'))
        out['funding_latest_settled'] = safe_float(p['data'].get('lastFundingRate'))
        if not p['source_time_valid']: warnings.append('FUNDING_STALE_OR_FUTURE')
    else: warnings.append('FUNDING_MISSING')
    klines=coin.get('klines') or {}
    for tf in ('5m','15m','1h'):
        data=klines.get(tf) or {}
        arr=data.get('data') or []
        out['closed_'+tf] = len(arr)
        if tf == '5m':out['gap_5m'] = (data.get('quality') or {}).get('gaps')
        if not arr or (data.get('quality') or {}).get('stale'):
            warnings.append(f'{tf}_MISSING_OR_STALE')
        if (data.get('quality') or {}).get('gaps',0) > 0:
            warnings.append(f'{tf}_GAPS')
    v=klines.get('5m',{}).get('data',[])
    if len(v)>=22:
        base = [safe_float(x[5]) for x in v[-21:-1]]
        current = safe_float(v[-1][5])
        if all(x is not None for x in base) and current is not None:
            m=sum(base)/len(base)
            out['rvol_5m'] = current/m if m>0 else None
    for tf in ('15m','1h'):
        v=klines.get(tf,{}).get('data',[])
        if len(v)>=22:
            values = [safe_float(x[4]) for x in v[-22:]]
            if all(x is not None for x in values):
                latest, avg=values[-1],sum(values[:-1])/21
                out['trend_'+tf] = 'UP' if latest>avg else 'DOWN' if latest<avg else 'FLAT'
    if out['price'] is not None and len(coin.get('klines',{}).get('5m',{}).get('data',[])):
        v=coin['klines']['5m']['data'][-1]
        candle_px=safe_float(v[4])
        if candle_px is not None and abs(out['price']/candle_px-1)>0.07:
            warnings.append('PRICE_LAST_TRADE_VS_CANDLE_GT7PCT')
        # Reject candles older than 3x5m relative to the FINAL ticker snapshot.
        candle_end=safe_ms(v[6])
        if (ticker_observed_ms is None or candle_end is None or
                ticker_observed_ms-candle_end>10*60_000 or candle_end>ticker_observed_ms+3000):
            warnings.append('CANDLE_VS_TICKER_NOT_SYNCHRONIZED')
    # Missing data cannot be silently filled from a different market.
    critical = any(w in warnings for w in ('5m_MISSING_OR_STALE','15m_MISSING_OR_STALE',
         '1h_MISSING_OR_STALE','5m_GAPS','15m_GAPS','1h_GAPS','OI_MISSING',
         'OI_TIME_OR_VALUE_INVALID','FUNDING_MISSING','FUNDING_STALE_OR_FUTURE',
          'TICKER_WINDOW_IN_FUTURE','TICKER_TIMESTAMP_MISSING',
          'TICKER_REFRESH_NOT_CONFIRMED','TICKER_PRICE_INVALID',
          'PRICE_LAST_TRADE_VS_CANDLE_GT7PCT','CANDLE_VS_TICKER_NOT_SYNCHRONIZED'))
    out['data_ok']=bool(not critical and out['closed_5m']>=22 and
                        out['closed_15m']>=22 and out['closed_1h']>=22)
    if coin.get('stage2_failed'):
        out['data_ok']=False
        warnings.append('STAGE2_INCOMPLETE')
    if out['data_ok']:
        if out['trend_15m']==out['trend_1h']=='UP' and (out['rvol_5m'] or 0)>=1.2:
            out['watch_direction']='LONG_WATCH'
        if out['trend_15m']==out['trend_1h']=='DOWN' and (out['rvol_5m'] or 0)>=1.2:
            out['watch_direction']='SHORT_WATCH'
    return out


def oi_candle_price(coin, observed_ms):
    """Last CLOSE already available when OI was observed. Never use later ticker price."""
    rows=((coin.get('klines') or {}).get('5m') or {}).get('data') or []
    valid=[]
    for row in rows[-8:]:
        close_at=safe_ms(row[6]) if len(row)>6 else None
        close=safe_float(row[4]) if len(row)>4 else None
        if close_at and close and close>0 and 0<=observed_ms-close_at<=10*60_000:
            valid.append((close_at,close))
    return max(valid) if valid else (None,None)


def validate_oi_state(data):
    if not isinstance(data, dict): raise ValueError('OI history must be dict')
    checked={}
    for key,raw in data.items():
        if not isinstance(key,str) or not isinstance(raw,list):
            raise ValueError('Invalid OI key or observation series')
        items=[]
        for entry in raw:
            if not isinstance(entry,dict):raise ValueError('Invalid OI record')
            t=safe_ms(entry.get('time_ms'))
            obs=safe_ms(entry.get('observed_at_ms'))
            v=safe_float(entry.get('contracts'))
            if t is None or obs is None or v is None or v<0 or obs<t-3000:
                raise ValueError('OI state contains invalid times/values')
            items.append(entry)
        checked[key]=items
    return checked


def build_oi_history(path, coins):
    """Persist raw OI with candle prices observed BEFORE each OI point.

    Legacy records without price remain usable for raw OI changes, NOT directional
    confirmation. No interpolation/backfill of legacy prices.
    """
    path=Path(path)
    try:
        cache=validate_oi_state(json.loads(path.read_text(encoding='utf-8'))) if path.exists() else {}
    except (json.JSONDecodeError,OSError,ValueError) as exc:
        raise RuntimeError(f'OI state unreadable; will NOT overwrite {path}: {exc}') from exc
    summary={}
    for c in coins:
        key=f"{c.get('market','CRYPTO')}|{c['symbol']}"
        oi=c.get('open_interest')
        if not oi or not oi.get('source_time_valid'):continue
        v=safe_float((oi.get('data') or {}).get('openInterest'))
        t=safe_ms(oi.get('source_time_ms'))
        observed=safe_ms((oi.get('observed') or {}).get('received_server_est_ms'))
        val=safe_float((c.get('meta') or {}).get('contractVal'))
        if v is None or v<0 or not t or not observed:continue
        bar_time, bar_price=oi_candle_price(c, observed)
        history=cache.get(key,cache.get(c['symbol'],[]) if c.get('market')=='CRYPTO' else [])
        # Contract units must match. Never manufacture missing historical prices.
        prior=[p for p in history if p.get('contractVal')==val and p['time_ms']<t]
        previous=[p for p in prior if 30*60_000<=t-p['time_ms']<=6*3600_000]
        summary_item={'oi_pct_change_since_previous':None,'oi_price_pct_change':None,
                      'oi_observation_span_min':None,'comparable':False,
                      'oi_direction_data_ready':False,'oi_price_pair_valid':bar_price is not None,
                      'oi_direction':'UNVERIFIED','oi_direction_reason':'MISSING_COMPARABLE_OI_HISTORY',
                      'unit':'contracts','source':'WEEX_V650_OBSERVED_CLOSES'}
        if previous:
            last=max(previous,key=lambda p:p['time_ms'])
            span=round((t-last['time_ms'])/60000,2)
            raw_change=(v/last['contracts']-1)*100 if last['contracts']>0 else None
            valid_raw=(raw_change is not None and abs(raw_change)<=200)
            summary_item.update(oi_pct_change_since_previous=raw_change,
                                oi_observation_span_min=span,
                                prev_time_ms=last['time_ms'],current_time_ms=t,
                                comparable=valid_raw)
            older_px=safe_float(last.get('price_at_oi'))
            older_t=safe_ms(last.get('price_time_ms'))
            older_obs=safe_ms(last.get('observed_at_ms'))
            price_pair_ok=(bar_price is not None and bar_time is not None
                           and older_px is not None and older_px>0 and older_t is not None
                           and older_obs is not None
                           and 0<=older_obs-older_t<=10*60_000
                           and 0<=observed-bar_time<=10*60_000
                           and t>last['time_ms'] and valid_raw)
            if price_pair_ok:
                price_change=(bar_price/older_px-1)*100
                if math.isfinite(price_change) and abs(price_change)<=200:
                    direction=('PRICE_UP_OI_UP' if price_change>=0.1 and raw_change>=0.2 else
                               'PRICE_DOWN_OI_UP' if price_change<=-0.1 and raw_change>=0.2 else
                               'OI_NOT_BUILDING' if raw_change<0.2 else 'PRICE_MOVE_TOO_SMALL')
                    summary_item.update(oi_price_pct_change=round(price_change,5),
                                        oi_direction_data_ready=True,
                                        oi_direction=direction,
                                        oi_direction_reason=direction)
                else:summary_item['oi_direction_reason']='PRICE_CHANGE_INVALID'
            else:summary_item['oi_direction_reason']='PAIR_UNAVAILABLE_OR_LEGACY_HISTORY'
        prior.append({'time_ms':t,'contracts':v,'contractVal':val,'observed_at_ms':observed,
                      'price_at_oi':bar_price,'price_time_ms':bar_time,
                      'price_source':'LAST_CLOSED_5M_BEFORE_OI' if bar_price is not None else None})
        cache[key]=sorted(prior,key=lambda p:p['time_ms'])[-250:]
        summary[key]=summary_item
    atomic_json(path,cache)
    return summary


def record_signal_event(events_jsonl, *, symbol, side, entry_type, entry, stop_loss,
                        take_profit, decision_at_ms, feature_timestamps_ms,
                        ttl_minutes=None, source='EXTERNAL_STRATEGY'):
    """Call this at original strategy signal CREATION; never after the fact.
    Historical signals lacking TTL cannot be retroactively repaired.
    Events are diagnostic only; do not place exchange orders.
    """
    decision=safe_ms(decision_at_ms)
    if decision is None or not symbol or side not in ('LONG','SHORT') or entry_type not in ('LIMIT','MARKET'):
        raise ValueError('Invalid event identity or decision timestamp')
    entry,stop_loss,take_profit=(safe_float(x) for x in (entry,stop_loss,take_profit))
    if None in (entry,stop_loss,take_profit) or min(entry,stop_loss,take_profit)<=0:
        raise ValueError('Invalid prices')
    if not (stop_loss<entry<take_profit if side=='LONG' else take_profit<entry<stop_loss):
        raise ValueError('Wrong stop/entry/target order')
    if not isinstance(feature_timestamps_ms,dict) or not feature_timestamps_ms:
        raise ValueError('Missing feature timestamps; cannot validate causality')
    for name, t in feature_timestamps_ms.items():
        ts=safe_ms(t)
        if ts is None or ts > decision or decision-ts > 15*60_000:
            raise ValueError(f'FUTURE_OR_STALE_FEATURE_{name}')
    expiry=None
    if entry_type=='LIMIT':
        ttl=safe_float(ttl_minutes)
        if ttl is None or not 1<=ttl<=1440:
            raise ValueError('Missing/out-of-range LIMIT TTL at creation')
        expiry=decision+int(ttl*60_000)
    else:
        ttl=None
    key=f'{source}|{symbol}|{side}|{entry_type}|{decision}|{entry:.12g}|{stop_loss:.12g}|{take_profit:.12g}'
    event={'event_id':hashlib.sha256(key.encode()).hexdigest()[:24],
           'symbol':symbol,'side':side,'entry_type':entry_type,'entry':entry,
           'stop_loss':stop_loss,'take_profit':take_profit,'decision_at_ms':decision,
           'created_at_ms':decision,'ttl_minutes':ttl,'expiry_at_ms':expiry,
           'feature_timestamps_ms':feature_timestamps_ms,'source':source,
           'do_handlu':False,'trade_gate':'DISABLED_UNTIL_VALIDATED'}
    outfile=Path(events_jsonl);outfile.parent.mkdir(parents=True,exist_ok=True)
    # Atomic append per local process; source engine should centralize writes on parallel runs.
    with open(outfile,'a',encoding='utf-8') as f:
        f.write(json.dumps(event,ensure_ascii=False)+'\n')
    return event


def restore_oi_state(previous_zip, state_file):
    """Merge previous archive into persistent state. Never clobber newer Drive history."""
    state_file=Path(state_file)
    if not previous_zip:
        return 'existing persistent OI state reused' if state_file.exists() else 'new OI history'
    with zipfile.ZipFile(previous_zip) as z:
        item='weex_data/oi_observations_state.json'
        if item not in z.namelist():
            raise RuntimeError(f'Previous ZIP lacks {item}')
        imported=validate_oi_state(json.loads(z.read(item)))
    try:
        persistent=validate_oi_state(json.loads(state_file.read_text(encoding='utf-8'))) if state_file.exists() else {}
    except (ValueError,json.JSONDecodeError,OSError) as exc:
        raise RuntimeError(f'Existing OI history invalid: {exc}; not overwritten') from exc
    for key, observations in imported.items():
        merged={ (p['time_ms'],str(p.get('contractVal'))):p for p in observations }
        for p in persistent.get(key,[]):
            k=(p['time_ms'],str(p.get('contractVal')))
            # Current persistent price-aware observations win vs historical legacy.
            if k not in merged or p.get('price_at_oi') is not None:
                merged[k]=p
        persistent[key]=sorted(merged.values(),key=lambda p:p['time_ms'])[-250:]
    atomic_json(state_file,persistent)
    return f'merged {len(imported)} imported OI symbol histories with persistent history'


def update_klines(client, coin, intervals):
    for tf in intervals:
        try:
            obj, stamp = client.request('/capi/v3/market/klines',
                {'symbol':coin['symbol'],'interval':tf,'limit':CANDLE_LIMITS[tf]})
            arr, qc = clean_klines(obj, tf, stamp['received_server_est_ms'])
            coin['klines'][tf] = {'data':arr,'quality':qc,'observed':stamp,
                                  'source':'WEEX_LAST_TRADE_FUTURES','only_closed':True}
            coin['errors'].pop('kline_'+tf,None)
        except Exception as exc:
            coin['errors']['kline_'+tf] = str(exc)

def refresh_snapshot_inputs(client, coin):
    """Refresh OI, premium & orderbook only for final candidates; fail closed on errors."""
    for label,path in [('open_interest','/capi/v3/market/openInterest'),
                       ('premium','/capi/v3/market/premiumIndex')]:
        try:
            obj,stamp=client.request(path,{'symbol':coin['symbol']})
            r=unwrap_symbol(obj,coin['symbol'])
            if r is None: raise ValueError('Missing or wrong symbol '+label)
            source_ms=safe_ms(r.get('time'))
            age=stamp['received_server_est_ms']-source_ms if source_ms else None
            valid=bool(source_ms is not None and -3000<=age<=15*60_000)
            coin[label]={'data':r,'observed':stamp,'source_time_ms':source_ms,
                         'source_age_ms':age,'source_time_valid':valid,
                         'units':'contracts' if label=='open_interest' else 'fraction_per_funding_period'}
            coin['errors'].pop(label,None)
            if not valid: raise ValueError('Invalid source timestamp '+label)
        except Exception as exc:
            coin['errors'][label]=str(exc)
            coin['stage2_failed']=True
    try:
        obj,stamp=client.request('/capi/v3/market/depth',{'symbol':coin['symbol'],'limit':15})
        coin['depth']={'data':obj,'observed':stamp}
        coin['errors'].pop('depth',None)
    except Exception as exc:
        coin['errors']['depth']=str(exc)
        # Score capped C when depth is missing; no synthetic spread.


def fetch_history_for_candidates(client, coin):
    try:
        obj, stamp=client.request('/capi/v3/market/fundingRate',
                                  {'symbol':coin['symbol'],'limit':20})
        if not isinstance(obj,list): raise ValueError('funding history not a list')
        coin['funding_history']=[v for v in obj if isinstance(v,dict)
            and v.get('symbol')==coin['symbol'] and safe_ms(v.get('fundingTime')) is not None
            and safe_ms(v.get('fundingTime'))<=stamp['received_server_est_ms']]
        coin['funding_history_observed']=stamp
    except Exception as exc:
        coin['errors']['funding_history']=str(exc)



def orderbook_spread_pct(coin):
    """Return spread % from actually observed best bid/ask; None if unsupported."""
    book=coin.get('depth') or {}
    d=book.get('data') if isinstance(book,dict) else None
    if not isinstance(d,dict):return None
    try:
        bid=float(d['bids'][0][0]);ask=float(d['asks'][0][0])
        obs=book['observed']['received_server_est_ms']
        decision=coin['ticker_observed']['received_server_est_ms']
        if not (math.isfinite(bid) and math.isfinite(ask) and 0<bid<ask and 0<=decision-obs<=90_000):return None
        return (ask-bid)/((ask+bid)/2)*100
    except (KeyError,ValueError,TypeError,IndexError,ZeroDivisionError):return None


def atr_from_rows(rows, periods=14):
    if len(rows)<periods+1:return None
    trs=[]
    for idx in range(len(rows)-periods,len(rows)):
        o,h,l,c=[safe_float(rows[idx][j]) for j in (1,2,3,4)]
        prev=safe_float(rows[idx-1][4])
        if None in (o,h,l,c,prev):return None
        trs.append(max(h-l,abs(h-prev),abs(l-prev)))
    atr=sum(trs)/len(trs)
    return atr if atr>0 else None


def price_round(value, decimals, mode='NEAREST'):
    q=Decimal(1).scaleb(-decimals)
    policy={'NEAREST':ROUND_HALF_UP,'DOWN':ROUND_DOWN,'UP':ROUND_UP}[mode]
    return float(Decimal(str(value)).quantize(q,rounding=policy))


def price_text(value,decimals):
    return f'{value:.{decimals}f}'


def preselection_rank(coin, feature):
    """Transparent selection BEFORE depth fetch; ranks activity, real volume, trend, SL feasibility.

    Scores measure data confluence, not expected future profitability.
    """
    v5=(coin.get('klines',{}).get('5m') or {}).get('data') or []
    v15=(coin.get('klines',{}).get('15m') or {}).get('data') or []
    side='LONG' if feature.get('watch_direction')=='LONG_WATCH' else 'SHORT'
    px=safe_float((coin.get('ticker') or {}).get('lastPrice'))
    atr=atr_from_rows(v5)
    rvol=feature.get('rvol_5m') or 0
    volume=feature.get('quote_volume_24h_usdt') or 0
    score_rvol=round(min(30,max(0,(rvol-1.0)*15)),2)
    score_liq=round(min(25,max(0,(math.log10(max(1,volume))-4.5)*9)),2)
    score_trend=15 if feature.get('trend_15m')==feature.get('trend_1h')==('UP' if side=='LONG' else 'DOWN') else 0
    stop_pct=None
    score_risk=0
    if px and atr and len(v5)>=6 and len(v15)>=3:
        swing=min(min(float(x[3]) for x in v5[-6:]),min(float(x[3]) for x in v15[-3:])) if side=='LONG' else max(max(float(x[2]) for x in v5[-6:]),max(float(x[2]) for x in v15[-3:]))
        stop=min(swing-.15*atr,px-1.15*atr) if side=='LONG' else max(swing+.15*atr,px+1.15*atr)
        stop_pct=abs(px-stop)/px*100
        score_risk=20 if .25<=stop_pct<=3.5 else 8 if .12<=stop_pct<=6 else 0
    score_funding=10 if feature.get('funding_forecast') is not None and abs(feature['funding_forecast'])<=.002 else 0
    total=round(score_rvol+score_liq+score_trend+score_risk+score_funding,2)
    return {'market':coin['market'],'symbol':coin['symbol'],'side':side,
            'score':total,'rvol':round(rvol,3),'quote_volume_24h':volume,
            'estimated_stop_pct':round(stop_pct,3) if stop_pct is not None else None,
            'score_components':{'rvol':score_rvol,'liquidity':score_liq,'trend':score_trend,
                                'stop_risk':score_risk,'funding':score_funding}}



def preview_structural_rr(coin, feature):
    """Conservative Stage-1 RR screen: only reject a clear known nearby barrier.

    Never invent 4h/1d levels at this stage. If there is no confirmed stage-1
    pivot, mark UNKNOWN and keep for Stage 2. An estimate is not an order signal.
    """
    side = 'LONG' if feature.get('watch_direction') == 'LONG_WATCH' else 'SHORT'
    rows5 = ((coin.get('klines') or {}).get('5m') or {}).get('data') or []
    rows15 = ((coin.get('klines') or {}).get('15m') or {}).get('data') or []
    rows1h = ((coin.get('klines') or {}).get('1h') or {}).get('data') or []
    px = safe_float((coin.get('ticker') or {}).get('lastPrice'))
    atr = atr_from_rows(rows5)
    if px is None or px <= 0 or atr is None or len(rows5) < 22 or len(rows15) < 3:
        return {'pre_rr':None, 'pre_rr_source':'UNKNOWN_INPUT', 'pre_rr_screen_pass':True}
    try:
        if side == 'LONG':
            swing = min(min(float(x[3]) for x in rows5[-6:]),
                        min(float(x[3]) for x in rows15[-3:]))
            stop = min(swing - .15 * atr, px - 1.15 * atr)
            risk = px - stop
        else:
            swing = max(max(float(x[2]) for x in rows5[-6:]),
                        max(float(x[2]) for x in rows15[-3:]))
            stop = max(swing + .15 * atr, px + 1.15 * atr)
            risk = stop - px
        if risk <= 0 or not .12 <= risk / px * 100 <= 6:
            return {'pre_rr':None,'pre_rr_source':'STOP_UNCERTAIN','pre_rr_screen_pass':True}
        # Same confirmed 3-bar pivot definition as final target, only shorter horizons.
        # Do not include 4h/1d yet; an unknown barrier must not be invented.
        levels=[]
        for tf, rows in [('15m',rows15),('1h',rows1h)]:
            for i in range(max(1,len(rows)-60),len(rows)-1):
                value=safe_float(rows[i][2 if side=='LONG' else 3])
                left=safe_float(rows[i-1][2 if side=='LONG' else 3])
                right=safe_float(rows[i+1][2 if side=='LONG' else 3])
                if None in (value,left,right):continue
                if side=='LONG' and value>left and value>right and value>px:
                    levels.append(value)
                elif side=='SHORT' and 0<value<left and value<right and value<px:
                    levels.append(value)
        if not levels:
            return {'pre_rr':None,'pre_rr_source':'NO_STAGE1_STRUCTURAL_LEVEL',
                    'pre_rr_screen_pass':True}
        target=min(levels,key=lambda x:abs(x-px))
        buffer=max(.1*atr,0)
        front=target-buffer if side=='LONG' else target+buffer
        reward=(front-px) if side=='LONG' else (px-front)
        rr=reward/risk
        return {'pre_rr':round(rr,4),'pre_rr_source':'15m_OR_1h_CONFIRMED_PIVOT',
                'pre_rr_screen_pass':True}  # caller applies its configured threshold
    except (ValueError,TypeError,IndexError,ZeroDivisionError):
        return {'pre_rr':None,'pre_rr_source':'INVALID_STAGE1_ROWS','pre_rr_screen_pass':True}


def structural_tp1(coin, side, entry, risk, atr, decimals):
    """Nearest confirmed pivot within a realistic local search horizon.

    Never use a remote daily pivot to certify a short-horizon 1.5R target.
    All candles must already be closed by the collector.
    """
    if not entry or not risk or risk<=0 or not atr or atr<=0:
        return None,None,None,'INVALID_TARGET_INPUT'
    levels=[]
    # A 4h pivot is useful only if physically close; 1d is not a
    # short-horizon confirming level for the 5m/15m entry.
    max_gap=max(4*risk,4*atr)
    for tf in ('15m','1h','4h'):
        rows=((coin.get('klines') or {}).get(tf) or {}).get('data') or []
        for i in range(max(1,len(rows)-60),len(rows)-1):
            v=safe_float(rows[i][2 if side=='LONG' else 3])
            left=safe_float(rows[i-1][2 if side=='LONG' else 3])
            right=safe_float(rows[i+1][2 if side=='LONG' else 3])
            if None in (v,left,right):continue
            if side=='LONG' and v>left and v>right and entry<v<=entry+max_gap:
                levels.append((v,tf))
            elif side=='SHORT' and v<left and v<right and 0<entry-max_gap<=v<entry:
                levels.append((v,tf))
    raw=entry+1.5*risk if side=='LONG' else entry-1.5*risk
    if not levels:
        return raw,None,None,'NO_NEARBY_CONFIRMED_PIVOT'
    sr,tf=min(levels,key=lambda x:abs(x[0]-entry))
    front=sr-max(.1*atr,10**(-decimals)) if side=='LONG' else sr+max(.1*atr,10**(-decimals))
    target=min(raw,front) if side=='LONG' else max(raw,front)
    if not (entry<target if side=='LONG' else 0<target<entry):
        return raw,None,None,'PIVOT_TOO_CLOSE_TO_ENTRY'
    return target,sr,tf,'LOCAL_STRUCTURAL_TARGET'



def estimate_setup(coin, feature, *, clock_ok=True):
    """Market ENTRY/SL/TP1 and transparent A-D quality grade (not execution).

    A/B require independently observed OI increase (short-build/long-build proxy)
    AND a fresh bid/ask spread; without either the best permitted grade is C.
    No grade implies historical win rate or statistically validated advantage.
    """
    side='LONG' if feature.get('watch_direction')=='LONG_WATCH' else 'SHORT'
    if feature.get('watch_direction') not in ('LONG_WATCH','SHORT_WATCH'):
        return None
    result={'market':coin['market'],'symbol':coin['symbol'],'side':side,
            'entry_type':'MARKET_INDICATIVE','entry':None,'sl':None,'tp1':None,
            'rr_gross':None,'rr_net_est':None,'risk_pct':None,'spread_pct':None,
            'fees_roundtrip_pct':None,'cost_in_r':None,'oi_pct':feature.get('oi_pct_change_since_previous'),
            'oi_confirmed':False,'oi_price_pct':feature.get('oi_price_pct_change'),
            'oi_direction':feature.get('oi_direction','UNVERIFIED'),
            'tp1_basis':None,'tp1_structural_level':None,'tp1_room_r':None,
            'grade':'D','score':0,'trade_allowed':False,
            'status':'WATCH_ONLY','reason_codes':[],'score_breakdown':{},'observed_at_utc':None}
    reasons=result['reason_codes']
    if not clock_ok:reasons.append('SERVER_CLOCK_UNVERIFIED')
    if not feature.get('data_ok'):reasons.append('DATA_QUALITY_REJECTED')
    if coin.get('stage2_failed'):reasons.append('STAGE2_INCOMPLETE')
    latest=coin.get('ticker_observed') or {}
    decision=safe_ms(latest.get('received_server_est_ms'))
    result['observed_at_utc']=utc(decision) if decision else None
    if decision is None:reasons.append('TICKER_NOT_TIMESTAMPED')
    for tf in ('5m','15m','1h','4h','1d'):
        a=(coin.get('klines') or {}).get(tf,{}).get('data') or []
        if len(a)<(22 if tf in ('5m','15m','1h') else 15):reasons.append(f'{tf}_CANDLES_MISSING')
    if reasons:return result
    v5=coin['klines']['5m']['data'];v15=coin['klines']['15m']['data']
    # Last candle must be closed and no older than 2 complete 5m bars.
    last_end=safe_ms(v5[-1][6]); last15_end=safe_ms(v15[-1][6])
    if not last_end or not last15_end or not (0<=decision-last_end<=10*60_000) \
        or not (0<=decision-last15_end<=30*60_000):
        reasons.append('FINAL_CANDLE_NOT_FRESH');return result
    last_oi=coin.get('open_interest') or {};last_p=coin.get('premium') or {}
    for name,datum in [('OI',last_oi),('FUNDING',last_p)]:
        observed=safe_ms((datum.get('observed') or {}).get('received_server_est_ms'))
        if not datum.get('source_time_valid') or not observed or not (0<=decision-observed<=90_000):
            reasons.append(f'{name}_NOT_FRESH')
    if reasons:return result
    p=safe_float(coin['ticker'].get('lastPrice'))
    atr=atr_from_rows(v5)
    if p is None or p<=0 or atr is None:reasons.append('PRICE_OR_ATR_INVALID');return result
    pr=coin['meta'].get('pricePrecision')
    decimals=int(pr) if isinstance(pr,int) and 0<=pr<=10 else min(10,len(str(coin['ticker'].get('lastPrice')).split('.')[-1]))
    # Candidate SL uses recent local swing plus ATR buffer; stops too distant
    # are rejected rather than labelled A/B.
    if side=='LONG':
        structure=min(min(float(x[3]) for x in v5[-6:]),min(float(x[3]) for x in v15[-3:]))
        stop=min(structure-.15*atr,p-1.15*atr)
    else:
        structure=max(max(float(x[2]) for x in v5[-6:]),max(float(x[2]) for x in v15[-3:]))
        stop=max(structure+.15*atr,p+1.15*atr)
    # Round to WEEX instrument price precision; stop conservatively outward.
    entry=price_round(p,decimals)
    stop=price_round(stop,decimals,'DOWN' if side=='LONG' else 'UP')
    risk=entry-stop if side=='LONG' else stop-entry
    risk_pct=100*risk/entry if entry>0 else float('inf')
    if risk<=0 or not .12<=risk_pct<=6.0:
        reasons.append('SL_TOO_TIGHT_OR_TOO_FAR');return result
    gross_target, sr_level, sr_tf, tp_reason=structural_tp1(coin,side,entry,risk,atr,decimals)
    result['tp1_basis']=('SR_'+sr_tf if sr_tf else 'UNCONFIRMED_1P5R')
    result['tp1_structural_level']=price_text(price_round(sr_level,decimals),decimals) if sr_level else None
    result['tp1_room_r']=round(abs(gross_target-entry)/risk,3)
    if sr_tf is None:reasons.append('NO_CONFIRMED_STRUCTURAL_TARGET')
    tp=price_round(gross_target,decimals,'DOWN' if side=='LONG' else 'UP')
    reward=tp-entry if side=='LONG' else entry-tp
    if tp<=0 or reward<=0:
        reasons.append('RR_OR_TP_INVALID');return result
    if reward/risk<1.35:
        reasons.append('STRUCTURE_LIMITS_REWARD')
    tick=10**(-decimals)
    # Require the tick size to preserve strictly ordered prices.
    if risk<2*tick or reward<2*tick:
        reasons.append('INSTRUMENT_PRECISION_RISK');return result
    result.update(entry=price_text(entry,decimals),sl=price_text(stop,decimals),
                  tp1=price_text(tp,decimals),rr_gross=round(reward/risk,3),
                  risk_pct=round(risk_pct,3))
    spread=orderbook_spread_pct(coin)
    result['spread_pct']=round(spread,5) if spread is not None else None
    fee=safe_float(coin['meta'].get('takerFeeRate'))
    if fee is None or not 0<=fee<.01:
        fee=.0012;reasons.append('FEE_ESTIMATED_NOT_CONFIRMED')
    cost_pct=2*fee*100+(spread or 0)
    result['fees_roundtrip_pct']=round(2*fee*100,4)
    result['cost_in_r']=round(cost_pct/risk_pct,3)
    result['rr_net_est']=round(reward/risk-cost_pct/risk_pct,3)
    if spread is None:reasons.append('SPREAD_NOT_VERIFIED')
    max_spread=.40 if coin['market']=='TRADFI' else .55
    if spread is not None and spread>max_spread:reasons.append('SPREAD_TOO_WIDE')
    if result['cost_in_r']>.25:reasons.append('TRANSACTION_COST_TOO_HIGH')
    minimum_vol=100_000 if coin['market']=='TRADFI' else 500_000
    if (feature.get('quote_volume_24h_usdt') or 0)<minimum_vol:reasons.append('INSUFFICIENT_24H_LIQUIDITY')
    if float(v5[-1][5])<=0:reasons.append('SESSION_NO_TRADES_IN_LAST_BAR')
    # Use latest bar activity rather than assuming conventional TradFi hours:
    # tokenized TradFi availability varies with venue/instrument.
    oi_change=feature.get('oi_pct_change_since_previous')
    comparable=feature.get('comparable') is True
    direction_expected='PRICE_UP_OI_UP' if side=='LONG' else 'PRICE_DOWN_OI_UP'
    result['oi_confirmed']=bool(comparable and oi_change is not None and .2<=oi_change<=40
                                and feature.get('oi_direction_data_ready') is True
                                and feature.get('oi_direction')==direction_expected)
    if not result['oi_confirmed']:
        reasons.append('OI_PRICE_DIRECTION_NOT_CONFIRMED')
    funding=feature.get('funding_forecast')
    if funding is None:reasons.append('FUNDING_VALUE_MISSING')
    if funding is not None and ((side=='LONG' and funding>.002) or (side=='SHORT' and funding<-.002)):
        reasons.append('ADVERSE_FUNDING')
    score_parts={
        'trend':30 if feature.get('trend_15m')==feature.get('trend_1h')==('UP' if side=='LONG' else 'DOWN') else 0,
        'rvol':20 if (feature.get('rvol_5m') or 0)>=2 else 12 if (feature.get('rvol_5m') or 0)>=1.2 else 0,
        'oi':15 if result['oi_confirmed'] else 0,
        'liquidity':10 if 'INSUFFICIENT_24H_LIQUIDITY' not in reasons else 0,
        'spread':10 if spread is not None and spread<=max_spread else 0,
        'cost':10 if result['cost_in_r']<=.25 else 0,
        'funding':5 if funding is not None and 'ADVERSE_FUNDING' not in reasons else 0,
    }
    score=sum(score_parts.values());result['score']=score;result['score_breakdown']=score_parts
    grade='A' if score>=85 else 'B' if score>=70 else 'C' if score>=50 else 'D'
    if (not result['oi_confirmed'] or spread is None or 'FEE_ESTIMATED_NOT_CONFIRMED' in reasons
            or 'NO_CONFIRMED_STRUCTURAL_TARGET' in reasons):
        grade='C' if grade in ('A','B') else grade
    hard={'SPREAD_TOO_WIDE','TRANSACTION_COST_TOO_HIGH','INSUFFICIENT_24H_LIQUIDITY',
          'STRUCTURE_LIMITS_REWARD',
          'SESSION_NO_TRADES_IN_LAST_BAR','ADVERSE_FUNDING'}
    if any(reason in hard for reason in reasons):grade='D'
    result['grade']=grade
    if not reasons:result['reason_codes']=['ALL_RULES_MET_WATCH_ONLY']
    return result



def classify_setup(setup):
    """Split observational but structurally usable setups from rejected ideas.

    A complete idea is still WATCH_ONLY: never permits automatic orders.
    """
    reasons=set(setup.get('reason_codes') or [])
    required=('entry','sl','tp1','rr_gross','rr_net_est')
    complete=all(setup.get(field) not in (None,'') for field in required)
    if complete:
        try:
            entry,sl,tp=map(float,[setup['entry'],setup['sl'],setup['tp1']])
            if not (entry>0 and sl>0 and tp>0 and
                (sl<entry<tp if setup['side']=='LONG' else tp<entry<sl)):
                complete=False
        except (TypeError,ValueError,KeyError):complete=False
    blocking={'STRUCTURE_LIMITS_REWARD','RR_OR_TP_INVALID',
              'TRANSACTION_COST_TOO_HIGH','INSUFFICIENT_24H_LIQUIDITY',
              'SPREAD_TOO_WIDE','SESSION_NO_TRADES_IN_LAST_BAR',
              'ADVERSE_FUNDING','STAGE2_INCOMPLETE','DATA_QUALITY_REJECTED',
              'FINAL_CANDLE_NOT_FRESH','OI_NOT_FRESH','FUNDING_NOT_FRESH'}
    rejection=[]
    if not complete:rejection.append('LEVELS_INCOMPLETE')
    rejection.extend(sorted(reasons & blocking))
    if complete and setup.get('rr_net_est') is not None and float(setup['rr_net_est']) < 1:
        rejection.append('NET_RR_BELOW_ONE')
    setup['status']='REJECTED' if rejection else 'WATCH_ONLY'
    setup['rejection_codes']=rejection
    setup['levels_complete']=complete
    setup['trade_allowed']=False
    return setup



# V6.50 MARKET/LIMIT bridge: not a replication of V6.17 statistical gate.
def confirmed_pivots(rows, radius=2):
    if not isinstance(rows,list) or len(rows)<radius*2+12:
        return [],[]
    highs,lows=[],[]
    for i in range(radius,len(rows)-radius):
        group=rows[i-radius:i+radius+1]
        h=safe_float(rows[i][2]);l=safe_float(rows[i][3])
        if h is not None and all(safe_float(x[2]) is not None for x in group):
            if h==max(float(x[2]) for x in group): highs.append((i,h))
        if l is not None and all(safe_float(x[3]) is not None for x in group):
            if l==min(float(x[3]) for x in group): lows.append((i,l))
    return highs,lows


def limit_pullback_candidate(coin,feature,clock_ok=True,ttl_minutes=180):
    if feature.get('watch_direction') not in ('LONG_WATCH','SHORT_WATCH'): return None
    side='LONG' if feature['watch_direction']=='LONG_WATCH' else 'SHORT'
    base=estimate_setup(coin,feature,clock_ok=clock_ok)
    if base is None:return None
    out=dict(base)
    critical={'SERVER_CLOCK_UNVERIFIED','DATA_QUALITY_REJECTED','STAGE2_INCOMPLETE',
              'TICKER_NOT_TIMESTAMPED','FINAL_CANDLE_NOT_FRESH','OI_NOT_FRESH',
              'FUNDING_NOT_FRESH','PRICE_OR_ATR_INVALID'}
    out['reason_codes']=[x for x in base.get('reason_codes',[]) if x in critical]
    out.update(entry_type='LIMIT',strategy_family='CONFIRMED_PIVOT_PULLBACK',
        entry=None,sl=None,tp1=None,rr_gross=None,rr_net_est=None,
        risk_pct=None,cost_in_r=None,levels_complete=False,trade_allowed=False,
        filled=False,ttl_minutes=ttl_minutes,expiry_at_utc=None,
        invalidated_if=None,fill_policy='NOT_OBSERVED_TOUCH_NOT_FILL',
        status='REJECTED',grade='D')
    reasons=out['reason_codes']
    if reasons:return classify_setup(out)
    bars=coin.get('klines',{}).get('15m',{}).get('data') or []
    bars5=coin.get('klines',{}).get('5m',{}).get('data') or []
    decision=safe_ms((coin.get('ticker_observed') or {}).get('received_server_est_ms'))
    if decision is None or len(bars)<35 or len(bars5)<22:
        reasons.append('LIMIT_BARS_MISSING');return classify_setup(out)
    if any(safe_ms(row[6]) is None or safe_ms(row[6])>decision for row in bars[-35:]):
        reasons.append('LIMIT_OPEN_CANDLE');return classify_setup(out)
    price=safe_float(coin.get('ticker',{}).get('lastPrice'))
    atr5=atr_from_rows(bars5);atr15=atr_from_rows(bars)
    if not price or not atr5 or not atr15:
        reasons.append('LIMIT_ATR_MISSING');return classify_setup(out)
    precision=coin['meta'].get('pricePrecision')
    dec=int(precision) if isinstance(precision,int) and 0<=precision<=10 else min(10,len(str(price).split('.')[-1]))
    hs,ls=confirmed_pivots(bars)
    if side=='LONG':
        levels=[v for _,v in ls if 0<price-v<=2*atr15]
        if not levels:reasons.append('LIMIT_NO_SUPPORT');return classify_setup(out)
        entry=price_round(max(levels),dec,'DOWN')
        stop=price_round(min(v for _,v in ls if v<=max(levels))-.25*atr15,dec,'DOWN')
    else:
        levels=[v for _,v in hs if 0<v-price<=2*atr15]
        if not levels:reasons.append('LIMIT_NO_RESISTANCE');return classify_setup(out)
        entry=price_round(min(levels),dec,'UP')
        stop=price_round(max(v for _,v in hs if v>=min(levels))+.25*atr15,dec,'UP')
    risk=entry-stop if side=='LONG' else stop-entry
    if entry<=0 or risk<=0 or not .12<=100*risk/entry<=6:
        reasons.append('LIMIT_BAD_SL');return classify_setup(out)
    target,sr,tf,_=structural_tp1(coin,side,entry,risk,atr5,dec)
    if sr is None or tf is None:
        reasons.append('LIMIT_NO_CONFIRMED_TP');return classify_setup(out)
    tp=price_round(target,dec,'DOWN' if side=='LONG' else 'UP')
    reward=tp-entry if side=='LONG' else entry-tp
    correct=(stop<entry<price and tp>entry) if side=='LONG' else (price<entry<stop and tp<entry)
    if not correct or reward<2*10**(-dec):
        reasons.append('LIMIT_PRICE_ORDER_INVALID');return classify_setup(out)
    if reward/risk<1.35:reasons.append('STRUCTURE_LIMITS_REWARD')
    spread=orderbook_spread_pct(coin)
    maker=safe_float(coin['meta'].get('makerFeeRate'))
    taker=safe_float(coin['meta'].get('takerFeeRate'))
    if maker is None or taker is None or not 0<=maker<.01 or not 0<=taker<.01:
        maker,taker=.0008,.0012
        reasons.append('LIMIT_FEE_ESTIMATED')
    cost=(maker+taker)*100+(spread or 0)
    risk_pct=100*risk/entry
    out.update(entry=price_text(entry,dec),sl=price_text(stop,dec),
       tp1=price_text(tp,dec),rr_gross=round(reward/risk,3),
       rr_net_est=round(reward/risk-cost/risk_pct,3),risk_pct=round(risk_pct,3),
       spread_pct=round(spread,5) if spread is not None else None,
       fees_roundtrip_pct=round((maker+taker)*100,5),
       cost_in_r=round(cost/risk_pct,3),
       tp1_basis='SR_'+tf,
       tp1_structural_level=price_text(price_round(sr,dec),dec),
       tp1_room_r=round(reward/risk,3),
       expiry_at_utc=utc(decision+ttl_minutes*60000),
       invalidated_if=('cancel on 15m close below support/SL before fill'
           if side=='LONG' else 'cancel on 15m close above resistance/SL before fill'))
    if spread is None:reasons.append('SPREAD_NOT_VERIFIED')
    if out['cost_in_r']>.25:reasons.append('TRANSACTION_COST_TOO_HIGH')
    min_vol=100000 if coin['market']=='TRADFI' else 500000
    if (feature.get('quote_volume_24h_usdt') or 0)<min_vol:
        reasons.append('INSUFFICIENT_24H_LIQUIDITY')
    out['grade']='C' if (out.get('score') or 0)>=50 and out['rr_net_est']>=1 and not set(reasons).intersection(
        {'STRUCTURE_LIMITS_REWARD','TRANSACTION_COST_TOO_HIGH','INSUFFICIENT_24H_LIQUIDITY'}) else 'D'
    return classify_setup(out)



def v650_fvg_limit(coin, feature, ttl_minutes=180):
    """Independent 15m fair-value-gap pullback candidate. Not V6.17 backtest."""
    base=limit_pullback_candidate(coin,feature,ttl_minutes=ttl_minutes)
    if base is None:return None
    base=dict(base)
    base.update(strategy_family='PULLBACK_FVG_V650',status='REJECTED',
                entry=None,sl=None,tp1=None,rr_net_est=None,rr_gross=None,
                cost_in_r=None,levels_complete=False,trade_allowed=False,filled=False)
    bars=(coin.get('klines') or {}).get('15m',{}).get('data') or []
    decision=safe_ms((coin.get('ticker_observed') or {}).get('received_server_est_ms'))
    price=safe_float(coin.get('ticker',{}).get('lastPrice'))
    atr=atr_from_rows(bars)
    if len(bars)<35 or decision is None or not price or not atr:return base
    if any(safe_ms(b[6]) is None or safe_ms(b[6])>decision for b in bars[-35:]):
        base['reason_codes']=['LIMIT_LOOKAHEAD_OR_OPEN_CANDLE'];return base
    side='LONG' if feature.get('watch_direction')=='LONG_WATCH' else 'SHORT'
    direction=1 if side=='LONG' else -1
    candidates=[]
    for i in range(max(2,len(bars)-32),len(bars)):
        left=bars[i-2];right=bars[i]
        bottom=safe_float(left[2] if side=='LONG' else right[2])
        top=safe_float(right[3] if side=='LONG' else left[3])
        if bottom is None or top is None or top-bottom < .10*atr:continue
        # Only unmitigated, confirmed three-candle gaps, below buy or above sell.
        if side=='LONG' and any(float(b[3])<=bottom for b in bars[i+1:]):continue
        if side=='SHORT' and any(float(b[2])>=top for b in bars[i+1:]):continue
        entry=top if side=='LONG' else bottom
        if not 0<direction*(price-entry)<2*atr:continue
        candidates.append((i,entry,bottom,top))
    if not candidates:
        base['reason_codes']=['FVG_UNCONFIRMED_OR_FILLED'];return base
    i,entry,bottom,top=candidates[-1]
    sl=(min(float(b[3]) for b in bars[max(0,i-8):i+1])-.20*atr if side=='LONG'
        else max(float(b[2]) for b in bars[max(0,i-8):i+1])+.20*atr)
    pr=coin.get('meta',{}).get('pricePrecision')
    decimals=int(pr) if isinstance(pr,int) and 0<=pr<=10 else min(10,len(str(coin['ticker'].get('lastPrice')).split('.')[-1]))
    entry=price_round(entry,decimals,'DOWN' if side=='LONG' else 'UP')
    sl=price_round(sl,decimals,'DOWN' if side=='LONG' else 'UP')
    risk=direction*(entry-sl)
    if risk<=0 or not .12<=100*risk/entry<=6:
        base['reason_codes']=['FVG_SL_OUTSIDE_RISK'];return base
    atr5=atr_from_rows((coin.get('klines') or {}).get('5m',{}).get('data') or [])
    if not atr5:return base
    target,sr,tf,_=structural_tp1(coin,side,entry,risk,atr5,decimals)
    if sr is None or tf is None:
        base['reason_codes']=['LIMIT_TP_NOT_CONFIRMED'];return base
    tp=price_round(target,decimals,'DOWN' if side=='LONG' else 'UP')
    reward=direction*(tp-entry)
    spread=orderbook_spread_pct(coin)
    maker=safe_float(coin.get('meta',{}).get('makerFeeRate'))
    taker=safe_float(coin.get('meta',{}).get('takerFeeRate'))
    if spread is None or maker is None or taker is None or maker<0 or taker<0:
        base['reason_codes']=['FVG_COSTS_UNVERIFIED'];return base
    cost_r=((maker+taker)*100+spread)/(100*risk/entry)
    rrnet=reward/risk-cost_r
    codes=[]
    if reward<=0 or reward/risk<1.35:codes.append('STRUCTURE_LIMITS_REWARD')
    if rrnet<1:codes.append('NET_RR_BELOW_ONE')
    if cost_r>.25:codes.append('TRANSACTION_COST_TOO_HIGH')
    if not feature.get('comparable'):codes.append('OI_PRICE_DIRECTION_NOT_CONFIRMED')
    base.update(entry=price_text(entry,decimals),sl=price_text(sl,decimals),
        tp1=price_text(tp,decimals),rr_gross=round(reward/risk,3),
        rr_net_est=round(rrnet,3),risk_pct=round(100*risk/entry,3),
        cost_in_r=round(cost_r,3),spread_pct=round(spread,5),
        fees_roundtrip_pct=round((maker+taker)*100,5),
        tp1_basis='SR_'+tf,ttl_minutes=ttl_minutes,
        expiry_at_utc=utc(decision+ttl_minutes*60000),
        invalidated_if='Cancel on 15m close through FVG or SL before fill',
        fill_policy='TOUCH_NOT_FILL; REQUIRE_TRADE_THROUGH_2BPS',
        reason_codes=codes or ['ALL_RULES_MET_WATCH_ONLY'],
        grade='C' if rrnet>=1 and cost_r<=.25 else 'D')
    return classify_setup(base)

def v650_cont_sweep_limit(coin, feature, ttl_minutes=180):
    """Conditional CONT_SWEEP pattern; no historical approval."""
    base=limit_pullback_candidate(coin,feature,ttl_minutes=ttl_minutes)
    if base is None:return None
    base=dict(base)
    base.update(strategy_family='CONT_SWEEP_V650',status='REJECTED',
                entry=None,sl=None,tp1=None,rr_net_est=None,rr_gross=None,
                cost_in_r=None,levels_complete=False,trade_allowed=False,filled=False)
    bars=(coin.get('klines') or {}).get('15m',{}).get('data') or []
    decision=safe_ms((coin.get('ticker_observed') or {}).get('received_server_est_ms'))
    price=safe_float(coin.get('ticker',{}).get('lastPrice'))
    atr=atr_from_rows(bars)
    if len(bars)<35 or decision is None or not price or not atr:return base
    if any(safe_ms(b[6]) is None or safe_ms(b[6])>decision for b in bars[-35:]):
        base['reason_codes']=['LIMIT_LOOKAHEAD_OR_OPEN_CANDLE'];return base
    side='LONG' if feature.get('watch_direction')=='LONG_WATCH' else 'SHORT'
    direction=1 if side=='LONG' else -1
    # Confirmed 1h pivot; require 15m breakout acceptance for two closes.
    hour=(coin.get('klines') or {}).get('1h',{}).get('data') or []
    if len(hour)<30 or any(safe_ms(b[6])>decision for b in hour[-30:]):
        base['reason_codes']=['CONT_1H_DATA_INVALID'];return base
    hi,lo=confirmed_pivots(hour)
    levels=[v for _,v in (hi if side=='LONG' else lo)]
    candidates=[]
    closes=[safe_float(b[4]) for b in bars[-12:]]
    if None in closes:
        base['reason_codes']=['CONT_CLOSE_DATA_INVALID'];return base
    for level in levels:
        if not 0<direction*(price-level)<2*atr:continue
        # Last two confirmed closes accept the breakout; earlier close was on wrong side.
        for j in range(2,len(closes)-2):
            if direction*(closes[j-1]-level)<=0 and all(
                    direction*(v-level)>0 for v in closes[j:j+2]) and all(
                    direction*(v-level)>0 for v in closes[j+2:]):
                i=len(bars)-12+j
                candidates.append((i,level,level,level))
                break
    if not candidates:
        base['reason_codes']=['CONT_SWEEP_PATTERN_NOT_CONFIRMED'];return base
    i,entry,bottom,top=candidates[-1]
    sl=(min(float(b[3]) for b in bars[max(0,i-8):i+1])-.20*atr if side=='LONG'
        else max(float(b[2]) for b in bars[max(0,i-8):i+1])+.20*atr)
    pr=coin.get('meta',{}).get('pricePrecision')
    decimals=int(pr) if isinstance(pr,int) and 0<=pr<=10 else min(10,len(str(coin['ticker'].get('lastPrice')).split('.')[-1]))
    entry=price_round(entry,decimals,'DOWN' if side=='LONG' else 'UP')
    sl=price_round(sl,decimals,'DOWN' if side=='LONG' else 'UP')
    risk=direction*(entry-sl)
    if risk<=0 or not .12<=100*risk/entry<=6:
        base['reason_codes']=['CONT_SWEEP_SL_OUTSIDE_RISK'];return base
    atr5=atr_from_rows((coin.get('klines') or {}).get('5m',{}).get('data') or [])
    if not atr5:return base
    target,sr,tf,_=structural_tp1(coin,side,entry,risk,atr5,decimals)
    if sr is None or tf is None:
        base['reason_codes']=['LIMIT_TP_NOT_CONFIRMED'];return base
    tp=price_round(target,decimals,'DOWN' if side=='LONG' else 'UP')
    reward=direction*(tp-entry)
    spread=orderbook_spread_pct(coin)
    maker=safe_float(coin.get('meta',{}).get('makerFeeRate'))
    taker=safe_float(coin.get('meta',{}).get('takerFeeRate'))
    if spread is None or maker is None or taker is None or maker<0 or taker<0:
        base['reason_codes']=['CONT_SWEEP_COSTS_UNVERIFIED'];return base
    cost_r=((maker+taker)*100+spread)/(100*risk/entry)
    rrnet=reward/risk-cost_r
    codes=[]
    if reward<=0 or reward/risk<1.35:codes.append('STRUCTURE_LIMITS_REWARD')
    if rrnet<1:codes.append('NET_RR_BELOW_ONE')
    if cost_r>.25:codes.append('TRANSACTION_COST_TOO_HIGH')
    if not feature.get('comparable'):codes.append('OI_PRICE_DIRECTION_NOT_CONFIRMED')
    base.update(entry=price_text(entry,decimals),sl=price_text(sl,decimals),
        tp1=price_text(tp,decimals),rr_gross=round(reward/risk,3),
        rr_net_est=round(rrnet,3),risk_pct=round(100*risk/entry,3),
        cost_in_r=round(cost_r,3),spread_pct=round(spread,5),
        fees_roundtrip_pct=round((maker+taker)*100,5),
        tp1_basis='SR_'+tf,ttl_minutes=ttl_minutes,
        expiry_at_utc=utc(decision+ttl_minutes*60000),
        invalidated_if='Cancel if 15m close loses accepted breakout level or SL before fill',
        fill_policy='TOUCH_NOT_FILL; REQUIRE_TRADE_THROUGH_2BPS',
        reason_codes=codes or ['ALL_RULES_MET_WATCH_ONLY'],
        grade='C' if rrnet>=1 and cost_r<=.25 else 'D')
    return classify_setup(base)

def v650_rev_mss_limit(coin, feature, ttl_minutes=180):
    """Conditional REV_MSS pattern; no historical approval."""
    base=limit_pullback_candidate(coin,feature,ttl_minutes=ttl_minutes)
    if base is None:return None
    base=dict(base)
    base.update(strategy_family='REV_MSS_V650',status='REJECTED',
                entry=None,sl=None,tp1=None,rr_net_est=None,rr_gross=None,
                cost_in_r=None,levels_complete=False,trade_allowed=False,filled=False)
    bars=(coin.get('klines') or {}).get('15m',{}).get('data') or []
    decision=safe_ms((coin.get('ticker_observed') or {}).get('received_server_est_ms'))
    price=safe_float(coin.get('ticker',{}).get('lastPrice'))
    atr=atr_from_rows(bars)
    if len(bars)<35 or decision is None or not price or not atr:return base
    if any(safe_ms(b[6]) is None or safe_ms(b[6])>decision for b in bars[-35:]):
        base['reason_codes']=['LIMIT_LOOKAHEAD_OR_OPEN_CANDLE'];return base
    side='LONG' if feature.get('watch_direction')=='LONG_WATCH' else 'SHORT'
    direction=1 if side=='LONG' else -1
    # 15m sweep of 1h pivot, reclaim, then 5m closed-candle MSS.
    hour=(coin.get('klines') or {}).get('1h',{}).get('data') or []
    five=(coin.get('klines') or {}).get('5m',{}).get('data') or []
    if len(hour)<30 or len(five)<30:
        base['reason_codes']=['REV_HISTORY_INSUFFICIENT'];return base
    if any(safe_ms(b[6])>decision for rows in (hour,five) for b in rows[-30:]):
        base['reason_codes']=['REV_OPEN_CANDLE'];return base
    hi,lo=confirmed_pivots(hour)
    levels=[v for _,v in (lo if side=='LONG' else hi)]
    candidates=[]
    for i in range(max(8,len(bars)-12),len(bars)-2):
        high=safe_float(bars[i][2]);low=safe_float(bars[i][3]);close=safe_float(bars[i][4])
        if None in (high,low,close):continue
        prev=bars[i-6:i]
        swing=(max(safe_float(b[2]) for b in prev) if side=='LONG'
               else min(safe_float(b[3]) for b in prev))
        # Closed 5m break of the pre-sweep opposite structure, not only wick reclaim.
        f5=safe_float(five[-1][4])
        if f5 is None or direction*(f5-swing)<=0:continue
        for level in levels:
            reclaimed=(low<level<close) if side=='LONG' else (high>level>close)
            if not reclaimed:continue
            if not 0<direction*(price-level)<2*atr:continue
            candidates.append((i,level,level,level))
    if not candidates:
        base['reason_codes']=['REV_MSS_PATTERN_NOT_CONFIRMED'];return base
    i,entry,bottom,top=candidates[-1]
    sl=(min(float(b[3]) for b in bars[max(0,i-8):i+1])-.20*atr if side=='LONG'
        else max(float(b[2]) for b in bars[max(0,i-8):i+1])+.20*atr)
    pr=coin.get('meta',{}).get('pricePrecision')
    decimals=int(pr) if isinstance(pr,int) and 0<=pr<=10 else min(10,len(str(coin['ticker'].get('lastPrice')).split('.')[-1]))
    entry=price_round(entry,decimals,'DOWN' if side=='LONG' else 'UP')
    sl=price_round(sl,decimals,'DOWN' if side=='LONG' else 'UP')
    risk=direction*(entry-sl)
    if risk<=0 or not .12<=100*risk/entry<=6:
        base['reason_codes']=['REV_MSS_SL_OUTSIDE_RISK'];return base
    atr5=atr_from_rows((coin.get('klines') or {}).get('5m',{}).get('data') or [])
    if not atr5:return base
    target,sr,tf,_=structural_tp1(coin,side,entry,risk,atr5,decimals)
    if sr is None or tf is None:
        base['reason_codes']=['LIMIT_TP_NOT_CONFIRMED'];return base
    tp=price_round(target,decimals,'DOWN' if side=='LONG' else 'UP')
    reward=direction*(tp-entry)
    spread=orderbook_spread_pct(coin)
    maker=safe_float(coin.get('meta',{}).get('makerFeeRate'))
    taker=safe_float(coin.get('meta',{}).get('takerFeeRate'))
    if spread is None or maker is None or taker is None or maker<0 or taker<0:
        base['reason_codes']=['REV_MSS_COSTS_UNVERIFIED'];return base
    cost_r=((maker+taker)*100+spread)/(100*risk/entry)
    rrnet=reward/risk-cost_r
    codes=[]
    if reward<=0 or reward/risk<1.35:codes.append('STRUCTURE_LIMITS_REWARD')
    if rrnet<1:codes.append('NET_RR_BELOW_ONE')
    if cost_r>.25:codes.append('TRANSACTION_COST_TOO_HIGH')
    if not feature.get('comparable'):codes.append('OI_PRICE_DIRECTION_NOT_CONFIRMED')
    base.update(entry=price_text(entry,decimals),sl=price_text(sl,decimals),
        tp1=price_text(tp,decimals),rr_gross=round(reward/risk,3),
        rr_net_est=round(rrnet,3),risk_pct=round(100*risk/entry,3),
        cost_in_r=round(cost_r,3),spread_pct=round(spread,5),
        fees_roundtrip_pct=round((maker+taker)*100,5),
        tp1_basis='SR_'+tf,ttl_minutes=ttl_minutes,
        expiry_at_utc=utc(decision+ttl_minutes*60000),
        invalidated_if='Cancel if 15m sweep reclaim or 5m MSS is invalidated before fill',
        fill_policy='TOUCH_NOT_FILL; REQUIRE_TRADE_THROUGH_2BPS',
        reason_codes=codes or ['ALL_RULES_MET_WATCH_ONLY'],
        grade='C' if rrnet>=1 and cost_r<=.25 else 'D')
    return classify_setup(base)

def collect(args):
    root=Path(args.out).expanduser();root.mkdir(parents=True,exist_ok=True)
    archive_dir=root/'weex_data'
    # Prevent stale instrument files or summaries from the previous run entering the new ZIP.
    if archive_dir.exists():
        shutil.rmtree(archive_dir)
    (archive_dir/'coins').mkdir(parents=True,exist_ok=True)
    client=Weex(delay=args.delay, timeout=args.timeout, retries=args.retries)
    state_path=Path(getattr(args,'state_path',None) or (root/'weex_v650_oi_history_state.json')).expanduser()
    state_path.parent.mkdir(parents=True,exist_ok=True)
    # On same output directory, recover last archive if persistent state vanished.
    # Colab's ephemeral disk is NOT persistent; notebook mounts Drive separately.
    previous=args.resume_from
    if previous is None and not state_path.exists():
        existing=sorted(root.glob('weex_v64*.zip'),key=lambda p:p.stat().st_mtime,reverse=True)
        if existing:previous=str(existing[0])
    resume_status=restore_oi_state(previous, state_path)
    if Path('/content').exists() and '/content/drive/' not in str(state_path):
        print('WARNING: OI state on Colab ephemeral disk. Use --state-path on mounted Drive or --resume-from previous ZIP.')
    start_ms=now_ms()
    calibration_start=client.calibrate()
    # Fetch markets separately. The WEEX contractType API enum intentionally
    # distinguishes crypto PERPETUAL from TRADIFI_PERPETUAL.
    crypto_exchange, crypto_exchange_stamp = client.request(
        '/capi/v3/market/exchangeInfo', {'contractType':'PERPETUAL'})
    tradfi_exchange = {'symbols': []}
    tradfi_discovery_error = None
    if args.tradfi:
        try:
            tradfi_exchange, tradfi_exchange_stamp = client.request(
                '/capi/v3/market/exchangeInfo', {'contractType':'TRADIFI_PERPETUAL'})
            if not isinstance(tradfi_exchange,dict) or not isinstance(tradfi_exchange.get('symbols'),list):
                raise ValueError('Unexpected TradFi exchangeInfo response: missing symbols[]')
        except Exception as exc:
            tradfi_discovery_error=str(exc)
            # No fabricated TradFi universe; preserve warning in metadata.
            print('WARNING: TradFi discovery failed:', tradfi_discovery_error)
    tickers,tick_meta=client.request('/capi/v3/market/ticker/24hr')
    crypto_picked=select_symbols(crypto_exchange,tickers,args.top,contract_type='PERPETUAL',volume_required=True)
    tradfi_picked=select_symbols(tradfi_exchange,tickers,args.tradfi_top,contract_type='TRADIFI_PERPETUAL',volume_required=False) if args.tradfi else []
    if not crypto_picked and not tradfi_picked:
        raise RuntimeError('No eligible WEEX crypto/TradFi perpetual instruments discovered')
    picked=[('CRYPTO',sym,meta,t) for sym,meta,t in crypto_picked]
    preflight_cache={}
    # Preflight TradFi Kline eligibility. Replace HTTP-400 symbols with the next
    # highest-volume eligible instrument when an alternative is available.
    if args.tradfi and tradfi_picked:
        all_tradfi=select_symbols(tradfi_exchange,tickers,0,contract_type='TRADIFI_PERPETUAL',volume_required=False)
        validated=[]; rejected=[]; preflight_cache={}
        for sym,meta,t in all_tradfi:
            if len(validated)>=args.tradfi_top: break
            try:
                raw,stamp=client.request('/capi/v3/market/klines',{'symbol':sym,'interval':'5m','limit':CANDLE_LIMITS['5m']})
                arr,qc=clean_klines(raw,'5m',stamp['received_server_est_ms'])
                if len(arr)<22 or qc.get('stale') or qc.get('gaps'):
                    raise ValueError('TradFi 5m missing, stale or gapped')
                preflight_cache[sym]={'data':arr,'quality':qc,'observed':stamp,
                                      'source':'WEEX_LAST_TRADE_FUTURES','only_closed':True}
                validated.append((sym,meta,t))
            except Exception as exc:
                rejected.append({'symbol':sym,'error':str(exc)})
        tradfi_picked=validated
        picked=[('CRYPTO',sym,meta,t) for sym,meta,t in crypto_picked]
        print(f'TradFi preflight: {len(validated)} selected, {len(rejected)} rejected')
    else:
        rejected=[]
    picked += [('TRADFI',sym,meta,t) for sym,meta,t in tradfi_picked]
    print(f'Selected: crypto {len(crypto_picked)}/{args.top}, TradFi {len(tradfi_picked)}/{args.tradfi_top}'
          f' | total {len(picked)} | snapshot = per-endpoint receive time')
    if args.tradfi and tradfi_discovery_error is None and not tradfi_picked:
        print('WARNING: TradFi exchangeInfo returned zero eligible USDT contracts; this is not a verified TradFi scan')
    coins=[]
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures={pool.submit(collect_symbol,client,s,meta,t,tick_meta,DEFAULT_TFS,
                             fetch_depth=args.depth,market_type=market,
                             preflight_bar=preflight_cache.get(s) if market=='TRADFI' else None): (market,s)
                 for market,s,meta,t in picked}
        for idx,future in enumerate(as_completed(futures),1):
            market,sym=futures[future]
            try:
                c=future.result();coins.append(c)
                dst=archive_dir/'coins'/market.lower()
                dst.mkdir(parents=True,exist_ok=True)
                atomic_json(dst/f'{sym}.json',c)
                if idx%25==0 or idx==len(futures): print(f'Collected {idx}/{len(futures)}')
            except Exception as exc:
                with client.lock:
                    client.errors.append({'symbol':sym,'market':market,'error':str(exc)})
    # One bulk ticker refresh after the full scan ensures the prices used in
    # rankings/watchlists are not from the start of a multi-minute download.
    final_tickers,final_ticker_stamp,refresh_error=None,None,None
    try:
        final_tickers,final_ticker_stamp=client.request('/capi/v3/market/ticker/24hr')
        if not isinstance(final_tickers,list):
            raise ValueError('Final ticker response must be an array')
    except Exception as exc:
        refresh_error=str(exc)
    final_map={v['symbol']:v for v in (final_tickers or []) if isinstance(v,dict) and v.get('symbol')}
    for c in coins:
        c['selection_ticker']=c['ticker']
        latest=final_map.get(c['symbol'])
        ok=bool(latest and safe_float(latest.get('lastPrice')) is not None and
                safe_ms(latest.get('closeTime')) is not None and final_ticker_stamp)
        c['ticker_refresh_ok']=ok
        if ok:
            c['ticker']=latest
            c['ticker_observed']=final_ticker_stamp
        else:
            c['errors']['ticker_refresh']=refresh_error or 'Symbol missing/invalid final ticker'
        # Rewrite the instrument JSON with the same final ticker as its CSV row.
        atomic_json(archive_dir/'coins'/c['market'].lower()/f"{c['symbol']}.json",c)
    # Stage 2: evaluate prelim candidates and refresh their 5m/15m candles
    # close to final ticker snapshot. Fail closed if refreshing any required bar fails.
    preliminary_all=[c for c in coins if basic_features(c)['watch_direction']!='NONE']
    candidate_ranked=[]
    early_rejections=[]
    for c in preliminary_all:
        feature=basic_features(c)
        rank=preselection_rank(c,feature)
        pre_rr=preview_structural_rr(c,feature)
        rank.update(pre_rr)
        if pre_rr['pre_rr'] is not None and pre_rr['pre_rr'] < args.min_pre_rr and args.strict_pre_rr:
            rank['pre_rr_screen_pass']=False
            rank['pre_rr_rejection_reason']='STAGE1_CONFIRMED_STRUCTURE_ROOM_TOO_SMALL'
            early_rejections.append({**rank, 'status':'REJECTED_PRE_STAGE2',
                                     'rejection_codes':['STAGE1_CONFIRMED_STRUCTURE_ROOM_TOO_SMALL']})
        else:
            rank['pre_rr_screen_pass']=True
            rank['pre_rr_rejection_reason']=('LOW_PRELIMINARY_RR_NEEDS_FULL_STRUCTURE' if pre_rr['pre_rr'] is not None and pre_rr['pre_rr'] < args.min_pre_rr else '')
            candidate_ranked.append((rank,c))
    candidate_ranked.sort(key=lambda pair:(pair[0]['score'],pair[0]['quote_volume_24h']),reverse=True)
    preliminary=[c for _,c in candidate_ranked[:args.max_deep]]
    ranking_export=[]
    for idx,(rank,c) in enumerate(candidate_ranked,1):
        ranking_export.append({**rank,'rank':idx,'selected_for_stage2':idx<=args.max_deep})
    csv_write(archive_dir/'stage2_selection.csv',ranking_export+early_rejections,
              ['rank','market','symbol','side','score','rvol','quote_volume_24h',
               'estimated_stop_pct','pre_rr','pre_rr_source','pre_rr_screen_pass',
               'pre_rr_rejection_reason','score_components','selected_for_stage2'])
    csv_write(archive_dir/'prestage_rejections.csv',early_rejections,
              ['market','symbol','side','score','quote_volume_24h','pre_rr',
               'pre_rr_source','status','rejection_codes'])
    print(f'Stage 2: refreshing {len(preliminary)} of {len(preliminary_all)} preliminary candidates'
          f' | pre-RR rejected {len(early_rejections)}')
    def complete_candidate(c):
        update_klines(client,c,('5m','15m','1h'))
        update_klines(client,c,EXTRA_TFS)
        refresh_snapshot_inputs(client,c)
        fetch_history_for_candidates(client,c)
        # Compare candle timestamp to refreshed final ticker time in basic_features.
        if any('kline_'+tf in c['errors'] for tf in ('5m','15m','1h','4h','1d')):
            c['stage2_failed']=True
        return c
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures=[pool.submit(complete_candidate,c) for c in preliminary]
        for future in as_completed(futures):
            future.result()
    for c in preliminary:
        atomic_json(archive_dir/'coins'/c['market'].lower()/f"{c['symbol']}.json", c)
    # Final bulk ticker after stage-2, ensuring market-entry price is as recent
    # as the refreshed candles and OI/funding snapshots.
    price_refresh_status='OK'
    try:
        last_tickers,last_stamp=client.request('/capi/v3/market/ticker/24hr')
        if not isinstance(last_tickers,list): raise ValueError('final prices not an array')
        last_map={t['symbol']:t for t in last_tickers if isinstance(t,dict) and t.get('symbol')}
        for c in coins:
            latest=last_map.get(c['symbol'])
            if latest and (safe_float(latest.get('lastPrice')) or 0)>0 and safe_ms(latest.get('closeTime')):
                c['ticker']=latest; c['ticker_observed']=last_stamp; c['ticker_refresh_ok']=True
            else:
                c['ticker_refresh_ok']=False; c['errors']['ticker_refresh']='Missing final ticker'
    except Exception as exc:
        price_refresh_status=str(exc)
        for c in coins:
            c['ticker_refresh_ok']=False; c['errors']['ticker_refresh']=price_refresh_status
    # Store exactly what the features below use, not earlier per-symbol copies.
    for c in coins:
        atomic_json(archive_dir/'coins'/c['market'].lower()/f"{c['symbol']}.json",c)
    calibration_end=client.calibrate()
    # Disallow signals when clock calibration fails or drifts substantially.
    drift=abs(calibration_start.get('offset_ms',0)-calibration_end.get('offset_ms',0))
    quality_ok=(calibration_start.get('clock_quality')=='OK'
                and calibration_end.get('clock_quality')=='OK' and drift<=5000)
    features=[basic_features(c) for c in coins]
    early_rejection_keys={(p['market'],p['symbol']) for p in early_rejections}
    selected_keys={(c['market'],c['symbol']) for c in preliminary}
    for f in features:
        if f['watch_direction']!='NONE' and (f['market'],f['symbol']) not in selected_keys:
            f['watch_direction']='NONE';f['data_ok']=False
            flag=('PRE_RR_STAGE1_REJECTED' if (f['market'],f['symbol']) in early_rejection_keys
                  else 'NOT_REVALIDATED_STAGE2')
            f['warnings'].append(flag)
    hist=build_oi_history(state_path,coins)
    # Persist the carried-forward state INSIDE the exported ZIP for the next Colab session.
    if state_path.exists():
        (archive_dir/'oi_observations_state.json').write_bytes(state_path.read_bytes())
    for f in features:
        hist_key=f"{f['market']}|{f['symbol']}"
        if hist_key in hist:
            f.update(hist[hist_key])
            if not hist[hist_key]['comparable']:
                # Missing comparable OI is not a bad market data point. It caps A/B,
                # but must NOT remove otherwise valid C/D watchlist candidates.
                raw_change=hist[hist_key].get('oi_pct_change_since_previous')
                if raw_change is not None and abs(raw_change)>200:
                    f['data_ok']=False;f['watch_direction']='NONE'
                    f['warnings'].append('OI_CHANGE_IMPLAUSIBLE')
                else:
                    f['warnings'].append('OI_HISTORY_NOT_YET_COMPARABLE')
        if not quality_ok:
            f['data_ok']=False;f['watch_direction']='NONE'
            f['warnings'].append('SERVER_CLOCK_NOT_TRUSTED')
    features.sort(key=lambda x:(x.get('quote_volume_24h_usdt') or 0),reverse=True)
    watch=[f for f in features if f['watch_direction']!='NONE']
    fields=['market','symbol','price','quote_volume_24h_usdt','change_24h_raw','oi_contracts',
            'oi_source_age_ms','funding_forecast','funding_latest_settled','closed_5m',
            'closed_15m','closed_1h','ticker_window_close_age_ms','gap_5m','rvol_5m','trend_15m','trend_1h','data_ok',
            'watch_direction','oi_pct_change_since_previous','oi_price_pct_change',
            'oi_direction','oi_direction_data_ready','comparable','trade_allowed','warnings']
    csv_write(archive_dir/'ranking_market.csv',features,fields)
    csv_write(archive_dir/'watchlist.csv',watch,fields)
    for market in ('CRYPTO','TRADFI'):
        selected=[f for f in features if f['market']==market]
        watched=[f for f in watch if f['market']==market]
        csv_write(archive_dir/f'ranking_{market.lower()}.csv',selected,fields)
        csv_write(archive_dir/f'watchlist_{market.lower()}.csv',watched,fields)
    # Only stage2-complete/watch-valid symbols get computed indicative levels.
    coin_map={(c['market'],c['symbol']):c for c in coins}
    setups=[estimate_setup(coin_map[(f['market'],f['symbol'])],f,clock_ok=quality_ok)
            for f in watch if (f['market'],f['symbol']) in coin_map]
    evaluated=[classify_setup(x) for x in setups if x]
    evaluated += [x for x in (limit_pullback_candidate(
        coin_map[(f['market'],f['symbol'])], f,clock_ok=quality_ok,
        ttl_minutes=args.limit_ttl_minutes)
        for f in watch if (f['market'],f['symbol']) in coin_map) if x]
    evaluated += [x for x in (v650_fvg_limit(
        coin_map[(f['market'],f['symbol'])],f,ttl_minutes=args.limit_ttl_minutes)
        for f in watch if (f['market'],f['symbol']) in coin_map) if x]
    for family_fn in (v650_cont_sweep_limit,v650_rev_mss_limit):
        evaluated += [x for x in (family_fn(
            coin_map[(f['market'],f['symbol'])],f,ttl_minutes=args.limit_ttl_minutes)
            for f in watch if (f['market'],f['symbol']) in coin_map) if x]
    evaluated.sort(key=lambda x:({'A':0,'B':1,'C':2,'D':3}[x['grade']],-x['score']))
    # One primary entry per (market, symbol, side, mode); alternatives never inflate risk.
    primary_ids=set()
    alternates=[]
    duplicates={}
    for item in evaluated:
        if item.get('entry_type')!='LIMIT' or item.get('status')!='WATCH_ONLY':continue
        group=(item.get('market'),item.get('symbol'),item.get('side'),'LIMIT')
        duplicates.setdefault(group,[]).append(item)
    for group, options in duplicates.items():
        options.sort(key=lambda x:(
            -(safe_float(x.get('rr_net_est')) or -999),
            -(safe_float(x.get('score')) or 0),
            str(x.get('strategy_family') or '')))
        primary_ids.add(id(options[0]))
        for item in options[1:]:
            item['status']='ALTERNATIVE_WATCH_ONLY'
            item['trade_allowed']=False
            item['rejection_codes']=list(item.get('rejection_codes') or [])+['DUPLICATE_SAME_EXPOSURE']
            alternates.append(item)
    # Keep a single exposure group across MARKET and LIMIT modes.
    exposure={}
    for item in evaluated:
        if item.get('status') not in ('WATCH_ONLY','ALTERNATIVE_WATCH_ONLY'):continue
        key=(item.get('market'),item.get('symbol'),item.get('side'))
        exposure.setdefault(key,[]).append(item)
    exposure_report=[]
    for key, group in exposure.items():
        exposure_report.append({'market':key[0],'symbol':key[1],'side':key[2],
            'watch_variants':len(group),
            'modes':sorted(set(str(i.get('entry_type')) for i in group)),
            'strategies':sorted(set(str(i.get('strategy_family')) for i in group)),
            'risk_note':('ONE_SHARED_EXPOSURE_DO_NOT_STACK_MARKET_AND_LIMIT'
                if len(group)>1 else 'SINGLE_WATCH_VARIANT')})
    atomic_json(archive_dir/'exposure_groups.json',exposure_report)
    strategy_diagnostics={}
    for item in evaluated:
        family=item.get('strategy_family') or 'MARKET'
        diag=strategy_diagnostics.setdefault(family,{'total':0,'watch_primary':0,'watch_alternative':0,'rejected':0,'reasons':{}})
        diag['total']+=1
        status=item.get('status')
        if status=='WATCH_ONLY':diag['watch_primary']+=1
        elif status=='ALTERNATIVE_WATCH_ONLY':diag['watch_alternative']+=1
        else:diag['rejected']+=1
        for reason in item.get('rejection_codes') or []:
            diag['reasons'][reason]=diag['reasons'].get(reason,0)+1
    trigger_diagnostics=[]
    for item in evaluated:
        family=str(item.get('strategy_family') or '')
        if 'CONT_SWEEP' in family or 'REV_MSS' in family:
            trigger_diagnostics.append({'market':item.get('market'),
                'symbol':item.get('symbol'),'side':item.get('side'),
                'strategy':family,'status':item.get('status'),
                'trigger':item.get('trigger'),
                'reason_codes':item.get('reason_codes'),
                'rejection_codes':item.get('rejection_codes'),
                'levels_complete':item.get('levels_complete')})
    atomic_json(archive_dir/'trigger_diagnostics.json',trigger_diagnostics)
    atomic_json(archive_dir/'strategy_diagnostics.json',strategy_diagnostics)
    csv_write(archive_dir/'limit_alternatives.csv',alternates,[
        'market','symbol','side','strategy_family','entry_type','entry','sl','tp1',
        'rr_net_est','grade','status','rejection_codes','expiry_at_utc','invalidated_if'])
    setups=[x for x in evaluated if x['status']=='WATCH_ONLY']
    rejected_setups=[x for x in evaluated if x['status']=='REJECTED']
    setup_fields=['market','symbol','side','entry_type','entry','sl','tp1','rr_gross',
                  'rr_net_est','risk_pct','spread_pct','fees_roundtrip_pct','cost_in_r',
                  'oi_pct','oi_price_pct','oi_direction','oi_confirmed',
                  'tp1_basis','tp1_structural_level','tp1_room_r','score','grade','status','trade_allowed',
                  'observed_at_utc','reason_codes','score_breakdown','levels_complete','rejection_codes',
                  'strategy_family','ttl_minutes','expiry_at_utc','invalidated_if','filled','fill_policy']
    csv_write(archive_dir/'market_setups.csv',
              [p for p in setups if p['entry_type'].startswith('MARKET')],setup_fields)
    csv_write(archive_dir/'limit_setups.csv',
              [p for p in setups if p['entry_type']=='LIMIT'],setup_fields)
    csv_write(archive_dir/'rejected_market.csv',
              [p for p in rejected_setups if p['entry_type'].startswith('MARKET')],setup_fields)
    csv_write(archive_dir/'rejected_limit.csv',
              [p for p in rejected_setups if p['entry_type']=='LIMIT'],setup_fields)
    csv_write(archive_dir/'setups.csv',setups,setup_fields)
    csv_write(archive_dir/'rejected_setups.csv',rejected_setups,setup_fields)
    csv_write(archive_dir/'all_evaluated_setups.csv',evaluated,setup_fields)
    for market in ('CRYPTO','TRADFI'):
        csv_write(archive_dir/f'setups_{market.lower()}.csv',
                  [p for p in setups if p['market']==market],setup_fields)
        csv_write(archive_dir/f'rejected_setups_{market.lower()}.csv',
                  [p for p in rejected_setups if p['market']==market],setup_fields)
    oi_diagnostics=[]
    for f in watch:
        oi_diagnostics.append({'market':f.get('market'),'symbol':f.get('symbol'),
            'oi_direction':f.get('oi_direction'),
            'oi_direction_reason':f.get('oi_direction_reason'),
            'oi_observation_span_min':f.get('oi_observation_span_min'),
            'oi_price_pair_valid':f.get('oi_price_pair_valid'),
            'oi_direction_data_ready':f.get('oi_direction_data_ready'),
            'oi_pct_change_since_previous':f.get('oi_pct_change_since_previous'),
            'oi_price_pct_change':f.get('oi_price_pct_change')})
    atomic_json(archive_dir/'oi_diagnostics.json',oi_diagnostics)
    atomic_json(archive_dir/'setups.json',setups)
    atomic_json(archive_dir/'rejected_setups.json',rejected_setups)
    atomic_json(archive_dir/'prestage_rejections.json',early_rejections)
    # Audit near misses separately, never present them as executable setups.
    reason_tallies=Counter(code for item in rejected_setups for code in item.get('rejection_codes',[]))
    near_misses=sorted(rejected_setups,key=lambda item:(
        item.get('levels_complete') is True,
        safe_float(item.get('rr_net_est')) if safe_float(item.get('rr_net_est')) is not None else -999,
        safe_float(item.get('score')) or 0),reverse=True)
    csv_write(archive_dir/'near_misses.csv',near_misses,setup_fields)
    atomic_json(archive_dir/'rejection_analysis.json',{
        'total_rejected':len(rejected_setups),
        'prestage_rejected':len(early_rejections),
        'reason_counts':dict(reason_tallies),
        'near_misses_top_10':near_misses[:10],
        'note':'WATCH ONLY; rejected ideas are not market entries'})

    all_bad=[{'symbol':c['symbol'],'errors':c['errors']} for c in coins if c['errors']]
    meta={'version':VERSION,'started_at_utc':utc(start_ms),'finished_at_utc':utc(),
          'ticker_observed_at_utc':utc(tick_meta['received_server_est_ms']),
          'ticker_refresh_at_utc':utc(final_ticker_stamp['received_server_est_ms']) if final_ticker_stamp else None,
          'ticker_refresh_error':refresh_error,
          'oi_state_resume_status':resume_status,
          'oi_state_has_prior_points':bool(resume_status and ('loaded' in str(resume_status).lower() or 'existing' in str(resume_status).lower())),
          'oi_state_storage':('PERSISTENT_GOOGLE_DRIVE' if '/content/drive/' in str(state_path) else 'LOCAL_OR_EPHEMERAL'),
          'notes':'Stage1 5m/15m/1h+OI+premium; Stage2 TOP 30 after conservative RR screen + refreshed candles/OI/premium/depth; OI direction requires paired historical CLOSED bar prices (legacy OI only capped C); TP uses nearby confirmed swings, R:R below threshold D. Indicative only; NO execution approval.',
          'selected_symbols':len(picked),'collected_symbols':len(coins),
          'crypto_requested':args.top,'crypto_selected':len(crypto_picked),
          'tradfi_requested':args.tradfi_top if args.tradfi else 'DISABLED',
          'tradfi_discovered':len(tradfi_exchange.get('symbols',[])),
          'tradfi_selected':len(tradfi_picked), 'tradfi_preflight_rejected':rejected,
          'tradfi_discovery_error':tradfi_discovery_error,
          'tradfi_scan_complete':bool(args.tradfi and not tradfi_discovery_error and len(tradfi_picked)>0),
          'crypto_collected':sum(c['market']=='CRYPTO' for c in coins),
          'tradfi_collected':sum(c['market']=='TRADFI' for c in coins),
          'stage2_preliminary_count':len(preliminary_all), 'stage2_selected_count':len(preliminary),
          'stage2_pre_rr_rejected':len(early_rejections),'stage2_pre_rr_min':args.min_pre_rr,
          'stage2_selection_policy':'liquidity_rvol_trend_stop_risk_funding_and_provisional_pre_rr',
          'stage2_completed_count':sum(not c.get('stage2_failed',False) for c in preliminary),
          'watchlist_count':len(watch),'setups_count':len(setups),
          'evaluated_count':len(evaluated),'rejected_setups_count':len(rejected_setups),
          'setups_grade_counts':dict(Counter(p['grade'] for p in setups)),
          'market_setups_count':sum(p['entry_type'].startswith('MARKET') for p in setups),
          'limit_setups_count':sum(p['entry_type']=='LIMIT' for p in setups),
          'limit_ttl_minutes':args.limit_ttl_minutes,
          'legacy_v617_statistical_gate_integrated':False,
          'strategy_scope':'V645 MARKET plus diagnostic confirmed-pivot LIMIT (V617 historical gate not migrated)',
          'price_refresh_after_stage2_status':price_refresh_status,
          'oi_state_path':str(state_path),'trade_gate':'HARD_DISABLED',
          'clock_calibration_start':calibration_start,
          'clock_calibration_end':calibration_end,'clock_stable':quality_ok,
          'calls':client.call_count,'errors_count':len(client.errors),
          'quote_volume_currency':'USDT','oi_raw_units':'CONTRACTS',
          'oi_usdt_automatically_estimated':False,
          'price_type_klines':'LAST traded price WEEX futures',
          'funding_history_policy':'settled rates + first-observed time; cannot backtest unobserved publication',
          'stage1_intervals':list(DEFAULT_TFS), 'stage2_intervals':list(DEFAULT_TFS+EXTRA_TFS), 'top_requested':args.top,
          'market_filter':'CRYPTO_PERPETUAL_TOP_N_PLUS_TRADIFI_TOP_N' if args.tradfi else 'CRYPTO_PERPETUAL_TOP_N',
          'data_flags':{'rvol_only_closed':True,'historical_ttl_inferred':False,
                        'open_candles_used':False,'signals_not_generated':False,
                        'orders_never_executed':True,'grades_not_backtested':True,
                         'end_of_run_ticker_refresh':True,'ticker_close_time_is_not_last_trade':True,
                        'oi_history_embedded_in_zip':True,
                        'oi_direction_requires_paired_closed_prices':True,
                        'tp1_uses_confirmed_swings':True,
                        'setups_and_rejected_split':True,
                        'pre_rr_only_from_confirmed_closed_stage1_bars':True,
                        'pre_rr_is_advisory_unless_strict':not args.strict_pre_rr,
                        'rejection_reason_diagnostics':True}}
    atomic_json(archive_dir/'run_meta.json',meta)
    atomic_json(archive_dir/'qc.json',{'partial_failures':all_bad,'requests_errors':client.errors,
                                    'clock_quality_ok':quality_ok})
    archive_name=f"weex_v650_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.zip"
    archive_path=root/archive_name
    with zipfile.ZipFile(archive_path,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
        for f in sorted(archive_dir.rglob('*')):
            if f.is_file(): z.write(f,f.relative_to(root))
    print(json.dumps({'archive':str(archive_path),'collected':len(coins),
                      'crypto_collected':sum(c['market']=='CRYPTO' for c in coins),
                      'tradfi_collected':sum(c['market']=='TRADFI' for c in coins),
                      'tradfi_discovery_error':tradfi_discovery_error,
                      'watchlist':len(watch),'setups':len(setups),
                      'rejected_setups':len(rejected_setups),'pre_rr_rejected':len(early_rejections),
                      'grades':dict(Counter(p['grade'] for p in setups)),
                      'trade_gate':'HARD_DISABLED',
                      'clock_stable':quality_ok,'requests':client.call_count},ensure_ascii=False,indent=2))
    return archive_path


def run_self_test():
    t0=1_796_000_100_000
    t0=t0//300_000*300_000
    r=[t0,'10','12','9','11','5',t0+300_000-1,'55',4,'3','33']
    closed,q=clean_klines([r], '5m', t0+300_000)
    assert len(closed)==1 and q['closed']==1
    r2=r.copy();r2[6]=t0+300_000
    closed,q=clean_klines([r2], '5m', t0+300_000)
    assert len(closed)==1, 'boundary closeTime is valid'
    closed,q=clean_klines([r2], '5m', t0+300_000-1)
    assert len(closed)==0, 'open candle rejected'
    closed,q=clean_klines([r,r], '5m', t0+300_001)
    assert len(closed)==0 and q['duplicate_or_ambiguous']==2
    from tempfile import TemporaryDirectory
    with TemporaryDirectory() as d:
        def good(**more):
            props={'symbol':'BTCUSDT','side':'LONG','entry_type':'LIMIT','entry':100,
                   'stop_loss':98,'take_profit':104,'decision_at_ms':t0+300_000,
                   'feature_timestamps_ms':{'last_5m':t0+300_000},
                   'ttl_minutes':60}
            props.update(more)
            return record_signal_event(Path(d)/'events.jsonl',**props)
        e=good(); assert e['expiry_at_ms']-e['decision_at_ms']==3600_000
        assert not e['trade_gate'].startswith('ENABLED')
        for bad in [{'ttl_minutes':None},{'ttl_minutes':-1},
                    {'feature_timestamps_ms':{'x':t0+300_001}},
                    {'feature_timestamps_ms':{}},
                    {'stop_loss':110}]:
            try:good(**bad)
            except ValueError:pass
            else:raise AssertionError('invalid signal accepted '+str(bad))
    symbols=select_symbols({'symbols':[{'symbol':'BTCUSDT','contractType':'PERPETUAL',
                         'quoteAsset':'USDT','marginAsset':'USDT','underlyingType':'COIN'}]},
                         [{'symbol':'BTCUSDT','lastPrice':'70000','quoteVolume':'999'}],30)
    assert len(symbols)==1
    # Crypto TOP 150 and TOP N TradFi are independent universes. TradFi may be closed.
    crypto_test={'symbols':[{'symbol':f'C{i}USDT','contractType':'PERPETUAL',
        'quoteAsset':'USDT','marginAsset':'USDT'} for i in range(180)]}
    tradfi_test={'symbols':[{'symbol':f'T{i}USDT','contractType':'TRADIFI_PERPETUAL',
        'quoteAsset':'USDT','marginAsset':'USDT'} for i in range(27)]}
    tickers=[{'symbol':f'C{i}USDT','lastPrice':'1','quoteVolume':str(i+1)} for i in range(180)]
    tickers += [{'symbol':'T0USDT','lastPrice':'100','quoteVolume':'0'}]
    top150=select_symbols(crypto_test,tickers,150,contract_type='PERPETUAL')
    alltradfi=select_symbols(tradfi_test,tickers,0,contract_type='TRADIFI_PERPETUAL',volume_required=False)
    assert len(top150)==150 and top150[0][0]=='C179USDT'
    assert len(alltradfi)==27 and alltradfi[0][0]=='T0USDT'
    assert not any(c[0] in {s[0] for s in alltradfi} for c in top150)
    for ex in [{'side':'LONG','entry':'100','sl':'98','tp1':'103','rr_gross':1.5,'rr_net_est':1.2,'reason_codes':[], 'trade_allowed':False},
               {'side':'SHORT','entry':'100','sl':None,'tp1':'98','rr_gross':None,'rr_net_est':None,'reason_codes':[], 'trade_allowed':False},
               {'side':'SHORT','entry':'100','sl':'102','tp1':'99','rr_gross':.5,'rr_net_est':.2,'reason_codes':['STRUCTURE_LIMITS_REWARD'],'trade_allowed':False}]:
        item=classify_setup(dict(ex))
        assert not item['trade_allowed']
        if ex['sl'] is None or 'STRUCTURE_LIMITS_REWARD' in ex['reason_codes']:
            assert item['status']=='REJECTED'
        else:assert item['status']=='WATCH_ONLY'
    print('PASS: candle edge, open bar, duplicates, LIMIT TTL, lookahead, SL order, symbols, classification')


def main():
    parser=argparse.ArgumentParser(description=VERSION)
    parser.add_argument('--top',type=int,default=150,help='Top N crypto USDT perpetual by 24h volume; 0 means ALL crypto')
    parser.add_argument('--out',default='/content/weex_v650_results' if Path('/content').exists() else './weex_v650_results')
    parser.add_argument('--workers',type=int,default=4)
    parser.add_argument('--delay',type=float,default=0.18,help='min interval in seconds between WEEX requests')
    parser.add_argument('--timeout',type=int,default=20)
    parser.add_argument('--retries',type=int,default=3)
    parser.add_argument('--depth',action='store_true',help='include WEEX depth per symbol')
    parser.add_argument('--tradfi-top',type=int,default=50,help='Top N TradFi USDT perpetual by 24h volume; 0 means ALL')
    parser.add_argument('--no-tradfi',dest='tradfi',action='store_false',help='disable TradFi scan (enabled by default)')
    parser.set_defaults(tradfi=True)
    parser.add_argument('--resume-from',default=None,help='previous WEEX V6.37/V6.41/V6.50 ZIP (alternative OI restore)')
    parser.add_argument('--state-path',default=None,help='Persistent OI JSON path, e.g. mounted Google Drive')
    parser.add_argument('--max-deep',type=int,default=MAX_STAGE2,help='Max candidates to refresh in stage2')
    parser.add_argument('--min-pre-rr',type=float,default=.85,help='Stage1 minimum plausible RR when a nearby 15m/1h pivot is confirmed')
    parser.add_argument('--strict-pre-rr',action='store_true',help='Optional: reject from preliminary structural RR before full 4h/1d verification')
    parser.add_argument('--limit-ttl-minutes',type=int,default=180)
    parser.add_argument('--self-test',action='store_true')
    parser.add_argument('--download-ready',action='store_true',help='informational, notebook handles Colab download')
    args=parser.parse_args()
    if args.self_test:run_self_test();return
    if not 5<=args.limit_ttl_minutes<=1440 or args.top<0 or args.tradfi_top<0 or not .1<=args.min_pre_rr<=2.0 or not 1<=args.max_deep<=100 or not 1<=args.workers<=8 or args.delay<0.08 or args.timeout<5:
        parser.error('Invalid limits: top>=0, tradfi-top>=0, min-pre-rr 0.1..2.0, workers 1..8, delay>=0.08, timeout>=5')
    collect(args)


if __name__=='__main__':main()

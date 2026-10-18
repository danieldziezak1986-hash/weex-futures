#!/usr/bin/env python3
"""WEEX V6.37: standalone public-market-data collector + causal signal guard.

NO ORDER EXECUTION. No API key. Not the original V6.35 proprietary strategy engine.
Install: pip install requests
Usage: python weex_v637_collector.py --top 30 --download-ready
Or: python weex_v637_collector.py --self-test

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

VERSION = 'WEEX V6.37 SAFE COLLECTOR'
API_BASE = 'https://api-contract.weex.com'
TF = {'1m': 60_000, '5m': 300_000, '15m': 900_000, '1h': 3600_000,
      '4h': 14400_000, '1d': 86400_000}
DEFAULT_TFS = ('5m', '15m', '1h', '4h', '1d')
CANDLE_LIMITS = {'5m': 400, '15m': 260, '1h': 240, '4h': 180, '1d': 120}
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
            THREAD_LOCAL.session.headers.update({'User-Agent': 'WEEX-V637-safe-public-collector/1.0',
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


def select_symbols(exchange_data, tickers, top, crypto_only=True):
    if isinstance(exchange_data, dict):
        raw = exchange_data.get('symbols') or []
    else:
        raw = []
    lookup = {}
    for s in raw:
        if (isinstance(s, dict) and s.get('symbol') and s.get('contractType') == 'PERPETUAL'
            and s.get('quoteAsset') == 'USDT' and s.get('marginAsset') == 'USDT'
            and (not crypto_only or s.get('underlyingType') == 'COIN')):
            lookup[s['symbol']] = s
    ticklist = tickers if isinstance(tickers, list) else ([tickers] if isinstance(tickers, dict) else [])
    tick_by_symbol = {t.get('symbol'): t for t in ticklist if isinstance(t, dict) and t.get('symbol') in lookup}
    ranked = []
    for sym,t in tick_by_symbol.items():
        quote_vol = safe_float(t.get('quoteVolume'))
        price = safe_float(t.get('lastPrice'))
        if quote_vol is not None and quote_vol > 0 and price is not None and price > 0:
            ranked.append((sym, quote_vol))
    ranked.sort(key=lambda x: x[1], reverse=True)
    return [(sym, lookup[sym], tick_by_symbol[sym]) for sym,_ in ranked[:top if top > 0 else None]]


def unwrap_symbol(resp, symbol):
    if isinstance(resp, list):
        matches = [r for r in resp if isinstance(r,dict) and r.get('symbol') == symbol]
        return matches[0] if len(matches) == 1 else None
    if isinstance(resp,dict) and resp.get('symbol') == symbol:
        return resp
    return None


def collect_symbol(client, symbol, meta, ticker, ticker_meta, tfs, fetch_depth=False):
    result = {'symbol': symbol, 'meta': meta, 'ticker': ticker,
              'ticker_observed': ticker_meta, 'klines': {}, 'errors': {}, 'open_interest': None,
              'premium': None, 'funding_history': [], 'depth': None, 'feature_status': {}}
    for tf in tfs:
        try:
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
    out = {'symbol':sym, 'price':safe_float(t.get('lastPrice')),
           'quote_volume_24h_usdt':safe_float(t.get('quoteVolume')),
           'change_24h_raw':safe_float(t.get('priceChangePercent')),
           'oi_contracts':None,'oi_source_age_ms':None, 'funding_forecast':None,
           'funding_latest_settled':None,'closed_5m':0,'closed_15m':0,'closed_1h':0,
           'gap_5m':None,'rvol_5m':None,'trend_15m':None,'trend_1h':None,
           'data_ok':False,'watch_direction':'NONE','trade_allowed':False,
           'warnings':[],'ticker_last_trade_age_ms':None}
    warnings = out['warnings']
    ticker_last_ms=safe_ms(t.get('closeTime'))
    ticker_observed_ms=coin.get('ticker_observed',{}).get('received_server_est_ms')
    if ticker_last_ms and ticker_observed_ms:
        delta=ticker_observed_ms-ticker_last_ms
        out['ticker_last_trade_age_ms']=delta
        if delta < -3000 or delta > 10*60_000:
            warnings.append('TICKER_LAST_TRADE_STALE_OR_FUTURE')
    else:
        warnings.append('TICKER_TIMESTAMP_MISSING')
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
    # Missing data cannot be silently filled from a different market.
    critical = any(w in warnings for w in ('5m_MISSING_OR_STALE','15m_MISSING_OR_STALE',
         '1h_MISSING_OR_STALE','5m_GAPS','15m_GAPS','1h_GAPS','OI_MISSING',
         'OI_TIME_OR_VALUE_INVALID','FUNDING_MISSING','FUNDING_STALE_OR_FUTURE',
         'TICKER_LAST_TRADE_STALE_OR_FUTURE','TICKER_TIMESTAMP_MISSING'))
    out['data_ok']=bool(not critical and out['closed_5m']>=22 and
                        out['closed_15m']>=22 and out['closed_1h']>=22)
    if out['data_ok']:
        if out['trend_15m']==out['trend_1h']=='UP' and (out['rvol_5m'] or 0)>=1.2:
            out['watch_direction']='LONG_WATCH'
        if out['trend_15m']==out['trend_1h']=='DOWN' and (out['rvol_5m'] or 0)>=1.2:
            out['watch_direction']='SHORT_WATCH'
    return out


def build_oi_history(path, coins):
    """Incremental raw contracts history. Compute changes only when observation comparable."""
    path=Path(path)
    try:
        cache=json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    except (json.JSONDecodeError,OSError):cache={}
    summary={}
    for c in coins:
        s=c['symbol']; oi=c.get('open_interest')
        if not oi or not oi['source_time_valid']: continue
        v=safe_float(oi['data'].get('openInterest'))
        t=oi['source_time_ms']; val=safe_float(c['meta'].get('contractVal'))
        if v is None or v < 0 or t is None:continue
        prior=cache.get(s,[])
        if not isinstance(prior,list):prior=[]
        prior=[p for p in prior if isinstance(p,dict) and p.get('time_ms')!=t and p.get('contractVal')==val
               and isinstance(p.get('contracts'),(float,int)) and p.get('time_ms',0)<t]
        previous=[p for p in prior if 30 * 60_000 <= t-p['time_ms'] <= 6 * 3600_000]
        if previous:
            most=max(previous,key=lambda p:p['time_ms'])
            change=(v/most['contracts']-1)*100 if most['contracts']>0 else None
            summary[s]={'oi_pct_change_since_previous':change,
                        'prev_time_ms':most['time_ms'],'current_time_ms':t,
                        'comparable':change is not None and abs(change)<=200,
                        'unit':'contracts','source':'WEEX_V637_OBSERVATIONS'}
        prior.append({'time_ms':t,'contracts':v,'contractVal':val,
                      'observed_at_ms':oi['observed']['received_server_est_ms']})
        cache[s]=sorted(prior,key=lambda x:x['time_ms'])[-250:]
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
    """Restore our exact state file from previous collector ZIP, never invent OI points."""
    if not previous_zip:
        return 'no previous ZIP provided'
    with zipfile.ZipFile(previous_zip) as z:
        item = 'weex_data/oi_observations_state.json'
        if item not in z.namelist():
            raise RuntimeError(f'Previous ZIP lacks {item}; no synthetic history will be created')
        data = json.loads(z.read(item))
        if not isinstance(data, dict):
            raise ValueError('Invalid previous OI observations state')
        atomic_json(state_file, data)
        return f'loaded {len(data)} OI histories from previous ZIP'


def collect(args):
    root=Path(args.out).expanduser();root.mkdir(parents=True,exist_ok=True)
    archive_dir=root/'weex_data'
    # Prevent stale instrument files or summaries from the previous run entering the new ZIP.
    if archive_dir.exists():
        shutil.rmtree(archive_dir)
    (archive_dir/'coins').mkdir(parents=True,exist_ok=True)
    client=Weex(delay=args.delay, timeout=args.timeout, retries=args.retries)
    state_path=root/'weex_v637_oi_history_state.json'
    resume_status=restore_oi_state(args.resume_from, state_path)
    start_ms=now_ms()
    calibration_start=client.calibrate()
    exchange,info_meta=client.request('/capi/v3/market/exchangeInfo',{'contractType':'PERPETUAL'})
    tickers,tick_meta=client.request('/capi/v3/market/ticker/24hr')
    picked=select_symbols(exchange,tickers,args.top,crypto_only=not args.include_tradfi)
    if not picked:raise RuntimeError('No USDT perpetual symbols with positive 24h quote volume')
    print(f'Selected {len(picked)} USDT perpetual symbols; snapshot timestamp = receive time, not run start')
    coins=[]
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures={pool.submit(collect_symbol,client,s,meta,t,tick_meta,DEFAULT_TFS,args.depth):s
                 for s,meta,t in picked}
        for idx,future in enumerate(as_completed(futures),1):
            sym=futures[future]
            try:
                c=future.result();coins.append(c)
                atomic_json(archive_dir/'coins'/f'{sym}.json',c)
                if idx%25==0 or idx==len(futures): print(f'Collected {idx}/{len(futures)}')
            except Exception as exc:
                client.errors.append({'symbol':sym,'error':str(exc)})
    calibration_end=client.calibrate()
    # Disallow signals when clock calibration fails or drifts substantially.
    drift=abs(calibration_start.get('offset_ms',0)-calibration_end.get('offset_ms',0))
    quality_ok=(calibration_start.get('clock_quality')=='OK'
                and calibration_end.get('clock_quality')=='OK' and drift<=5000)
    features=[basic_features(c) for c in coins]
    hist=build_oi_history(state_path,coins)
    # Persist the carried-forward state INSIDE the exported ZIP for the next Colab session.
    if state_path.exists():
        (archive_dir/'oi_observations_state.json').write_bytes(state_path.read_bytes())
    for f in features:
        if f['symbol'] in hist:
            f.update(hist[f['symbol']])
            if not hist[f['symbol']]['comparable']:
                f['data_ok']=False;f['watch_direction']='NONE'
                f['warnings'].append('OI_CHANGE_UNVERIFIED')
        if not quality_ok:
            f['data_ok']=False;f['watch_direction']='NONE'
            f['warnings'].append('SERVER_CLOCK_NOT_TRUSTED')
    features.sort(key=lambda x:(x.get('quote_volume_24h_usdt') or 0),reverse=True)
    watch=[f for f in features if f['watch_direction']!='NONE']
    fields=['symbol','price','quote_volume_24h_usdt','change_24h_raw','oi_contracts',
            'oi_source_age_ms','funding_forecast','funding_latest_settled','closed_5m',
            'closed_15m','closed_1h','ticker_last_trade_age_ms','gap_5m','rvol_5m','trend_15m','trend_1h','data_ok',
            'watch_direction','oi_pct_change_since_previous','comparable','trade_allowed','warnings']
    csv_write(archive_dir/'ranking_market.csv',features,fields)
    csv_write(archive_dir/'watchlist.csv',watch,fields)
    csv_write(archive_dir/'setups.csv',[],['symbol','side','entry','sl','tp1','tp2','trade_allowed','reason'])
    all_bad=[{'symbol':c['symbol'],'errors':c['errors']} for c in coins if c['errors']]
    meta={'version':VERSION,'started_at_utc':utc(start_ms),'finished_at_utc':utc(),
          'ticker_observed_at_utc':utc(tick_meta['received_server_est_ms']),
          'oi_state_resume_status':resume_status,
          'notes':'Timestamps are per-endpoint receive; START anchor is NOT used for live causality',
          'selected_symbols':len(picked),'collected_symbols':len(coins),
          'watchlist_count':len(watch),'trade_gate':'HARD_DISABLED',
          'clock_calibration_start':calibration_start,
          'clock_calibration_end':calibration_end,'clock_stable':quality_ok,
          'calls':client.call_count,'errors_count':len(client.errors),
          'quote_volume_currency':'USDT','oi_raw_units':'CONTRACTS',
          'oi_usdt_automatically_estimated':False,
          'price_type_klines':'LAST traded price WEEX futures',
          'funding_history_policy':'settled rates + first-observed time; cannot backtest unobserved publication',
          'intervals':list(DEFAULT_TFS), 'top_requested':args.top,
          'market_filter':'CRYPTO_PERPETUAL' if not args.include_tradfi else 'ALL_PERPETUAL',
          'data_flags':{'rvol_only_closed':True,'historical_ttl_inferred':False,
                        'open_candles_used':False,'signals_not_generated':True,
                        'oi_history_embedded_in_zip':True}}
    atomic_json(archive_dir/'run_meta.json',meta)
    atomic_json(archive_dir/'qc.json',{'partial_failures':all_bad,'requests_errors':client.errors,
                                    'clock_quality_ok':quality_ok})
    archive_name=f"weex_v637_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.zip"
    archive_path=root/archive_name
    with zipfile.ZipFile(archive_path,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
        for f in sorted(archive_dir.rglob('*')):
            if f.is_file(): z.write(f,f.relative_to(root))
    print(json.dumps({'archive':str(archive_path),'collected':len(coins),
                      'watchlist':len(watch),'trade_gate':'HARD_DISABLED',
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
    print('PASS: candle edge, open bar, duplicates, LIMIT TTL, lookahead, SL order, symbols')


def main():
    parser=argparse.ArgumentParser(description=VERSION)
    parser.add_argument('--top',type=int,default=30,help='Top N USDT perpetual; 0 means ALL')
    parser.add_argument('--out',default='/content/weex_v637_results' if Path('/content').exists() else './weex_v637_results')
    parser.add_argument('--workers',type=int,default=4)
    parser.add_argument('--delay',type=float,default=0.18,help='min interval in seconds between WEEX requests')
    parser.add_argument('--timeout',type=int,default=20)
    parser.add_argument('--retries',type=int,default=3)
    parser.add_argument('--depth',action='store_true',help='include WEEX depth per symbol')
    parser.add_argument('--include-tradfi',action='store_true',help='add TRADFI underlyings from perpetual contracts')
    parser.add_argument('--resume-from',default=None,help='previous WEEX V6.37 collector ZIP (resume OI observations)')
    parser.add_argument('--self-test',action='store_true')
    parser.add_argument('--download-ready',action='store_true',help='informational, notebook handles Colab download')
    args=parser.parse_args()
    if args.self_test:run_self_test();return
    if args.top<0 or not 1<=args.workers<=8 or args.delay<0.08 or args.timeout<5:
        parser.error('Invalid limits: top>=0, workers 1..8, delay>=0.08, timeout>=5')
    collect(args)


if __name__=='__main__':main()
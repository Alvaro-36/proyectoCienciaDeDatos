"""Alpha Vantage -> bronze inmutable -> plata validada. Sin cálculos de oro.

Los helpers se pueden importar sin Airflow. Las credenciales se leen sólo durante
la ejecución. Consultar README.md para configuración, límites y trazabilidad.
"""
from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import json
import logging
import math
import os
import re
import shutil
import tempfile
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

BASE = 'https://www.alphavantage.co/query'
PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / 'include' / 'output'
BRONZE = OUTPUT_DIR / 'bronze'
PLATA = OUTPUT_DIR / 'silver'
UNIVERSO = PROJECT_DIR / 'include' / 'universo' / 'universo.csv'
BENCHMARK = 'SPY'
ENDPOINTS = ('EARNINGS', 'BALANCE_SHEET', 'CASH_FLOW', 'INCOME_STATEMENT',
             'OVERVIEW', 'SPLITS', 'EARNINGS_ESTIMATES', 'TIME_SERIES_DAILY')
HISTORICOS = frozenset(('EARNINGS', 'BALANCE_SHEET', 'CASH_FLOW',
                        'INCOME_STATEMENT', 'SPLITS'))
PUNTUALES = frozenset(('EARNINGS_ESTIMATES',))
TTL = dict(zip(ENDPOINTS, (80, 80, 80, 80, 30, 90, 1, 1)))
CANONICAS = {'EARNINGS': 'quarterlyEarnings', 'BALANCE_SHEET': 'quarterlyReports',
             'CASH_FLOW': 'quarterlyReports', 'INCOME_STATEMENT': 'quarterlyReports',
             'SPLITS': 'data', 'EARNINGS_ESTIMATES': 'estimates',
             'TIME_SERIES_DAILY': 'Time Series (Daily)', 'MARKET_STATUS': 'markets'}
CONSENSO = ['eps_estimate_high', 'eps_estimate_low',
            'eps_estimate_analyst_count', 'revenue_estimate_average']
CATEGORICAS = ['ticker', 'sector', 'industry', 'exchange', 'report_time', 'reported_currency']
FECHAS = ['fiscal_quarter_end', 'reported_date', 'snapshot_date']
COLUMNAS_EVENTOS = ['ticker', 'sector', 'industry', 'exchange', 'fiscal_quarter_end',
                    'reported_date', 'report_time', 'reported_eps', 'consensus_eps',
                    'surprise', 'surprise_pct', 'reported_currency', 'total_assets',
                    'short_long_term_debt_total', 'total_current_assets',
                    'total_current_liabilities', 'operating_cashflow',
                    'capital_expenditures', 'dividend_payout', 'net_income',
                    'total_revenue', 'shares_outstanding', *CONSENSO, 'snapshot_date']
COLUMNAS_PRECIOS = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'close_adj']
FUNDAMENTALES = {
    'BALANCE_SHEET': {'reportedCurrency': 'reported_currency', 'totalAssets': 'total_assets',
                      'shortLongTermDebtTotal': 'short_long_term_debt_total',
                      'totalCurrentAssets': 'total_current_assets',
                      'totalCurrentLiabilities': 'total_current_liabilities'},
    'CASH_FLOW': {'operatingCashflow': 'operating_cashflow',
                  'capitalExpenditures': 'capital_expenditures',
                  'dividendPayout': 'dividend_payout', 'netIncome': 'net_income'},
    'INCOME_STATEMENT': {'totalRevenue': 'total_revenue'},
}
CAUSAS_NULOS = {
    **{c: 'Cobertura de la fuente o campo no informado; no se imputa.' for c in COLUMNAS_EVENTOS},
    **{c: 'Sin captura trimestral estrictamente anterior al anuncio, o campo no informado.' for c in CONSENSO},
    'dividend_payout': 'Puede indicar ausencia de dividendos o falta de cobertura; no se asume cero sin verificar.',
    'total_current_assets': 'Faltante estructural posible en financieras o falta de cobertura.',
    'total_current_liabilities': 'Faltante estructural posible en financieras o falta de cobertura.',
    'short_long_term_debt_total': 'Taxonomía sectorial o falta de cobertura.',
    'report_time': 'Horario no informado o fuera de pre-market/post-market.',
}
log = logging.getLogger(__name__)


def configurar():
    """Invocar al ejecutar, nunca al descubrir el DAG."""
    load_dotenv(PROJECT_DIR / '.env', override=False)


def claves_api():
    configurar()
    try:
        keys = json.loads(os.environ.get('ALPHAVANTAGE_API_KEYS', '[]'))
    except (ValueError, TypeError):
        raise ValueError('ALPHAVANTAGE_API_KEYS debe ser un array JSON de strings.') from None
    if not isinstance(keys, list) or not keys or any(not isinstance(k, str) or not k.strip() for k in keys):
        raise ValueError('Configurá ALPHAVANTAGE_API_KEYS con al menos una clave en .env.')
    return list(dict.fromkeys(k.strip() for k in keys))


def numero_env(nombre, default, minimo=0):
    value = float(os.environ.get(nombre, default))
    if not math.isfinite(value) or value < minimo:
        raise ValueError(f'{nombre} debe ser un número >= {minimo}.')
    return value


@contextmanager
def bloqueo(path):
    """Lock entre procesos locales/contenedores que comparten el volumen."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def escribir_atomico(path, contenido):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix='.tmp-')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(contenido)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def av_get(funcion, key, **params):
    """Una request. Jamás propaga excepciones con URLs que contengan apikey."""
    try:
        response = requests.get(BASE, params={'function': funcion, **params, 'apikey': key},
                                timeout=(10, 60), allow_redirects=False)
    except requests.RequestException:
        raise RuntimeError(f'Error de conexión con Alpha Vantage ({funcion}).') from None
    if response.status_code == 429:
        return response.text, {'Note': 'HTTP 429 rate limit', '_retry_after': response.headers.get('Retry-After')}
    if response.status_code != 200:
        raise RuntimeError(f'Alpha Vantage HTTP {response.status_code} ({funcion}).')
    try:
        payload = response.json()
    except ValueError:
        raise RuntimeError(f'Alpha Vantage devolvió JSON inválido ({funcion}).') from None
    return response.text, payload


def espera_cuota(payload, now=None):
    """None si no es cuota; segundos de enfriamiento si corresponde.

    Premium/permiso no es cuota: no agota ni rota claves por esos mensajes.
    """
    if not isinstance(payload, dict):
        return None
    message = ' '.join(str(payload.get(k, '')) for k in ('Information', 'Note', 'Error Message')).lower()
    if any(s in message for s in ('premium endpoint', 'premium feature', 'invalid api key', 'invalid apikey')):
        return None
    rate = any(s in message for s in ('rate limit', 'call frequency', 'api call volume',
                                      'requests per', 'calls per', 'request limit',
                                      'api limit', 'quota', 'limit reached', 'limit exceeded'))
    if not rate:
        return None
    now = time.time() if now is None else now
    retry = payload.get('_retry_after')
    if retry:
        try:
            return max(1, float(retry))
        except (ValueError, TypeError):
            try:
                return max(1, parsedate_to_datetime(retry).timestamp() - now)
            except (ValueError, TypeError, OverflowError):
                pass
    daily = any(s in message for s in ('per day', 'daily', 'a day', '24 hour'))
    return numero_env('AV_DAILY_COOLDOWN_SECONDS' if daily else 'AV_RATE_COOLDOWN_SECONDS',
                      86400 if daily else 65, 1)


def av_get_rotando(funcion, **params):
    keys = claves_api()
    state_path = OUTPUT_DIR / 'state' / 'api_keys.json'
    # Toda la selección/request/actualización es atómica entre lotes y reintentos.
    with bloqueo(state_path.with_suffix('.lock')):
        state = json.loads(state_path.read_text()) if state_path.exists() else {}
        records = state.setdefault('keys', {})
        ids = [hashlib.sha256(k.encode()).hexdigest() for k in keys]
        active = ids.index(state['active']) if state.get('active') in ids else 0
        for offset in range(len(keys)):
            idx = (active + offset) % len(keys)
            record = records.setdefault(ids[idx], {})
            if record.get('blocked_until', 0) > time.time():
                continue
            interval = numero_env('AV_MIN_REQUEST_INTERVAL_SECONDS', 1.1)
            delay = record.get('last_request', 0) + interval - time.time()
            if delay > 0:
                time.sleep(delay)
            record['last_request'] = time.time()
            # Persistir incluso si luego la request falla.
            escribir_atomico(state_path, json.dumps(state))
            raw, payload = av_get(funcion, keys[idx], **params)
            cooldown = espera_cuota(payload)
            if cooldown is not None:
                record['blocked_until'] = time.time() + cooldown
                state['active'] = ids[(idx + 1) % len(keys)]
                escribir_atomico(state_path, json.dumps(state))
                log.warning('API_KEY_LIMIT key_slot=%s endpoint=%s cooldown_seconds=%s; intentando siguiente clave',
                            idx + 1, funcion, cooldown)
                continue
            state['active'] = ids[idx]
            escribir_atomico(state_path, json.dumps(state))
            return raw, payload
    log.warning('API_KEYS_EXHAUSTED endpoint=%s; descarga pendiente para próxima corrida', funcion)
    return '', {'Information': 'Todas las claves están en enfriamiento.', '_quota_exhausted': True}


def es_util(funcion, payload):
    if not isinstance(payload, dict) or any(k in payload for k in ('Information', 'Note', 'Error Message')):
        return False
    if funcion == 'OVERVIEW':
        return bool(texto(payload.get('Symbol'))) and any(texto(payload.get(k)) for k in ('Sector', 'Industry', 'Exchange'))
    value = payload.get(CANONICAS.get(funcion))
    if funcion == 'TIME_SERIES_DAILY':
        return isinstance(value, dict) and bool(value) and all(isinstance(r, dict) for r in value.values())
    return isinstance(value, list) and all(isinstance(r, dict) for r in value) and (bool(value) or funcion == 'SPLITS')


def fecha(value):
    try:
        return date.fromisoformat(str(value)).isoformat()
    except ValueError:
        return None


def texto(value):
    if value is None or str(value).strip().lower() in ('', 'none', 'null', 'nan', 'n/a', '-'):
        return None
    return str(value).strip()


def num(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError):
        return None


def ruta_bronce(funcion, ticker, snapshot):
    if funcion not in ENDPOINTS or not re.fullmatch(r'[A-Z0-9][A-Z0-9.\-]{0,19}', ticker) or not fecha(snapshot):
        raise ValueError('Partición bronze inválida.')
    return BRONZE / funcion / f'ticker={ticker}' / f'snapshot={snapshot}.json'


def snapshots(funcion, ticker):
    folder = ruta_bronce(funcion, ticker, '2000-01-01').parent
    return sorted(p for p in folder.glob('snapshot=*.json') if fecha(p.stem.removeprefix('snapshot=')))


def ultimo_snapshot(funcion, ticker):
    paths = snapshots(funcion, ticker)
    return paths[-1].stem.removeprefix('snapshot=') if paths else None


def leer_ultimo(funcion, ticker):
    paths = snapshots(funcion, ticker)
    return json.loads(paths[-1].read_text()) if paths else None


def leer_todos(funcion, ticker):
    return [(p.stem.removeprefix('snapshot='), json.loads(p.read_text())) for p in snapshots(funcion, ticker)]


def tickers_en_bronce(funcion):
    return sorted(p.name.removeprefix('ticker=') for p in (BRONZE / funcion).glob('ticker=*') if p.is_dir())


def leer_universo(mode='full'):
    if mode not in ('subset', 'full'):
        raise ValueError('mode debe ser subset o full.')
    with UNIVERSO.open(newline='', encoding='utf-8') as handle:
        rows = list(csv.DictReader(handle))
    tickers = [r['ticker'].strip().upper() for r in rows]
    if not tickers or len(set(tickers)) != len(tickers) or BENCHMARK in tickers:
        raise ValueError('Universo vacío, duplicado o con benchmark incluido.')
    for ticker in tickers:
        ruta_bronce('EARNINGS', ticker, '2000-01-01')
    if mode == 'subset':
        if 'NVDA' not in tickers:
            raise ValueError('El universo debe incluir NVDA para el subset de prueba.')
        return ['NVDA']
    return tickers


def pendientes(mode='full', force=False, snapshot=None, outputsize='full'):
    snapshot = snapshot or datetime.now(timezone.utc).date().isoformat()
    if outputsize not in ('compact', 'full'):
        raise ValueError('outputsize debe ser compact o full.')
    tickers = leer_universo(mode)
    items = []
    # Benchmark primero; luego cada empresa completa para avanzar con cuotas chicas.
    for ticker in [BENCHMARK, *tickers]:
        endpoints = ('SPLITS', 'TIME_SERIES_DAILY') if ticker == BENCHMARK else ENDPOINTS
        for endpoint in endpoints:
            if ruta_bronce(endpoint, ticker, snapshot).exists():
                continue  # force nunca sobreescribe una captura del mismo día.
            last = ultimo_snapshot(endpoint, ticker)
            if force or last is None or (date.fromisoformat(snapshot) - date.fromisoformat(last)).days >= TTL[endpoint]:
                items.append({'funcion': endpoint, 'ticker': ticker, 'snapshot': snapshot, 'outputsize': outputsize})
    # Primero completar el universo; después refrescar lo más antiguo.
    # Sin esta prioridad las cuotas chicas sólo refrescarían los primeros tickers.
    items.sort(key=lambda item: ultimo_snapshot(item['funcion'], item['ticker']) or '')
    return items


def armar_lotes(items, size=25):
    if size < 1:
        raise ValueError('El tamaño de lote debe ser positivo.')
    return [{'nombre': f'lote_{i // size + 1:03d}', 'items': items[i:i + size]} for i in range(0, len(items), size)]


def resumen_payload(funcion, payload):
    rows = payload.get(CANONICAS.get(funcion), [])
    if funcion == 'OVERVIEW':
        return {'records': 1, 'fields': sorted(payload)}
    dates = list(rows) if isinstance(rows, dict) else [r.get('fiscalDateEnding') or r.get('effective_date') or r.get('date') for r in rows]
    dates = sorted(d for d in dates if fecha(d))
    return {'records': len(rows), 'date_min': dates[0] if dates else None, 'date_max': dates[-1] if dates else None}


def registrar_bronce(path, funcion, ticker, snapshot, raw, payload):
    event = {'event': 'bronze_obtenido', 'obtained_at': datetime.now(timezone.utc).isoformat(),
             'endpoint': funcion, 'ticker': ticker, 'snapshot': snapshot, 'path': str(path),
             'bytes': len(raw.encode('utf-8')), 'sha256': hashlib.sha256(raw.encode()).hexdigest(),
             **resumen_payload(funcion, payload)}
    message = json.dumps(event, ensure_ascii=False)
    log.info('BRONZE_OBTENIDO %s', message)
    path_log = OUTPUT_DIR / 'logs' / 'bronze.jsonl'
    with bloqueo(path_log.with_suffix('.lock')):
        with path_log.open('a', encoding='utf-8') as handle:
            handle.write(message + '\n')
            handle.flush()
            os.fsync(handle.fileno())


def bajar_bronce(lote):
    paths = []
    transient = 0
    for item in lote['items']:
        funcion, ticker, snapshot = (item[k] for k in ('funcion', 'ticker', 'snapshot'))
        path = ruta_bronce(funcion, ticker, snapshot)
        with bloqueo(path.with_suffix('.lock')):
            if path.exists():
                paths.append(str(path))
                continue
            params = {'symbol': ticker}
            if funcion == 'TIME_SERIES_DAILY':
                params['outputsize'] = item['outputsize']
            try:
                raw, payload = av_get_rotando(funcion, **params)
            except RuntimeError:
                log.warning('BRONZE_PENDIENTE endpoint=%s ticker=%s reason=transporte', funcion, ticker)
                transient += 1
                continue
            if isinstance(payload, dict) and payload.get('_quota_exhausted'):
                break
            if not es_util(funcion, payload):
                log.warning('BRONZE_RECHAZADO endpoint=%s ticker=%s reason=respuesta_no_util', funcion, ticker)
                continue
            symbol = payload.get('symbol') or payload.get('Symbol') or payload.get('Meta Data', {}).get('2. Symbol')
            if symbol and symbol.upper() != ticker:
                log.warning('BRONZE_RECHAZADO endpoint=%s ticker=%s reason=simbolo_incorrecto', funcion, ticker)
                continue
            escribir_atomico(path, raw)
            registrar_bronce(path, funcion, ticker, snapshot, raw, payload)
            paths.append(str(path))
    if transient:
        raise RuntimeError(f'{transient} descargas con error de transporte; reintento conserva bronze existente.')
    return paths


def parse_eventos(ticker, payload):
    result = []
    discarded = 0
    for row in (payload or {}).get('quarterlyEarnings', []):
        eps, estimate = num(row.get('reportedEPS')), num(row.get('estimatedEPS'))
        fq, rd = fecha(row.get('fiscalDateEnding')), fecha(row.get('reportedDate'))
        if eps is None or estimate is None or abs(estimate) < 0.05 or not fq or not rd:
            discarded += 1
            continue
        rt = texto(row.get('reportTime'))
        rt = rt.lower() if rt else None
        result.append({'ticker': ticker, 'fiscal_quarter_end': fq, 'reported_date': rd,
                       'report_time': rt if rt in ('pre-market', 'post-market') else None,
                       'reported_eps': eps, 'consensus_eps': estimate,
                       'surprise': num(row.get('surprise')), 'surprise_pct': num(row.get('surprisePercentage'))})
    log.info('LIMPIEZA_EVENTOS ticker=%s descartados=%s conservados=%s', ticker, discarded, len(result))
    return result


def parse_fundamentales(ticker, payloads):
    result = {}
    for endpoint, mapping in FUNDAMENTALES.items():
        seen = set()
        for row in (payloads.get(endpoint) or {}).get('quarterlyReports', []):
            fq = fecha(row.get('fiscalDateEnding'))
            if not fq or fq in seen:
                continue
            seen.add(fq)
            record = result.setdefault(fq, {})
            for source, target in mapping.items():
                record[target] = texto(row.get(source)) if target == 'reported_currency' else num(row.get(source))
    return result


def parse_consenso(ticker, snapshot, payload):
    return [{'ticker': ticker, 'fiscal_quarter_end': fecha(r.get('date')), 'consensus_snapshot': snapshot,
             **{c: num(r.get(c)) for c in CONSENSO}}
            for r in (payload or {}).get('estimates', [])
            if str(r.get('horizon', '')).lower() == 'fiscal quarter' and fecha(r.get('date'))]


def parse_precios(ticker, payloads_diarios, payload_splits):
    if payload_splits is None:
        log.warning('PRECIOS_SIN_SPLITS ticker=%s; close_adj queda nulo', ticker)
    splits = {}
    for r in (payload_splits or {}).get('data', []):
        d, factor = fecha(r.get('effective_date')), num(r.get('split_factor'))
        if not d or factor is None or factor <= 0:
            raise ValueError(f'Split inválido para {ticker}.')
        if d in splits and splits[d] != factor:
            raise ValueError(f'Splits contradictorios para {ticker} en {d}.')
        splits[d] = factor
    rows = {}
    for snapshot, payload in sorted(payloads_diarios, key=lambda p: p[0]):
        for d, r in payload.get('Time Series (Daily)', {}).items():
            if not fecha(d):
                raise ValueError(f'Fecha de precio inválida para {ticker}.')
            rows[d] = {'ticker': ticker, 'date': d,
                       **{target: num(r.get(source)) for source, target in
                          [('1. open', 'open'), ('2. high', 'high'), ('3. low', 'low'),
                           ('4. close', 'close'), ('5. volume', 'volume')]}}
    ordered_splits = sorted(splits.items(), reverse=True)
    index, factor = 0, 1.0
    for d in sorted(rows, reverse=True):
        while index < len(ordered_splits) and ordered_splits[index][0] > d:
            factor *= ordered_splits[index][1]
            index += 1
        close = rows[d]['close']
        rows[d]['close_adj'] = close / factor if payload_splits is not None and close is not None else None
    return [rows[d] for d in sorted(rows)]


def parse_calendario(filas_benchmark):
    return [{'date': d, 't': i} for i, d in enumerate(sorted({r['date'] for r in filas_benchmark if r['ticker'] == BENCHMARK}))]


def estado_cuotas():
    """Diagnóstico local sin requests, sin claves en el resultado y sin exigir .env."""
    configurar()
    try:
        keys = json.loads(os.environ.get('ALPHAVANTAGE_API_KEYS', '[]'))
    except (ValueError, TypeError):
        return {'configuradas': 0, 'disponibles': 0, 'proxima_disponible_utc': None}
    if not isinstance(keys, list):
        keys = []
    keys = list(dict.fromkeys(k.strip() for k in keys if isinstance(k, str) and k.strip()))
    path = OUTPUT_DIR / 'state' / 'api_keys.json'
    with bloqueo(path.with_suffix('.lock')):
        state = json.loads(path.read_text()) if path.exists() else {}
    now = time.time()
    blocked = [state.get('keys', {}).get(hashlib.sha256(k.encode()).hexdigest(), {}).get('blocked_until', 0)
               for k in keys]
    available = sum(until <= now for until in blocked)
    next_available = min(blocked) if blocked and not available else None
    return {'configuradas': len(keys), 'disponibles': available,
            'proxima_disponible_utc': datetime.fromtimestamp(next_available, timezone.utc).isoformat()
            if next_available else None}


def revisar_bronce(mode='full', incluir_consenso=False):
    """Distingue una carga aún incompleta de datos descargados con mala calidad.

    Requiere SPY y al menos una empresa con todos los endpoints básicos. No exige
    todo el universo: los mínimos de volumen/cobertura siguen en validar().
    Un archivo presente pero corrupto no se trata como una descarga pendiente.
    """
    tickers = leer_universo(mode)
    required = [ep for ep in ENDPOINTS if incluir_consenso or ep != 'EARNINGS_ESTIMATES']
    missing, complete = [], []
    benchmark_ready = False
    for ticker in [BENCHMARK, *tickers]:
        endpoints = ['SPLITS', 'TIME_SERIES_DAILY'] if ticker == BENCHMARK else required
        ticker_missing = []
        for endpoint in endpoints:
            payload = leer_ultimo(endpoint, ticker)
            if payload is None:
                ticker_missing.append({'ticker': ticker, 'endpoint': endpoint})
            elif not es_util(endpoint, payload):
                raise ValueError(f'Bronze inválido: {endpoint}/{ticker}; revisar el snapshot existente.')
        missing.extend(ticker_missing)
        if ticker == BENCHMARK:
            benchmark_ready = not ticker_missing
        elif not ticker_missing:
            complete.append(ticker)
    ready = benchmark_ready and bool(complete)
    quota = estado_cuotas()
    reason = 'listo' if ready else ('cuota_en_enfriamiento' if quota['configuradas'] and not quota['disponibles'] else 'bronze_incompleto')
    report = {'estado': 'listo_para_refinar' if ready else 'esperando_datos',
              'ready': ready, 'mode': mode, 'incluir_consenso': incluir_consenso,
              'checked_at': datetime.now(timezone.utc).isoformat(), 'motivo': reason,
              'tickers_completos': complete, 'benchmark_completo': benchmark_ready,
              'faltantes': missing, 'cuotas': quota}
    path = OUTPUT_DIR / 'state' / f'ingesta_{mode}.json'
    escribir_atomico(path, json.dumps(report, ensure_ascii=False, indent=2))
    if not ready:
        log.warning('INGESTA_PENDIENTE motivo=%s descargas_faltantes=%s proxima_clave_utc=%s informe=%s',
                    reason, len(missing), quota['proxima_disponible_utc'], path)
        log.warning('BRONZE_FALTANTE %s', json.dumps(missing, ensure_ascii=False))
    return report


def refinar_plata(mode='full', incluir_consenso=False):
    tickers = leer_universo(mode)
    events, prices = [], []
    lineage = {}
    for ticker in tickers:
        data = parse_eventos(ticker, leer_ultimo('EARNINGS', ticker))
        fundamentals = parse_fundamentales(ticker, {ep: leer_ultimo(ep, ticker) for ep in FUNDAMENTALES})
        overview = leer_ultimo('OVERVIEW', ticker) or {}
        consensus = {}
        for snap, payload in leer_todos('EARNINGS_ESTIMATES', ticker):
            for r in parse_consenso(ticker, snap, payload):
                consensus.setdefault(r['fiscal_quarter_end'], []).append(r)
        lineage[ticker] = {ep: ultimo_snapshot(ep, ticker) for ep in ENDPOINTS}
        for event in data:
            event.update(fundamentals.get(event['fiscal_quarter_end'], {}))
            event.update({target: texto(overview.get(source)) for source, target in
                          [('Sector', 'sector'), ('Industry', 'industry'), ('Exchange', 'exchange')]})
            event['shares_outstanding'] = num(overview.get('SharesOutstanding'))
            event['snapshot_date'] = ultimo_snapshot('EARNINGS', ticker)
            candidates = [r for r in consensus.get(event['fiscal_quarter_end'], [])
                          if r['consensus_snapshot'] < event['reported_date']]
            if candidates:
                selected = max(candidates, key=lambda r: r['consensus_snapshot'])
                event.update({c: selected[c] for c in CONSENSO})
            events.append(event)
    columns = [c for c in COLUMNAS_EVENTOS if incluir_consenso or c not in CONSENSO]
    ev = pd.DataFrame(events, columns=columns)
    ev = ev.drop_duplicates(['ticker', 'fiscal_quarter_end'], keep='first')
    zero_fraction = float(ev['surprise'].eq(0).mean()) if len(ev) else 0
    log.info('SORPRESA_CERO proporcion=%.6f umbral=0.03', zero_fraction)
    if zero_fraction > 0.03:
        log.warning('SORPRESA_CERO descartando=%s por regla del plan; posible relleno, no confirmado', ev['surprise'].eq(0).sum())
        ev = ev.loc[~ev['surprise'].eq(0)].copy()
    for c in columns:
        if c in FECHAS:
            ev[c] = pd.to_datetime(ev[c], errors='raise')
        elif c in CATEGORICAS:
            ev[c] = ev[c].astype('string')
        else:
            ev[c] = pd.to_numeric(ev[c], errors='coerce').astype('float64')
    for ticker in [*tickers, BENCHMARK]:
        prices.extend(parse_precios(ticker, leer_todos('TIME_SERIES_DAILY', ticker), leer_ultimo('SPLITS', ticker)))
    pr = pd.DataFrame(prices, columns=COLUMNAS_PRECIOS).drop_duplicates(['ticker', 'date'])
    cal = pd.DataFrame(parse_calendario(prices), columns=['date', 't'])
    PLATA.mkdir(parents=True, exist_ok=True)
    paths = {}
    for name, df in [('eventos', ev.sort_values(['ticker', 'fiscal_quarter_end'])),
                     ('precios', pr.sort_values(['ticker', 'date'])), ('calendario', cal)]:
        path = PLATA / f'slv_{name}.csv'
        escribir_atomico(path, df.to_csv(index=False, date_format='%Y-%m-%d'))
        paths[name] = str(path)
    escribir_atomico(PLATA / 'linaje.json', json.dumps(lineage, indent=2))
    return paths


def validar(paths, mode='full', incluir_consenso=False):
    if mode not in ('subset', 'full'):
        raise ValueError('mode inválido.')
    ev, pr, cal = (pd.read_csv(paths[k]) for k in ('eventos', 'precios', 'calendario'))
    problemas = []
    expected = [c for c in COLUMNAS_EVENTOS if incluir_consenso or c not in CONSENSO]
    if list(ev.columns) != expected or list(pr.columns) != COLUMNAS_PRECIOS or list(cal.columns) != ['date', 't']:
        raise ValueError('Esquema de plata inesperado.')
    perfil = {c: {'null_count': int(ev[c].isna().sum()), 'null_fraction': float(ev[c].isna().mean()) if len(ev) else 1.0,
                  'causa': CAUSAS_NULOS[c]} for c in ev}
    log.info('NULOS_POR_COLUMNA %s', json.dumps(perfil, ensure_ascii=False))
    minimo = 1001 if mode == 'full' else 1
    if len(ev) < minimo:
        problemas.append(f'slv_eventos tiene {len(ev)} filas, mínimo {minimo} ({mode})')
    for name, df, keys in [('eventos', ev, ['ticker', 'fiscal_quarter_end']),
                           ('precios', pr, ['ticker', 'date']), ('calendario', cal, ['date'])]:
        if df[keys].isna().any().any() or df.duplicated(keys).any():
            problemas.append(f'{name}: clave nula o repetida')
        if df.empty:
            problemas.append(f'{name}: tabla vacía')
    vacias = ev.columns[ev.isna().all()].tolist()
    if vacias:
        problemas.append(f'columnas 100% nulas: {vacias}')
    if len(ev.columns) < 5 or not any(c in ev for c in CATEGORICAS) or not any(pd.api.types.is_numeric_dtype(ev[c]) for c in ev):
        problemas.append('Faltan columnas o mezcla de tipos')
    for c in FECHAS:
        parsed = pd.to_datetime(ev[c], errors='coerce')
        if parsed.isna().any():
            problemas.append(f'fecha nula o inválida: {c}')
        ev[c] = parsed
    for c in [c for c in ev if c not in CATEGORICAS + FECHAS]:
        numeric = pd.to_numeric(ev[c], errors='coerce')
        if (ev[c].notna() & numeric.isna()).any() or numeric.dropna().isin([float('inf'), float('-inf')]).any():
            problemas.append(f'valor numérico inválido: {c}')
    if ev[['reported_eps', 'consensus_eps']].isna().any().any():
        problemas.append('EPS reportado/consenso nulo')
    if pd.to_numeric(ev.consensus_eps, errors='coerce').abs().lt(0.05).any():
        problemas.append('Consenso demasiado cercano a cero')
    lag = (ev.reported_date - ev.fiscal_quarter_end).dt.days
    if lag.lt(0).any() or not 10 <= lag.median() <= 90:
        problemas.append(f'Coherencia temporal inválida; lag mediano={lag.median()}')
    for df, label in [(pr, 'precios'), (cal, 'calendario')]:
        if pd.to_datetime(df.date, errors='coerce').isna().any():
            problemas.append(f'Fechas inválidas en {label}')
    if BENCHMARK not in set(pr.ticker):
        problemas.append('Falta el benchmark SPY')
    missing = sorted(set(ev.ticker) - set(pr.ticker))
    if len(missing) > 0.10 * ev.ticker.nunique():
        problemas.append(f'{len(missing)} tickers sin precios: {missing}')
    numeric_prices = pr[COLUMNAS_PRECIOS[2:]].apply(pd.to_numeric, errors='coerce')
    if numeric_prices.isna().any().any() or numeric_prices.isin([float('inf'), float('-inf')]).any().any():
        problemas.append('Precios/volumen nulos o inválidos; verificar cobertura de SPLITS')
    if numeric_prices[['open', 'high', 'low', 'close', 'close_adj']].le(0).any().any() or numeric_prices.volume.lt(0).any():
        problemas.append('Precios no positivos o volumen negativo')
    if (numeric_prices.low > numeric_prices[['open', 'close', 'high']].min(axis=1)).any() or (numeric_prices.high < numeric_prices[['open', 'close', 'low']].max(axis=1)).any():
        problemas.append('OHLC incoherente')
    expected_dates = sorted(set(pr.loc[pr.ticker == BENCHMARK, 'date']))
    if cal.date.tolist() != expected_dates or cal.t.tolist() != list(range(len(cal))):
        problemas.append('Calendario no coincide con las ruedas del benchmark o t no es consecutivo')
    coverage = {'eventos_fuera_rango_benchmark': int((~ev.reported_date.between(pd.to_datetime(expected_dates[0]), pd.to_datetime(expected_dates[-1]))).sum()) if expected_dates else len(ev),
                'tickers_sin_precios': missing}
    report = {'valid': not problemas, 'mode': mode, 'incluir_consenso': incluir_consenso,
              'counts': {'eventos': len(ev), 'precios': len(pr), 'calendario': len(cal)},
              'null_profile': perfil, 'coverage': coverage, 'problems': problemas,
              'sha256': {k: hashlib.sha256(Path(p).read_bytes()).hexdigest() for k, p in paths.items()}}
    escribir_atomico(PLATA / 'validacion.json', json.dumps(report, ensure_ascii=False, indent=2))
    log.info('COBERTURA %s', json.dumps(coverage))
    if problemas:
        raise ValueError('Validación fallida:\n  - ' + '\n  - '.join(problemas))
    return paths


def guardar(paths, fecha_entrega, mode='full'):
    if not fecha(fecha_entrega):
        raise ValueError('Fecha de entrega inválida.')
    report = json.loads((PLATA / 'validacion.json').read_text())
    hashes = {k: hashlib.sha256(Path(p).read_bytes()).hexdigest() for k, p in paths.items()}
    if not report['valid'] or report['mode'] != mode or report['sha256'] != hashes:
        raise ValueError('Sólo se publica la plata validada sin modificaciones posteriores.')
    prefix = 'entrega' if mode == 'full' else 'prueba_subset'
    dest = OUTPUT_DIR / f'{prefix}_{fecha_entrega}'
    with bloqueo(OUTPUT_DIR / 'state' / 'publicar.lock'):
        if dest.exists():
            old = json.loads((dest / 'validacion.json').read_text())
            if old.get('sha256') == hashes:
                return str(dest)
            raise ValueError(f'Ya existe una entrega distinta en {dest}; se preserva.')
        temp = Path(tempfile.mkdtemp(dir=OUTPUT_DIR, prefix='.entrega-'))
        try:
            for p in paths.values():
                shutil.copy2(p, temp / Path(p).name)
            for name in ('validacion.json', 'linaje.json'):
                shutil.copy2(PLATA / name, temp / name)
            os.replace(temp, dest)
        finally:
            if temp.exists():
                shutil.rmtree(temp)
    return str(dest)


def fecha_corrida(dag_run):
    dt = dag_run.logical_date or dag_run.run_after
    if dt is None:
        raise ValueError('La corrida no tiene logical_date ni run_after.')
    return dt.date().isoformat()


# Sin Airflow instalado los parsers y CLI siguen siendo utilizables/testeables.
try:
    from airflow.sdk import dag, task, Param, get_current_context, PokeReturnValue
    from airflow.sdk.exceptions import AirflowSkipException, AirflowFailException
except ModuleNotFoundError as exc:
    if exc.name != 'airflow':
        raise
else:
    @dag(dag_id='earnings_ingest', start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
         schedule='@daily', catchup=False, max_active_runs=1,
         default_args={'owner': 'ciencia-de-datos', 'retries': 2, 'retry_delay': timedelta(minutes=2)},
         tags=['earnings', 'bronze', 'silver'], doc_md=__doc__, params={
             'mode': Param('subset', enum=['subset', 'full']),
             'force': Param(False, type='boolean'), 'solo_plata': Param(False, type='boolean'),
             'outputsize': Param('compact', enum=['compact', 'full']),
             'incluir_consenso': Param(False, type='boolean'),
         })
    def earnings_pipeline():
        @task.sensor(poke_interval=300, timeout=1800, mode='reschedule', soft_fail=True)
        def esperar_fuente():
            context = get_current_context()
            p = context['params']
            if p['solo_plata'] or not pendientes(p['mode'], p['force'], outputsize=p['outputsize']):
                return PokeReturnValue(is_done=True, xcom_value={'ok': True})
            try:
                _, payload = av_get_rotando('MARKET_STATUS')
            except RuntimeError:
                return PokeReturnValue(is_done=False)
            if isinstance(payload, dict) and payload.get('_quota_exhausted'):
                return PokeReturnValue(is_done=True, xcom_value={'ok': False})
            return PokeReturnValue(is_done=es_util('MARKET_STATUS', payload), xcom_value={'ok': True})

        @task.branch(trigger_rule='all_done')
        def decidir_rama():
            ctx = get_current_context()
            source = ctx['ti'].xcom_pull(task_ids='esperar_fuente') or {}
            return 'hay_trabajo' if source.get('ok') and not ctx['params']['solo_plata'] else 'sin_bronce'

        @task.short_circuit
        def hay_trabajo():
            ctx = get_current_context()
            p = ctx['params']
            items = pendientes(p['mode'], p['force'], outputsize=p['outputsize'])
            ctx['ti'].xcom_push(key='pendientes', value=items)
            log.info('DESCARGAS_PENDIENTES cantidad=%s', len(items))
            return bool(items)

        @task(task_id='armar_lotes')
        def task_lotes():
            return armar_lotes(get_current_context()['ti'].xcom_pull(task_ids='hay_trabajo', key='pendientes'))

        @task(task_id='bajar_bronce', map_index_template="{{ task.op_kwargs['lote']['nombre'] }}", max_active_tis_per_dag=1)
        def task_bronce(lote):
            return bajar_bronce(lote)

        @task
        def sin_bronce():
            log.info('SIN_BRONCE: reprocesando snapshots locales por solo_plata, caída o cuota agotada.')

        @task(task_id='refinar_plata', trigger_rule='none_failed_min_one_success')
        def task_plata():
            p = get_current_context()['params']
            report = revisar_bronce(p['mode'], p['incluir_consenso'])
            if not report['ready']:
                raise AirflowSkipException(
                    f"Ingesta pendiente ({report['motivo']}): faltan {len(report['faltantes'])} descargas. "
                    'Se omiten plata, validación y entrega; consultar state/ingesta_'
                    f"{p['mode']}.json.")
            return refinar_plata(p['mode'], p['incluir_consenso'])

        @task(task_id='validar', retries=0)
        def task_validar(paths):
            p = get_current_context()['params']
            try:
                return validar(paths, p['mode'], p['incluir_consenso'])
            except ValueError as exc:
                # Reintentar los mismos CSV no cambia su calidad ni su cobertura.
                raise AirflowFailException(str(exc)) from None

        @task(task_id='guardar')
        def task_guardar(paths):
            ctx = get_current_context()
            return guardar(paths, fecha_corrida(ctx['dag_run']), ctx['params']['mode'])

        sensor = esperar_fuente()
        branch = decidir_rama()
        work = hay_trabajo()
        batches = task_lotes()
        bronze = task_bronce.expand(lote=batches)
        fallback = sin_bronce()
        silver = task_plata()
        sensor >> branch >> [work, fallback]
        work >> batches
        [bronze, fallback] >> silver
        task_guardar(task_validar(silver))

    earnings_ingest = earnings_pipeline()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--solo-plata', action='store_true')
    parser.add_argument('--mode', choices=['subset', 'full'], default='subset')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--outputsize', choices=['compact', 'full'], default='compact')
    parser.add_argument('--incluir-consenso', action='store_true')
    parser.add_argument('--plan', action='store_true', help='Mostrar pendientes sin red ni escrituras.')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if args.plan:
        items = pendientes(args.mode, args.force, outputsize=args.outputsize)
        print(json.dumps({'pendientes': len(items), 'lotes': armar_lotes(items)}, indent=2))
        return
    # CLI y Airflow no se deben ejecutar a la vez sobre el mismo output.
    with bloqueo(OUTPUT_DIR / 'state' / 'cli.lock'):
        if not args.solo_plata:
            items = pendientes(args.mode, args.force, outputsize=args.outputsize)
            if not items:
                log.info('Sin descargas pendientes; usar --solo-plata para reprocesar.')
                return
            for lote in armar_lotes(items):
                bajar_bronce(lote)
        if not revisar_bronce(args.mode, args.incluir_consenso)['ready']:
            raise SystemExit(2)  # Ingesta pendiente; sin traceback ni entrega vacía.
        paths = refinar_plata(args.mode, args.incluir_consenso)
        validar(paths, args.mode, args.incluir_consenso)
        dest = guardar(paths, datetime.now(timezone.utc).date().isoformat(), args.mode)
        log.info('PUBLICADO %s', dest)


if __name__ == '__main__':
    main()

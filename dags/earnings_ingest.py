"""Yahoo Finance -> bronze inmutable -> plata validada.

Los helpers se pueden importar sin Airflow y sin yfinance: la fuente se importa
recién dentro de la descarga, así los parsers quedan testeables en frío.
No hay API key ni cuota diaria; el límite es throttling por IP.
Consultar README.md para configuración, límites y trazabilidad.
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
import random
import re
import shutil
import tempfile
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from pandas.tseries.offsets import MonthEnd

PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / 'include' / 'output'
BRONZE = OUTPUT_DIR / 'bronze'
PLATA = OUTPUT_DIR / 'silver'
UNIVERSO = PROJECT_DIR / 'include' / 'universo' / 'universo.csv'
BENCHMARK = 'SPY'
EXCLUIDOS = frozenset({'CMA'})  # ponytail: se filtra acá y no en el CSV, que lo regenera la cátedra.
ENDPOINTS = ('EARNINGS', 'PERFIL', 'FUNDAMENTALES', 'PRECIOS')
TTL = dict(zip(ENDPOINTS, (7, 30, 30, 1)))
# Yahoo publica dos registros contradictorios para un mismo anuncio en su tramo
# más viejo: medido, 14 de 63 tickers, y todos salvo uno son de 2002. Se corta la
# época entera en vez de arbitrar par por par (cuesta el 3,7% de los eventos).
PISO_EVENTOS = '2003-01-01'
# Cada worker espera RITMO_GLOBAL * workers, así el ritmo agregado contra Yahoo
# queda igual (~4 req/s) sin importar cuántos workers pida la corrida.
RITMO_GLOBAL_SEGUNDOS = 0.25
CATEGORICAS = ['ticker', 'sector', 'industry', 'exchange', 'report_time', 'reported_currency']
FECHAS = ['fiscal_quarter_end', 'reported_date', 'snapshot_date']
COLUMNAS_EVENTOS = ['ticker', 'sector', 'industry', 'exchange', 'fiscal_quarter_end',
                    'reported_date', 'report_time', 'reported_eps', 'consensus_eps',
                    'surprise', 'surprise_pct', 'reported_currency', 'total_assets',
                    'short_long_term_debt_total', 'total_current_assets',
                    'total_current_liabilities', 'operating_cashflow',
                    'capital_expenditures', 'dividend_payout', 'net_income',
                    'total_revenue', 'shares_outstanding',
                    'close_adj_lag3', 'close_adj_lag2', 'close_adj_lag1',
                    'close_adj_lead1', 'close_adj_lead2', 'close_adj_lead3',
                    'ma30', 'snapshot_date']
# Desplazamiento en ruedas respecto del día de reacción; ver agregar_ventana().
VENTANA = [('close_adj_lag3', -3), ('close_adj_lag2', -2), ('close_adj_lag1', -1),
           ('close_adj_lead1', 1), ('close_adj_lead2', 2), ('close_adj_lead3', 3)]
COLUMNAS_PRECIOS = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'close_adj']
PERFIL = {'sector': 'sector', 'industry': 'industry', 'exchange': 'exchange',
          'financialCurrency': 'reported_currency', 'sharesOutstanding': 'shares_outstanding'}
FUNDAMENTALES = {
    'balance': {'Total Assets': 'total_assets', 'Total Debt': 'short_long_term_debt_total',
                'Current Assets': 'total_current_assets',
                'Current Liabilities': 'total_current_liabilities'},
    'cashflow': {'Operating Cash Flow': 'operating_cashflow',
                 'Capital Expenditure': 'capital_expenditures',
                 'Cash Dividends Paid': 'dividend_payout',
                 'Net Income From Continuing Operations': 'net_income'},
    'income': {'Total Revenue': 'total_revenue'},
}
CAUSAS_NULOS = {
    **{c: 'Cobertura de la fuente o campo no informado; no se imputa.' for c in COLUMNAS_EVENTOS},
    **{c: 'Yahoo publica sólo 5-7 trimestres de estados contables; los eventos más '
          'viejos no tienen fundamental asociado.' for c in
       ('total_assets', 'short_long_term_debt_total', 'total_current_assets',
        'total_current_liabilities', 'operating_cashflow', 'capital_expenditures',
        'dividend_payout', 'net_income', 'total_revenue')},
    'dividend_payout': 'Puede indicar ausencia de dividendos o falta de cobertura; no se asume cero sin verificar.',
    'total_current_assets': 'Faltante estructural en financieras (JPM no abre corriente/no corriente) o falta de cobertura.',
    'total_current_liabilities': 'Faltante estructural en financieras o falta de cobertura.',
    'short_long_term_debt_total': 'Taxonomía sectorial o falta de cobertura.',
    'report_time': 'Anuncio en horario de rueda; sólo se marca pre-market (<9:30) y post-market (>=16:00).',
    **{c: 'La rueda cae fuera de la serie de precios del ticker (borde inicial o final).'
       for c, _ in [('close_adj_lag3', 0), ('close_adj_lag2', 0), ('close_adj_lag1', 0),
                    ('close_adj_lead1', 0), ('close_adj_lead2', 0), ('close_adj_lead3', 0)]},
    'ma30': 'Menos de 30 ruedas previas al evento en la serie del ticker.',
}
log = logging.getLogger(__name__)


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


def yf_get(funcion, ticker):
    """Una descarga. Devuelve payload JSON-serializable o None si no sirve."""
    import yfinance as yf  # Import diferido: los parsers y el CLI no lo necesitan.
    t = yf.Ticker(ticker)
    if funcion == 'EARNINGS':
        # limit=100 es el techo duro de Yahoo; alcanza para llegar a 2002-2008.
        df = t.get_earnings_dates(limit=100)
        if df is None or df.empty:
            return None
        return {'rows': [{'anuncio': i.isoformat(), 'estimate': num(r.get('EPS Estimate')),
                          'reported': num(r.get('Reported EPS')),
                          'surprise_pct': num(r.get('Surprise(%)'))}
                         for i, r in df.iterrows()]}
    if funcion == 'PERFIL':
        info = t.info or {}
        return {k: info.get(k) for k in PERFIL} if info.get('sector') or info.get('exchange') else None
    if funcion == 'FUNDAMENTALES':
        out = {}
        for name, attr in (('balance', 'quarterly_balance_sheet'),
                           ('cashflow', 'quarterly_cashflow'),
                           ('income', 'quarterly_income_stmt')):
            df = getattr(t, attr)
            out[name] = {} if df is None or df.empty else {
                c.date().isoformat(): {k: num(df.at[k, c]) for k in FUNDAMENTALES[name] if k in df.index}
                for c in df.columns}
        return out if any(out.values()) else None
    if funcion == 'PRECIOS':
        # ponytail: el bronze de precios pesa ~1 MB por ticker y snapshot en JSON;
        # si el volumen molesta, gzip en escribir_atomico o parquet por partición.
        h = t.history(period='max', auto_adjust=False)
        if h is None or h.empty:
            return None
        return {'rows': [{'date': i.date().isoformat(), 'open': num(r.Open), 'high': num(r.High),
                          'low': num(r.Low), 'close': num(r.Close), 'volume': num(r.Volume),
                          'close_adj': num(r['Adj Close']), 'split': num(r['Stock Splits'])}
                         for i, r in h.iterrows()]}
    raise ValueError(f'Endpoint desconocido: {funcion}.')


def es_util(funcion, payload):
    if not isinstance(payload, dict):
        return False
    if funcion == 'PERFIL':
        return any(texto(payload.get(k)) for k in ('sector', 'industry', 'exchange'))
    if funcion == 'FUNDAMENTALES':
        return any(isinstance(v, dict) and v for v in payload.values())
    rows = payload.get('rows')
    return isinstance(rows, list) and bool(rows) and all(isinstance(r, dict) for r in rows)


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


def leer_universo(mode='fullset'):
    if mode not in ('subset', 'fullset'):
        raise ValueError('mode debe ser subset o fullset.')
    with UNIVERSO.open(newline='', encoding='utf-8') as handle:
        rows = list(csv.DictReader(handle))
    tickers = [r['ticker'].strip().upper() for r in rows]
    if not tickers or len(set(tickers)) != len(tickers) or BENCHMARK in tickers:
        raise ValueError('Universo vacío, duplicado o con benchmark incluido.')
    for ticker in tickers:
        ruta_bronce('EARNINGS', ticker, '2000-01-01')
    tickers = [t for t in tickers if t not in EXCLUIDOS]
    if mode == 'subset':
        if 'NVDA' not in tickers:
            raise ValueError('El universo debe incluir NVDA para el subset de prueba.')
        return ['NVDA']
    return tickers


def pendientes(mode='fullset', force=False, snapshot=None):
    snapshot = snapshot or datetime.now(timezone.utc).date().isoformat()
    tickers = leer_universo(mode)
    items = []
    # Benchmark primero; luego cada empresa completa.
    for ticker in [BENCHMARK, *tickers]:
        endpoints = ('PRECIOS',) if ticker == BENCHMARK else ENDPOINTS
        for endpoint in endpoints:
            if ruta_bronce(endpoint, ticker, snapshot).exists() and not force:
                continue
            last = ultimo_snapshot(endpoint, ticker)
            if force or last is None or (date.fromisoformat(snapshot) - date.fromisoformat(last)).days >= TTL[endpoint]:
                items.append({'funcion': endpoint, 'ticker': ticker, 'snapshot': snapshot, 'force': force})
    # Primero completar el universo; después refrescar lo más antiguo.
    items.sort(key=lambda item: ultimo_snapshot(item['funcion'], item['ticker']) or '')
    return items


def armar_lotes(items, workers=8):
    """Un lote por worker: la cantidad de lotes ES el paralelismo real.

    max_active_tis_per_dag es atributo de parseo y no se puede variar por corrida,
    así que pone sólo el techo duro; quien decide cuántas descargas van en paralelo
    de verdad es cuántos lotes se arman.
    """
    if workers < 1:
        raise ValueError('workers debe ser >= 1.')
    if not items:
        return []
    size = math.ceil(len(items) / workers)
    return [{'nombre': f'lote_{i // size + 1:03d}', 'workers': workers,
             'items': items[i:i + size]} for i in range(0, len(items), size)]


def resumen_payload(funcion, payload):
    if funcion == 'PERFIL':
        return {'records': 1, 'fields': sorted(k for k, v in payload.items() if v is not None)}
    if funcion == 'FUNDAMENTALES':
        dates = sorted({d for tabla in payload.values() for d in tabla})
        return {'records': len(dates), 'date_min': dates[0] if dates else None,
                'date_max': dates[-1] if dates else None}
    rows = payload['rows']
    dates = sorted((fecha(r.get('date') or str(r.get('anuncio'))[:10]) or '') for r in rows)
    return {'records': len(rows), 'date_min': dates[0] or None, 'date_max': dates[-1] or None}


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
            # force vuelve a bajar aunque ya exista; sin force, el archivo del día
            # se respeta y por eso los reintentos del lote salen gratis.
            if path.exists() and not item.get('force'):
                paths.append(str(path))
                continue
            time.sleep(RITMO_GLOBAL_SEGUNDOS * lote.get('workers', 8))
            try:
                payload = yf_get(funcion, ticker)
            except Exception as exc:  # yfinance envuelve red, parseo y esquema en excepciones propias.
                log.warning('BRONZE_PENDIENTE endpoint=%s ticker=%s reason=%s', funcion, ticker, type(exc).__name__)
                transient += 1
                continue
            if not es_util(funcion, payload):
                log.warning('BRONZE_RECHAZADO endpoint=%s ticker=%s reason=respuesta_no_util', funcion, ticker)
                continue
            raw = json.dumps(payload, ensure_ascii=False)
            escribir_atomico(path, raw)
            registrar_bronce(path, funcion, ticker, snapshot, raw, payload)
            paths.append(str(path))
    if transient:
        raise RuntimeError(f'{transient} descargas con error de transporte; reintento conserva bronze existente.')
    return paths


def fase_fiscal(payload_fundamentales):
    """Mes de cierre del año fiscal, módulo 3. 0 = cierra en mes calendario (3/6/9/12).

    Medido sobre el universo: 23 de 149 empresas están desfasadas (NVDA, WMT y
    COST entre ellas), así que el trimestre no se puede derivar del mes solo.
    """
    payload = payload_fundamentales or {}
    fechas = sorted(payload.get('income') or payload.get('balance') or {})
    return date.fromisoformat(fechas[-1]).month % 3 if fechas else 0


def cierre_fiscal(reported_date, fase):
    """Último cierre de mes anterior al anuncio cuyo mes cae en la fase fiscal.

    Yahoo normaliza todos los cierres a fin de mes (nunca calendario 52/53
    semanas), así que la fase determina la fecha exacta. Validado contra las
    fechas reales de los estados: 388 de 388.
    """
    m = pd.Timestamp(reported_date).normalize().replace(day=1) - pd.Timedelta(days=1)
    while m.month % 3 != fase:
        m = m.replace(day=1) - pd.Timedelta(days=1)
    return (m + MonthEnd(0)).date().isoformat()


def consenso_real(reported, estimate, pct):
    """Despeja el estimate sin redondear desde Surprise(%).

    Yahoo publica el EPS a 2 decimales pero calcula Surprise(%) contra el valor
    completo. Sin esto la sorpresa da exactamente cero en el 12% de los eventos
    por puro redondeo, y refinar_plata los descarta creyendo que son relleno.
    """
    if reported is None or estimate is None or pct is None:
        return estimate
    d = (1 + pct / 100.0) if estimate > 0 else (1 - pct / 100.0)
    return reported / d if abs(d) > 1e-9 else estimate


def horario(anuncio):
    """pre-market / post-market según la hora del anuncio en horario del mercado."""
    ts = datetime.fromisoformat(anuncio)
    minutos = ts.hour * 60 + ts.minute
    if minutos < 9 * 60 + 30:
        return 'pre-market'
    return 'post-market' if minutos >= 16 * 60 else None


def elegir_una(filas):
    """Duplicados que sobreviven al piso de fecha: se conserva una al azar.

    Los dos registros son internamente consistentes y difieren en nivel, así que
    no hay señal para arbitrar. La semilla sale de la clave para que la elección
    sea la misma en cada corrida: con azar real el sha256 cambiaría en cada
    pasada y guardar() rechazaría la entrega por no coincidir con validacion.json.
    """
    if len(filas) == 1:
        return filas[0]
    clave = f"{filas[0]['ticker']}|{filas[0]['fiscal_quarter_end']}"
    log.warning('EVENTOS_CONTRADICTORIOS clave=%s candidatos=%s', clave,
                [(f['reported_date'], f['reported_eps']) for f in filas])
    return random.Random(clave).choice(filas)


def parse_eventos(ticker, payload, fase):
    result = {}
    discarded = 0
    for row in (payload or {}).get('rows', []):
        anuncio = str(row.get('anuncio') or '')
        rd = fecha(anuncio[:10])
        eps = num(row.get('reported'))
        pct = num(row.get('surprise_pct'))
        estimate = consenso_real(eps, num(row.get('estimate')), pct)
        if eps is None or estimate is None or abs(estimate) < 0.05 or not rd or rd < PISO_EVENTOS:
            discarded += 1
            continue
        fq = cierre_fiscal(rd, fase)
        result.setdefault(fq, []).append(
            {'ticker': ticker, 'fiscal_quarter_end': fq, 'reported_date': rd,
             'report_time': horario(anuncio), 'reported_eps': eps, 'consensus_eps': estimate,
             'surprise': eps - estimate, 'surprise_pct': pct})
    filas = [elegir_una(v) for v in result.values()]
    log.info('LIMPIEZA_EVENTOS ticker=%s descartados=%s conservados=%s', ticker, discarded, len(filas))
    return filas


def parse_fundamentales(ticker, payload):
    result = {}
    for tabla, mapping in FUNDAMENTALES.items():
        for fq, row in ((payload or {}).get(tabla) or {}).items():
            if not fecha(fq):
                continue
            result.setdefault(fq, {}).update({target: num(row.get(source))
                                              for source, target in mapping.items()})
    return result


def parse_precios(ticker, payload, snapshot):
    """OHLC ya viene ajustado por splits desde Yahoo; close_adj suma dividendos.

    Distinto de la versión con Alpha Vantage, donde close era el precio crudo y
    close_adj el ajuste por splits: Yahoo no publica la serie sin ajustar, así que
    ajustar de nuevo a mano ajustaría dos veces. El campo split queda en bronze
    como rastro auditable del ajuste que ya trae la fuente.

    La rueda del día de captura se descarta entera: Yahoo publica la sesión en
    curso como si fuera una barra diaria cerrada, con OHLC tomado de momentos
    distintos del feed (llega a dar high < open) y volumen parcial. Comparar
    contra el snapshot evita razonar sobre horarios de mercado y husos.
    """
    rows = (payload or {}).get('rows', [])
    if rows and not fecha(snapshot):
        raise ValueError(f'Snapshot inválido para los precios de {ticker}.')
    result = []
    for r in rows:
        d = fecha(r.get('date'))
        if not d:
            raise ValueError(f'Fecha de precio inválida para {ticker}.')
        if d >= snapshot:
            continue
        result.append({'ticker': ticker, 'date': d, 'open': num(r.get('open')),
                       'high': num(r.get('high')), 'low': num(r.get('low')),
                       'close': num(r.get('close')), 'volume': num(r.get('volume')),
                       'close_adj': num(r.get('close_adj'))})
    return result


def parse_calendario(filas_benchmark):
    return [{'date': d, 't': i} for i, d in enumerate(sorted({r['date'] for r in filas_benchmark if r['ticker'] == BENCHMARK}))]


def agregar_ventana(ev, pr, cal):
    """close_adj de las 3 ruedas previas y las 3 posteriores al evento, más MA30.

    El día cero no es el del anuncio sino el de la reacción del mercado: el mismo
    día si salió pre-market, el siguiente si salió post-market. Sin esa corrección
    la ventana queda corrida un día para las post-market y el día de la noticia
    cae del lado de las "previas". report_time nulo se trata como post-market, que
    es el caso más frecuente. Si el anuncio no cayó en rueda (fin de semana o
    feriado), la próxima rueda ya es la de reacción y no se corre nada.

    Yahoo no publica medias móviles históricas -info sólo trae fiftyDayAverage de
    hoy-, así que MA30 son las 30 ruedas hasta el día cero inclusive, calculadas
    sobre la misma serie: nunca mira hacia adelante.
    """
    if ev.empty or pr.empty or cal.empty:
        return ev
    t_de_fecha = pd.Series(cal.t.values, index=cal.date.astype(str).values)
    precios = pr.assign(t=pr.date.astype(str).map(t_de_fecha)).dropna(subset=['t'])
    precios = precios.sort_values(['ticker', 't'])
    precios['ma30'] = precios.groupby('ticker', sort=False).close_adj.transform(
        lambda serie: serie.rolling(30).mean())
    precios = precios.assign(t=precios.t.astype('int64')).set_index(['ticker', 't'])
    fechas = cal.date.astype(str).values
    pos = fechas.searchsorted(ev.reported_date.astype(str).values)
    dentro = pos < len(fechas)
    t_anuncio = pd.Series(pd.NA, index=ev.index, dtype='Int64')
    t_anuncio[dentro] = cal.t.values[pos[dentro]]
    en_rueda = pd.Series(False, index=ev.index)
    en_rueda[dentro] = fechas[pos[dentro]] == ev.reported_date.astype(str).values[dentro]
    corrimiento = ((ev.report_time != 'pre-market') & en_rueda).astype('int64')
    # -1 como centinela: no existe en el calendario, así que no matchea nada.
    t0 = (t_anuncio + corrimiento).fillna(-1).astype('int64')
    faltan = int((t0 < 0).sum())
    if faltan:
        log.warning('VENTANA_SIN_RUEDA eventos=%s; reported_date posterior al calendario', faltan)
    for columna, offset in VENTANA:
        idx = pd.MultiIndex.from_arrays([ev.ticker.values, (t0 + offset).values])
        ev[columna] = precios.close_adj.reindex(idx).values
    ev['ma30'] = precios.ma30.reindex(
        pd.MultiIndex.from_arrays([ev.ticker.values, t0.values])).values
    log.info('VENTANA_EVENTOS filas=%s sin_lead3=%s sin_ma30=%s',
             len(ev), int(ev.close_adj_lead3.isna().sum()), int(ev.ma30.isna().sum()))
    return ev


def revisar_bronce(mode='fullset'):
    """Distingue una carga aún incompleta de datos descargados con mala calidad.

    Requiere SPY y al menos una empresa con todos los endpoints. No exige todo el
    universo: los mínimos de volumen/cobertura siguen en validar().
    Un archivo presente pero corrupto no se trata como una descarga pendiente.
    """
    tickers = leer_universo(mode)
    missing, complete = [], []
    benchmark_ready = False
    for ticker in [BENCHMARK, *tickers]:
        endpoints = ['PRECIOS'] if ticker == BENCHMARK else list(ENDPOINTS)
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
    report = {'estado': 'listo_para_refinar' if ready else 'esperando_datos',
              'ready': ready, 'mode': mode,
              'checked_at': datetime.now(timezone.utc).isoformat(),
              'motivo': 'listo' if ready else 'bronze_incompleto',
              'tickers_completos': complete, 'benchmark_completo': benchmark_ready,
              'faltantes': missing}
    path = OUTPUT_DIR / 'state' / f'ingesta_{mode}.json'
    escribir_atomico(path, json.dumps(report, ensure_ascii=False, indent=2))
    if not ready:
        log.warning('INGESTA_PENDIENTE motivo=%s descargas_faltantes=%s informe=%s',
                    report['motivo'], len(missing), path)
        log.warning('BRONZE_FALTANTE %s', json.dumps(missing, ensure_ascii=False))
    return report


def escribir_por_stock(ev, pr):
    """Un par de tablas por stock, además de las generales.

    Misma data, sólo particionada: nadie que quiera mirar una empresa tiene que
    cargar 1,5 M de filas de precios. El ticker va en el nombre del archivo, así
    que cada uno se identifica solo si lo sacan de la carpeta. El calendario no se
    parte porque es uno solo -las ruedas del benchmark- e idéntico para todos.
    """
    destino = PLATA / 'por_stock'
    if destino.exists():
        # Se reescribe entera para que no queden restos de un universo anterior.
        shutil.rmtree(destino)
    for tabla, df in (('stocks', ev), ('precios', pr)):
        for ticker, grupo in df.groupby('ticker', sort=True):
            escribir_atomico(destino / f'silver_{tabla}_{ticker}.csv',
                             grupo.to_csv(index=False, date_format='%Y-%m-%d'))
    log.info('PLATA_POR_STOCK archivos=%s ruta=%s',
             len(list(destino.glob('*.csv'))) if destino.exists() else 0, destino)
    return destino


def refinar_plata(mode='fullset'):
    tickers = leer_universo(mode)
    events, prices = [], []
    lineage = {}
    for ticker in tickers:
        fundamentals_raw = leer_ultimo('FUNDAMENTALES', ticker)
        data = parse_eventos(ticker, leer_ultimo('EARNINGS', ticker), fase_fiscal(fundamentals_raw))
        fundamentals = parse_fundamentales(ticker, fundamentals_raw)
        perfil = leer_ultimo('PERFIL', ticker) or {}
        lineage[ticker] = {ep: ultimo_snapshot(ep, ticker) for ep in ENDPOINTS}
        for event in data:
            event.update(fundamentals.get(event['fiscal_quarter_end'], {}))
            event.update({target: texto(perfil.get(source)) for source, target in PERFIL.items()
                          if target != 'shares_outstanding'})
            event['shares_outstanding'] = num(perfil.get('sharesOutstanding'))
            event['snapshot_date'] = ultimo_snapshot('EARNINGS', ticker)
            events.append(event)
    ev = pd.DataFrame(events, columns=COLUMNAS_EVENTOS)
    ev = ev.drop_duplicates(['ticker', 'fiscal_quarter_end'], keep='first')
    zero_fraction = float(ev['surprise'].eq(0).mean()) if len(ev) else 0
    log.info('SORPRESA_CERO proporcion=%.6f umbral=0.03', zero_fraction)
    if zero_fraction > 0.03:
        log.warning('SORPRESA_CERO descartando=%s por regla del plan; posible relleno, no confirmado', ev['surprise'].eq(0).sum())
        ev = ev.loc[~ev['surprise'].eq(0)].copy()
    for ticker in [*tickers, BENCHMARK]:
        prices.extend(parse_precios(ticker, leer_ultimo('PRECIOS', ticker),
                                    ultimo_snapshot('PRECIOS', ticker)))
    pr = pd.DataFrame(prices, columns=COLUMNAS_PRECIOS).drop_duplicates(['ticker', 'date'])
    cal = pd.DataFrame(parse_calendario(prices), columns=['date', 't'])
    # La ventana se arma antes de tipar: necesita reported_date como texto ISO.
    ev = agregar_ventana(ev, pr, cal)
    for c in COLUMNAS_EVENTOS:
        if c in FECHAS:
            ev[c] = pd.to_datetime(ev[c], errors='raise')
        elif c in CATEGORICAS:
            ev[c] = ev[c].astype('string')
        else:
            ev[c] = pd.to_numeric(ev[c], errors='coerce').astype('float64')
    PLATA.mkdir(parents=True, exist_ok=True)
    destino = escribir_por_stock(ev.sort_values(['ticker', 'fiscal_quarter_end']),
                                 pr.sort_values(['ticker', 'date']))
    escribir_atomico(PLATA / 'linaje.json', json.dumps(lineage, indent=2))
    return str(destino)


def consolidar():
    """Junta las particiones por ticker en las tres tablas generales.

    Concatena el texto tal cual en vez de re-parsear a DataFrame: las partes ya
    son la plata escrita, y un round-trip por pandas puede reformatear floats.
    El orden alfabético del glob reproduce el sort por ticker de refinar_plata.
    """
    origen = PLATA / 'por_stock'
    paths = {}
    for name in ('stocks', 'precios'):
        partes = sorted(origen.glob(f'silver_{name}_*.csv'))
        if not partes:
            raise ValueError(f'No hay particiones de silver_{name}; ¿corrió refinar_plata?')
        lineas = []
        for i, parte in enumerate(partes):
            propias = parte.read_text(encoding='utf-8').splitlines(keepends=True)
            if propias and not propias[-1].endswith('\n'):
                propias[-1] += '\n'
            lineas.extend(propias if i == 0 else propias[1:])
        path = PLATA / f'silver_{name}.csv'
        escribir_atomico(path, ''.join(lineas))
        paths[name] = str(path)
        log.info('CONSOLIDADO tabla=%s particiones=%s filas=%s', name, len(partes), len(lineas) - 1)
    # El calendario no se particiona: es uno solo, las ruedas del benchmark.
    ruedas = pd.read_csv(origen / f'silver_precios_{BENCHMARK}.csv', usecols=['date'])
    cal = pd.DataFrame(parse_calendario([{'ticker': BENCHMARK, 'date': str(d)} for d in ruedas.date]),
                       columns=['date', 't'])
    path = PLATA / 'silver_calendario.csv'
    escribir_atomico(path, cal.to_csv(index=False))
    paths['calendario'] = str(path)
    log.info('CONSOLIDADO tabla=calendario ruedas=%s', len(cal))
    return paths


def validar(paths, mode='fullset'):
    if mode not in ('subset', 'fullset'):
        raise ValueError('mode inválido.')
    ev, pr, cal = (pd.read_csv(paths[k]) for k in ('stocks', 'precios', 'calendario'))
    problemas = []
    if list(ev.columns) != COLUMNAS_EVENTOS or list(pr.columns) != COLUMNAS_PRECIOS or list(cal.columns) != ['date', 't']:
        raise ValueError('Esquema de plata inesperado.')
    perfil = {c: {'null_count': int(ev[c].isna().sum()), 'null_fraction': float(ev[c].isna().mean()) if len(ev) else 1.0,
                  'causa': CAUSAS_NULOS[c]} for c in ev}
    log.info('NULOS_POR_COLUMNA %s', json.dumps(perfil, ensure_ascii=False))
    minimo = 1001 if mode == 'fullset' else 1
    if len(ev) < minimo:
        problemas.append(f'silver_stocks tiene {len(ev)} filas, mínimo {minimo} ({mode})')
    for name, df, keys in [('stocks', ev, ['ticker', 'fiscal_quarter_end']),
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
    if ev.reported_date.min() < pd.Timestamp(PISO_EVENTOS):
        problemas.append(f'Eventos anteriores al piso {PISO_EVENTOS}')
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
        problemas.append('Precios/volumen nulos o inválidos')
    if numeric_prices[['open', 'high', 'low', 'close', 'close_adj']].le(0).any().any() or numeric_prices.volume.lt(0).any():
        problemas.append('Precios no positivos o volumen negativo')
    if (numeric_prices.low > numeric_prices[['open', 'close', 'high']].min(axis=1)).any() or (numeric_prices.high < numeric_prices[['open', 'close', 'low']].max(axis=1)).any():
        problemas.append('OHLC incoherente')
    expected_dates = sorted(set(pr.loc[pr.ticker == BENCHMARK, 'date']))
    if cal.date.tolist() != expected_dates or cal.t.tolist() != list(range(len(cal))):
        problemas.append('Calendario no coincide con las ruedas del benchmark o t no es consecutivo')
    # La partición por ticker es la misma data: si no reconstruye las generales,
    # quedó a medio escribir y no se publica.
    por_stock = PLATA / 'por_stock'
    for tabla, df in (('stocks', ev), ('precios', pr)):
        for ticker, esperadas in df.groupby('ticker').size().items():
            archivo = por_stock / f'silver_{tabla}_{ticker}.csv'
            if not archivo.exists():
                problemas.append(f'falta la partición {archivo.name}')
            elif sum(1 for _ in archivo.open(encoding='utf-8')) - 1 != esperadas:
                problemas.append(f'{archivo.name}: filas distintas de la tabla general')
    coverage = {'eventos_fuera_rango_benchmark': int((~ev.reported_date.between(pd.to_datetime(expected_dates[0]), pd.to_datetime(expected_dates[-1]))).sum()) if expected_dates else len(ev),
                'tickers_sin_precios': missing}
    report = {'valid': not problemas, 'mode': mode,
              'counts': {'stocks': len(ev), 'precios': len(pr), 'calendario': len(cal)},
              'null_profile': perfil, 'coverage': coverage, 'problems': problemas,
              'sha256': {k: hashlib.sha256(Path(p).read_bytes()).hexdigest() for k, p in paths.items()}}
    escribir_atomico(PLATA / 'validacion.json', json.dumps(report, ensure_ascii=False, indent=2))
    log.info('COBERTURA %s', json.dumps(coverage))
    if problemas:
        raise ValueError('Validación fallida:\n  - ' + '\n  - '.join(problemas))
    return paths


def guardar(paths, fecha_entrega, mode='fullset'):
    if not fecha(fecha_entrega):
        raise ValueError('Fecha de entrega inválida.')
    report = json.loads((PLATA / 'validacion.json').read_text())
    hashes = {k: hashlib.sha256(Path(p).read_bytes()).hexdigest() for k, p in paths.items()}
    if not report['valid'] or report['mode'] != mode or report['sha256'] != hashes:
        raise ValueError('Sólo se publica la plata validada sin modificaciones posteriores.')
    prefix = 'entrega' if mode == 'fullset' else 'prueba_subset'
    dest = OUTPUT_DIR / f'{prefix}_{fecha_entrega}'
    with bloqueo(OUTPUT_DIR / 'state' / 'publicar.lock'):
        temp = Path(tempfile.mkdtemp(dir=OUTPUT_DIR, prefix='.entrega-'))
        # La entrega previa se aparta y recién después entra la nueva: os.replace no
        # pisa un directorio con contenido. ponytail: la ventana sin entrega dura
        # dos renames; si molesta, versionar el nombre en vez de sobrescribir.
        previa = dest.with_name(f'.previa-{dest.name}')
        try:
            for p in paths.values():
                shutil.copy2(p, temp / Path(p).name)
            for name in ('validacion.json', 'linaje.json'):
                shutil.copy2(PLATA / name, temp / name)
            shutil.copytree(PLATA / 'por_stock', temp / 'por_stock')
            if previa.exists():
                shutil.rmtree(previa)
            if dest.exists():
                os.replace(dest, previa)
            os.replace(temp, dest)
        finally:
            for resto in (temp, previa):
                if resto.exists():
                    shutil.rmtree(resto)
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
             'workers': Param(8, type='integer', minimum=1, maximum=8,
                              description='Descargas en paralelo. Se arma un lote por worker. '
                                          'El máximo es 8, el techo fijado en bajar_bronce.'),
             'mode': Param('subset', enum=['subset', 'fullset']),
             'force': Param(False, type='boolean',
                            description='Vuelve a bajar el bronze aunque ya esté en disco.'),
             'solo_plata': Param(False, type='boolean',
                                 description='Rearma la plata desde el bronze existente, sin red.'),
         })
    def earnings_pipeline():
        @task.sensor(poke_interval=300, timeout=1800, mode='reschedule', soft_fail=True)
        def esperar_fuente():
            p = get_current_context()['params']
            if p['solo_plata'] or not pendientes(p['mode'], p['force']):
                return PokeReturnValue(is_done=True, xcom_value={'ok': True})
            try:
                return PokeReturnValue(is_done=es_util('PRECIOS', yf_get('PRECIOS', BENCHMARK)),
                                       xcom_value={'ok': True})
            except Exception:
                return PokeReturnValue(is_done=False)

        @task.branch(trigger_rule='all_done')
        def decidir_rama():
            ctx = get_current_context()
            p = ctx['params']
            source = ctx['ti'].xcom_pull(task_ids='esperar_fuente') or {}
            corte = 'solo_plata' if p['solo_plata'] else ('' if source.get('ok') else 'fuente_no_disponible')
            items = [] if corte else pendientes(p['mode'], p['force'])
            log.info('DECIDIR_RAMA motivo=%s pendientes=%s',
                     corte or ('hay_pendientes' if items else 'sin_pendientes'), len(items))
            # Sin pendientes se va por sin_bronce y la plata igual se rearma con lo
            # que ya hay en bronze; un short_circuit acá saltearía también plata,
            # validación y entrega, ignorando sus trigger_rule.
            return 'armar_lotes' if items else 'sin_bronce'

        @task(task_id='armar_lotes')
        def task_lotes():
            p = get_current_context()['params']
            lotes = armar_lotes(pendientes(p['mode'], p['force']), p['workers'])
            log.info('LOTES workers=%s lotes=%s items=%s', p['workers'], len(lotes),
                     sum(len(l['items']) for l in lotes))
            return lotes

        # Techo duro: es atributo de parseo, no se puede variar por corrida. El Param
        # workers mueve el paralelismo real vía cantidad de lotes, hasta este número.
        @task(task_id='bajar_bronce', map_index_template="{{ task.op_kwargs['lote']['nombre'] }}", max_active_tis_per_dag=8)
        def task_bronce(lote):
            return bajar_bronce(lote)

        @task
        def sin_bronce():
            log.info('SIN_BRONCE: reprocesando snapshots locales por solo_plata o caída de la fuente.')

        @task(task_id='refinar_plata', trigger_rule='none_failed_min_one_success')
        def task_plata():
            p = get_current_context()['params']
            report = revisar_bronce(p['mode'])
            if not report['ready']:
                raise AirflowSkipException(
                    f"Ingesta pendiente ({report['motivo']}): faltan {len(report['faltantes'])} descargas. "
                    'Se omiten plata, validación y entrega; consultar state/ingesta_'
                    f"{p['mode']}.json.")
            return refinar_plata(p['mode'])

        @task(task_id='consolidar')
        def task_consolidar(particiones):
            log.info('CONSOLIDANDO desde %s', particiones)
            return consolidar()

        @task(task_id='validar', retries=0)
        def task_validar(paths):
            p = get_current_context()['params']
            try:
                return validar(paths, p['mode'])
            except ValueError as exc:
                # Reintentar los mismos CSV no cambia su calidad ni su cobertura.
                raise AirflowFailException(str(exc)) from None

        @task(task_id='guardar')
        def task_guardar(paths):
            ctx = get_current_context()
            return guardar(paths, fecha_corrida(ctx['dag_run']), ctx['params']['mode'])

        sensor = esperar_fuente()
        branch = decidir_rama()
        batches = task_lotes()
        bronze = task_bronce.expand(lote=batches)
        fallback = sin_bronce()
        silver = task_plata()
        sensor >> branch >> [batches, fallback]
        [bronze, fallback] >> silver
        task_guardar(task_validar(task_consolidar(silver)))

    earnings_ingest = earnings_pipeline()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--solo-plata', action='store_true')
    parser.add_argument('--mode', choices=['subset', 'fullset'], default='subset')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--plan', action='store_true', help='Mostrar pendientes sin red ni escrituras.')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if args.plan:
        items = pendientes(args.mode, args.force)
        print(json.dumps({'pendientes': len(items),
                          'lotes': armar_lotes(items, args.workers)}, indent=2))
        return
    # CLI y Airflow no se deben ejecutar a la vez sobre el mismo output.
    with bloqueo(OUTPUT_DIR / 'state' / 'cli.lock'):
        if not args.solo_plata:
            items = pendientes(args.mode, args.force)
            if not items:
                log.info('Sin descargas pendientes; usar --solo-plata para reprocesar.')
                return
            # El CLI corre los lotes en serie; el paralelismo es cosa de Airflow.
            for lote in armar_lotes(items, args.workers):
                bajar_bronce(lote)
        if not revisar_bronce(args.mode)['ready']:
            raise SystemExit(2)  # Ingesta pendiente; sin traceback ni entrega vacía.
        refinar_plata(args.mode)
        paths = consolidar()
        validar(paths, args.mode)
        dest = guardar(paths, datetime.now(timezone.utc).date().isoformat(), args.mode)
        log.info('PUBLICADO %s', dest)


if __name__ == '__main__':
    main()

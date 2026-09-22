"""Pruebas offline: respuestas sintéticas, nunca llaman a Alpha Vantage."""
import importlib.util
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

SPEC = importlib.util.spec_from_file_location('earnings_module', Path(__file__).resolve().parents[1] / 'dags' / 'earnings_ingest.py')
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


@pytest.fixture(autouse=True)
def sandbox(tmp_path, monkeypatch):
    monkeypatch.setattr(m, 'OUTPUT_DIR', tmp_path / 'output')
    monkeypatch.setattr(m, 'BRONZE', tmp_path / 'output' / 'bronze')
    monkeypatch.setattr(m, 'PLATA', tmp_path / 'output' / 'silver')
    universe = tmp_path / 'universo.csv'
    universe.write_text('ticker,sector\nNVDA,Technology\n')
    monkeypatch.setattr(m, 'UNIVERSO', universe)
    monkeypatch.setattr(m, 'configurar', lambda: None)
    monkeypatch.setenv('ALPHAVANTAGE_API_KEYS', '["secret-one", "secret-two"]')
    monkeypatch.setenv('AV_MIN_REQUEST_INTERVAL_SECONDS', '0')
    def forbidden(*a, **kw):
        raise AssertionError('Las pruebas no deben acceder a la red')
    monkeypatch.setattr(m.requests, 'get', forbidden)


def earnings(fq='2026-03-31', rd='2026-04-22', **overrides):
    return {'fiscalDateEnding': fq, 'reportedDate': rd, 'reportedEPS': '1.2',
            'estimatedEPS': '1.0', 'surprise': '0.2', 'surprisePercentage': '20',
            'reportTime': 'post-market', **overrides}


def prices(values):
    return {'Time Series (Daily)': {d: {'1. open': str(close), '2. high': str(close+1),
                                        '3. low': str(close-1), '4. close': str(close),
                                        '5. volume': '1000'} for d, close in values.items()}}


def store(endpoint, payload, ticker='NVDA', snapshot='2026-09-08'):
    path = m.ruta_bronce(endpoint, ticker, snapshot)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))
    return path


def fill_ticker(ticker='NVDA', periods=2):
    quarters = pd.date_range(end='2026-03-31', periods=periods, freq='QE')
    events = [earnings(d.date().isoformat(), (d + pd.Timedelta(days=25)).date().isoformat()) for d in quarters]
    store('EARNINGS', {'quarterlyEarnings': events}, ticker)
    store('OVERVIEW', {'Symbol': ticker, 'Sector': 'TECHNOLOGY', 'Industry': 'SOFTWARE',
                      'Exchange': 'NYSE', 'SharesOutstanding': '100000'}, ticker)
    for endpoint, mapping in m.FUNDAMENTALES.items():
        rows = [{'fiscalDateEnding': d.date().isoformat(),
                 **{source: 'USD' if source == 'reportedCurrency' else '100' for source in mapping}} for d in quarters]
        store(endpoint, {'quarterlyReports': rows}, ticker)
    store('SPLITS', {'data': []}, ticker)
    store('TIME_SERIES_DAILY', prices({'2026-04-23': 100, '2026-04-24': 101, '2026-04-27': 102}), ticker)


def fill_benchmark():
    store('SPLITS', {'data': []}, 'SPY')
    store('TIME_SERIES_DAILY', prices({'2026-04-23': 500, '2026-04-24': 501, '2026-04-27': 502}), 'SPY')


@pytest.mark.parametrize('value', [None, 'None', 'N/A', '', 'nan', float('inf'), '-inf'])
def test_missing_numbers(value):
    assert m.num(value) is None


@pytest.mark.parametrize('raw', ['{}', '"key"', '[]', '[null]', '[""]', '[1]', 'not-json'])
def test_env_array_validation(raw, monkeypatch):
    monkeypatch.setenv('ALPHAVANTAGE_API_KEYS', raw)
    with pytest.raises(ValueError):
        m.claves_api()


def test_rotate_200_daily_limit_persist_and_no_secrets(monkeypatch, caplog):
    calls = []
    def get(endpoint, key, **params):
        calls.append(key)
        if key == 'secret-one':
            return '{}', {'Information': 'Our standard API rate limit is 25 requests per day.'}
        return '{"data":[]}', {'data': []}
    monkeypatch.setattr(m, 'av_get', get)
    caplog.set_level(logging.INFO)
    assert m.av_get_rotando('SPLITS')[1] == {'data': []}
    assert m.av_get_rotando('SPLITS')[1] == {'data': []}
    assert calls == ['secret-one', 'secret-two', 'secret-two']
    saved = (m.OUTPUT_DIR / 'state' / 'api_keys.json').read_text()
    assert all(k not in caplog.text + saved for k in ('secret-one', 'secret-two'))


def test_shared_rotation_between_concurrent_batches(monkeypatch):
    calls = []
    def get(endpoint, key, **params):
        calls.append(key)
        return ('{}', {'Note': 'API call frequency per day exceeded'}) if key == 'secret-one' else ('{"data":[]}', {'data': []})
    monkeypatch.setattr(m, 'av_get', get)
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: m.av_get_rotando('SPLITS'), range(4)))
    assert all(p == {'data': []} for _, p in results)
    assert calls.count('secret-one') == 1
    assert calls.count('secret-two') == 4


def test_exhausted_keys_bounded_and_resume_after_cooldown(monkeypatch):
    calls = []
    monkeypatch.setattr(m.time, 'time', lambda: 1000)
    def limited(endpoint, key, **params):
        calls.append(key)
        return '{}', {'Note': 'API call frequency exceeded, 5 calls per minute'}
    monkeypatch.setattr(m, 'av_get', limited)
    assert m.av_get_rotando('SPLITS')[1]['_quota_exhausted']
    assert m.av_get_rotando('SPLITS')[1]['_quota_exhausted']
    assert len(calls) == 2
    monkeypatch.setattr(m.time, 'time', lambda: 1100)
    monkeypatch.setattr(m, 'av_get', lambda *a, **kw: ('{"data":[]}', {'data': []}))
    assert m.av_get_rotando('SPLITS')[1] == {'data': []}


def test_premium_does_not_exhaust_or_rotate(monkeypatch):
    calls = []
    def premium(endpoint, key, **params):
        calls.append(key)
        return '{}', {'Information': 'This is a premium endpoint. Subscribe for 75 requests per minute.'}
    monkeypatch.setattr(m, 'av_get', premium)
    assert not m.es_util('SPLITS', m.av_get_rotando('SPLITS')[1])
    assert calls == ['secret-one']


def test_http429_rotates_using_retry_after(monkeypatch):
    calls = []
    def get(url, **kw):
        calls.append(kw['params']['apikey'])
        if len(calls) == 1:
            return SimpleNamespace(status_code=429, text='too many', headers={'Retry-After': '120'})
        return SimpleNamespace(status_code=200, text='{"data":[]}', json=lambda: {'data': []})
    monkeypatch.setattr(m.requests, 'get', get)
    assert m.av_get_rotando('SPLITS')[1] == {'data': []}
    assert calls == ['secret-one', 'secret-two']
    assert m.espera_cuota({'Note': 'HTTP 429 rate limit', '_retry_after': '120'}) == 120
    assert m.espera_cuota({'Note': 'HTTP 429 rate limit', '_retry_after': 'Thu, 01 Jan 1970 00:20:00 GMT'}, now=1000) == 200


def test_transport_errors_do_not_leak_key(monkeypatch):
    def error(*a, **kw):
        raise m.requests.ConnectionError('https://example?apikey=secret-one')
    monkeypatch.setattr(m.requests, 'get', error)
    with pytest.raises(RuntimeError) as exc:
        m.av_get('EARNINGS', 'secret-one')
    assert 'secret-one' not in str(exc.value)
    assert exc.value.__suppress_context__


@pytest.mark.parametrize('payload', [{'Information': 'rate limit'}, {'Error Message': 'invalid'}, {'Note': 'quota'}, {}, {'data': [3]}])
def test_errors_not_useful(payload):
    assert not m.es_util('SPLITS', payload)


def test_bronze_raw_log_and_idempotency(monkeypatch, caplog):
    raw = '{\n  "symbol": "NVDA", "data": []\n}'
    calls = []
    def get(*args, **kwargs):
        calls.append(1)
        return raw, json.loads(raw)
    monkeypatch.setattr(m, 'av_get_rotando', get)
    item = {'funcion': 'SPLITS', 'ticker': 'NVDA', 'snapshot': '2026-09-08', 'outputsize': 'full'}
    caplog.set_level(logging.INFO)
    first = m.bajar_bronce({'items': [item]})
    assert m.bajar_bronce({'items': [item]}) == first
    assert Path(first[0]).read_text() == raw
    assert len(calls) == 1
    lines = (m.OUTPUT_DIR / 'logs' / 'bronze.jsonl').read_text().splitlines()
    entry = json.loads(lines[0])
    assert len(lines) == 1 and entry['records'] == 0 and entry['ticker'] == 'NVDA'
    assert entry['sha256'] == m.hashlib.sha256(raw.encode()).hexdigest()
    assert 'BRONZE_OBTENIDO' in caplog.text


def test_bad_response_remains_pending(monkeypatch):
    monkeypatch.setattr(m, 'av_get_rotando', lambda *a, **k: ('{}', {'Information': 'premium endpoint'}))
    item = {'funcion': 'SPLITS', 'ticker': 'NVDA', 'snapshot': '2026-09-08', 'outputsize': 'full'}
    assert m.bajar_bronce({'items': [item]}) == []
    assert not m.ruta_bronce('SPLITS', 'NVDA', '2026-09-08').exists()


def test_wrong_symbol_not_saved(monkeypatch):
    monkeypatch.setattr(m, 'av_get_rotando', lambda *a, **k: ('{}', {'symbol': 'AAPL', 'data': []}))
    assert m.bajar_bronce({'items': [{'funcion': 'SPLITS', 'ticker': 'NVDA', 'snapshot': '2026-09-08'}]}) == []


def test_transient_retry_preserves_successes(monkeypatch):
    def get(endpoint, **params):
        if endpoint == 'EARNINGS':
            raise RuntimeError('transport')
        return '{"data":[]}', {'data': []}
    monkeypatch.setattr(m, 'av_get_rotando', get)
    items = [{'funcion': ep, 'ticker': 'NVDA', 'snapshot': '2026-09-08'} for ep in ['SPLITS', 'EARNINGS']]
    with pytest.raises(RuntimeError, match='reintento'):
        m.bajar_bronce({'items': items})
    assert m.ruta_bronce('SPLITS', 'NVDA', '2026-09-08').exists()
    assert not m.ruta_bronce('EARNINGS', 'NVDA', '2026-09-08').exists()


def test_ttl_force_same_day_and_missing_first():
    store('EARNINGS', {'quarterlyEarnings': []}, snapshot='2026-09-01')
    assert not any(i['funcion'] == 'EARNINGS' for i in m.pendientes(snapshot='2026-09-08'))
    assert any(i['funcion'] == 'EARNINGS' for i in m.pendientes(snapshot='2026-09-08', force=True))
    assert not any(i['funcion'] == 'EARNINGS' for i in m.pendientes(snapshot='2026-09-01', force=True))
    assert any(i['funcion'] == 'EARNINGS' for i in m.pendientes(snapshot='2026-11-20'))
    assert m.pendientes(snapshot='2026-11-20')[-1]['funcion'] == 'EARNINGS'


def test_event_cleaning_and_annual_exclusion():
    rows = [earnings(), earnings(estimatedEPS='None'), earnings(estimatedEPS='0.01'),
            earnings(reportedEPS='None'), earnings(reportTime='unknown'), earnings(rd='bad')]
    result = m.parse_eventos('NVDA', {'quarterlyEarnings': rows, 'annualEarnings': [earnings()]})
    assert len(result) == 2
    assert result[1]['report_time'] is None
    assert result[0]['fiscal_quarter_end'] == '2026-03-31'
    assert result[0]['reported_date'] == '2026-04-22'


def test_fundamental_join_by_fiscal_end():
    result = m.parse_fundamentales('NVDA', {
        'BALANCE_SHEET': {'quarterlyReports': [{'fiscalDateEnding': '2026-03-31', 'totalAssets': '100', 'reportedCurrency': 'USD'}]},
        'CASH_FLOW': {'quarterlyReports': [{'fiscalDateEnding': '2026-03-31', 'capitalExpenditures': '4'}, {'fiscalDateEnding': '2025-12-31', 'netIncome': '8'}]}})
    assert result['2026-03-31']['capital_expenditures'] == 4
    assert result['2026-03-31']['total_assets'] == 100
    assert result['2025-12-31']['net_income'] == 8


def test_split_adjustment_accumulates_and_excludes_effective_day():
    p = prices({'2020-08-28': 400, '2020-08-31': 100, '2022-01-03': 50})
    rows = m.parse_precios('AAPL', [('2026-01-01', p)], {'data': [
        {'effective_date': '2020-08-31', 'split_factor': '4'},
        {'effective_date': '2022-01-03', 'split_factor': '2'}]})
    assert [r['close_adj'] for r in rows] == [50, 50, 50]
    reverse = m.parse_precios('TEST', [('2026-01-01', prices({'2020-01-01': 1}))],
                             {'data': [{'effective_date': '2020-01-02', 'split_factor': '0.1'}]})
    assert reverse[0]['close_adj'] == 10


def test_price_snapshots_keep_newest_overlap_and_old_history():
    rows = m.parse_precios('NVDA', [('2026-09-01', prices({'2026-08-28': 100, '2026-08-31': 101})),
                                  ('2026-09-02', prices({'2026-08-31': 102, '2026-09-01': 103}))], {'data': []})
    assert [r['close'] for r in rows] == [100, 102, 103]


def test_no_splits_is_not_same_as_empty_splits():
    args = ('NVDA', [('2026-09-01', prices({'2026-08-31': 100}))])
    assert m.parse_precios(*args, None)[0]['close_adj'] is None
    assert m.parse_precios(*args, {'data': []})[0]['close_adj'] == 100
    with pytest.raises(ValueError, match='Split inválido'):
        m.parse_precios(*args, {'data': [{'effective_date': '2026-01-01', 'split_factor': 0}]})


def test_calendar_only_benchmark():
    assert m.parse_calendario([{'ticker': 'NVDA', 'date': '2026-04-25'}, {'ticker': 'SPY', 'date': '2026-04-24'},
                              {'ticker': 'SPY', 'date': '2026-04-23'}]) == [{'date': '2026-04-23', 't': 0}, {'date': '2026-04-24', 't': 1}]


def test_consensus_strict_prior_date_same_quarter_not_annual():
    fill_ticker(periods=1)
    fill_benchmark()
    for snap, value in [('2026-04-01', '1'), ('2026-04-24', '2'), ('2026-04-25', '3'), ('2026-04-26', '4')]:
        store('EARNINGS_ESTIMATES', {'estimates': [
            {'date': '2026-03-31', 'horizon': 'fiscal quarter', **{c: value for c in m.CONSENSO}},
            {'date': '2026-03-31', 'horizon': 'fiscal year', **{c: '999' for c in m.CONSENSO}},
            {'date': '2026-06-30', 'horizon': 'fiscal quarter', **{c: '888' for c in m.CONSENSO}},
        ]}, snapshot=snap)
    paths = m.refinar_plata('subset', True)
    ev = pd.read_csv(paths['eventos'])
    assert ev.eps_estimate_high.tolist() == [2]
    m.validar(paths, 'subset', True)


def test_latest_earnings_snapshot_dedup_and_null_consensus():
    fill_ticker(periods=1)
    fill_benchmark()
    store('EARNINGS', {'quarterlyEarnings': [earnings(rd='2026-04-23')]}, snapshot='2026-09-01')
    store('EARNINGS', {'quarterlyEarnings': [earnings(rd='2026-04-24'), earnings(rd='2026-04-25')]})
    paths = m.refinar_plata('subset', True)
    ev = pd.read_csv(paths['eventos'])
    assert len(ev) == 1 and ev.reported_date.iloc[0] == '2026-04-24'
    assert ev[m.CONSENSO].isna().all().all()
    with pytest.raises(ValueError, match='100% nulas'):
        m.validar(paths, 'subset', True)


def test_zero_surprise_global_rule():
    fill_ticker(periods=2)
    store('EARNINGS', {'quarterlyEarnings': [earnings(), earnings(fq='2025-12-31', rd='2026-01-25', surprise='0')]})
    ev = pd.read_csv(m.refinar_plata('subset')['eventos'])
    assert len(ev) == 1 and ev.surprise.iloc[0] == 0.2


def test_offline_end_to_end_and_publication_guard():
    fill_ticker()
    fill_benchmark()
    paths = m.refinar_plata('subset')
    assert len(pd.read_csv(paths['eventos']).columns) == 23
    m.validar(paths, 'subset')
    dest = Path(m.guardar(paths, '2026-09-08', 'subset'))
    assert dest.name == 'prueba_subset_2026-09-08'
    assert {p.name for p in dest.iterdir()} == {'slv_eventos.csv', 'slv_precios.csv', 'slv_calendario.csv', 'linaje.json', 'validacion.json'}
    assert m.guardar(paths, '2026-09-08', 'subset') == str(dest)
    Path(paths['eventos']).write_text(Path(paths['eventos']).read_text() + '\n')
    with pytest.raises(ValueError, match='sin modificaciones'):
        m.guardar(paths, '2026-09-08', 'subset')


def test_full_volume_and_missing_benchmark_fail():
    fill_ticker()
    paths = m.refinar_plata('full')
    with pytest.raises(ValueError, match='mínimo 1001'):
        m.validar(paths, 'full')
    report = json.loads((m.PLATA / 'validacion.json').read_text())
    assert not report['valid'] and any('benchmark' in p for p in report['problems'])
    assert not list(m.OUTPUT_DIR.glob('entrega_*'))


def test_full_1050_event_delivery():
    tickers = [f'T{i}' for i in range(10)]
    m.UNIVERSO.write_text('ticker,sector\n' + ''.join(f'{t},Technology\n' for t in tickers))
    for t in tickers:
        fill_ticker(t, 105)
    fill_benchmark()
    paths = m.refinar_plata('full')
    m.validar(paths, 'full')
    dest = Path(m.guardar(paths, '2026-09-08'))
    assert len(pd.read_csv(dest / 'slv_eventos.csv')) == 1050
    assert json.loads((dest / 'validacion.json').read_text())['valid']


@pytest.mark.parametrize('mutation,match', [('lag', 'Coherencia temporal'), ('duplicate', 'clave nula o repetida'),
                                          ('calendar', 'Calendario no coincide'), ('price', 'Precios no positivos'),
                                          ('empty', '100% nulas')])
def test_validation_domain_failures(mutation, match):
    fill_ticker()
    fill_benchmark()
    paths = m.refinar_plata('subset')
    ev, pr, cal = (pd.read_csv(paths[k]) for k in ('eventos', 'precios', 'calendario'))
    if mutation == 'lag':
        ev['reported_date'] = ev['fiscal_quarter_end']
    elif mutation == 'duplicate':
        ev = pd.concat([ev, ev.iloc[:1]])
    elif mutation == 'calendar':
        cal.loc[0, 't'] = 10
    elif mutation == 'price':
        pr.loc[0, 'close_adj'] = -1
    elif mutation == 'empty':
        ev['sector'] = None
    for key, df in [('eventos', ev), ('precios', pr), ('calendario', cal)]:
        df.to_csv(paths[key], index=False)
    with pytest.raises(ValueError, match=match):
        m.validar(paths, 'subset')


def test_manual_run_date_fallback():
    dt = datetime(2026, 9, 8, tzinfo=timezone.utc)
    assert m.fecha_corrida(SimpleNamespace(logical_date=None, run_after=dt)) == '2026-09-08'


def test_seed_150_unique_stratified():
    seed = pd.read_csv(Path(__file__).resolve().parents[1] / 'include' / 'universo' / 'universo.csv')
    assert len(seed) == 150 and seed.ticker.is_unique and seed.sector.nunique() == 11
    assert seed.iloc[:10].sector.nunique() == 10


def test_readiness_empty_bronze_reports_missing_without_creating_silver():
    report = m.revisar_bronce('subset')
    assert not report['ready'] and report['estado'] == 'esperando_datos'
    assert {r['ticker'] for r in report['faltantes']} == {'NVDA', 'SPY'}
    assert len(report['faltantes']) == 9  # 7 de NVDA y 2 del benchmark; consenso opcional.
    assert not m.PLATA.exists()
    assert json.loads((m.OUTPUT_DIR / 'state' / 'ingesta_subset.json').read_text()) == report


def test_readiness_quota_diagnostic_safe_and_old_silver_preserved(monkeypatch, caplog):
    m.PLATA.mkdir(parents=True)
    old = m.PLATA / 'slv_eventos.csv'
    old.write_text('previous valid data')
    ids = [m.hashlib.sha256(k.encode()).hexdigest() for k in ('secret-one', 'secret-two')]
    m.escribir_atomico(m.OUTPUT_DIR / 'state' / 'api_keys.json', json.dumps({'keys': {i: {'blocked_until': 2000} for i in ids}}))
    monkeypatch.setattr(m.time, 'time', lambda: 1000)
    report = m.revisar_bronce('subset')
    assert report['motivo'] == 'cuota_en_enfriamiento'
    assert report['cuotas']['disponibles'] == 0
    assert report['cuotas']['proxima_disponible_utc'] == '1970-01-01T00:33:20+00:00'
    assert old.read_text() == 'previous valid data'
    assert not list(m.OUTPUT_DIR.glob('prueba_subset_*'))
    assert 'INGESTA_PENDIENTE' in caplog.text
    assert all(k not in caplog.text + json.dumps(report) for k in ('secret-one', 'secret-two'))


def test_readiness_complete_bronze_works_offline_without_keys(monkeypatch):
    fill_ticker()
    fill_benchmark()
    monkeypatch.delenv('ALPHAVANTAGE_API_KEYS')
    assert m.revisar_bronce('subset')['ready']
    paths = m.refinar_plata('subset')
    m.validar(paths, 'subset')
    assert Path(m.guardar(paths, '2026-09-08', 'subset')).exists()


def test_readiness_partial_universe_does_not_require_all_tickers():
    fill_ticker()
    fill_benchmark()
    m.UNIVERSO.write_text('ticker,sector\nNVDA,Technology\nMSFT,Technology\n')
    report = m.revisar_bronce('full')
    assert report['ready'] and report['tickers_completos'] == ['NVDA']
    assert all(r['ticker'] == 'MSFT' for r in report['faltantes'])
    with pytest.raises(ValueError, match='mínimo 1001'):
        m.validar(m.refinar_plata('full'), 'full')


def test_readiness_does_not_hide_bad_downloaded_data():
    fill_ticker()
    fill_benchmark()
    store('EARNINGS', {'quarterlyEarnings': [earnings(estimatedEPS='None')]})
    assert m.revisar_bronce('subset')['ready']
    with pytest.raises(ValueError, match='0 filas'):
        m.validar(m.refinar_plata('subset'), 'subset')
    store('EARNINGS', {'Information': 'quota'})
    with pytest.raises(ValueError, match='Bronze inválido'):
        m.revisar_bronce('subset')


def test_readiness_requires_optional_consensus_only_when_requested():
    fill_ticker()
    fill_benchmark()
    assert m.revisar_bronce('subset')['ready']
    report = m.revisar_bronce('subset', True)
    assert not report['ready']
    assert report['faltantes'] == [{'ticker': 'NVDA', 'endpoint': 'EARNINGS_ESTIMATES'}]


def test_cli_pending_data_exits_cleanly_without_silver(monkeypatch):
    import sys
    monkeypatch.setattr(sys, 'argv', ['earnings_ingest.py', '--solo-plata', '--mode', 'subset'])
    with pytest.raises(SystemExit) as exc:
        m.main()
    assert exc.value.code == 2
    assert not m.PLATA.exists()


def test_rueda_coherente_descarta_las_filas_rotas_de_yahoo():
    """Las dos ruedas reales de HUBB que voltearon la validación del 2026-09-22."""
    base = {'open': 10.0, 'high': 11.0, 'low': 9.0, 'close': 10.5,
            'volume': 100.0, 'close_adj': 10.5}
    assert m.rueda_coherente(base)
    vacia = {**base, 'open': None, 'high': None, 'low': None, 'close': None, 'close_adj': None}
    assert not m.rueda_coherente(vacia)                     # HUBB 1977-08-08
    assert not m.rueda_coherente({**base, 'low': 10.2})     # HUBB 2021-05-05: low > open
    assert not m.rueda_coherente({**base, 'high': 10.4})    # high < close
    assert not m.rueda_coherente({**base, 'close_adj': 0.0})
    assert not m.rueda_coherente({**base, 'volume': -1.0})

"""Pruebas de la integración SEC EDGAR: offline, hechos sintéticos identificados.

No consumen cuota ni red; independientes del resto de la suite (que quedó
referenciando la versión previa del pipeline). Cubren desacumulación YTD,
normalización 52/53 semanas, restatements, migración de tags, acciones en
circulación y el merge EDGAR+Yahoo en refinar_plata.
"""
import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

SPEC = importlib.util.spec_from_file_location('earnings_module', Path(__file__).resolve().parents[1] / 'dags' / 'earnings_ingest.py')
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)

FULL = {'total_assets': 1000.0, 'short_long_term_debt_total': 100.0,
        'total_current_assets': 300.0, 'total_current_liabilities': 200.0,
        'operating_cashflow': 50.0, 'capital_expenditures': -10.0,
        'dividend_payout': -5.0, 'net_income': 20.0, 'total_revenue': 100.0}


@pytest.fixture(autouse=True)
def sandbox(tmp_path, monkeypatch):
    monkeypatch.setattr(m, 'OUTPUT_DIR', tmp_path / 'output')
    monkeypatch.setattr(m, 'BRONZE', tmp_path / 'output' / 'bronze')
    monkeypatch.setattr(m, 'PLATA', tmp_path / 'output' / 'silver')
    universe = tmp_path / 'universo.csv'
    universe.write_text('ticker,sector\nNVDA,Technology\n')
    monkeypatch.setattr(m, 'UNIVERSO', universe)
    monkeypatch.setattr(m, '_CIK_CACHE', {'NVDA': '0001045810'})


def store(endpoint, payload, ticker='NVDA', snapshot='2026-09-23'):
    path = m.ruta_bronce(endpoint, ticker, snapshot)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))
    return path


# ---------------------------------------------------------------------------
# Parser EDGAR
# ---------------------------------------------------------------------------

def test_fin_mes_normaliza_fiscal_52_53():
    assert m.fin_mes('2020-06-27') == '2020-06-30'
    assert m.fin_mes('2020-09-26') == '2020-09-30'
    assert m.fin_mes('2020-12-31') == '2020-12-31'


def test_desacumulacion_ytd_dentro_del_ano_fiscal():
    us = {'NetIncomeLoss': {'units': {'USD': [
        {'form': '10-Q', 'start': '2019-09-29', 'end': '2019-12-28', 'val': 100, 'filed': '2020-01-30'},
        {'form': '10-Q', 'start': '2019-09-29', 'end': '2020-03-28', 'val': 250, 'filed': '2020-04-30'},
        {'form': '10-Q', 'start': '2019-09-29', 'end': '2020-06-27', 'val': 400, 'filed': '2020-07-30'},
        {'form': '10-K', 'start': '2019-09-29', 'end': '2020-09-26', 'val': 600, 'filed': '2020-10-29'},
    ]}}}
    v = m.valores_por_periodo(m.hechos_por_etiqueta(us, ('NetIncomeLoss',)), 'duration')
    assert [v[f]['valor'] for f in sorted(v)] == [100.0, 150.0, 150.0, 200.0]
    assert sorted(v) == ['2019-12-31', '2020-03-31', '2020-06-30', '2020-09-30']


def test_hueco_no_fabrica_trimestre():
    us = {'NetIncomeLoss': {'units': {'USD': [
        {'form': '10-Q', 'start': '2019-09-29', 'end': '2019-12-28', 'val': 100, 'filed': '2020-01-30'},
        {'form': '10-Q', 'start': '2019-09-29', 'end': '2020-09-26', 'val': 500, 'filed': '2020-10-29'},
    ]}}}
    v = m.valores_por_periodo(m.hechos_por_etiqueta(us, ('NetIncomeLoss',)), 'duration')
    # Q1 queda; el siguiente supera los 120 días de la cadena y se omite.
    assert sorted(v) == ['2019-12-31']


def test_instantaneo_elige_el_filing_mas_reciente():
    us = {'Assets': {'units': {'USD': [
        {'form': '10-Q', 'end': '2020-06-27', 'val': 1000, 'filed': '2020-07-30'},
        {'form': '10-Q', 'end': '2020-06-30', 'val': 999, 'filed': '2020-08-01'},
    ]}}}
    v = m.valores_por_periodo(m.hechos_por_etiqueta(us, ('Assets',)), 'instant')
    assert v['2020-06-30']['valor'] == 999.0


def test_migracion_de_tag_asc606_por_filing():
    us = {'Revenues': {'units': {'USD': [
        {'form': '10-Q', 'start': '2019-09-29', 'end': '2019-12-28', 'val': 50, 'filed': '2020-01-30'}]}},
          'RevenueFromContractWithCustomerExcludingAssessedTax': {'units': {'USD': [
        {'form': '10-Q', 'start': '2019-09-29', 'end': '2019-12-28', 'val': 52, 'filed': '2020-02-01'}]}}}
    p = m.parse_edgar_full('NVDA', '0001045810', us)
    assert p['periodos']['2019-12-31']['total_revenue'] == 52.0


def test_acciones_opcional_y_resumen():
    us = {'Assets': {'units': {'USD': [
        {'form': '10-Q', 'end': '2020-06-27', 'val': 1000, 'filed': '2020-07-30'}]}},
          'EntityCommonStockSharesOutstanding': {'units': {'shares': [
        {'form': '10-Q', 'end': '2020-06-27', 'val': 999999999, 'filed': '2020-07-30'}]}}}
    p = m.parse_edgar_full('NVDA', '0001045810', us)
    assert p['periodos']['2020-06-30']['shares_outstanding'] == 999999999.0
    assert m.es_util('EDGAR', p)
    assert not m.es_util('EDGAR', {'periodos': {}})
    assert m.resumen_payload('EDGAR', p)['records'] == 1
    assert m.parse_edgar('NVDA', p)['2020-06-30']['total_assets'] == 1000.0


def test_edgar_vacio_no_util():
    assert m.parse_edgar_full('NVDA', '0001045810', {}) is None
    assert m.es_util('EDGAR', None) is False


def test_edgar_sin_cik_no_hace_red(monkeypatch):
    monkeypatch.setattr(m, '_CIK_CACHE', {'AAPL': '0000320193'})
    assert m.edgar_get('ZZZZ') is None  # sin CIK -> rechazo, no transitorio


# ---------------------------------------------------------------------------
# Merge en refinar_plata: EDGAR llena los trimestres históricos
# ---------------------------------------------------------------------------

def _payload_earnings():
    return {'rows': [
        {'anuncio': '2026-05-20T16:00:00', 'estimate': 1.0, 'reported': 1.2, 'surprise_pct': 20.0},
        {'anuncio': '2020-05-20T16:00:00', 'estimate': 1.0, 'reported': 1.2, 'surprise_pct': 20.0},
    ]}


def _payload_fundamentales_yahoo():
    return {
        'income': {'2026-04-30': {'Total Revenue': 100.0}},
        'balance': {'2026-04-30': {'Total Assets': 1000.0, 'Total Debt': 100.0,
                                   'Current Assets': 300.0, 'Current Liabilities': 200.0}},
        'cashflow': {'2026-04-30': {'Operating Cash Flow': 50.0, 'Capital Expenditure': -10.0,
                                    'Cash Dividends Paid': -5.0,
                                    'Net Income From Continuing Operations': 20.0}},
    }


def test_refinar_plata_merge_edgar_historico():
    store('EARNINGS', _payload_earnings())
    store('FUNDAMENTALES', _payload_fundamentales_yahoo())
    store('EDGAR', {'cik': '0001045810',
                    'periodos': {'2020-04-30': {**FULL, 'shares_outstanding': 5000.0}},
                    'fechas': {'min': '2020-04-30', 'max': '2020-04-30'}, 'fuentes': {}})
    store('PERFIL', {'sector': 'Information Technology', 'industry': 'Semiconductors',
                     'exchange': 'NASDAQ', 'financialCurrency': 'USD',
                     'sharesOutstanding': 60000})
    m.refinar_plata('subset')
    ev = pd.read_csv(m.PLATA / 'por_stock' / 'silver_stocks_NVDA.csv')
    assert len(ev) == 2
    reciente = ev.loc[ev.fiscal_quarter_end == '2026-04-30'].iloc[0]
    historico = ev.loc[ev.fiscal_quarter_end == '2020-04-30'].iloc[0]
    assert reciente['total_assets'] == 1000.0 and reciente['net_income'] == 20.0
    assert reciente['shares_outstanding'] == 60000.0      # PERFIL actual
    assert historico['total_assets'] == 1000.0 and historico['net_income'] == 20.0
    assert historico['shares_outstanding'] == 5000.0      # EDGAR del trimestre
    assert historico['report_time'] == 'post-market'
"""Carga real, serialización y caminos del DAG. Opcional fuera de Astro."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip('airflow')
SPEC = importlib.util.spec_from_file_location('dag_under_test', Path(__file__).resolve().parents[2] / 'dags' / 'earnings_ingest.py')
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


def test_dag_graph_and_serialization():
    try:
        from airflow.serialization.serialized_objects import DagSerialization
    except ImportError:  # Airflow 3.1
        from airflow.serialization.serialized_objects import SerializedDAG as DagSerialization
    dag = m.earnings_ingest
    assert len(dag.tasks) == 9  # El texto dice ocho; el grafo enumera nueve.
    assert dag.catchup is False and dag.max_active_runs == 1
    assert dag.get_task('decidir_rama').trigger_rule == 'all_done'
    assert dag.get_task('refinar_plata').trigger_rule == 'none_failed_min_one_success'
    assert dag.get_task('refinar_plata').upstream_task_ids == {'bajar_bronce', 'sin_bronce'}
    assert dag.get_task('decidir_rama').upstream_task_ids == {'esperar_fuente'}
    assert dag.get_task('hay_trabajo').ignore_downstream_trigger_rules is True
    assert dag.get_task('esperar_fuente').mode == 'reschedule'
    assert dag.get_task('esperar_fuente').soft_fail
    assert dag.get_task('bajar_bronce').max_active_tis_per_dag == 1
    serialized = DagSerialization.to_dict(dag)
    assert DagSerialization.from_dict(serialized).dag_id == 'earnings_ingest'


@pytest.mark.parametrize('source,solo,expected', [({'ok': True}, False, 'hay_trabajo'),
                                                (None, False, 'sin_bronce'),
                                                ({'ok': False}, False, 'sin_bronce'),
                                                ({'ok': True}, True, 'sin_bronce')])
def test_branch_after_skip_failure_or_solo(source, solo, expected, monkeypatch):
    context = {'params': {'solo_plata': solo}, 'ti': SimpleNamespace(xcom_pull=lambda **kw: source)}
    monkeypatch.setattr(m, 'get_current_context', lambda: context)
    assert m.earnings_ingest.get_task('decidir_rama').python_callable() == expected


@pytest.mark.parametrize('solo,pending', [(True, True), (False, False)])
def test_sensor_no_network_when_solo_or_no_work(solo, pending, monkeypatch):
    monkeypatch.setattr(m, 'get_current_context', lambda: {'params': {'solo_plata': solo, 'mode': 'full', 'force': False, 'outputsize': 'full'}})
    monkeypatch.setattr(m, 'pendientes', lambda *a, **kw: [1] if pending else [])
    def forbidden(*a, **kw):
        raise AssertionError('No debe pedir fuente')
    monkeypatch.setattr(m, 'av_get_rotando', forbidden)
    assert m.earnings_ingest.get_task('esperar_fuente').python_callable().is_done


def test_incomplete_ingestion_skips_refinement_and_downstream(monkeypatch):
    monkeypatch.setattr(m, 'get_current_context', lambda: {'params': {'mode': 'subset', 'incluir_consenso': False}})
    monkeypatch.setattr(m, 'revisar_bronce', lambda *a: {'ready': False, 'motivo': 'cuota_en_enfriamiento', 'faltantes': [1]})
    def forbidden(*a):
        raise AssertionError('No se debe refinar ni sobrescribir plata sin bronze suficiente')
    monkeypatch.setattr(m, 'refinar_plata', forbidden)
    with pytest.raises(m.AirflowSkipException, match='Ingesta pendiente'):
        m.earnings_ingest.get_task('refinar_plata').python_callable()
    assert m.earnings_ingest.get_task('validar').trigger_rule == 'all_success'
    assert m.earnings_ingest.get_task('guardar').trigger_rule == 'all_success'


def test_complete_bronze_reaches_refinement(monkeypatch):
    monkeypatch.setattr(m, 'get_current_context', lambda: {'params': {'mode': 'subset', 'incluir_consenso': False}})
    monkeypatch.setattr(m, 'revisar_bronce', lambda *a: {'ready': True})
    monkeypatch.setattr(m, 'refinar_plata', lambda *a: {'eventos': 'path'})
    assert m.earnings_ingest.get_task('refinar_plata').python_callable() == {'eventos': 'path'}


def test_quality_failure_is_still_failure_without_retries(monkeypatch):
    monkeypatch.setattr(m, 'get_current_context', lambda: {'params': {'mode': 'subset', 'incluir_consenso': False}})
    def invalid(*a):
        raise ValueError('Clave repetida')
    monkeypatch.setattr(m, 'validar', invalid)
    task = m.earnings_ingest.get_task('validar')
    assert task.retries == 0
    with pytest.raises(m.AirflowFailException, match='Clave repetida'):
        task.python_callable({'eventos': 'path'})

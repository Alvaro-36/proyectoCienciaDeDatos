"""Integridad del proyecto, compatible con el descubridor de DAGs de Airflow 3."""
import inspect
from pathlib import Path

import pytest

pytest.importorskip('airflow')
try:
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # Airflow anterior a la separación del parser.
    from airflow.models import DagBag


@pytest.fixture(scope='module')
def dag_bag():
    kwargs = {'dag_folder': str(Path(__file__).resolve().parents[2] / 'dags')}
    if 'include_examples' in inspect.signature(DagBag).parameters:
        kwargs['include_examples'] = False
    return DagBag(**kwargs)


def test_file_imports(dag_bag):
    assert not dag_bag.import_errors, dag_bag.import_errors
    assert set(dag_bag.dags) == {'earnings_ingest'}


def test_dag_tags(dag_bag):
    assert all(dag.tags for dag in dag_bag.dags.values())


def test_dag_retries(dag_bag):
    assert all(dag.default_args.get('retries', 0) >= 2 for dag in dag_bag.dags.values())

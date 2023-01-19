from configparser import ConfigParser
from importlib import import_module

import pytest
from sqlalchemy import text
from sqlalchemy_utils import create_database, drop_database

import migrate.systemdata
import odp.db
from odp.config import config


@pytest.fixture(scope='session', autouse=True)
def ensure_coverage():
    """Coverage cannot automatically discover modules within namespace
    packages, which prevents reporting on unexecuted files and leads to
    an inflated coverage score.

    Any modules and subpackages within namespace packages (viz. `odp`,
    `odp.lib` and `odp.ui`) that we do want covered must be declared
    in .coveragerc and also imported here.

    TODO: create issue/PR on coverage to better support namespace packages
    """
    coveragerc = ConfigParser()
    coveragerc.read('.coveragerc')
    for module in coveragerc['run']['source'].split():
        import_module(module)


@pytest.fixture(scope='session', autouse=True)
def database():
    """An auto-use, run-once fixture that provides a clean
    database with an up-to-date ODP schema."""
    create_database(url := config.ODP.DB.URL)
    try:
        migrate.systemdata.init_database_schema()
        yield
    finally:
        drop_database(url)


@pytest.fixture(autouse=True)
def session():
    """An auto-use, per-test fixture that disposes of the current
    session after every test."""
    try:
        yield
    finally:
        odp.db.Session.remove()


@pytest.fixture(autouse=True)
def delete_all_data():
    """An auto-use, per-test fixture that deletes all table data
    after every test."""
    try:
        yield
    finally:
        with odp.db.engine.begin() as conn:
            for table in odp.db.Base.metadata.tables:
                conn.execute(text(f'ALTER TABLE "{table}" DISABLE TRIGGER ALL'))
                conn.execute(text(f'DELETE FROM "{table}"'))
                conn.execute(text(f'ALTER TABLE "{table}" ENABLE TRIGGER ALL'))

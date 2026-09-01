"""
Test the Informix SQLAlchemy dialect registration and URL construction.

These guard failures that are silent at runtime: a changed dialect name makes
the profiler emit generic SQL that Informix rejects, and the wrong JDBC driver
generation reads only the tables it created itself.
"""

import pytest
from sqlalchemy import Column, Integer, create_engine
from sqlalchemy.engine.url import make_url

from metadata.ingestion.source.database.informix.dialect import (
    INFORMIX_JDBC_VERSION,
    RECOMMENDED_JDBC_DRIVERS,
    InformixDialect,
)
from metadata.profiler.orm.functions.median import MedianFn
from metadata.profiler.orm.registry import Dialects


@pytest.fixture(name="dialect")
def dialect_fixture() -> InformixDialect:
    return InformixDialect()


class TestInformixDialect:
    """Validate dialect identity, JDBC URL shape and driver coordinates."""

    def test_dialect_name_matches_profiler_registry(self, dialect):
        """@compiles dispatches on dialect.name, so it must equal Dialects.Informix."""
        assert dialect.name == "informix"
        assert dialect.name == str(Dialects.Informix)

    def test_engine_resolves_registered_dialect(self):
        engine = create_engine("informix://user:pw@host:9088/db?INFORMIXSERVER=ol_prod")
        assert isinstance(engine.dialect, InformixDialect)

    def test_jdbc_url_carries_informixserver(self, dialect):
        _, kwargs = dialect.create_connect_args(make_url("informix://user:pw@host:9088/db?INFORMIXSERVER=ol_prod"))
        assert kwargs["url"] == "jdbc:informix-sqli://host:9088/db:INFORMIXSERVER=ol_prod"
        assert kwargs["jclassname"] == "com.informix.jdbc.IfxDriver"
        assert kwargs["driver_args"] == {"user": "user", "password": "pw"}

    def test_jdbc_url_defaults(self, dialect):
        _, kwargs = dialect.create_connect_args(make_url("informix://host/db"))
        assert kwargs["url"] == "jdbc:informix-sqli://host:9088/db:INFORMIXSERVER=informix"

    def test_extra_query_params_use_semicolon_separator(self, dialect):
        _, kwargs = dialect.create_connect_args(
            make_url("informix://host:9088/db?INFORMIXSERVER=ol_prod&DB_LOCALE=en_US.819")
        )
        assert kwargs["url"].endswith(";DB_LOCALE=en_US.819")

    def test_driver_generation_is_not_the_large_rowid_blind_one(self):
        """4.50 cannot open tables using large rowids; 15.x can."""
        driver = RECOMMENDED_JDBC_DRIVERS["informix"]
        assert driver.artifact_id == "jdbc"
        assert not INFORMIX_JDBC_VERSION.startswith("4.")
        assert "informix_bson" in RECOMMENDED_JDBC_DRIVERS

    def test_profiler_median_compiles_to_informix_sql(self, dialect):
        """Regression guard: the merged @compiles override must win."""
        sql = str(
            MedianFn(Column("age", Integer), "t", 0.5).compile(dialect=dialect, compile_kwargs={"literal_binds": True})
        )
        assert "ROW_NUMBER() OVER" in sql
        assert "percentile_cont" not in sql.lower()

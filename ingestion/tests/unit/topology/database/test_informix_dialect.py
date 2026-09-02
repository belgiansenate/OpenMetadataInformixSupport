"""
Test the Informix SQLAlchemy dialect registration and URL construction.

These guard failures that are silent at runtime: a changed dialect name makes
the profiler emit generic SQL that Informix rejects, and the wrong JDBC driver
generation reads only the tables it created itself.
"""

import re
from pathlib import Path

import pytest
from sqlalchemy import Column, Integer, create_engine
from sqlalchemy.engine.url import make_url

from metadata.ingestion.source.database.informix.dialect import (
    BSON_VERSION,
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


REPO_ROOT = Path(__file__).parents[5]
FETCH_SCRIPT = REPO_ROOT / "ingestion/scripts/fetch_informix_jdbc.sh"
# Discovered rather than listed: an image added later is covered automatically.
# A hardcoded list silently exempts the next Dockerfile someone writes, which is
# how ingestion/operators/docker/Dockerfile.ci was missed the first time round.
INSTALLS_INGESTION = re.compile(r'pip install .*(openmetadata-ingestion|"\.\[)')


def _images_that_can_run_the_connector() -> list[Path]:
    return sorted(
        path for path in (REPO_ROOT / "ingestion").rglob("Dockerfile*") if INSTALLS_INGESTION.search(path.read_text())
    )


DOCKERFILES = _images_that_can_run_the_connector()


def _pinned_in_script(variable: str) -> str:
    match = re.search(rf'^{variable}="([^"]+)"$', FETCH_SCRIPT.read_text(), re.MULTILINE)
    assert match, f"{variable} not found in {FETCH_SCRIPT.name}"
    return match.group(1)


class TestInformixDriverIsBakedIntoTheImage:
    """The jars must be pre-fetched, and pre-fetched under the name the loader looks for.

    sqlalchemy-jdbcapi decides its cache is warm purely on the jar filename, so a
    version bump in dialect.py alone leaves the image holding a jar nobody asks
    for and silently reinstates the Maven download this baking exists to remove.
    That failure only shows up in a network-restricted deployment, which is
    exactly where nobody is watching a build log.
    """

    def test_script_versions_match_the_dialect(self):
        assert _pinned_in_script("INFORMIX_JDBC_VERSION") == INFORMIX_JDBC_VERSION
        assert _pinned_in_script("BSON_VERSION") == BSON_VERSION

    def test_script_writes_the_filenames_the_loader_resolves(self):
        script = FETCH_SCRIPT.read_text()
        for key in ("informix", "informix_bson"):
            assert RECOMMENDED_JDBC_DRIVERS[key].filename in script.replace(
                "${INFORMIX_JDBC_VERSION}", INFORMIX_JDBC_VERSION
            ).replace("${BSON_VERSION}", BSON_VERSION)

    def test_checksums_are_pinned(self):
        for variable in ("INFORMIX_JDBC_SHA256", "BSON_SHA256"):
            assert re.fullmatch(r"[0-9a-f]{64}", _pinned_in_script(variable))

    def test_the_discovery_actually_finds_the_images(self):
        """A filter that matches nothing would make the check below vacuous."""
        assert len(DOCKERFILES) >= 4

    @pytest.mark.parametrize("dockerfile", DOCKERFILES, ids=lambda p: str(p.relative_to(REPO_ROOT)))
    def test_every_image_bakes_the_driver_where_the_loader_reads_it(self, dockerfile):
        """A jar baked somewhere the cache lookup never reads is worse than none."""
        content = dockerfile.read_text()
        assert "fetch_informix_jdbc.sh" in content, f"{dockerfile} does not pre-fetch the driver"
        run = re.search(r"bash \S*fetch_informix_jdbc\.sh (\S+)", content)
        assert run, f"{dockerfile} copies the script but never runs it"
        target = run.group(1)
        assert f"SQLALCHEMY_JDBCAPI_DRIVER_CACHE={target}" in content, (
            f"{dockerfile} bakes the driver into {target} but does not point the cache there"
        )

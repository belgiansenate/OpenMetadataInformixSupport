#  Copyright 2026 OpenMetadata
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
SQLAlchemy dialect for IBM Informix, over the IBM Informix JDBC driver.

Server compatibility
--------------------
Verified end to end with the pinned 15.0.0.1.1 driver against both
Informix 14.10.FC9W1DE and 15.0.1.0.3: reflection, and median, Q1, Q3, sum,
length and modulo all returning correct values, including against tables created
by a different client (dbaccess) rather than by the connector itself.

Servers older than 14.10 are untested here.

Do not downgrade the driver to the 4.50 line. 4.50 cannot open a table whose
tblspace uses large rowids and rejects it with
"-242 Could not open database table". Only Informix 15 and later produce that
layout -- on 14.10 the same table carries tblspace flags 902 against 15.0.1's
400000902 -- which is why 4.50 appears to work on older servers and fails on new
ones. See the note on the pin below for why the failure is easy to miss.
"""

from sqlalchemy.dialects import registry
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.url import URL
from sqlalchemy_jdbcapi.dialects.base import JDBCDriverConfig
from sqlalchemy_jdbcapi.dialects.gbase import GBase8sDialect
from sqlalchemy_jdbcapi.jdbc.driver_manager import (
    RECOMMENDED_JDBC_DRIVERS,
    JDBCDriver,
    get_driver_path,
)

INFORMIX_JDBC_VERSION = "15.0.0.1.1"
BSON_VERSION = "4.11.1"
DEFAULT_INFORMIX_SERVER = "informix"

# The JDBC driver generation must match the server generation. Driver 4.50
# cannot open a table whose tblspace uses "large rowids" -- the layout an
# Informix 15 server gives tables created by a modern client -- and rejects it
# with "-242 Could not open database table". The server negotiates the older
# layout for tables the 4.50 driver creates itself, so that driver reads its own
# tables and nothing else: a self-seeded test passes while every real catalog
# fails silently.
#
# com.ibm.informix:jdbc calls org.bson.BSONObject (Informix's JSON/BSON
# compatibility layer) from getResultSet(), so bson must be on the classpath as
# well. informix-jdbc-complete bundles it, but is only published at 4.50.4.1 --
# the broken generation -- so the 15.x driver is paired with bson explicitly.
RECOMMENDED_JDBC_DRIVERS["informix"] = JDBCDriver(
    group_id="com.ibm.informix",
    artifact_id="jdbc",
    version=INFORMIX_JDBC_VERSION,
)
RECOMMENDED_JDBC_DRIVERS["informix_bson"] = JDBCDriver(
    group_id="org.mongodb",
    artifact_id="bson",
    version=BSON_VERSION,
)


def _informix_jars() -> list[str]:
    """Resolve just the two jars this dialect needs.

    Passing an explicit classpath is not an optimisation. sqlalchemy-jdbcapi
    starts the JVM with every driver in RECOMMENDED_JDBC_DRIVERS when none is
    given -- roughly 190 MB fetched from Maven Central, 155 MB of it Phoenix, to
    run a connector that needs 1.7 MB -- and logs an ERROR for the gbase entry,
    whose published coordinate 404s. On a fresh ingestion container that is a
    long first connection and a failure surface with no relation to Informix.

    The JVM takes the classpath of whichever connector starts it first and keeps
    it for the life of the process, so this only holds when Informix connects first.
    """
    return [str(get_driver_path("informix")), str(get_driver_path("informix_bson"))]


class InformixDialect(GBase8sDialect, DefaultDialect):
    """IBM Informix dialect.

    GBase 8s is Informix-derived, so its dialect only differs in the driver
    class and URL scheme. DefaultDialect is mixed in because sqlalchemy-jdbcapi
    pairs GBase8sDialect with SQLAlchemy's abstract Dialect interface, leaving it
    without a usable __init__ -- create_engine() raises "takes no arguments".

    The dialect name must stay "informix": the profiler's @compiles overrides
    for MedianFn, SumFn, LenFn, ModuloFn, ColumnCountFn and ColunNameFn dispatch
    on it, and fall back to generic SQL that Informix rejects if it changes.
    """

    name = "informix"
    driver = "jdbcapi"
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls) -> type:
        """Declared explicitly: SQLAlchemy 2.0 warns on the inherited dbapi()."""
        return super().import_dbapi()

    @classmethod
    def get_driver_config(cls) -> JDBCDriverConfig:
        return JDBCDriverConfig(
            driver_class="com.informix.jdbc.IfxDriver",
            jdbc_url_template="jdbc:informix-sqli://{host}:{port}/{database}",
            default_port=9088,
            supports_transactions=True,
            supports_schemas=True,
            supports_sequences=True,
        )

    def create_connect_args(self, url: URL) -> tuple[list, dict]:
        """Informix separates URL properties with ':' and then ';', not '?' and
        '&', and requires INFORMIXSERVER -- which is why serverName is mandatory
        in informixConnection.json."""
        config = self.get_driver_config()
        query = dict(url.query or {})
        server = query.pop("INFORMIXSERVER", None) or DEFAULT_INFORMIX_SERVER

        jdbc_url = config.jdbc_url_template.format(
            host=url.host or "localhost",
            port=url.port or config.default_port,
            database=url.database or "",
        )
        jdbc_url = f"{jdbc_url}:INFORMIXSERVER={server}"
        for key, value in query.items():
            jdbc_url += f";{key}={value}"

        driver_args = {}
        if url.username:
            driver_args["user"] = url.username
        if url.password:
            driver_args["password"] = url.password

        return (
            [],
            {
                "jclassname": config.driver_class,
                "url": jdbc_url,
                "driver_args": driver_args or None,
                "jars": _informix_jars(),
            },
        )


registry.register("informix", __name__, "InformixDialect")

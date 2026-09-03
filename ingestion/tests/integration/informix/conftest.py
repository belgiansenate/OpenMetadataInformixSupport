#  Copyright 2026 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Fixtures for the Informix connector integration tests.

The IBM Informix developer image is the only publicly pullable Informix, and it
takes the better part of a minute to initialise its dbspaces on first boot, so
readiness is polled with a real query rather than a log line -- the startup
banner is printed while the server is still booting.

Tables are created through dbaccess, the server's own client, rather than through
the connector. That is deliberate: driver 4.50 can read tables it created itself
but not tables a modern client created, so seeding over JDBC would hide exactly
the class of failure the pinned driver exists to avoid.
"""

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from tenacity import retry, stop_after_delay, wait_fixed
from testcontainers.core.container import DockerContainer

import metadata.ingestion.source.database.informix.dialect  # noqa: F401  (registers the dialect)
from _openmetadata_testutils.helpers.docker import try_bind
from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.entity.services.connections.database.informixConnection import (
    InformixConnection,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseConnection,
    DatabaseServiceType,
)

INFORMIX_IMAGE = "icr.io/informix/informix-developer-database:14.10.FC9W1DE"
INFORMIX_PORT = 9088
SERVER_NAME = "informix"
USERNAME = "informix"
PASSWORD = "in4mix"
DATABASE = "itest"

# Every type this connector has to correct, plus the ones that must be left alone,
# and one view and two routines -- Informix ships ~560 built-in routines and two
# catalogue views that look exactly like user objects, so the tests need real user
# ones to prove the filters keep what they should.
#
# The synonym and the sequence are here to prove the opposite: Informix records
# both in systables alongside tables and views, OpenMetadata has no entity type
# for either, and Oracle -- the other connector whose engine has synonyms -- does
# not ingest them. They must stay out of the catalogue.
# c_bool is load-bearing: BOOLEAN shares coltype 41 with CLOB and BLOB, so a
# regression that matches on 41 alone makes every boolean column unprofilable.
# b_char/d_vchar cover the two collength encodings -- CHAR is the raw width and
# may exceed 255, VARCHAR packs a reserved minimum into the high byte.
SEED_SQL = """
CREATE TABLE lob_types (
    id        INTEGER PRIMARY KEY,
    c_char    CHAR(10),
    c_vchar   VARCHAR(50),
    c_lvchar  LVARCHAR(100),
    c_bool    BOOLEAN,
    c_text    TEXT,
    c_byte    BYTE,
    c_clob    CLOB,
    c_blob    BLOB
);
INSERT INTO lob_types (id, c_char, c_vchar, c_lvchar, c_bool)
    VALUES (1, 'a', 'alpha', 'long alpha', 't');
INSERT INTO lob_types (id, c_char, c_vchar, c_lvchar, c_bool)
    VALUES (2, 'b', 'beta', 'long beta', 'f');
CREATE VIEW lob_view AS SELECT id, c_char, c_vchar FROM lob_types;
CREATE PROCEDURE add_two(a INT, b INT) RETURNING INT;
  RETURN a + b;
END PROCEDURE;
CREATE FUNCTION triple(a INT) RETURNING INT;
  RETURN a * 3;
END FUNCTION;
CREATE SYNONYM lob_syn FOR lob_types;
CREATE SEQUENCE probe_seq;
CREATE TABLE char_widths (
    a_char     CHAR(10),
    b_char     CHAR(300),
    c_vchar    VARCHAR(50),
    d_vchar    VARCHAR(50,10),
    e_lvchar   LVARCHAR(1000),
    f_nchar    NCHAR(20),
    g_nvarchar NVARCHAR(60)
);
"""


def _dbaccess(container, database: str, sql: str) -> None:
    """Run SQL through the server's own client, inside the container."""
    target = database or "- "
    exit_code, output = container.get_wrapped_container().exec_run(
        ["bash", "-lc", f"dbaccess {target} - <<'EOSQL'\n{sql}\nEOSQL"]
    )
    decoded = output.decode(errors="replace")
    # dbaccess exits 0 even when a statement fails, so the output is the signal.
    assert exit_code == 0 and "Error" not in decoded, f"dbaccess failed:\n{decoded}"


@pytest.fixture(scope="module")
def informix_container():
    container = (
        DockerContainer(INFORMIX_IMAGE)
        .with_env("LICENSE", "accept")
        .with_env("SIZE", "small")
        .with_exposed_ports(INFORMIX_PORT)
    )
    with try_bind(container, INFORMIX_PORT, None) if not os.getenv("CI") else container as container:
        port = container.get_exposed_port(INFORMIX_PORT)

        @retry(stop=stop_after_delay(600), wait=wait_fixed(5), reraise=True)
        def _wait_until_answering_sql():
            engine = create_engine(
                f"informix://{USERNAME}:{PASSWORD}@localhost:{port}/sysmaster?INFORMIXSERVER={SERVER_NAME}"
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT FIRST 1 tabname FROM systables"))

        _wait_until_answering_sql()
        _dbaccess(container, "", f"CREATE DATABASE {DATABASE} WITH LOG;")
        _dbaccess(container, DATABASE, SEED_SQL)
        yield container


@pytest.fixture(scope="module")
def create_service_request(informix_container, tmp_path_factory):
    return CreateDatabaseServiceRequest(
        name="docker_test_informix_" + uuid.uuid4().hex[:8],
        serviceType=DatabaseServiceType.Informix,
        connection=DatabaseConnection(
            config=InformixConnection(
                username=USERNAME,
                password=PASSWORD,
                hostPort=f"localhost:{informix_container.get_exposed_port(INFORMIX_PORT)}",
                database=DATABASE,
                serverName=SERVER_NAME,
            )
        ),
    )

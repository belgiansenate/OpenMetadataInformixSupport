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
Informix source module
"""

import traceback
from collections import OrderedDict
from collections.abc import Iterable

from sqlalchemy import BLOB, CLOB, TEXT, LargeBinary, text

from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.services.connections.database.informixConnection import (
    InformixConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.common_db_source import CommonDbSourceService
from metadata.ingestion.source.database.informix.queries import (
    INFORMIX_GET_DATABASE_NAMES,
    INFORMIX_GET_LOB_COLUMNS,
)
from metadata.ingestion.source.database.multi_db_source import MultiDBSource
from metadata.utils import fqn
from metadata.utils.filters import filter_by_database
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

COLTYPE_BYTE = 11
COLTYPE_TEXT = 12
COLTYPE_OPAQUE = 41

# The SQLAlchemy type here is what ColumnTypeParser converts into an OM DataType:
# LargeBinary -> BYTES, TEXT -> TEXT, CLOB -> CLOB, BLOB -> BLOB. Those are exactly
# the types is_blob() recognises, which is how the profiler learns to skip them.
# The second element becomes dataTypeDisplay, so the UI shows the native name.
LOB_TYPES_BY_COLTYPE = {
    COLTYPE_BYTE: (LargeBinary, "BYTE"),
    COLTYPE_TEXT: (TEXT, "TEXT"),
}
LOB_TYPES_BY_SUBTYPE = {
    "clob": (CLOB, "CLOB"),
    "blob": (BLOB, "BLOB"),
}

# One entry per schema, holding only its large columns -- rare enough that an
# entry stays small. Bounded so a catalogue with many schemas cannot grow it.
MAX_CACHED_SCHEMAS = 64


class InformixSource(CommonDbSourceService, MultiDBSource):
    """
    Implements the necessary methods to extract Database metadata from Informix.

    Table, view, column and stored-procedure reflection all come from the JDBC
    DatabaseMetaData API via the dialect, so this class only has to supply what
    is genuinely Informix-shaped: enumerating the databases on the server.
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata) -> None:
        super().__init__(config, metadata)
        self._lob_columns_cache: OrderedDict[str, dict[tuple[str, str], tuple]] = OrderedDict()

    @classmethod
    def create(cls, config_dict: dict, metadata: OpenMetadata, pipeline_name: str | None = None):
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        service_conn = config.serviceConnection
        connection = service_conn.root.config if service_conn is not None else None
        if not isinstance(connection, InformixConnection):
            raise InvalidSourceException(f"Expected InformixConnection, but got {connection}")
        return cls(config, metadata)

    def get_configured_database(self) -> str | None:
        return self.service_connection.database

    def get_database_names_raw(self) -> Iterable[str]:
        yield from self._execute_database_query(INFORMIX_GET_DATABASE_NAMES)

    def get_database_names(self) -> Iterable[str]:
        """
        Yield the databases to ingest, re-pointing the inspector at each one.

        An Informix JDBC connection is bound to a single database, so crossing to
        another means rebuilding the engine -- which is what set_inspector does.
        """
        if not self.service_connection.ingestAllDatabases:
            configured_db = self.service_connection.database
            self.set_inspector(database_name=configured_db)
            yield configured_db
            return

        for new_database in self.get_database_names_raw():
            database_fqn = fqn.build(
                self.metadata,
                entity_type=Database,
                service_name=self.context.get().database_service,
                database_name=new_database,
            )

            if filter_by_database(
                self.source_config.databaseFilterPattern,
                (database_fqn if self.source_config.useFqnForFiltering else new_database),
            ):
                self.status.filter(database_fqn, "Database Filtered Out")
                continue

            try:
                self.set_inspector(database_name=new_database)
                yield new_database
            except Exception as exc:
                logger.debug(traceback.format_exc())
                logger.error(f"Error trying to connect to database {new_database}: {exc}")

    def _lob_columns(self, schema_name: str) -> dict[tuple[str, str], tuple]:
        """Map (table, column) to its true large-object type for one schema.

        Queried per schema rather than per table: the result holds only large
        columns, so it stays small, and a wide catalogue costs one round trip per
        schema instead of one per table.
        """
        cached = self._lob_columns_cache.get(schema_name)
        if cached is not None:
            self._lob_columns_cache.move_to_end(schema_name)
            return cached

        found: dict[tuple[str, str], tuple] = {}
        try:
            rows = self.connection.execute(text(INFORMIX_GET_LOB_COLUMNS), {"owner": schema_name}).fetchall()
        except Exception as exc:
            # Reflection still works without this; the columns just keep the
            # VARCHAR the driver reported, so degrade rather than fail the run.
            logger.debug(traceback.format_exc())
            logger.warning(f"Could not read large-object types for schema {schema_name}: {exc}")
            rows = []

        for tabname, colname, basetype, xtype in rows:
            mapped = LOB_TYPES_BY_COLTYPE.get(basetype)
            if mapped is None and basetype == COLTYPE_OPAQUE:
                # 41 is also BOOLEAN and every other opaque type; only the named
                # smart large objects belong here.
                mapped = LOB_TYPES_BY_SUBTYPE.get((xtype or "").lower())
            if mapped is not None:
                found[(tabname, colname)] = mapped

        self._lob_columns_cache[schema_name] = found
        while len(self._lob_columns_cache) > MAX_CACHED_SCHEMAS:
            self._lob_columns_cache.popitem(last=False)
        return found

    def _get_columns_internal(
        self,
        schema_name: str,
        table_name: str,
        db_name: str,
        inspector,
        table_type=None,
    ):
        """Restore the large-object types the JDBC driver flattens.

        Informix reports BYTE, TEXT, CLOB and BLOB all as VARCHAR(2147483647).
        Left alone, a column of scanned documents is catalogued as ordinary text,
        and the profiler then tries to aggregate over it and is rejected.
        """
        columns = super()._get_columns_internal(schema_name, table_name, db_name, inspector, table_type)
        lob_columns = self._lob_columns(schema_name)
        if not lob_columns:
            return columns

        for column in columns:
            mapped = lob_columns.get((table_name, column["name"]))
            if mapped is None:
                continue
            sqa_type, display_name = mapped
            column["type"] = sqa_type()
            column["system_data_type"] = display_name
            logger.debug(f"{table_name}.{column['name']} reported as VARCHAR, corrected to {display_name}")

        return columns

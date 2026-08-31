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
from collections.abc import Iterable

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
)
from metadata.ingestion.source.database.multi_db_source import MultiDBSource
from metadata.utils import fqn
from metadata.utils.filters import filter_by_database
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class InformixSource(CommonDbSourceService, MultiDBSource):
    """
    Implements the necessary methods to extract Database metadata from Informix.

    Table, view, column and stored-procedure reflection all come from the JDBC
    DatabaseMetaData API via the dialect, so this class only has to supply what
    is genuinely Informix-shaped: enumerating the databases on the server.
    """

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

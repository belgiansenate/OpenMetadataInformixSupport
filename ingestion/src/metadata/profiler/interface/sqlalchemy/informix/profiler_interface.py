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
Profiler interface for Informix.

Informix rejects its large-object types in most SQL expressions, so profiling
them is not a matter of getting a poor result -- the statement errors and the
whole column's metrics are lost. Measured against 14.10.FC9W1DE and 15.0.1.0.3:

                      BYTE   TEXT   CLOB   BLOB
    COUNT(col)         no     no    yes    yes
    COUNT(DISTINCT)    no     no     no     no
    MIN / MAX          no     no     no     no
    LENGTH(col)       yes    yes     no     no
    GROUP BY           no     no     no     no

Why no metrics at all, when LENGTH works on BYTE and TEXT
---------------------------------------------------------
We could still collect length statistics for those two. We deliberately do not.
BYTE and TEXT are indistinguishable from CLOB and BLOB in the UI at a glance, and
showing size statistics for two of the four while the others show nothing reads
as a bug rather than a rule. One rule -- large objects are not profiled -- is
also what registry.is_blob() already documents. If someone later wants length
statistics here, the table above says exactly which types can support them.
"""

from metadata.ingestion.ometa.utils import model_str
from metadata.profiler.interface.sqlalchemy.profiler_interface import (
    SQAProfilerInterface,
)
from metadata.profiler.orm.registry import is_blob
from metadata.utils.logger import profiler_interface_registry_logger

logger = profiler_interface_registry_logger()


class InformixProfilerInterface(SQAProfilerInterface):
    """
    Interface to interact with registry supporting sqlalchemy.
    """

    def _blob_column_names(self) -> set[str]:
        """Names of the columns Informix will not let us aggregate over.

        Read from the ingested entity rather than the ORM table: building the ORM
        column maps the type through a registry that drops precision, so a CLOB
        arrives as an undetermined type and a BYTE is indistinguishable from any
        other binary. The entity still carries the dataType metadata ingestion
        resolved from Informix's own catalogue.
        """
        return {model_str(column.name) for column in (self.table_entity.columns or []) if is_blob(column.dataType)}

    def get_columns(self):
        """Profile every column except the large objects."""
        blob_columns = self._blob_column_names()
        if not blob_columns:
            return super().get_columns()

        logger.info(
            f"Skipping profiler metrics for large-object columns on "
            f"{model_str(self.table_entity.name)}: {', '.join(sorted(blob_columns))}"
        )
        return [column for column in super().get_columns() if column.name not in blob_columns]

    def _programming_error_static_metric(self, runner, column, exc, session, metrics):
        """Drop a column's metrics instead of failing the run.

        The type-based skip above catches the large objects we know about. This is
        the backstop for the ones we do not: an Informix type that rejects profiler
        SQL for some other reason costs that column's metrics, not the whole table's.
        """
        logger.warning(
            f"Skipping profiler metrics for {runner.table_name}.{column.name}: Informix rejected the query ({exc})"
        )
        return

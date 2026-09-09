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
Sampler for Informix.

A column whose type the JDBC driver cannot convert does not fail on its own --
it fails the SELECT it appears in, so one such column costs the whole table its
sample data, and with it the classification of every other column.

Casting it to LVARCHAR in the SELECT is what keeps the data: the server applies
the type's own output function and the driver only ever sees text. Columns whose
type has no such cast are the only ones left out.

Which types need this, and how that was measured, is documented on
INFORMIX_GET_DRIVER_UNFRIENDLY_COLUMNS. Deciding from the catalogue rather than
from the ingested entity is deliberate: a column added since the last metadata
run would otherwise break its table until someone re-ingested it.
"""

import traceback

from sqlalchemy import cast, inspect, text
from sqlalchemy.types import UserDefinedType

from metadata.generated.schema.entity.data.table import TableData
from metadata.ingestion.source.database.informix.queries import (
    INFORMIX_GET_DRIVER_UNFRIENDLY_COLUMNS,
)
from metadata.profiler.processor.handle_partition import RANDOM_LABEL
from metadata.sampler.sqlalchemy.sampler import SQASampler
from metadata.utils.logger import profiler_interface_registry_logger

logger = profiler_interface_registry_logger()


class LVarchar(UserDefinedType):
    """The cast target. LVARCHAR is the widest type an output function returns."""

    cache_ok = True

    def get_col_spec(self, **_) -> str:
        return "LVARCHAR"


class InformixSampler(SQASampler):
    """Samples every column, casting the ones the driver would otherwise reject."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # One table per sampler, so this holds at most one lookup's worth.
        self._driver_unfriendly: dict[str, bool] | None = None

    def _unfriendly_columns(self) -> dict[str, bool]:
        """Column name -> whether a cast to LVARCHAR can recover it."""
        if self._driver_unfriendly is not None:
            return self._driver_unfriendly

        table = self.raw_dataset.__table__
        try:
            with self.connection.connect() as conn:
                rows = conn.execute(
                    text(INFORMIX_GET_DRIVER_UNFRIENDLY_COLUMNS),
                    {"table_name": table.name, "owner": table.schema},
                ).fetchall()
        except Exception as exc:
            # Sampling the table as-is is still worth attempting: most tables
            # have no such column, and the query above is what says whether
            # this one does.
            logger.debug(traceback.format_exc())
            logger.warning(f"Could not read column types for {table.schema}.{table.name}: {exc}")
            rows = []

        self._driver_unfriendly = {name: bool(casts_to_text) for name, casts_to_text in rows}
        return self._driver_unfriendly

    def get_columns(self):
        """Every column the driver can return a value for, cast or otherwise."""
        columns = super().get_columns()
        dropped = {name for name, castable in self._unfriendly_columns().items() if not castable}
        if not dropped:
            return columns

        table = self.raw_dataset.__table__
        logger.info(
            f"Leaving out of the sample for {table.schema}.{table.name} the columns whose type the "
            f"Informix JDBC driver cannot convert and that cannot be cast to text: {', '.join(sorted(dropped))}"
        )
        return [column for column in columns if column.name not in dropped]

    def fetch_sample_data(self, columns=None) -> TableData:
        """Read the sample, casting the opaque columns to text on the way out.

        Only the columns syscasts says are castable are wrapped. Everything else
        goes through the base implementation untouched, so tables without an
        opaque column -- almost all of them -- behave exactly as before.
        """
        castable = {name for name, ok in self._unfriendly_columns().items() if ok}
        if not castable:
            return super().fetch_sample_data(columns)

        dataset = self.get_dataset()
        wanted = None if not columns else {column.name for column in columns}
        sqa_columns = [
            column
            for column in inspect(dataset).c
            if column.name != RANDOM_LABEL and (wanted is None or column.name in wanted)
        ]
        selected = [
            cast(column, LVarchar()).label(column.name) if column.name in castable else column for column in sqa_columns
        ]

        with self.session_factory() as client:
            rows = client.query(*selected).select_from(dataset).limit(self.sample_limit).all()

        return TableData(
            columns=[column.name for column in sqa_columns],
            rows=[[self._truncate_cell(cell) for cell in row] for row in rows],
        )

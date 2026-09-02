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
"""The profiler must skip Informix's large objects, and profile everything else.

Skipping is not a quality choice. Informix rejects its large-object types in
COUNT(DISTINCT), MIN, MAX and GROUP BY outright, so a profiler that includes them
loses the whole column's metrics to a failed statement -- and on BYTE and TEXT
even a plain COUNT fails.

Both halves are asserted here. A profiler that skipped everything would pass a
test that only checked the large objects, and would be just as broken.
"""

import pytest

from metadata.generated.schema.entity.data.table import Table
from metadata.ingestion.lineage.sql_lineage import search_cache
from metadata.workflow.metadata import MetadataWorkflow
from metadata.workflow.profiler import ProfilerWorkflow

SKIPPED_COLUMNS = ["c_text", "c_byte", "c_clob", "c_blob"]
PROFILED_COLUMNS = ["id", "c_char", "c_vchar", "c_lvchar", "c_bool"]


@pytest.fixture(scope="module")
def profiled_table(
    patch_passwords_for_db_services,
    run_workflow,
    ingestion_config,
    profiler_config,
    metadata,
    db_service,
) -> Table:
    search_cache.clear()
    run_workflow(MetadataWorkflow, ingestion_config)
    run_workflow(ProfilerWorkflow, profiler_config)

    fqn = f"{db_service.fullyQualifiedName.root}.itest.informix.lob_types"
    table = metadata.get_latest_table_profile(fqn)
    assert table is not None, f"no profile written for {fqn}"
    return table


def _profile_of(table: Table, column_name: str):
    column = next((col for col in table.columns if col.name.root == column_name), None)
    assert column is not None, f"{column_name} missing from the profiled table"
    return column.profile


class TestProfilerSkipsLargeObjects:
    @pytest.mark.parametrize("column_name", SKIPPED_COLUMNS)
    def test_large_object_columns_carry_no_metrics(self, profiled_table, column_name):
        assert _profile_of(profiled_table, column_name) is None

    @pytest.mark.parametrize("column_name", PROFILED_COLUMNS)
    def test_every_other_column_is_still_profiled(self, profiled_table, column_name):
        """The skip has to be narrow.

        Informix rejects the large objects specifically; if the whole table
        stopped being profiled the tests above would still pass.
        """
        assert _profile_of(profiled_table, column_name) is not None

    def test_the_table_itself_is_profiled(self, profiled_table):
        assert profiled_table.profile is not None
        assert profiled_table.profile.rowCount == 2

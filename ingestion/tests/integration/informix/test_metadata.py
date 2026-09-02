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
"""What the JDBC driver loses, and the connector puts back.

Informix hands JDBC a VARCHAR(2147483647) for all four of its large-object types
and no length at all for its string types. Both are corrected from the server's
own catalogue, and both corrections are invisible in unit tests: they only show
up once a real workflow has written a real entity.
"""

import pytest

from metadata.generated.schema.entity.data.table import DataType, Table
from metadata.workflow.metadata import MetadataWorkflow

# The four types that arrive indistinguishable from a short text column, and the
# OM type each must end up as. CLOB and BLOB are what is_blob() keys on, which is
# what stops the profiler issuing SQL Informix rejects.
LARGE_OBJECT_COLUMNS = {
    "c_text": (DataType.TEXT, "text"),
    "c_byte": (DataType.BYTES, "byte"),
    "c_clob": (DataType.CLOB, "clob"),
    "c_blob": (DataType.BLOB, "blob"),
}

# Declared width -> what must reach the catalogue. Every one of these was 1
# before the fix, so a CHAR(300) claimed to hold a single byte.
DECLARED_WIDTHS = {
    "a_char": 10,
    "b_char": 300,
    "c_vchar": 50,
    "d_vchar": 50,
    "e_lvchar": 1000,
    "f_nchar": 20,
    "g_nvarchar": 60,
}


@pytest.fixture(scope="module")
def ingested_tables(patch_passwords_for_db_services, run_workflow, ingestion_config, metadata, db_service):
    run_workflow(MetadataWorkflow, ingestion_config)

    def _get(table_name: str) -> Table:
        fqn = f"{db_service.fullyQualifiedName.root}.itest.informix.{table_name}"
        table = metadata.get_by_name(entity=Table, fqn=fqn)
        assert table is not None, f"{fqn} was not ingested"
        return table

    return _get


def _column(table: Table, name: str):
    column = next((col for col in table.columns if col.name.root == name), None)
    assert column is not None, f"{name} missing from {table.fullyQualifiedName.root}"
    return column


class TestLargeObjectTypes:
    @pytest.mark.parametrize(("column_name", "expected"), LARGE_OBJECT_COLUMNS.items())
    def test_large_object_keeps_its_real_type(self, ingested_tables, column_name, expected):
        expected_type, expected_display = expected
        column = _column(ingested_tables("lob_types"), column_name)
        assert column.dataType == expected_type
        assert column.dataTypeDisplay == expected_display

    def test_boolean_is_not_mistaken_for_a_large_object(self, ingested_tables):
        """BOOLEAN shares coltype 41 with CLOB and BLOB.

        Matching on the code alone would silently make every boolean column in a
        catalogue unprofilable, which is invisible until someone asks why a
        column has no metrics.
        """
        assert _column(ingested_tables("lob_types"), "c_bool").dataType == DataType.BOOLEAN

    def test_ordinary_columns_are_left_alone(self, ingested_tables):
        table = ingested_tables("lob_types")
        assert _column(table, "id").dataType == DataType.INT
        assert _column(table, "c_char").dataType == DataType.CHAR
        assert _column(table, "c_vchar").dataType == DataType.VARCHAR


class TestDeclaredWidths:
    @pytest.mark.parametrize(("column_name", "expected_length"), DECLARED_WIDTHS.items())
    def test_declared_width_reaches_the_catalogue(self, ingested_tables, column_name, expected_length):
        assert _column(ingested_tables("char_widths"), column_name).dataLength == expected_length

    def test_the_two_collength_encodings_are_told_apart(self, ingested_tables):
        """One field, two meanings, and masking the wrong one corrupts silently.

        VARCHAR(50,10) stores 50 + 10*256; masking recovers the 50. CHAR(300)
        stores 300 outright, and masking it would yield 44 -- a plausible-looking
        width that no one would question.
        """
        table = ingested_tables("char_widths")
        assert _column(table, "d_vchar").dataLength == 50
        assert _column(table, "b_char").dataLength == 300

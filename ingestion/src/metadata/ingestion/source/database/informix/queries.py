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
Informix SQL queries.

Verified against Informix 14.10.FC9W1DE and 15.0.1.0.3. ``name`` and ``procname``
are CHAR columns, so both are TRIMmed -- an untrimmed database name becomes a
padded FQN and a second, phantom Database entity on the next ingestion run.
"""

# sysmaster is cross-referenced explicitly: the connector is attached to the
# configured database, not to sysmaster.
INFORMIX_GET_DATABASE_NAMES = """
SELECT TRIM(name) AS dbname
FROM sysmaster:sysdatabases
ORDER BY name
"""

# FIRST 1 keeps the test-connection step cheap; sysprocedures is populated with
# built-ins on every database, so this probes privilege, not user content.
INFORMIX_TEST_GET_STORED_PROCEDURES = """
SELECT FIRST 1 TRIM(procname) AS procname
FROM sysprocedures
"""

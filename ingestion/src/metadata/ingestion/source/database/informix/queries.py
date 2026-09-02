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

# Two things the JDBC driver loses, both recovered here in one pass.
#
# Type: Informix reports BYTE, TEXT, CLOB and BLOB identically, as
# VARCHAR(2147483647), so the true type has to come from the catalogue or the
# catalogue records a binary column as ordinary short text.
#
# Length: the driver reports no length at all for CHAR/VARCHAR/LVARCHAR, and
# OpenMetadata then defaults every one of them to 1 -- a CHAR(10) is catalogued
# as char(1). collength carries the real width, but is encoded per type:
# VARCHAR and NVARCHAR pack an optional reserved minimum into the high byte
# (VARCHAR(50,10) is 50 + 10*256 = 2610), so those need masking, while CHAR,
# NCHAR and LVARCHAR are the plain width and must not be masked -- they are the
# only ones that legitimately exceed 255 (CHAR(300) is 300).
#
# coltype carries flags in its high bits -- NOT NULL adds 256, so BYTE NOT NULL
# arrives as 267 -- hence MOD. Code 41 is the generic opaque type and covers
# BOOLEAN as well as the smart large objects, so the subtype name in sysxtdtypes
# is what separates them; filtering on 41 alone would make every boolean column
# unprofilable.
#
# The ANSI JOIN syntax is deliberate. With Informix's older "OUTER" form a
# predicate mentioning the outer table is folded into the join instead of
# filtering, and this query silently returns every column in the schema.
INFORMIX_GET_COLUMN_TYPES = """
SELECT TRIM(t.tabname) AS tabname,
       TRIM(c.colname) AS colname,
       MOD(c.coltype, 256) AS basetype,
       c.collength AS collength,
       TRIM(x.name) AS xtype
FROM systables t
JOIN syscolumns c ON c.tabid = t.tabid
LEFT JOIN sysxtdtypes x ON x.extended_id = c.extended_id
WHERE t.owner = :owner
  AND t.tabtype IN ('T', 'V')
  AND MOD(c.coltype, 256) IN (0, 11, 12, 13, 15, 16, 40, 41)
"""

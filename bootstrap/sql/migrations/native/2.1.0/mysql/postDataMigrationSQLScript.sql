-- Backfill test_case_incident from existing history: fold each stateId chain to its
-- first/last timestamps, pick the latest record (MAX(id) tie-break, matching the read
-- query this table replaces), and upsert one summary row per incident. Idempotent.
INSERT INTO test_case_incident (stateId, entityFQNHash, testCaseResolutionStatusType, assignee, severity, createdAt, updatedAt, latestRecordId)
WITH chain AS (
  SELECT stateId, MIN(timestamp) AS createdAt, MAX(timestamp) AS updatedAt
  FROM test_case_resolution_status_time_series
  GROUP BY stateId
),
latestRecord AS (
  SELECT c.stateId, c.createdAt, c.updatedAt, MAX(t.id) AS latestId
  FROM chain c
  INNER JOIN test_case_resolution_status_time_series t
    ON t.stateId = c.stateId AND t.timestamp = c.updatedAt
  GROUP BY c.stateId, c.createdAt, c.updatedAt
)
SELECT t.stateId, t.entityFQNHash, t.testCaseResolutionStatusType, t.assignee,
       JSON_UNQUOTE(JSON_EXTRACT(t.json, '$.severity')), l.createdAt, l.updatedAt, t.id
FROM latestRecord l
INNER JOIN test_case_resolution_status_time_series t ON t.id = l.latestId
WHERE t.entityFQNHash IS NOT NULL
ON DUPLICATE KEY UPDATE
  testCaseResolutionStatusType = VALUES(testCaseResolutionStatusType),
  assignee = VALUES(assignee),
  severity = VALUES(severity),
  test_case_incident.createdAt = LEAST(test_case_incident.createdAt, VALUES(createdAt)),
  updatedAt = VALUES(updatedAt),
  latestRecordId = VALUES(latestRecordId);

-- Activity comments are retained indefinitely unless an administrator explicitly configures a
-- positive retention period. Preserve any value already chosen by an administrator.
UPDATE installed_apps
SET json = JSON_INSERT(json, '$.appConfiguration.activityCommentsRetentionPeriod', 0)
WHERE name = 'DataRetentionApplication';

UPDATE apps_marketplace
SET json = JSON_INSERT(json, '$.appConfiguration.activityCommentsRetentionPeriod', 0)
WHERE name = 'DataRetentionApplication';

UPDATE entity_extension
SET json = JSON_INSERT(json, '$.appConfiguration.activityCommentsRetentionPeriod', 0)
WHERE extension LIKE 'app.version.%'
  AND json->>'$.name' = 'DataRetentionApplication';

-- Informix no longer declares supportsLineageExtraction or supportsUsageExtraction:
-- the connector implements neither, and while they were declared the UI offered
-- Lineage and Usage pipelines that fail on import. The connection schema sets
-- additionalProperties false, so a stored service still carrying either field can
-- no longer be deserialised -- and one such row fails the whole databaseServices
-- listing, not just its own service.
UPDATE dbservice_entity
SET json = JSON_REMOVE(json,
    '$.connection.config.supportsLineageExtraction',
    '$.connection.config.supportsUsageExtraction')
WHERE serviceType = 'Informix'
  AND (JSON_CONTAINS_PATH(json, 'one', '$.connection.config.supportsLineageExtraction')
       OR JSON_CONTAINS_PATH(json, 'one', '$.connection.config.supportsUsageExtraction'));

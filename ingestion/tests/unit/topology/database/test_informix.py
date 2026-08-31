"""
Test the Informix connector package: connection URL construction, SSL policy,
test-connection step coverage, source wiring and the scoped JDBC classpath.
"""

import json
import unittest
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from metadata.generated.schema.entity.services.connections.database.informixConnection import (
    InformixConnection as InformixConnectionConfig,
)
from metadata.generated.schema.entity.services.serviceType import ServiceType
from metadata.generated.schema.security.ssl.verifySSLConfig import SslMode
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.informix.connection import (
    InformixConnection,
    get_connection_url,
)
from metadata.ingestion.source.database.informix.metadata import InformixSource
from metadata.utils.service_spec.service_spec import BaseSpec

TEST_CONNECTION_JSON = (
    Path(__file__).parents[5]
    / "openmetadata-service/src/main/resources/json/data/testConnections/database/informix.json"
)

# Steps test_connection_db_common supplies from the generic inspector.
COMMON_STEPS = {"CheckAccess", "GetSchemas", "GetTables", "GetViews"}


def config(**overrides) -> InformixConnectionConfig:
    base = {
        "hostPort": "db.local:9088",
        "database": "spike",
        "username": "informix",
        "password": "in4mix",
        "serverName": "informix",
    }
    base.update(overrides)
    return InformixConnectionConfig(**base)


class InformixConnectionUrlTest(TestCase):
    def test_url_carries_informixserver(self):
        """serverName is required by the schema because the driver demands it."""
        self.assertEqual(
            get_connection_url(config()),
            "informix://informix:in4mix@db.local:9088/spike?INFORMIXSERVER=informix",
        )

    def test_password_special_characters_are_escaped(self):
        """':' or '@' in a password silently truncates an unescaped URL."""
        url = get_connection_url(config(password="p@ss:w/rd?x"))
        self.assertIn("informix:p%40ss%3Aw%2Frd%3Fx@db.local", url)

    def test_connection_options_are_appended(self):
        url = get_connection_url(config(connectionOptions={"DELIMIDENT": "y"}))
        self.assertIn("DELIMIDENT=y", url)


class InformixSslPolicyTest(TestCase):
    def test_require_enables_ssl(self):
        self.assertIn("SSLCONNECTION=true", get_connection_url(config(sslMode=SslMode.require)))

    def test_disable_and_unset_are_plaintext(self):
        for mode in (None, SslMode.disable):
            self.assertNotIn("SSLCONNECTION", get_connection_url(config(sslMode=mode)), f"mode={mode}")

    def test_opportunistic_modes_are_plaintext(self):
        """The driver has no opportunistic mode; allow/prefer permit plaintext."""
        for mode in (SslMode.allow, SslMode.prefer):
            self.assertNotIn("SSLCONNECTION", get_connection_url(config(sslMode=mode)), f"mode={mode}")

    def test_verify_modes_refuse_rather_than_downgrade(self):
        """A verifying mode must never quietly become an unverified channel."""
        for mode in (SslMode.verify_ca, SslMode.verify_full):
            with self.assertRaises(NotImplementedError, msg=f"mode={mode}"):
                get_connection_url(config(sslMode=mode))


class InformixTestConnectionTest(TestCase):
    @unittest.skipUnless(TEST_CONNECTION_JSON.exists(), "service resources not on disk")
    def test_every_declared_step_is_covered(self):
        """
        Guards drift between informix.json and the code. A step declared in the
        JSON with nothing to run it reports as a failure in the UI.
        """
        declared = {s["name"] for s in json.loads(TEST_CONNECTION_JSON.read_text())["steps"]}

        with (
            patch("metadata.ingestion.source.database.informix.connection.test_connection_db_common") as common,
            patch.object(InformixConnection, "client", new=object()),
        ):
            InformixConnection(config()).test_connection(metadata=OpenMetadata.__new__(OpenMetadata))

        supplied = set(common.call_args.kwargs["queries"])
        self.assertEqual(
            declared,
            COMMON_STEPS | supplied,
            "informix.json steps and the steps the connector runs have diverged",
        )


class InformixSourceTest(TestCase):
    def test_service_spec_resolves(self):
        """
        BaseSpec resolves connectors purely by import path. A rename or a missing
        service_spec.py fails only at ingestion time, never at import time.
        """
        spec = BaseSpec.get_for_source(service_type=ServiceType.Database, source_type="informix")
        self.assertEqual(
            spec.metadata_source_class,
            "metadata.ingestion.source.database.informix.metadata.InformixSource",
        )
        self.assertEqual(
            spec.connection_class,
            "metadata.ingestion.source.database.informix.connection.InformixConnection",
        )

    def test_rejects_foreign_connection_config(self):
        cockroach = {
            "type": "cockroach",
            "serviceName": "x",
            "serviceConnection": {
                "config": {
                    "type": "Cockroach",
                    "username": "u",
                    "authType": {"password": "p"},
                    "hostPort": "localhost:26257",
                    "database": "c",
                }
            },
            "sourceConfig": {"config": {"type": "DatabaseMetadata"}},
        }
        with self.assertRaises(InvalidSourceException):
            InformixSource.create(cockroach, OpenMetadata.__new__(OpenMetadata))


class InformixClasspathTest(TestCase):
    def test_classpath_is_scoped_to_informix_jars(self):
        """
        Without an explicit ``jars`` list sqlalchemy-jdbcapi starts the JVM with
        every recommended driver -- ~190MB, and a 404 on the gbase coordinate.
        """
        from sqlalchemy.engine.url import make_url

        from metadata.ingestion.source.database.informix.dialect import InformixDialect

        with patch(
            "metadata.ingestion.source.database.informix.dialect.get_driver_path",
            side_effect=lambda name: Path(f"/cache/{name}.jar"),
        ) as get_path:
            _, kwargs = InformixDialect().create_connect_args(make_url("informix://u:p@h:9088/db?INFORMIXSERVER=srv"))

        self.assertEqual([c.args[0] for c in get_path.call_args_list], ["informix", "informix_bson"])
        self.assertEqual(kwargs["jars"], ["/cache/informix.jar", "/cache/informix_bson.jar"])

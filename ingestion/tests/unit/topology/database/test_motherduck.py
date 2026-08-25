#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Unit tests for the MotherDuck metadata source."""

from unittest.mock import MagicMock, patch

from metadata.generated.schema.entity.data.table import TableType
from metadata.generated.schema.entity.services.connections.database.motherDuckConnection import (
    MotherDuckConnection,
)
from metadata.ingestion.source.database.common_db_source import TableNameAndType
from metadata.ingestion.source.database.motherduck.metadata import MotherDuckSource

MOTHERDUCK_METADATA_CONFIG = {
    "type": "motherduck",
    "serviceName": "motherduck",
    "serviceConnection": {
        "config": {
            "type": "MotherDuck",
            "token": "test-token",
        }
    },
    "sourceConfig": {"config": {"type": "DatabaseMetadata"}},
}


def _source(database: str | None) -> MotherDuckSource:
    source = MotherDuckSource.__new__(MotherDuckSource)
    source.service_connection = MotherDuckConnection(
        token="test-token",
        database=database,
    )
    source.metadata = MagicMock()
    source.context = MagicMock()
    source.source_config = MagicMock(
        databaseFilterPattern=None,
        useFqnForFiltering=False,
    )
    source.status = MagicMock()
    source.set_inspector = MagicMock()
    return source


def test_configured_database_is_the_only_database_ingested():
    source = _source("analytics")

    with (
        patch(
            "metadata.ingestion.source.database.motherduck.metadata.fqn.build",
            return_value="service.analytics",
        ),
        patch(
            "metadata.ingestion.source.database.motherduck.metadata.filter_by_database",
            return_value=False,
        ),
    ):
        databases = list(source.get_database_names())

    assert databases == ["analytics"]
    source.set_inspector.assert_called_once_with(database_name="analytics")


def test_create_validates_motherduck_workflow_config():
    metadata = MagicMock()

    with patch.object(MotherDuckSource, "__init__", return_value=None) as init:
        source = MotherDuckSource.create(MOTHERDUCK_METADATA_CONFIG, metadata)

    assert isinstance(source, MotherDuckSource)
    init.assert_called_once()
    assert init.call_args.args[1] is metadata


def test_database_discovery_applies_filter_before_connecting():
    source = _source(None)
    source.get_database_names_raw = MagicMock(return_value=iter(["analytics", "sandbox"]))

    with (
        patch(
            "metadata.ingestion.source.database.motherduck.metadata.fqn.build",
            side_effect=lambda *args, **kwargs: f"service.{kwargs['database_name']}",
        ),
        patch(
            "metadata.ingestion.source.database.motherduck.metadata.filter_by_database",
            side_effect=lambda pattern, name: name == "sandbox",
        ),
    ):
        databases = list(source.get_database_names())

    assert databases == ["analytics"]
    source.set_inspector.assert_called_once_with(database_name="analytics")
    source.status.filter.assert_called_once_with(
        "service.sandbox",
        "Database Filtered Out",
    )


def test_schema_discovery_uses_current_database_query():
    source = _source("analytics")
    source.engine = MagicMock()
    result = source.engine.connect.return_value.__enter__.return_value.execute.return_value
    result.__iter__.return_value = iter([("main",), ("reporting",)])

    assert list(source.get_raw_database_schema_names()) == ["main", "reporting"]


def test_raw_database_discovery_delegates_to_catalog_query():
    source = _source(None)
    source._execute_database_query = MagicMock(return_value=iter(["analytics"]))

    assert list(source.get_database_names_raw()) == ["analytics"]
    source._execute_database_query.assert_called_once()


def test_table_discovery_uses_current_database_catalog():
    source = _source("analytics")
    source.engine = MagicMock()
    result = source.engine.connect.return_value.__enter__.return_value.execute.return_value
    result.scalars.return_value = ["events", "users"]

    assert list(source.query_table_names_and_types("reporting")) == [
        TableNameAndType(name="events"),
        TableNameAndType(name="users"),
    ]
    assert result.scalars.called


def test_view_discovery_uses_current_database_catalog():
    source = _source("analytics")
    source.engine = MagicMock()
    result = source.engine.connect.return_value.__enter__.return_value.execute.return_value
    result.scalars.return_value = ["daily_events"]

    assert list(source.query_view_names_and_types("reporting")) == [
        TableNameAndType(name="daily_events", type_=TableType.View),
    ]
    assert result.scalars.called


def test_database_connection_failure_does_not_stop_other_databases():
    source = _source(None)
    source.get_database_names_raw = MagicMock(return_value=iter(["broken", "analytics"]))
    source.set_inspector.side_effect = [RuntimeError("unavailable"), None]

    with (
        patch(
            "metadata.ingestion.source.database.motherduck.metadata.fqn.build",
            side_effect=lambda *args, **kwargs: f"service.{kwargs['database_name']}",
        ),
        patch(
            "metadata.ingestion.source.database.motherduck.metadata.filter_by_database",
            return_value=False,
        ),
    ):
        databases = list(source.get_database_names())

    assert databases == ["analytics"]
    assert source.set_inspector.call_count == 2

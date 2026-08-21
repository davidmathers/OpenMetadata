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
"""Unit tests for MotherDuck connection handling."""

from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.pool import StaticPool

from metadata.core.connections.lifetime import Borrowed
from metadata.core.connections.test_connection.check import collect_checks
from metadata.core.connections.test_connection.checks.database import DatabaseStep
from metadata.generated.schema.entity.services.connections.database.motherDuckConnection import (
    MotherDuckConnection as MotherDuckConnectionConfig,
)
from metadata.ingestion.source.database.motherduck.connection import (
    MotherDuckChecks,
    MotherDuckConnection,
)
from metadata.ingestion.source.database.motherduck.metadata import MotherDuckSource
from metadata.ingestion.source.database.motherduck.queries import (
    MOTHERDUCK_GET_DATABASES,
    MOTHERDUCK_GET_SCHEMAS,
)

CONNECTION_MODULE = "metadata.ingestion.source.database.motherduck.connection"


def _connection(database: str | None = None) -> MotherDuckConnectionConfig:
    return MotherDuckConnectionConfig(
        token="test-token",
        database=database,
        connectionArguments={"config": {"threads": "2"}},
    )


def test_workspace_connection_url_uses_motherduck_protocol():
    assert MotherDuckConnection.get_connection_url(_connection()) == "duckdb:///md:"


def test_database_connection_url_targets_configured_database():
    assert MotherDuckConnection.get_connection_url(_connection("analytics")) == "duckdb:///md:analytics"


def test_connection_args_merge_user_config_and_secret_token():
    config = _connection("analytics")

    args = MotherDuckConnection.get_connection_args(config)

    assert args == {
        "config": {
            "threads": "2",
            "motherduck_token": "test-token",
            "custom_user_agent": "openmetadata/2.0.0",
        }
    }
    assert config.connectionArguments.root == {"config": {"threads": "2"}}


def test_checks_cover_registered_database_steps():
    engine = create_engine("sqlite://", poolclass=StaticPool)
    checks = MotherDuckChecks(db=Borrowed.of(engine), database_schema=None)

    assert set(collect_checks(checks)) == {
        DatabaseStep.CheckAccess,
        DatabaseStep.GetDatabases,
        DatabaseStep.GetSchemas,
        DatabaseStep.GetTables,
        DatabaseStep.GetViews,
    }


def test_connection_exposes_motherduck_checks_over_owned_engine():
    connection = MotherDuckConnection(_connection())
    connection._client = MagicMock()

    checks = connection.checks()

    assert isinstance(checks, MotherDuckChecks)
    assert checks._db.client is connection._client


def test_get_client_uses_connector_url_and_arguments_and_disposes_engine():
    engine = MagicMock()
    connection = MotherDuckConnection(_connection("analytics"))

    with patch(
        f"{CONNECTION_MODULE}.create_generic_db_connection",
        return_value=engine,
    ) as create_connection:
        assert connection.client is engine
        connection.close()

    assert create_connection.call_args.kwargs["get_connection_url_fn"] is MotherDuckConnection.get_connection_url
    assert create_connection.call_args.kwargs["get_connection_args_fn"] is MotherDuckConnection.get_connection_args
    assert create_connection.call_args.kwargs["isolation_level"] is None
    engine.dispose.assert_called_once_with()


def test_check_methods_delegate_to_database_check_helpers():
    engine = MagicMock()
    checks = MotherDuckChecks(db=Borrowed.of(engine), database_schema="analytics")

    with (
        patch(f"{CONNECTION_MODULE}.run_sql", return_value="sql") as run_sql,
        patch(f"{CONNECTION_MODULE}.list_schemas", return_value="schemas") as list_schemas,
        patch(f"{CONNECTION_MODULE}.list_tables", return_value="tables") as list_tables,
        patch(f"{CONNECTION_MODULE}.list_views", return_value="views") as list_views,
    ):
        assert checks.check_access() == "sql"
        assert checks.get_databases() == "sql"
        assert checks.get_schemas() == "schemas"
        assert checks.get_tables() == "tables"
        assert checks.get_views() == "views"

    assert run_sql.call_count == 2
    list_schemas.assert_called_once_with(engine)
    list_tables.assert_called_once_with(engine, "analytics", checks.SYSTEM_SCHEMAS)
    list_views.assert_called_once_with(engine, "analytics", checks.SYSTEM_SCHEMAS)


def test_catalog_queries_scope_metadata_to_current_database(tmp_path):
    database_path = tmp_path / "catalog.duckdb"
    engine = create_engine(f"duckdb:///{database_path}")
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA analytics"))
        connection.execute(text("CREATE TABLE analytics.events (event_id INTEGER, event_name VARCHAR)"))
        connection.execute(text("CREATE VIEW analytics.event_names AS SELECT event_name FROM analytics.events"))
        databases = connection.execute(text(MOTHERDUCK_GET_DATABASES)).scalars().all()
        schemas = connection.execute(text(MOTHERDUCK_GET_SCHEMAS)).scalars().all()

    inspector = inspect(engine)
    columns = inspector.get_columns("events", schema="analytics")

    engine.dispose()

    assert databases == ["catalog"]
    assert schemas == ["analytics", "main"]
    assert inspector.get_table_names(schema="analytics") == ["events"]
    assert inspector.get_view_names(schema="analytics") == ["event_names"]
    assert [column["name"] for column in columns] == ["event_id", "event_name"]


def test_array_columns_include_element_type(tmp_path):
    database_path = tmp_path / "arrays.duckdb"
    engine = create_engine(f"duckdb:///{database_path}")
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA analytics"))
        connection.execute(
            text("CREATE TABLE analytics.measurements (powers_dbm DOUBLE[], labels VARCHAR[], samples INTEGER[3])")
        )

    source = MotherDuckSource.__new__(MotherDuckSource)
    source.engine = engine
    columns = source._get_columns_internal(
        schema_name="analytics",
        table_name="measurements",
        db_name="arrays",
        inspector=inspect(engine),
    )
    processed_types = {column["name"]: source._process_col_type(column, "analytics")[2] for column in columns}

    engine.dispose()

    assert processed_types["powers_dbm"] == {
        "name": "powers_dbm",
        "dataType": "ARRAY",
        "arrayDataType": "DOUBLE",
        "dataTypeDisplay": "array<double>",
    }
    assert processed_types["labels"]["arrayDataType"] == "VARCHAR"
    assert processed_types["samples"]["arrayDataType"] == "INT"


def test_connection_capabilities_are_enabled():
    connection = _connection()

    assert connection.supportsMetadataExtraction.root is True
    assert connection.supportsDBTExtraction.root is True
    assert connection.supportsProfiler.root is True
    assert connection.supportsViewLineageExtraction.root is True

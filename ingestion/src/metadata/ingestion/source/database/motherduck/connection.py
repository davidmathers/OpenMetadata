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
"""MotherDuck connection handler."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Any

from sqlalchemy.engine import Engine

from metadata.core.connections.test_connection import ErrorPack, Matchers, check, when
from metadata.core.connections.test_connection.checks.database import (
    DEFAULT_SAMPLE_ROWS,
    DatabaseStep,
    list_schemas,
    list_tables,
    list_views,
    run_sql,
)
from metadata.core.connections.test_connection.checks.summary import enumerated
from metadata.generated.schema.entity.services.connections.database.motherDuckConnection import (
    MotherDuckConnection as MotherDuckConnectionConfig,
)
from metadata.generated.schema.entity.services.connections.database.motherDuckConnection import (
    MotherDuckScheme,
)
from metadata.ingestion.connections.builders import (
    create_generic_db_connection,
    get_connection_args_common,
)
from metadata.ingestion.connections.connection import BaseConnection
from metadata.ingestion.source.database.motherduck.queries import MOTHERDUCK_GET_DATABASES
from metadata.utils.client_version import get_client_version

if TYPE_CHECKING:
    from metadata.core.connections.lifetime import Borrowed
    from metadata.core.connections.test_connection import ChecksProvider
    from metadata.core.connections.test_connection.records import Evidence


MOTHERDUCK_ERRORS = ErrorPack(
    when(Matchers.contains("authentication")).diagnose(
        "Authentication failed",
        fix="Verify the MotherDuck access token is valid and has not expired.",
    ),
    when(Matchers.contains("token")).diagnose(
        "MotherDuck token rejected",
        fix="Create a new read-only MotherDuck token and update the service connection.",
    ),
)


class MotherDuckChecks:
    """Test-connection checks for MotherDuck."""

    errors = MOTHERDUCK_ERRORS
    SYSTEM_SCHEMAS = frozenset({"information_schema", "pg_catalog"})

    def __init__(self, db: Borrowed[Engine], database_schema: str | None) -> None:
        self._db = db
        self.database_schema = database_schema

    @check(DatabaseStep.CheckAccess)
    def check_access(self) -> Evidence:
        return run_sql(self._db.client, "SELECT 1", lambda _: "connection established")

    @check(DatabaseStep.GetDatabases)
    def get_databases(self) -> Evidence:
        return run_sql(
            self._db.client,
            MOTHERDUCK_GET_DATABASES,
            lambda rows: enumerated(len(rows), "database", DEFAULT_SAMPLE_ROWS),
        )

    @check(DatabaseStep.GetSchemas)
    def get_schemas(self) -> Evidence:
        return list_schemas(self._db.client)

    @check(DatabaseStep.GetTables)
    def get_tables(self) -> Evidence:
        return list_tables(
            self._db.client,
            self.database_schema,
            self.SYSTEM_SCHEMAS,
        )

    @check(DatabaseStep.GetViews)
    def get_views(self) -> Evidence:
        return list_views(
            self._db.client,
            self.database_schema,
            self.SYSTEM_SCHEMAS,
        )


class MotherDuckConnection(BaseConnection[MotherDuckConnectionConfig, Engine]):
    """Build and test a SQLAlchemy engine backed by MotherDuck."""

    @staticmethod
    def get_connection_url(connection: MotherDuckConnectionConfig) -> str:
        database = connection.database or ""
        scheme = connection.scheme.value if connection.scheme else MotherDuckScheme.duckdb.value
        return f"{scheme}:///md:{database}"

    @staticmethod
    def get_connection_args(connection: MotherDuckConnectionConfig) -> dict[str, Any]:
        connection_args = deepcopy(get_connection_args_common(connection))
        driver_config = connection_args.setdefault("config", {})
        driver_config["motherduck_token"] = connection.token.get_secret_value()
        driver_config.setdefault(
            "custom_user_agent",
            f"openmetadata/{get_client_version()}",
        )
        return connection_args

    def _get_client(self) -> Engine:
        engine = create_generic_db_connection(
            connection=self.service_connection,
            get_connection_url_fn=self.get_connection_url,
            get_connection_args_fn=self.get_connection_args,
            isolation_level=None,
        )
        self._on_close(engine.dispose)
        return engine

    def checks(self) -> ChecksProvider:
        return MotherDuckChecks(
            db=self.borrow(),
            database_schema=None,
        )

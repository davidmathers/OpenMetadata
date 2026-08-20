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
"""MotherDuck metadata source."""

import traceback
from collections.abc import Iterable
from typing import Any, Optional

from sqlalchemy import text

from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.table import TableType
from metadata.generated.schema.entity.services.connections.database.motherDuckConnection import (
    MotherDuckConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import Source as WorkflowSource
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.common_db_source import (
    CommonDbSourceService,
    TableNameAndType,
)
from metadata.ingestion.source.database.motherduck.queries import (
    MOTHERDUCK_GET_DATABASES,
    MOTHERDUCK_GET_SCHEMAS,
    MOTHERDUCK_GET_TABLES,
    MOTHERDUCK_GET_VIEWS,
)
from metadata.ingestion.source.database.multi_db_source import MultiDBSource
from metadata.utils import fqn
from metadata.utils.filters import filter_by_database
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class MotherDuckSource(CommonDbSourceService, MultiDBSource):
    """Extract metadata from one or every database visible to a MotherDuck token."""

    @classmethod
    def create(
        cls,
        config_dict: dict[str, Any],
        metadata: OpenMetadata,
        pipeline_name: Optional[str] = None,  # noqa: UP045
    ):
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        if config.serviceConnection is None:
            raise InvalidSourceException("MotherDuck service connection is required")
        connection = config.serviceConnection.root.config
        if not isinstance(connection, MotherDuckConnection):
            raise InvalidSourceException(f"Expected MotherDuckConnection, but got {connection}")
        return cls(config, metadata)

    def get_configured_database(self) -> Optional[str]:  # noqa: UP045
        return self.service_connection.database

    def get_database_names_raw(self) -> Iterable[str]:
        yield from self._execute_database_query(MOTHERDUCK_GET_DATABASES)

    def get_database_names(self) -> Iterable[str]:
        configured_database = self.get_configured_database()
        database_names = [configured_database] if configured_database else self.get_database_names_raw()
        for database_name in database_names:
            database_fqn = (
                fqn.build(
                    self.metadata,
                    entity_type=Database,
                    service_name=self.context.get().database_service,  # pyright: ignore[reportAttributeAccessIssue]
                    database_name=database_name,
                )
                or database_name
            )
            if filter_by_database(
                self.source_config.databaseFilterPattern,
                database_fqn if self.source_config.useFqnForFiltering else database_name,
            ):
                self.status.filter(database_fqn, "Database Filtered Out")
                continue
            try:
                self.set_inspector(database_name=database_name)
                yield database_name
            except Exception as exc:
                logger.debug(traceback.format_exc())
                logger.error(f"Error trying to connect to database {database_name}: {exc}")

    def get_raw_database_schema_names(self) -> Iterable[str]:
        with self.engine.connect() as connection:
            result = connection.execute(text(MOTHERDUCK_GET_SCHEMAS))
            yield from (row[0] for row in result)

    def query_table_names_and_types(self, schema_name: str) -> Iterable[TableNameAndType]:
        with self.engine.connect() as connection:
            result = connection.execute(
                text(MOTHERDUCK_GET_TABLES),
                {"schema_name": schema_name},
            )
            yield from (TableNameAndType(name=name) for name in result.scalars())

    def query_view_names_and_types(self, schema_name: str) -> Iterable[TableNameAndType]:
        with self.engine.connect() as connection:
            result = connection.execute(
                text(MOTHERDUCK_GET_VIEWS),
                {"schema_name": schema_name},
            )
            yield from (TableNameAndType(name=name, type_=TableType.View) for name in result.scalars())

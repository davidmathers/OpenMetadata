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
"""Unit tests for MotherDuck lineage registration."""

from unittest.mock import MagicMock, patch

from metadata.generated.schema.entity.services.connections.database.motherDuckConnection import (
    MotherDuckType,
)
from metadata.ingestion.lineage.models import ConnectionTypeDialectMapper, Dialect
from metadata.ingestion.source.database.motherduck.connection import MotherDuckConnection
from metadata.ingestion.source.database.motherduck.lineage import MotherDuckLineageSource
from metadata.ingestion.source.database.motherduck.metadata import MotherDuckSource
from metadata.ingestion.source.database.motherduck.service_spec import ServiceSpec
from metadata.utils.importer import get_class_path

MOTHERDUCK_LINEAGE_CONFIG = {
    "type": "motherduck-lineage",
    "serviceName": "motherduck",
    "serviceConnection": {
        "config": {
            "type": "MotherDuck",
            "token": "test-token",
        }
    },
    "sourceConfig": {
        "config": {
            "type": "DatabaseLineage",
            "queryLogDuration": 1,
            "resultLimit": 1000,
        }
    },
}


def test_motherduck_uses_duckdb_lineage_dialect():
    assert ConnectionTypeDialectMapper.dialect_of(MotherDuckType.MotherDuck.value) is Dialect.DUCKDB


def test_create_validates_motherduck_lineage_workflow_config():
    metadata = MagicMock()

    with patch.object(MotherDuckLineageSource, "__init__", return_value=None) as init:
        source = MotherDuckLineageSource.create(MOTHERDUCK_LINEAGE_CONFIG, metadata)

    assert isinstance(source, MotherDuckLineageSource)
    init.assert_called_once()
    assert init.call_args.args[1] is metadata


def test_service_spec_registers_metadata_lineage_and_connection_classes():
    assert ServiceSpec.metadata_source_class == get_class_path(MotherDuckSource)
    assert ServiceSpec.lineage_source_class == get_class_path(MotherDuckLineageSource)
    assert ServiceSpec.connection_class == get_class_path(MotherDuckConnection)

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

"""Dagster source models."""

import json
from typing import List, Optional  # noqa: UP035

from pydantic import BaseModel, ConfigDict, Field

from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.tests.testDefinition import DataQualityDimensions

# Models for get_task_runs


class RunStepStats(BaseModel):
    runId: str  # noqa: N815
    startTime: Optional[float] = None  # noqa: N815, UP045
    endTime: Optional[float] = None  # noqa: N815, UP045
    status: Optional[str] = None  # noqa: UP045


class SolidStepStatsConnection(BaseModel):
    nodes: Optional[List[RunStepStats]] = None  # noqa: UP006, UP045


class TaskSolidHandle(BaseModel):
    stepStats: Optional[SolidStepStatsConnection] = None  # noqa: N815, UP045


class DagsterPipeline(BaseModel):
    id: str
    name: str
    description: Optional[str] = None  # noqa: UP045
    solidHandle: Optional[TaskSolidHandle] = None  # noqa: N815, UP045


class PipelineOrErrorModel(BaseModel):
    pipelineOrError: DagsterPipeline  # noqa: N815


# Models for get_run_list
class DagsterLocation(BaseModel):
    id: str
    name: str


class Node(BaseModel):
    id: str
    name: str
    location: Optional[DagsterLocation] = None  # noqa: UP045
    pipelines: List[DagsterPipeline]  # noqa: UP006


class RepositoryConnection(BaseModel):
    nodes: List[Node]  # noqa: UP006


class RepositoriesOrErrorModel(BaseModel):
    repositoriesOrError: RepositoryConnection  # noqa: N815


# Models for get_jobs
class SolidName(BaseModel):
    name: str


class DependsOnSolid(BaseModel):
    solid: Optional[SolidName] = None  # noqa: UP045


class SolidInput(BaseModel):
    dependsOn: Optional[List[DependsOnSolid]] = None  # noqa: N815, UP006, UP045


class Solid(BaseModel):
    name: str
    inputs: Optional[List[SolidInput]] = None  # noqa: UP006, UP045


class SolidHandle(BaseModel):
    handleID: str  # noqa: N815
    solid: Optional[Solid] = None  # noqa: UP045


class GraphOrError(BaseModel):
    id: str
    name: str
    description: Optional[str] = None  # noqa: UP045
    solidHandles: Optional[List[SolidHandle]] = None  # noqa: N815, UP006, UP045


class GraphOrErrorModel(BaseModel):
    graphOrError: GraphOrError  # noqa: N815


class AssetKey(BaseModel):
    path: List[str]  # noqa: UP006

    def to_string(self) -> str:
        """Convert asset key path to dot-separated string"""
        return ".".join(self.path)

    def normalize(self, strip_prefix: int = 0) -> "AssetKey":
        """
        Return a new AssetKey with N leading segments removed.

        Args:
            strip_prefix: Number of leading segments to remove

        Returns:
            New AssetKey with normalized path
        """
        if strip_prefix <= 0:
            return self

        if strip_prefix >= len(self.path):
            from metadata.utils.logger import ingestion_logger  # noqa: PLC0415

            logger = ingestion_logger()
            logger.warning(
                f"stripAssetKeyPrefixLength ({strip_prefix}) is >= asset key length "
                f"({len(self.path)}). Asset key: {self.to_string()}"
            )
            return self

        return AssetKey(path=self.path[strip_prefix:])


class DagsterAssetReference(BaseModel):
    assetKey: AssetKey  # noqa: N815


class AssetDependency(BaseModel):
    asset: Optional[DagsterAssetReference] = None  # noqa: UP045


class MetadataEntry(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    typename: str = Field(alias="__typename")
    label: str
    text: Optional[str] = None  # noqa: UP045
    path: Optional[str] = None  # noqa: UP045
    jsonString: Optional[str] = None  # noqa: N815, UP045
    url: Optional[str] = None  # noqa: UP045
    mdStr: Optional[str] = None  # noqa: N815, UP045
    floatValue: Optional[float] = None  # noqa: N815, UP045
    intRepr: Optional[str] = None  # noqa: N815, UP045
    boolValue: Optional[bool] = None  # noqa: N815, UP045
    timestamp: Optional[float] = None  # noqa: UP045

    def value(self) -> Optional[object]:  # noqa: UP045
        """Return the represented Dagster metadata value."""
        if self.jsonString is not None:
            try:
                return json.loads(self.jsonString)
            except json.JSONDecodeError:
                return self.jsonString

        for value in (
            self.text,
            self.path,
            self.url,
            self.mdStr,
            self.floatValue,
            self.intRepr,
            self.boolValue,
            self.timestamp,
        ):
            if value is not None:
                return value

        return None

    def result_value(self) -> Optional[str]:  # noqa: UP045
        """Serialize a metadata value for an OpenMetadata test result."""
        value = self.value()
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value, separators=(",", ":"), sort_keys=True)
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)


class AssetMaterialization(BaseModel):
    runId: str  # noqa: N815
    timestamp: Optional[float] = None  # noqa: UP045
    metadataEntries: Optional[List[MetadataEntry]] = None  # noqa: N815, UP006, UP045


class JobReference(BaseModel):
    name: str
    id: str


class DagsterAssetNode(BaseModel):
    id: str
    assetKey: AssetKey  # noqa: N815
    description: Optional[str] = None  # noqa: UP045
    computeKind: Optional[str] = None  # noqa: N815, UP045
    opNames: Optional[List[str]] = None  # noqa: N815, UP006, UP045
    dependencies: Optional[List[AssetDependency]] = None  # noqa: UP006, UP045
    definitionMetadataEntries: Optional[List[MetadataEntry]] = None  # noqa: N815, UP006, UP045
    assetMaterializations: Optional[List[AssetMaterialization]] = None  # noqa: N815, UP006, UP045
    assetChecksOrError: Optional["AssetChecksOrError"] = None  # noqa: N815
    jobs: Optional[List[JobReference]] = None  # noqa: UP006, UP045


class DagsterAssetCheck(BaseModel):
    """A Dagster asset-check definition."""

    name: str
    assetKey: AssetKey  # noqa: N815
    description: Optional[str] = None  # noqa: UP045
    jobNames: list[str] = []  # noqa: N815
    blocking: bool = False


class AssetChecksOrError(BaseModel):
    """Asset-check query result or Dagster compatibility error."""

    model_config = ConfigDict(populate_by_name=True)

    typename: str = Field(alias="__typename")
    checks: Optional[List[DagsterAssetCheck]] = None  # noqa: UP006, UP045
    message: Optional[str] = None  # noqa: UP045


class AssetCheckEvaluation(BaseModel):
    """Result emitted by a completed Dagster asset check."""

    timestamp: float
    checkName: str  # noqa: N815
    assetKey: AssetKey  # noqa: N815
    success: bool
    severity: str
    description: Optional[str] = None  # noqa: UP045
    partition: Optional[str] = None  # noqa: UP045
    metadataEntries: list[MetadataEntry] = []  # noqa: N815


class AssetCheckExecution(BaseModel):
    """One Dagster asset-check execution."""

    id: str
    runId: str  # noqa: N815
    status: str
    timestamp: float
    partition: Optional[str] = None  # noqa: UP045
    stepKey: Optional[str] = None  # noqa: N815, UP045
    evaluation: Optional[AssetCheckEvaluation] = None  # noqa: UP045


class AssetCheckExecutionsResponse(BaseModel):
    """GraphQL response containing asset-check executions."""

    assetCheckExecutions: list[AssetCheckExecution]  # noqa: N815


class OpenMetadataAssetCheckOverride(BaseModel):
    """Optional target overrides for one named Dagster asset check."""

    enabled: Optional[bool] = None  # noqa: UP045
    entity_fqn: Optional[str] = None  # noqa: UP045
    column_name: Optional[str] = None  # noqa: UP045
    quality_dimension: Optional[DataQualityDimensions] = None  # noqa: UP045
    history_limit: Optional[int] = Field(default=None, ge=1, le=1000)  # noqa: UP045


class OpenMetadataAssetCheckMapping(BaseModel):
    """Opt-in mapping stored in a Dagster asset's static metadata."""

    enabled: bool = False
    entity_fqn: str
    column_name: Optional[str] = None  # noqa: UP045
    quality_dimension: DataQualityDimensions = DataQualityDimensions.NoDimension
    history_limit: int = Field(default=100, ge=1, le=1000)
    checks: dict[str, OpenMetadataAssetCheckOverride] = {}

    def for_check(self, check_name: str) -> "OpenMetadataAssetCheckMapping":
        """Apply an optional named-check override to the asset defaults."""
        override = self.checks.get(check_name)
        if not override:
            return self

        updates = {
            field: value
            for field in (
                "enabled",
                "entity_fqn",
                "column_name",
                "quality_dimension",
                "history_limit",
            )
            if (value := getattr(override, field)) is not None
        }
        return self.model_copy(update=updates)


class OpenMetadataAssetMetadata(BaseModel):
    """OpenMetadata settings stored under the ``openmetadata`` metadata key."""

    asset_checks: Optional[OpenMetadataAssetCheckMapping] = None  # noqa: UP045


class AssetRepository(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    typename: str = Field(alias="__typename")
    id: Optional[str] = None  # noqa: UP045
    name: Optional[str] = None  # noqa: UP045
    assetNodes: Optional[List[DagsterAssetNode]] = None  # noqa: N815, UP006, UP045


class AssetsQueryResponse(BaseModel):
    repositoryOrError: AssetRepository  # noqa: N815


class TableResolutionResult(BaseModel):
    """Result of resolving a Dagster asset to an OpenMetadata table"""

    table_fqn: Optional[str] = None  # noqa: UP045
    table_entity: Optional[Table] = None  # noqa: UP045

    @property
    def is_resolved(self) -> bool:
        """Check if the asset was successfully resolved to a table"""
        return self.table_entity is not None

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
"""
Dagster source to extract metadata from OM UI
"""

import traceback
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional  # noqa: UP035

from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.api.tests.createTestCase import CreateTestCaseRequest
from metadata.generated.schema.api.tests.createTestDefinition import (
    CreateTestDefinitionRequest,
)
from metadata.generated.schema.entity.data.pipeline import (
    Pipeline,
    PipelineStatus,
    StatusType,
    Task,
    TaskStatus,
)
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.connections.pipeline.dagsterConnection import (
    DagsterConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.tests.basic import (
    TestCaseResult,
    TestCaseStatus,
    TestResultValue,
)
from metadata.generated.schema.tests.testCase import TestCase
from metadata.generated.schema.tests.testDefinition import (
    EntityType,
    TestPlatform,
)
from metadata.generated.schema.type.basic import (
    EntityLink,
    EntityName,
    FullyQualifiedEntityName,
    Markdown,
    SourceUrl,
    Timestamp,
)
from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
from metadata.generated.schema.type.entityLineage import Source as LineageSource
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.step import WorkflowFatalError
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.models.ometa_classification import OMetaTagAndClassification
from metadata.ingestion.models.pipeline_status import OMetaPipelineStatus
from metadata.ingestion.models.tests_data import OMetaTestCaseResultsSample
from metadata.ingestion.models.topology import NodeStage, TopologyContextManager
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.pipeline.dagster.models import (
    AssetCheckExecution,
    DagsterAssetCheck,
    DagsterAssetNode,
    DagsterPipeline,
    OpenMetadataAssetCheckMapping,
    OpenMetadataAssetMetadata,
    RunStepStats,
    SolidHandle,
    TableResolutionResult,
)
from metadata.ingestion.source.pipeline.pipeline_service import PipelineServiceSource
from metadata.utils import entity_link as entity_link_utils
from metadata.utils import fqn
from metadata.utils.filters import filter_by_pipeline
from metadata.utils.helpers import clean_uri
from metadata.utils.logger import ingestion_logger
from metadata.utils.tag_utils import get_ometa_tag_and_classification, get_tag_labels

logger = ingestion_logger()

STATUS_MAP = {
    "success": StatusType.Successful.value,
    "failure": StatusType.Failed.value,
    "queued": StatusType.Pending.value,
}

DAGSTER_TAG_CATEGORY = "DagsterTags"
OPENMETADATA_METADATA_LABEL = "openmetadata"


@dataclass(frozen=True)
class DagsterAssetCheckProjection:
    """OpenMetadata entities and results projected from one Dagster asset check."""

    test_definition: CreateTestDefinitionRequest
    test_case: CreateTestCaseRequest
    test_case_fqn: str
    results: tuple[TestCaseResult, ...]


class DagsterSource(PipelineServiceSource):
    """
    Implements the necessary methods ot extract
    Pipeline metadata from Dagster's metadata db
    """

    topology = PipelineServiceSource.topology.model_copy(deep=True)
    topology.pipeline.stages.extend(
        [
            NodeStage(
                type_=CreateTestDefinitionRequest,
                processor="yield_asset_check_definitions",
                nullable=True,
            ),
            NodeStage(
                type_=CreateTestCaseRequest,
                processor="yield_asset_check_cases",
                nullable=True,
            ),
            NodeStage(
                type_=OMetaTestCaseResultsSample,
                processor="yield_asset_check_results",
                nullable=True,
            ),
        ]
    )
    context = TopologyContextManager(topology)

    @classmethod
    def create(cls, config_dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None):  # noqa: UP045
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection: DagsterConnection = config.serviceConnection.root.config
        if not isinstance(connection, DagsterConnection):
            raise InvalidSourceException(f"Expected DagsterConnection, but got {connection}")
        return cls(config, metadata)

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__(config, metadata)
        self.strip_asset_key_prefix_length = self.service_connection.stripAssetKeyPrefixLength or 0
        self._asset_check_projection_cache: dict[tuple[str, str, str], tuple[DagsterAssetCheckProjection, ...]] = {}

    def _get_downstream_tasks(self, job: SolidHandle) -> Optional[List[str]]:  # noqa: UP006, UP045
        """Method to get downstream tasks"""
        down_stream_tasks = []
        if job.solid:
            for tasks in job.solid.inputs or []:
                if tasks:
                    for task in tasks.dependsOn or []:
                        down_stream_tasks.append(task.solid.name)  # noqa: PERF401
        return down_stream_tasks or None

    def _get_task_list(self, pipeline_name: str) -> Optional[List[Task]]:  # noqa: UP006, UP045
        """Method to collect all the tasks from dagster and return it in a task list"""
        jobs = self.client.get_jobs(
            pipeline_name=pipeline_name,
            repository_name=self.context.get().repository_name,
            repository_location=self.context.get().repository_location,
        )
        task_list: List[Task] = []  # noqa: UP006
        if jobs:
            for job in jobs.solidHandles or []:
                try:
                    task = Task(
                        name=job.handleID,
                        displayName=job.handleID,
                        downstreamTasks=self._get_downstream_tasks(job=job),
                        sourceUrl=self.get_source_url(pipeline_name=pipeline_name, task_name=job.handleID),
                    )
                    task_list.append(task)
                except Exception as exc:
                    logger.debug(traceback.format_exc())
                    logger.warning(f"Error to fetch tasks for {pipeline_name}:{job}: {exc}")

        return task_list or None

    def yield_pipeline(self, pipeline_details: DagsterPipeline) -> Iterable[Either[CreatePipelineRequest]]:
        """Convert a DAG into a Pipeline Entity"""

        try:
            pipeline_request = CreatePipelineRequest(
                name=EntityName(pipeline_details.id.replace(":", "")),
                displayName=pipeline_details.name,
                description=(Markdown(pipeline_details.description) if pipeline_details.description else None),
                tasks=self._get_task_list(pipeline_name=pipeline_details.name),
                service=FullyQualifiedEntityName(self.context.get().pipeline_service),
                tags=get_tag_labels(
                    metadata=self.metadata,
                    tags=[self.context.get().repository_name],
                    classification_name=DAGSTER_TAG_CATEGORY,
                    include_tags=self.source_config.includeTags,
                ),
                sourceUrl=self.get_source_url(pipeline_name=pipeline_details.name, task_name=None),
            )
            yield Either(right=pipeline_request)
            self.register_record(pipeline_request=pipeline_request)
        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=pipeline_details.name,
                    error=f"Error to yield pipeline for {pipeline_details}: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def yield_tag(self, pipeline_details: DagsterPipeline) -> Iterable[Either[OMetaTagAndClassification]]:
        yield from get_ometa_tag_and_classification(
            tags=[self.context.get().repository_name],
            classification_name=DAGSTER_TAG_CATEGORY,
            tag_description="Dagster Tag",
            classification_description="Tags associated with dagster entities",
            include_tags=self.source_config.includeTags,
        )

    def _get_task_status(self, run: RunStepStats, task_name: str) -> Iterable[Either[OMetaPipelineStatus]]:
        """Prepare the OMetaPipelineStatus"""
        try:
            # Convert Dagster timestamps from seconds to milliseconds
            task_status = TaskStatus(
                name=task_name,
                executionStatus=STATUS_MAP.get(run.status.lower(), StatusType.Pending.value),
                startTime=int(run.startTime * 1000) if run.startTime else None,
                endTime=int(run.endTime * 1000) if run.endTime else None,
            )
            pipeline_status = PipelineStatus(
                taskStatus=[task_status],
                executionStatus=STATUS_MAP.get(run.status.lower(), StatusType.Pending.value),
                timestamp=Timestamp(int(run.startTime * 1000)) if run.startTime else None,
            )
            pipeline_fqn = fqn.build(
                metadata=self.metadata,
                entity_type=Pipeline,
                service_name=self.context.get().pipeline_service,
                pipeline_name=self.context.get().pipeline,
            )
            pipeline_status_yield = OMetaPipelineStatus(
                pipeline_fqn=pipeline_fqn,
                pipeline_status=pipeline_status,
            )
            yield Either(right=pipeline_status_yield)
        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=run.runId,
                    error=f"Error to yield run status for {run}: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def yield_pipeline_status(self, pipeline_details: DagsterPipeline) -> Iterable[Either[OMetaPipelineStatus]]:
        """Yield the pipeline and task status"""
        pipeline_fqn = fqn.build(
            metadata=self.metadata,
            entity_type=Pipeline,
            service_name=self.context.get().pipeline_service,
            pipeline_name=self.context.get().pipeline,
        )
        pipeline_entity = self.metadata.get_by_name(entity=Pipeline, fqn=pipeline_fqn, fields=["tasks"])
        for task in pipeline_entity.tasks or []:
            try:
                runs = self.client.get_task_runs(
                    task.name,
                    pipeline_name=pipeline_details.name,
                    repository_name=self.context.get().repository_name,
                    repository_location=self.context.get().repository_location,
                )
                for run in runs.solidHandle.stepStats.nodes or []:
                    yield from self._get_task_status(run=run, task_name=task.name)
            except Exception as exc:
                yield Either(
                    left=StackTraceError(
                        name=f"{pipeline_details.name} Pipeline Status",
                        error=f"Error to yield pipeline status for {pipeline_details}: {exc}",
                        stackTrace=traceback.format_exc(),
                    )
                )

    def yield_pipeline_lineage_details(self, pipeline_details: DagsterPipeline) -> Iterable[Either[AddLineageRequest]]:
        """
        Extract lineage between pipeline and data assets.
        Based on Dagster assets and their dependencies.
        """
        try:
            pipeline_fqn = fqn.build(
                metadata=self.metadata,
                entity_type=Pipeline,
                service_name=self.context.get().pipeline_service,
                pipeline_name=self.context.get().pipeline,
            )

            pipeline_entity = self.metadata.get_by_name(entity=Pipeline, fqn=pipeline_fqn)

            if not pipeline_entity:
                logger.warning(f"Pipeline entity not found for FQN: {pipeline_fqn}")
                return

            assets = self.client.get_assets(
                repository_name=self.context.get().repository_name,
                repository_location=self.context.get().repository_location,
            )

            if not assets:
                logger.debug("No assets found for lineage extraction")
                return

            asset_by_key = {asset.assetKey.to_string(): asset for asset in assets}

            pipeline_assets = [asset for asset in assets if self._is_asset_in_pipeline(asset, pipeline_details.name)]

            lineage_details = LineageDetails(
                pipeline=EntityReference(id=pipeline_entity.id.root, type="pipeline"),
                source=LineageSource.PipelineLineage,
            )

            for asset in pipeline_assets:
                to_result = self._resolve_asset_to_table(asset, self.get_db_service_names() or ["*"])

                if not to_result.is_resolved:
                    normalized_key = asset.assetKey.normalize(self.strip_asset_key_prefix_length).to_string()
                    logger.debug(
                        f"Could not resolve table for asset: {asset.assetKey.to_string()} "
                        f"(normalized: {normalized_key})"
                    )
                    continue

                for dependency in asset.dependencies or []:
                    if not dependency.asset:
                        continue

                    dep_asset_key = dependency.asset.assetKey.to_string()
                    dep_asset = asset_by_key.get(dep_asset_key)

                    if not dep_asset:
                        continue

                    from_result = self._resolve_asset_to_table(dep_asset, self.get_db_service_names() or ["*"])

                    if not from_result.is_resolved:
                        continue

                    yield Either(
                        right=AddLineageRequest(
                            edge=EntitiesEdge(
                                fromEntity=EntityReference(
                                    id=from_result.table_entity.id,
                                    type="table",
                                ),
                                toEntity=EntityReference(
                                    id=to_result.table_entity.id,
                                    type="table",
                                ),
                                lineageDetails=lineage_details,
                            )
                        )
                    )

        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=pipeline_details.name,
                    error=f"Error extracting pipeline lineage: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def yield_asset_check_definitions(
        self, pipeline_details: DagsterPipeline
    ) -> Iterable[Either[CreateTestDefinitionRequest]]:
        """Yield unique OpenMetadata definitions required by Dagster asset checks."""
        seen = set()
        for projection in self._get_asset_check_projections(pipeline_details):
            definition_name = projection.test_definition.name.root
            if definition_name in seen:
                continue
            seen.add(definition_name)
            yield Either(right=projection.test_definition)

    def yield_asset_check_cases(self, pipeline_details: DagsterPipeline) -> Iterable[Either[CreateTestCaseRequest]]:
        """Reconcile Dagster asset-check definitions into OpenMetadata test cases."""
        for projection in self._get_asset_check_projections(pipeline_details):
            yield Either(right=projection.test_case)

    def yield_asset_check_results(
        self, pipeline_details: DagsterPipeline
    ) -> Iterable[Either[OMetaTestCaseResultsSample]]:
        """Publish new Dagster asset-check evaluations to OpenMetadata."""
        for projection in self._get_asset_check_projections(pipeline_details):
            if not projection.results:
                continue

            timestamps = [result.timestamp.root for result in projection.results]
            existing_results = self.metadata.get_test_case_results(
                test_case_fqn=projection.test_case_fqn,
                start_ts=min(timestamps),
                end_ts=max(timestamps),
            )
            existing_timestamps = {result.timestamp.root for result in existing_results or []}
            for result in projection.results:
                if result.timestamp.root in existing_timestamps:
                    continue
                yield Either(
                    right=OMetaTestCaseResultsSample(
                        test_case_results=result,
                        test_case_name=projection.test_case_fqn,
                    )
                )

    def _get_asset_check_projections(
        self, pipeline_details: DagsterPipeline
    ) -> tuple[DagsterAssetCheckProjection, ...]:
        cache_key = (
            self.context.get().repository_location,
            self.context.get().repository_name,
            pipeline_details.name,
        )
        cached = self._asset_check_projection_cache.get(cache_key)
        if cached is not None:
            return cached

        assets = self.client.get_assets(
            repository_name=self.context.get().repository_name,
            repository_location=self.context.get().repository_location,
        )
        projections = []
        for asset in assets or []:
            mapping = self._get_asset_check_mapping(asset)
            if not mapping or not mapping.enabled:
                continue

            checks_result = asset.assetChecksOrError
            if not checks_result or checks_result.typename != "AssetChecks":
                if checks_result and checks_result.message:
                    logger.warning(
                        f"Cannot ingest asset checks for {asset.assetKey.to_string()}: {checks_result.message}"
                    )
                continue

            for check in checks_result.checks or []:
                if pipeline_details.name not in check.jobNames:
                    continue
                check_mapping = mapping.for_check(check.name)
                if not check_mapping.enabled:
                    continue
                projection = self._build_asset_check_projection(
                    asset=asset,
                    check=check,
                    mapping=check_mapping,
                )
                if projection:
                    projections.append(projection)

        cached_projections = tuple(projections)
        self._asset_check_projection_cache[cache_key] = cached_projections
        return cached_projections

    def _get_asset_check_mapping(self, asset: DagsterAssetNode) -> Optional[OpenMetadataAssetCheckMapping]:  # noqa: UP045
        for entry in asset.definitionMetadataEntries or []:
            if entry.label != OPENMETADATA_METADATA_LABEL:
                continue
            value = entry.value()
            if not isinstance(value, dict):
                logger.warning(
                    f"Dagster asset {asset.assetKey.to_string()} has non-object {OPENMETADATA_METADATA_LABEL} metadata"
                )
                return None
            try:
                return OpenMetadataAssetMetadata.model_validate(value).asset_checks
            except Exception as exc:
                logger.warning(f"Invalid OpenMetadata asset-check mapping for {asset.assetKey.to_string()}: {exc}")
                return None
        return None

    def _build_asset_check_projection(
        self,
        asset: DagsterAssetNode,
        check: DagsterAssetCheck,
        mapping: OpenMetadataAssetCheckMapping,
    ) -> Optional[DagsterAssetCheckProjection]:  # noqa: UP045
        table = self.metadata.get_by_name(
            entity=Table,
            fqn=mapping.entity_fqn,
            fields=["columns"],
        )
        if not table:
            logger.warning(
                f"Cannot ingest Dagster asset check {asset.assetKey.to_string()}:{check.name}: "
                f"OpenMetadata table {mapping.entity_fqn} was not found"
            )
            return None

        if mapping.column_name and not any(column.name.root == mapping.column_name for column in table.columns or []):
            logger.warning(
                f"Cannot ingest Dagster asset check {asset.assetKey.to_string()}:{check.name}: "
                f"column {mapping.column_name} was not found on {mapping.entity_fqn}"
            )
            return None

        table_fqn = table.fullyQualifiedName.root
        table_fqn_parts = fqn.split(table_fqn)
        if len(table_fqn_parts) != 4:
            logger.warning(f"Unexpected OpenMetadata table FQN for Dagster asset checks: {table_fqn}")
            return None

        entity_type = EntityType.COLUMN if mapping.column_name else EntityType.TABLE
        definition_name = f"dagsterAssetCheck{entity_type.value.title()}{mapping.quality_dimension.value}"
        test_case_fqn = fqn.build(
            metadata=self.metadata,
            entity_type=TestCase,
            service_name=table_fqn_parts[0],
            database_name=table_fqn_parts[1],
            schema_name=table_fqn_parts[2],
            table_name=table_fqn_parts[3],
            column_name=mapping.column_name,
            test_case_name=check.name,
        )
        executions = self.client.get_asset_check_executions(
            asset_key=check.assetKey,
            check_name=check.name,
            limit=mapping.history_limit,
        )
        results = tuple(
            result
            for execution in sorted(
                executions or [],
                key=lambda execution: execution.timestamp,
            )
            if (result := self._build_asset_check_result(asset, check, execution))
        )

        return DagsterAssetCheckProjection(
            test_definition=CreateTestDefinitionRequest(
                name=definition_name,
                displayName=f"Dagster {entity_type.value.title()} Asset Check",
                description=(
                    "A data-quality check executed by Dagster and projected into OpenMetadata without re-execution."
                ),
                entityType=entity_type,
                testPlatforms=[TestPlatform.Other],
                dataQualityDimension=mapping.quality_dimension,
            ),
            test_case=CreateTestCaseRequest(
                name=check.name,
                displayName=check.name,
                description=check.description,
                testDefinition=FullyQualifiedEntityName(definition_name),
                entityLink=EntityLink(
                    entity_link_utils.get_entity_link(
                        Table,
                        table_fqn,
                        column_name=mapping.column_name,
                    )
                ),
            ),
            test_case_fqn=test_case_fqn,
            results=results,
        )

    def _build_asset_check_result(
        self,
        asset: DagsterAssetNode,
        check: DagsterAssetCheck,
        execution: AssetCheckExecution,
    ) -> Optional[TestCaseResult]:  # noqa: UP045
        if execution.status in {"IN_PROGRESS", "SKIPPED"}:
            return None

        evaluation = execution.evaluation
        if execution.status == "EXECUTION_FAILED":
            status = TestCaseStatus.Aborted
            timestamp = execution.timestamp
            message = f'Dagster asset check "{check.name}" did not complete'
        elif execution.status in {"SUCCEEDED", "FAILED"} and evaluation:
            status = TestCaseStatus.Success if evaluation.success else TestCaseStatus.Failed
            timestamp = evaluation.timestamp
            message = evaluation.description or (
                f'Dagster asset check "{check.name}" {"passed" if evaluation.success else "failed"}'
            )
        else:
            logger.warning(
                f"Ignoring unsupported Dagster asset-check execution state "
                f"{execution.status} for {asset.assetKey.to_string()}:{check.name}"
            )
            return None

        values = {
            "dagsterAssetKey": asset.assetKey.to_string(),
            "dagsterRunId": execution.runId,
            "dagsterExecutionId": execution.id,
            "dagsterBlocking": str(check.blocking).lower(),
        }
        if execution.stepKey:
            values["dagsterStepKey"] = execution.stepKey
        partition = evaluation.partition if evaluation else execution.partition
        if partition:
            values["dagsterPartitionKey"] = partition
        if evaluation:
            values["dagsterSeverity"] = evaluation.severity
            for entry in evaluation.metadataEntries:
                if entry.label == OPENMETADATA_METADATA_LABEL:
                    continue
                value = entry.result_value()
                if value is not None:
                    values[entry.label] = value

        return TestCaseResult(
            timestamp=int(timestamp * 1000),
            testCaseStatus=status,
            result=message,
            testResultValue=[TestResultValue(name=name, value=value) for name, value in values.items()],
        )

    def get_pipelines_list(self) -> Iterable[DagsterPipeline]:
        """Get List of all pipelines"""
        try:
            results = self.client.get_run_list()
            for result in results:
                for job in result.pipelines or []:
                    if filter_by_pipeline(
                        self.source_config.pipelineFilterPattern,
                        job.name,
                    ):
                        self.status.filter(
                            job.name,
                            "Pipeline Filtered Out",
                        )
                        continue
                    self.context.get().repository_location = result.location.name
                    self.context.get().repository_name = result.name
                    yield job
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.error(
                f"Unable to get pipelines list\n"
                f"Please check if dagster is running correctly and is in good state: {exc}"
            )
            raise WorkflowFatalError("Unable to get pipeline list")  # noqa: B904

    def get_pipeline_name(self, pipeline_details: DagsterPipeline) -> str:
        return pipeline_details.name

    def get_source_url(self, pipeline_name: str, task_name: Optional[str]) -> Optional[SourceUrl]:  # noqa: UP045
        """
        Method to get source url for pipelines and tasks for dagster
        """
        try:
            url = (
                f"{clean_uri(self.service_connection.host)}/locations/"
                f"{self.context.get().repository_location}/jobs/{pipeline_name}/"
            )
            if task_name:
                url = f"{url}{task_name}"
            return SourceUrl(url)
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(f"Error to get pipeline url: {exc}")
        return None

    def _is_asset_in_pipeline(self, asset: DagsterAssetNode, pipeline_name: str) -> bool:
        """Check if asset is associated with the given pipeline/job"""
        if not asset.jobs:
            return False
        return any(job.name == pipeline_name for job in asset.jobs)

    def _resolve_asset_to_table(self, asset: DagsterAssetNode, db_services: List[str]) -> TableResolutionResult:  # noqa: UP006
        """
        Resolve Dagster asset to OpenMetadata Table entity.
        Tries multiple strategies to parse asset key into database/schema/table.

        Returns: TableResolutionResult with table_fqn and table_entity (or None if not found)
        """
        normalized_asset_key = asset.assetKey.normalize(self.strip_asset_key_prefix_length)
        asset_key_str = normalized_asset_key.to_string()

        parts = normalized_asset_key.path
        if len(parts) == 3:
            database, schema, table = parts
        elif len(parts) == 2:
            database = None
            schema, table = parts
        elif len(parts) == 1:
            database = None
            schema = None
            table = parts[0]
        else:
            logger.debug(
                f"Unexpected asset key format after normalization: {asset_key_str} "
                f"(original: {asset.assetKey.to_string()}, stripped {self.strip_asset_key_prefix_length} segments)"
            )
            return TableResolutionResult()

        if not database or not schema:
            metadata_parts = self._parse_asset_from_materialization(asset)
            if metadata_parts:
                database = database or metadata_parts.get("database")
                schema = schema or metadata_parts.get("schema")

        for service_name in db_services or ["*"]:
            try:
                table_fqn = fqn.build(
                    metadata=self.metadata,
                    entity_type=Table,
                    service_name=service_name,
                    database_name=database,
                    schema_name=schema,
                    table_name=table,
                )

                table_entity = self.metadata.get_by_name(entity=Table, fqn=table_fqn)

                if table_entity:
                    return TableResolutionResult(table_fqn=table_fqn, table_entity=table_entity)

            except Exception as exc:
                logger.debug(f"Failed to resolve for service {service_name}: {exc}")

        return TableResolutionResult()

    def _parse_asset_from_materialization(self, asset: DagsterAssetNode) -> Optional[Dict[str, str]]:  # noqa: UP006, UP045
        """
        Extract table info from asset materialization metadata.
        """
        if not asset.assetMaterializations:
            return None

        materialization = asset.assetMaterializations[0]

        if not materialization.metadataEntries:
            return None

        metadata_map = {}
        for entry in materialization.metadataEntries:
            label = entry.label.lower()
            if label in ["database", "db", "database_name"]:
                metadata_map["database"] = entry.text
            elif label in ["schema", "schema_name"]:
                metadata_map["schema"] = entry.text
            elif label in ["table", "table_name"]:
                metadata_map["table"] = entry.text

        return metadata_map if metadata_map.get("table") else None

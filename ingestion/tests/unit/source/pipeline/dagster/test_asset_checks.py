from dagster_graphql.schema import create_schema
from graphql import parse, validate

from metadata.generated.schema.api.tests.createTestCase import CreateTestCaseRequest
from metadata.generated.schema.api.tests.createTestDefinition import (
    CreateTestDefinitionRequest,
)
from metadata.generated.schema.entity.data.table import Column, DataType, Table
from metadata.generated.schema.tests.basic import (
    TestCaseResult,
    TestCaseStatus,
)
from metadata.generated.schema.tests.testDefinition import DataQualityDimensions, EntityType, TestPlatform
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.source.pipeline.dagster.metadata import (
    DagsterAssetCheckProjection,
    DagsterSource,
)
from metadata.ingestion.source.pipeline.dagster.models import (
    AssetCheckEvaluation,
    AssetCheckExecution,
    AssetKey,
    DagsterAssetCheck,
    DagsterAssetNode,
    MetadataEntry,
)
from metadata.ingestion.source.pipeline.dagster.queries import (
    ASSET_CHECK_EXECUTIONS_QUERY,
    DAGSTER_ASSETS_QUERY,
)


def make_asset() -> DagsterAssetNode:
    return DagsterAssetNode(
        id="asset-id",
        assetKey=AssetKey(path=["raw", "garfield_tp6"]),
        definitionMetadataEntries=[
            MetadataEntry(
                __typename="JsonMetadataEntry",
                label="openmetadata",
                jsonString=(
                    '{"asset_checks":{"enabled":true,'
                    '"entity_fqn":"motherduck.lab_raw.main.garfield_tp6",'
                    '"quality_dimension":"Validity","history_limit":25,'
                    '"checks":{"schema_valid_column":{"column_name":"schema_valid",'
                    '"quality_dimension":"Completeness"}}}}'
                ),
            )
        ],
    )


def make_check() -> DagsterAssetCheck:
    return DagsterAssetCheck(
        name="json_schema_valid",
        assetKey=AssetKey(path=["raw", "garfield_tp6"]),
        description="The source document conforms to its JSON Schema.",
        jobNames=["garfield_tp6_job"],
        blocking=True,
    )


def make_execution(
    status: str,
    evaluation: AssetCheckEvaluation | None,
) -> AssetCheckExecution:
    return AssetCheckExecution(
        id="321",
        runId="dagster-run-id",
        status=status,
        timestamp=1_775_000_000.125,
        partition="2026-08-20",
        stepKey="raw__garfield_tp6_json_schema_valid",
        evaluation=evaluation,
    )


def test_asset_check_queries_match_dagster_graphql_schema() -> None:
    schema = create_schema().graphql_schema

    assert validate(schema, parse(DAGSTER_ASSETS_QUERY)) == []
    assert validate(schema, parse(ASSET_CHECK_EXECUTIONS_QUERY)) == []


def test_reads_explicit_openmetadata_asset_check_mapping() -> None:
    source = object.__new__(DagsterSource)

    mapping = source._get_asset_check_mapping(make_asset())

    assert mapping is not None
    assert mapping.enabled is True
    assert mapping.entity_fqn == "motherduck.lab_raw.main.garfield_tp6"
    assert mapping.quality_dimension.value == "Validity"
    assert mapping.history_limit == 25

    override = mapping.for_check("schema_valid_column")
    assert override.entity_fqn == mapping.entity_fqn
    assert override.column_name == "schema_valid"
    assert override.quality_dimension.value == "Completeness"


def test_maps_completed_dagster_check_to_openmetadata_result() -> None:
    source = object.__new__(DagsterSource)
    evaluation = AssetCheckEvaluation(
        timestamp=1_775_000_001.5,
        checkName="json_schema_valid",
        assetKey=AssetKey(path=["raw", "garfield_tp6"]),
        success=False,
        severity="ERROR",
        description="Two documents failed validation",
        partition="2026-08-20",
        metadataEntries=[
            MetadataEntry(
                __typename="IntMetadataEntry",
                label="errorCount",
                intRepr="2",
            ),
            MetadataEntry(
                __typename="JsonMetadataEntry",
                label="invalidFiles",
                jsonString='["s3://landing/bad-1.json","s3://landing/bad-2.json"]',
            ),
        ],
    )

    result = source._build_asset_check_result(
        make_asset(),
        make_check(),
        make_execution("FAILED", evaluation),
    )

    assert result is not None
    assert result.timestamp.root == 1_775_000_001_500
    assert result.testCaseStatus == TestCaseStatus.Failed
    assert result.result == "Two documents failed validation"
    values = {value.name: value.value for value in result.testResultValue or []}
    assert values == {
        "dagsterAssetKey": "raw.garfield_tp6",
        "dagsterRunId": "dagster-run-id",
        "dagsterExecutionId": "321",
        "dagsterBlocking": "true",
        "dagsterStepKey": "raw__garfield_tp6_json_schema_valid",
        "dagsterPartitionKey": "2026-08-20",
        "dagsterSeverity": "ERROR",
        "errorCount": "2",
        "invalidFiles": '["s3://landing/bad-1.json","s3://landing/bad-2.json"]',
    }


def test_builds_openmetadata_definition_and_case_for_mapped_table() -> None:
    source = object.__new__(DagsterSource)
    table = Table(
        id="a58b1856-729c-493b-bc87-6d2269b43ec0",
        name="garfield_tp6",
        fullyQualifiedName="motherduck.lab_raw.main.garfield_tp6",
        columns=[Column(name="schema_valid", dataType=DataType.BOOLEAN)],
        databaseSchema=EntityReference(
            id="85811038-099a-11ed-861d-0242ac120002",
            type="databaseSchema",
        ),
    )

    class Metadata:
        @staticmethod
        def get_by_name(**_):
            return table

    class Client:
        @staticmethod
        def get_asset_check_executions(**_):
            return []

    source.metadata = Metadata()
    source.client = Client()
    mapping = source._get_asset_check_mapping(make_asset())

    assert mapping is not None
    projection = source._build_asset_check_projection(
        make_asset(),
        make_check(),
        mapping,
    )

    assert projection is not None
    assert projection.test_definition.name.root == "dagsterAssetCheckTableValidity"
    assert projection.test_definition.entityType == EntityType.TABLE
    assert projection.test_definition.dataQualityDimension == DataQualityDimensions.Validity
    assert projection.test_case_fqn == ("motherduck.lab_raw.main.garfield_tp6.json_schema_valid")
    assert projection.test_case.entityLink.root == ("<#E::table::motherduck.lab_raw.main.garfield_tp6>")


def test_maps_execution_failure_to_aborted_and_ignores_skipped() -> None:
    source = object.__new__(DagsterSource)
    asset = make_asset()
    check = make_check()

    aborted = source._build_asset_check_result(
        asset,
        check,
        make_execution("EXECUTION_FAILED", None),
    )
    skipped = source._build_asset_check_result(
        asset,
        check,
        make_execution("SKIPPED", None),
    )

    assert aborted is not None
    assert aborted.testCaseStatus == TestCaseStatus.Aborted
    assert aborted.timestamp.root == 1_775_000_000_125
    assert skipped is None


def test_does_not_publish_an_existing_result_timestamp() -> None:
    source = object.__new__(DagsterSource)
    existing = TestCaseResult(
        timestamp=1_775_000_001_500,
        testCaseStatus=TestCaseStatus.Failed,
    )
    new = TestCaseResult(
        timestamp=1_775_000_002_500,
        testCaseStatus=TestCaseStatus.Success,
    )
    projection = DagsterAssetCheckProjection(
        test_definition=CreateTestDefinitionRequest(
            name="dagsterAssetCheckTableValidity",
            entityType=EntityType.TABLE,
            testPlatforms=[TestPlatform.Other],
        ),
        test_case=CreateTestCaseRequest(
            name="json_schema_valid",
            testDefinition="dagsterAssetCheckTableValidity",
            entityLink="<#E::table::motherduck.lab_raw.main.garfield_tp6>",
        ),
        test_case_fqn=("motherduck.lab_raw.main.garfield_tp6.json_schema_valid"),
        results=(existing, new),
    )

    class Metadata:
        @staticmethod
        def get_test_case_results(test_case_fqn: str, start_ts: int, end_ts: int):
            assert test_case_fqn == projection.test_case_fqn
            assert start_ts == existing.timestamp.root
            assert end_ts == new.timestamp.root
            return [existing]

    source.metadata = Metadata()
    source._get_asset_check_projections = lambda _: (projection,)

    yielded = list(source.yield_asset_check_results(object()))

    assert len(yielded) == 1
    assert yielded[0].right.test_case_results == new

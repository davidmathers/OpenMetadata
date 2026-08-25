# Dagster

In this section, we provide guides and references to use the Dagster connector.

## Requirements

OpenMetadata is integrated with dagster up to version <a href="https://docs.dagster.io/getting-started" target="_blank">1.0.13</a> and will continue to work for future dagster versions.

The ingestion framework uses <a href="https://docs.dagster.io/_apidocs/libraries/dagster-graphql#dagster_graphql.DagsterGraphQLClient" target="_blank">dagster graphql python client</a> to connect to the dagster instance and perform the API calls.

You can find further information on the Dagster connector in the <a href="https://docs.open-metadata.org/connectors/pipeline/dagster" target="_blank">docs</a>.

## Connection Details

$$section
### Host $(id="host")

Pipeline Service Management URI. This should be specified as a URI string in the format `scheme://hostname:port`. E.g., `http://localhost:3000`.
$$

$$section
### Token $(id="token")

To Connect to Dagster Cloud.
- Log in to your Dagster account.
- Click on the `Settings` link in the top navigation bar.
- Click on the `API Keys` tab.
- Click on the `Create a New API Key` button.
- Give your API key a name and click on the `Create API Key` button.
- Copy the generated API key to your clipboard and paste it in the field.
$$

$$section
### Timeout $(id="timeout")

Connection Time Limit between OpenMetadata and Dagster Graphql API in seconds.

$$

$$section
### Strip Asset Key Prefix Length $(id="stripAssetKeyPrefixLength")

Number of leading segments to remove from asset key paths before resolving to tables.

**About Dagster Asset Keys:**

Dagster asset keys are path-like identifiers represented as arrays of strings (e.g., `["project", "environment", "schema", "table"]`). When OpenMetadata ingests Dagster pipelines, it tries to match these asset keys to table entities using the standard format: `database.schema.table` or `schema.table`.

**When to Use This Setting:**

If your Dagster asset keys include additional prefix segments beyond the database/schema/table hierarchy, use this setting to strip those prefixes. For example:
- Asset key: `["project", "environment", "schema", "table"]`
- Set value to `2` to strip `project` and `environment`
- Result: `schema.table` (matches OpenMetadata table entities)

Common use cases include stripping:
- Project/workspace identifiers
- Environment names (dev/staging/prod)
- Storage bucket/container prefixes

Default value is `0` (no stripping).

$$

## Asset Checks

The connector can project Dagster asset checks into OpenMetadata data-quality
test cases and publish their results without executing the checks again.
Dagster remains the execution and gating authority; OpenMetadata stores the
definitions, history, dashboard state, and alerts.

Asset-check ingestion is opt-in per Dagster asset. Add an `openmetadata` JSON
object to the asset's static metadata:

```python
from dagster import asset


@asset(
    metadata={
        "openmetadata": {
            "asset_checks": {
                "enabled": True,
                "entity_fqn": "motherduck.lab_raw.main.garfield_tp6",
                "quality_dimension": "Validity",
                "history_limit": 100,
            }
        }
    }
)
def garfield_tp6():
    ...
```

Every asset check targeting this asset is registered against the specified
OpenMetadata table. To apply different settings to individual checks, add a
`checks` object keyed by the Dagster check name. For example, this targets one
check at a column and classifies it as a completeness check while retaining the
asset defaults for all other checks:

```python
"asset_checks": {
    "enabled": True,
    "entity_fqn": "motherduck.lab_raw.main.garfield_tp6",
    "quality_dimension": "Validity",
    "checks": {
        "schema_valid_column": {
            "column_name": "schema_valid",
            "quality_dimension": "Completeness",
        }
    },
}
```

An individual override can set `enabled`, `entity_fqn`, `column_name`,
`quality_dimension`, or `history_limit`. This is also how a single check can be
disabled while the asset's other checks remain enabled.

The table and optional column must already exist in OpenMetadata. Supported
quality dimensions are `Completeness`, `Accuracy`, `Consistency`, `Validity`,
`Uniqueness`, `Integrity`, `SQL`, and `NoDimension`.

The connector maps results as follows:

| Dagster execution | OpenMetadata status |
| --- | --- |
| Successful evaluation with `success=True` | `Success` |
| Completed evaluation with `success=False` | `Failed` |
| Check execution error | `Aborted` |
| In progress or skipped | No result |

Dagster run ID, execution ID, asset key, blocking flag, severity, step key,
partition key, and supported evaluation metadata are retained as test result
values. `history_limit` controls how many recent executions are inspected on
each ingestion run and defaults to `100`. Results already present at the same
test-case timestamp are not published again.

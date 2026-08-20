# MotherDuck

In this section, we provide guides and references to use the MotherDuck connector.

## Requirements

Create a dedicated, read-only MotherDuck access token for OpenMetadata. The connector uses the official DuckDB MotherDuck integration and does not require a host or port.

## Connection Details

$$section
### MotherDuck Token $(id="token")

The access token used to authenticate with MotherDuck. OpenMetadata stores this value as a secret.
$$

$$section
### Database $(id="database")

An optional MotherDuck database to ingest. Leave this field blank to discover and ingest every non-system database visible to the token.
$$

$$section
### Connection Arguments $(id="connectionArguments")

Optional DuckDB connection arguments. Values are passed to the DuckDB driver. The connector adds the MotherDuck token and an OpenMetadata user agent automatically.
$$

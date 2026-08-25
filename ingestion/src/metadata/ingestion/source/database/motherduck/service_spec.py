from metadata.ingestion.source.database.motherduck.connection import MotherDuckConnection
from metadata.ingestion.source.database.motherduck.lineage import MotherDuckLineageSource
from metadata.ingestion.source.database.motherduck.metadata import MotherDuckSource
from metadata.utils.service_spec.default import DefaultDatabaseSpec

ServiceSpec = DefaultDatabaseSpec(
    metadata_source_class=MotherDuckSource,  # pyright: ignore[reportArgumentType]
    lineage_source_class=MotherDuckLineageSource,  # pyright: ignore[reportArgumentType]
    connection_class=MotherDuckConnection,  # pyright: ignore[reportArgumentType]
)

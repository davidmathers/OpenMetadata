#  Copyright 2025 OpenMetadata
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""MotherDuck SQL queries."""

MOTHERDUCK_GET_DATABASES = """
SELECT database_name
FROM duckdb_databases()
WHERE NOT internal
  AND database_name NOT IN ('memory', 'system', 'temp')
ORDER BY database_name
"""

MOTHERDUCK_GET_SCHEMAS = """
SELECT schema_name
FROM duckdb_schemas()
WHERE database_name = current_database()
  AND schema_name NOT LIKE 'pg\\_%' ESCAPE '\\'
ORDER BY schema_name
"""

MOTHERDUCK_GET_TABLES = """
SELECT table_name
FROM duckdb_tables()
WHERE database_name = current_database()
  AND schema_name = :schema_name
  AND NOT internal
  AND NOT temporary
ORDER BY table_name
"""

MOTHERDUCK_GET_VIEWS = """
SELECT view_name
FROM duckdb_views()
WHERE database_name = current_database()
  AND schema_name = :schema_name
  AND NOT internal
  AND NOT temporary
ORDER BY view_name
"""

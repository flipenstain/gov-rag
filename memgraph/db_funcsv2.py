import duckdb
from typing import Optional, List, Tuple
from gqlalchemy import Memgraph
import json
import os
duckdb_path = os.path.abspath("/app/initial_db.duckdb")
conn = duckdb.connect(duckdb_path)
MEMGRAPH_HOST = "memgraph-mage"  # Or your Memgraph host IP/DNS name
MEMGRAPH_PORT = 7687

memgraph = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)

# --- Helper Function to Parse Identifiers ---
def parse_identifier(identifier):
    """Parses 'schema.table.column' into (schema, table, column)."""
    parts = identifier.split('.')
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2: # Handle case like 'schema.table' if needed, though column expected
         print(f"Warning: Identifier '{identifier}' only has 2 parts. Assuming table level.")
         #return parts[0], parts[1], None
         return "main", parts[0], parts[1] #, None
    else:
        raise ValueError(f"Invalid identifier format: {identifier}")


def get_table_schema_duckdb(db_conn: duckdb.DuckDBPyConnection, target_full_table_name: str) -> Optional[Tuple[List[Tuple[str, str, Optional[str]]], Optional[str]]]:
    """
    Gets table schema and table comment from DuckDB.

    Args:
        db_conn: DuckDB connection object.
        target_full_table_name: Name of the table with schema

    Returns:
        A tuple containing:
        - A list of tuples, where each tuple contains:
            - column_name (str)
            - column_type (str)
            - column_description (Optional[str]) -  Primary key status.
        - table_comment (Optional[str]): The comment for the table, or None if no comment.
        Returns (None, None) if the schema cannot be retrieved or is empty.
    """
    parts = target_full_table_name.split('.')
    target_table_name = parts[1]
    try:
        schema_info = db_conn.execute(f"PRAGMA table_info('{target_full_table_name}');").fetchall()
        if not schema_info:
            print(f"Warning: Could not get schema for table '{target_full_table_name}' or it's empty.")
            return None, None

        # From the DuckDB PRAGMA table_info, the columns are:
        # 0: cid, 1: name, 2: type, 3: notnull, 4: dflt_value, 5: pk
        # We're interested in name, type, and pk (for description)
        fields = [
            (row[1], row[2], f"Primary Key: {row[5]}" if row[5] else None)
            for row in schema_info
        ]

        # Get table comment
        table_comment_query = f"""
            SELECT obj_description('{target_table_name}'::regclass, 'table');
        """
        table_comment_result = db_conn.execute(table_comment_query).fetchone()
        table_comment = table_comment_result[0] if table_comment_result else None

        # Get column comments
        column_comments_query = f"""
            SELECT column_name, comment
            FROM duckdb_columns()
            WHERE table_name = '{target_table_name}';
        """

        column_comments_result = db_conn.execute(column_comments_query).fetchall()
        column_comments = {row[0]: row[1] for row in column_comments_result}
        
        # Augment fields with column comments.
        augmented_fields = []
        for column_name, column_type, column_description in fields:
            comment = column_comments.get(column_name)  # Get comment, None if not found.
            augmented_fields.append((column_name, column_type, column_description, comment))

        return augmented_fields, table_comment

    except Exception as e:
        print(f"Error getting schema for table {target_table_name}: {e}")
        return None, None



def import_schema_to_memgraph(
    memgraph_conn,
    duckdb_conn: duckdb.DuckDBPyConnection,
    target_full_table_name: str,
) -> None:
    """
    Imports table schema from DuckDB into Memgraph, including column types, primary key status, and comments.

    Args:
        memgraph_conn: Memgraph connection object (assumed to be established).
        duckdb_conn: DuckDB connection object.
        target_schema_name: Name of the schema in Memgraph.
        target_table_name: Name of the table in Memgraph.
        target_full_table_name: Full name of the table in Memgraph.
    """
    table_schema, table_comment = get_table_schema_duckdb(duckdb_conn, target_full_table_name)

    parts = target_full_table_name.split('.')
    target_table_name = parts[1]
    target_schema_name = parts[0
                               ]
    if table_schema is None:
        print(f"Skipping schema import for table '{target_full_table_name}' due to missing schema information.")
        return  # Important: Exit if no schema

    # Construct the Cypher query.  We build the properties string dynamically.
    properties_string = ", ".join(
        f"{column_name}: ${column_name}" for column_name, _, _, _ in table_schema
    )

    create_table_query = f"""
    MERGE (s:Schema {{name: $schema_name}})
    MERGE (t:Table {{full_name: $table_full_name}})
    ON CREATE SET t.name = $table_name
    MERGE (t)-[:BELONGS_TO]->(s)
    SET t.table_name = $table_name
    """
    if properties_string:
        create_table_query += f", t.properties = {{{properties_string}}}"
    if table_comment:
        create_table_query += ", t.comment = $table_comment"

    params = {
        "schema_name": target_schema_name,
        "table_full_name": target_full_table_name,
        "table_name": target_table_name,
    }
    if table_comment:
        params["table_comment"] = table_comment

    for column_name, column_type, column_description, _ in table_schema:
        params[column_name] = column_type

    try:
        memgraph_conn.execute(create_table_query, params)
        print(
            f"Merged target schema '{target_schema_name}' and table '{target_full_table_name}'"
            f" with schema properties."
        )

        # Create nodes for columns and connect them to the table.
        for column_name, column_type, column_description, column_comment in table_schema:
            column_properties = {
                "name": column_name,
                "type": column_type,
            }
            if column_description:
                column_properties["description"] = column_description
            if column_comment:
                column_properties["comment"] = column_comment
            create_column_query = """
            MERGE (c:Column {name: $name, table_full_name: $table_full_name})
            SET c.type = $type
            """
            if column_description:
                create_column_query += ", c.description = $description"
            if column_comment:
                create_column_query += ", c.comment = $comment"
            create_column_query += """
            MERGE (t:Table {full_name: $table_full_name})
            MERGE (c)-[:BELONGS_TO]->(t)
            """
            column_params = {
                "name": column_name,
                "table_full_name": target_full_table_name,
                "type": column_type,
            }
            if column_description:
                column_params["description"] = column_description
            if column_comment:
                column_params["comment"] = column_comment
            memgraph_conn.execute(create_column_query, column_params)
            print(f"Created column node '{column_name}' for table '{target_full_table_name}'")

    except Exception as e:
        print(f"Error importing schema to Memgraph for table {target_full_table_name}: {e}")
        # Consider whether to raise the exception or just log and continue
        raise


json_file = "/app/src/main/pipeline_mapping.json"
with open(json_file, "r", encoding="utf-8") as f:
                PIPELINE_SCRIPT_MAP_DATA = json.load(f)

print(PIPELINE_SCRIPT_MAP_DATA)


SCRIPT_TO_PIPELINE_MAP = {
    script: pipeline
    for pipeline, scripts in PIPELINE_SCRIPT_MAP_DATA.items()
    for script in scripts
}


#if not all([script_name, pipeline_name]):

if script_name and pipeline_name:
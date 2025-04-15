import json
from gqlalchemy import Memgraph
import json
from pathlib import Path
import duckdb
from typing import Optional, List, Tuple
import os
import re
import os

# --- Configuration ---
MEMGRAPH_HOST = "memgraph-mage"  # Or your Memgraph host IP/DNS name
MEMGRAPH_PORT = 7687
#memgraph = Memgraph(host='127.0.0.1', port=7687)

duckdb_path = os.path.abspath("/app/initial_db.duckdb")
duckdb_conn = duckdb.connect(duckdb_path)

def update_function_name(data):
    for source in data.get("sources_summary", []):
        name = source.get("name", "")
        match = re.search(r"read_csv\(['\"](.+?\.csv)['\"]", name)
        if match:
            file_path = match.group(1)
            # Normalize and convert path to dot notation
            norm_path = file_path.replace("\\", "/")
            dot_path = os.path.splitext(norm_path)[0].replace("/", ".")
            new_name = f'"{dot_path}.csv"'
            source["name"] = new_name
    return data

# --- Helper Function to Parse Identifiers ---
def parse_identifier(identifier):
    """Parses 'schema.table.column' into (schema, table, column)."""
    parts = identifier.split('.')
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2: # Handle case like 'schema.table' if needed, though column expected
         print(f"Warning: Identifier '{identifier}' only has 2 parts. Assuming table level.")
         return "main", parts[0], parts[1] #, None
    else:
        raise ValueError(f"Invalid identifier format: {identifier}")

# --- Main Loading Logic ---
def load_lineage_to_memgraph(db, data):
    """Loads the lineage JSON data into Memgraph."""
    target_full_table_name = data['target_table']
    target_full_table_name = normalize_table_name(target_full_table_name)
    target_schema_name, target_table_name = target_full_table_name.split('.')

    # Use MERGE for idempotency (create if not exists)
    # Create/Merge Target Schema, Table
    db.execute(
        """
        MERGE (s:Schema {name: $schema_name})
        MERGE (t:Table {full_name: $table_full_name})
        ON CREATE SET t.name = $table_name
        MERGE (t)-[:BELONGS_TO]->(s)
        """,
        {
            "schema_name": target_schema_name,
            "table_full_name": target_full_table_name,
            "table_name": target_table_name,
        }
    )
    print(f"Merged target schema '{target_schema_name}' and table '{target_full_table_name}'")

    # Process each target column's lineage
    for target_col_name, lineage_info in data['lineage'].items():
        target_col_full_name = f"{target_full_table_name}.{target_col_name}"

        # Create/Merge Target Column and link to its table
        db.execute(
            """
            MATCH (t:Table {full_name: $table_full_name})
            MERGE (c:Column {full_name: $col_full_name})
            ON CREATE SET c.name = $col_name
            MERGE (c)-[:BELONGS_TO]->(t)
            """,
            {
                "table_full_name": target_full_table_name,
                "col_full_name": target_col_full_name,
                "col_name": target_col_name,
            }
        )
        print(f"  Merged target column '{target_col_full_name}'")

        # Process sources for this target column
        for source_info in lineage_info['sources']:
            try:
                source_identifier = source_info['source_identifier']
                src_schema_name, src_table_name, src_col_name = parse_identifier(source_identifier)
                src_full_table_name = f"{src_schema_name}.{src_table_name}"
                src_col_full_name = f"{src_full_table_name}.{src_col_name}"

                # Create/Merge Source Schema, Table, Column and relationships
                db.execute(
                    """
                    MERGE (s_src:Schema {name: $src_schema_name})
                    MERGE (t_src:Table {full_name: $src_table_full_name})
                    ON CREATE SET t_src.name = $src_table_name
                    MERGE (t_src)-[:BELONGS_TO]->(s_src)
                    MERGE (c_src:Column {full_name: $src_col_full_name})
                    ON CREATE SET c_src.name = $src_col_name
                    MERGE (c_src)-[:BELONGS_TO]->(t_src)
                    """,
                    {
                        "src_schema_name": src_schema_name,
                        "src_table_full_name": src_full_table_name,
                        "src_table_name": src_table_name,
                        "src_col_full_name": src_col_full_name,
                        "src_col_name": src_col_name,
                    }
                )
                print(f"    Merged source column '{src_col_full_name}'")

                # Create the DERIVES relationship with properties
                rel_props = {
                    "transformation_type": lineage_info.get("transformation_type"),
                    "transformation_logic": lineage_info.get("transformation_logic"),
                    "path": str(source_info.get("path")), # Store list as string or use Memgraph Maps
                    "role": source_info.get("role"),
                    # Store complex join_info as JSON string
                    "join_info": json.dumps(source_info.get("join_info")) if source_info.get("join_info") else None,
                    "notes": lineage_info.get("notes"),
                }
                # Remove None properties before creating relationship
                rel_props = {k: v for k, v in rel_props.items() if v is not None}

                db.execute(
                    """
                    MATCH (c_src:Column {full_name: $src_col_full_name})
                    MATCH (c_tgt:Column {full_name: $tgt_col_full_name})
                    MERGE (c_src)-[r:DERIVES]->(c_tgt)
                    ON CREATE SET r = $props
                    ON MATCH SET r += $props // Update properties if relationship already exists
                    """,
                    {
                        "src_col_full_name": src_col_full_name,
                        "tgt_col_full_name": target_col_full_name,
                        "props": rel_props,
                    }
                )
                print(f"      Merged DERIVES relationship from '{src_col_full_name}' to '{target_col_full_name}'")

            except ValueError as e:
                print(f"    Skipping source due to error: {e}")
            except Exception as e:
                 print(f"    An unexpected error occurred processing source {source_info.get('source_identifier', 'UNKNOWN')}: {e}")


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
    if target_full_table_name is None:
        return None
    target_full_table_name = normalize_table_name(target_full_table_name)
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

def normalize_table_name(name: str) -> str:
    """Ensure the table name contains a dot. If not, prepend 'main.'"""
    if not name:
        return ""
    return name if "." in name else f"main.{name}"



def extract_table_names(data: dict) -> list[str]:
    target_full_table_name = data['target_table'] if isinstance(data['target_table'], list) else [data['target_table']]
    # Safely get source names from sources_summary
    sources_list = data.get("sources_summary", [])
    source_names = [source.get("name") for source in sources_list if "name" in source]

    # Combine source names and target list
    list_of_tables = source_names + target_full_table_name

    # Ensure all entries have a dot (.)
    normalized_tables = [
        normalize_table_name(name)
        for name in list_of_tables
        if name  # skip None or empty strings
    ]

    return normalized_tables

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

def load_all_json_files(directory_path: str | Path) -> list[dict]:
    """Loads all JSON files in a directory and returns them as a list of dicts."""
    path = Path(directory_path)
    if not path.is_dir():
        raise NotADirectoryError(f"Not a directory: {path}")

    json_dicts = []
    for json_file in path.glob("*.json"):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                json_dicts.append(data)
        except (json.JSONDecodeError, OSError) as e:
            print(f"⚠️ Skipped {json_file.name}: {e}")
    
    return json_dicts



# --- Connect and Run ---
try:
    # Establish connection
    # Add user/password if required: Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT, username=MEMGRAPH_USER, password=MEMGRAPH_PASSWORD)
    memgraph = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
    print("Connected to Memgraph.")

    all_jsons = load_all_json_files("/app/src/LLM_answers/llm_prompt_for_column_level_lineage_hard_w_ex/")
    for lineage in all_jsons:
        lineage = update_function_name(lineage)
        #if "read_csv(" in lineage["sources_summary"][0]["name"]: #TO DO csv files
         #   continue
        sources_targets = extract_table_names(lineage)
        for table in sources_targets:
            print("##  Processing table:   ##   ", table)
            import_schema_to_memgraph(memgraph, duckdb_conn, table)

        load_lineage_to_memgraph(memgraph, lineage)

    # Create Constraints/Indexes (Optional but Recommended for performance/consistency)
    # Run these only once or ensure they are idempotent
    try:
        memgraph.execute("CREATE CONSTRAINT ON (s:Schema) ASSERT s.name IS UNIQUE;")
        memgraph.execute("CREATE INDEX ON :Schema(name);")
        memgraph.execute("CREATE CONSTRAINT ON (t:Table) ASSERT t.full_name IS UNIQUE;")
        memgraph.execute("CREATE INDEX ON :Table(full_name);")
        memgraph.execute("CREATE CONSTRAINT ON (c:Column) ASSERT c.full_name IS UNIQUE;")
        memgraph.execute("CREATE INDEX ON :Column(full_name);")
        print("Ensured necessary constraints and indexes exist.")
    except Exception as e:
        # Ignore errors if constraints/indexes already exist
        if "already exists" in str(e).lower() or "constraint requires" in str(e).lower():
             print("Constraints/indexes already exist.")
        else:
             print(f"Warning: Could not create constraints/indexes: {e}")


    # Load the data
    #load_lineage_to_memgraph(memgraph, lineage_data)
    print("\nLineage loading process complete.")

except Exception as e:
    print(f"An error occurred: {e}")
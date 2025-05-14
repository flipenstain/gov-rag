import json
from gqlalchemy import Memgraph
# import json # Duplicate import removed
from pathlib import Path
import duckdb
from typing import Optional, List, Tuple
import os
import re
import logging # Added for better output
from memgraph_utils import read_all_sql_files, update_script_properties, dependencies

# --- Configuration ---
MEMGRAPH_HOST = "memgraph-mage"  # Or your Memgraph host IP/DNS name
MEMGRAPH_PORT = 7687
DUCKDB_PATH = os.path.abspath("/app/initial_db.duckdb") # Use constant

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Connect to Databases ---
try:
    duckdb_conn = duckdb.connect(DUCKDB_PATH, read_only=True) # read_only if not modifying
    logging.info(f"Connected to DuckDB at {DUCKDB_PATH}")
except Exception as e:
    logging.error(f"Failed to connect to DuckDB: {e}")
    # Decide how to handle failure - exit or continue without DuckDB features?
    duckdb_conn = None # Set to None to handle checks later

try:
    # Establish connection - Add user/password if required
    memgraph = Memgraph(host=MEMGRAPH_HOST, port=MEMGRAPH_PORT)
    # Ensure connection is alive (optional, depends on gqlalchemy version/behavior)
    memgraph.execute("RETURN 1")
    logging.info(f"Connected to Memgraph at {MEMGRAPH_HOST}:{MEMGRAPH_PORT}")
except Exception as e:
    logging.error(f"Failed to connect to Memgraph: {e}")
    memgraph = None # Set to None to prevent further operations

# --- Reading in sql pipeline to process mapping ---
json_file = "/app/src/main/pipeline_mapping.json"
with open(json_file, "r", encoding="utf-8") as f:
                PIPELINE_SCRIPT_MAP_DATA = json.load(f)

# --- Create a reverse map for easy lookup ---
SCRIPT_TO_PIPELINE_MAP = {
    script: pipeline
    for pipeline, scripts in PIPELINE_SCRIPT_MAP_DATA.items()
    for script in scripts
}
logging.info("Created script-to-pipeline lookup map.")


def parse_file_path(file_path):
    """
    Parses a raw file path into schema name (from dir), table name (filename),
    and full name (schema.table).
    Handles both / and \ separators.
    """
    if not file_path:
        return None, None, None

    norm_path = file_path.replace("\\", "/")
    table_name = os.path.basename(norm_path)
    dir_path = os.path.dirname(norm_path)

    # Create schema name from directory structure
    if dir_path:
        schema_name = dir_path.replace("/", ".")
    else:
        # Handle files in the root or without explicit directory
        schema_name = "file_root" # Or choose another default schema name

    # Create full name
    full_name = f"{schema_name}.{table_name}"

    return schema_name, table_name, full_name


# --- Utility Functions (Keep yours, slightly adjusted logging/checks) ---

def update_function_name(data):
    """Updates read_csv paths in sources_summary."""
    for source in data.get("sources_summary", []):
        name = source.get("name", "")
        # Consider making regex more robust if needed
        match = re.search(r"read_csv\(['\"](.+?\.csv)['\"]", name)
        if match:
            file_path = match.group(1)
            # Normalize and convert path to dot notation
            norm_path = file_path.replace("\\", "/")
            # Avoid leading dot if path starts with /
            dot_path = os.path.splitext(norm_path.lstrip('/'))[0].replace("/", ".")
            new_name = f'"{dot_path}.csv"' # Keep quotes consistent
            source["name"] = new_name
            logging.debug(f"Updated CSV source name to: {new_name}")
    return data

def parse_identifier(identifier, file_source_details=None):
    """
    Parses 'schema.table.column' into (schema, table, column).
    Handles file placeholders if file_source_details is provided.
    """
    parts = identifier.split('.')

    # --- Start: Handle File Placeholders ---
    # Check if it looks like a file placeholder and we have pre-processed file info
    if identifier.startswith("file.") and len(parts) == 2 and file_source_details:
         placeholder_col_name = parts[1] # e.g., "placeholder_source_for_col1"
         logging.debug(f"Interpreting file placeholder '{identifier}' using pre-processed info.")
         # Return the schema/table from the actual file path,
         # and use the placeholder part as a *conceptual* column name for the file source
         return (
             file_source_details['schema'],
             file_source_details['table'],
             placeholder_col_name # Or a generic name like 'file_content'
         )
    # --- End: Handle File Placeholders ---

    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        # Keep the original warning for non-file cases
        logging.warning(f"Identifier '{identifier}' only has 2 parts. Assuming 'main' schema.")
        return "main", parts[0], parts[1]
    else:
         # --- Keep CSV handling if needed ---
        if identifier.startswith('"') and identifier.endswith('.csv"'):
             clean_id = identifier.strip('"')
             parts = clean_id.split('.')
             if len(parts) >= 2:
                 table_name = parts[-2]
                 schema_name = ".".join(parts[:-2]) if len(parts) > 2 else "csv_files"
                 column_name = "file_content"
                 logging.debug(f"Interpreted CSV path '{identifier}' as: {schema_name}.{table_name}.{column_name}")
                 return schema_name, table_name, column_name

        logging.error(f"Invalid identifier format: {identifier}")
        raise ValueError(f"Invalid identifier format: {identifier}")


def normalize_table_name(name: str) -> str:
    """
    Ensures the table name is in schema.table format.
    - Handles quoted file paths (e.g., '"path/to/file.txt"') by parsing them
      into schema.table format (e.g., 'path.to.file').
    - If name contains '.' and is not a quoted file, assumes it's already schema.table.
    - If name doesn't contain '.' and is not a quoted file, prepends 'main.'.
    """
    if not name:
        logging.debug("normalize_table_name received empty or None input, returning empty string.")
        return ""

    # Check for quoted file paths first
    quoted_file_match = re.match(r'^"(.+)\.(csv|txt|dat)"$', name, re.IGNORECASE)

    if quoted_file_match:
        try: # Add try-except for robustness during parsing
            full_path_part = quoted_file_match.group(1)
            normalized_path = full_path_part.replace("\\", "/")
            parts = normalized_path.split('/')
            table_name = parts[-1].replace(".", "_") # Also sanitize dots in filename part

            schema_parts = parts[:-1]
            if schema_parts:
                schema_name = ".".join(part.replace(".", "_") for part in schema_parts)
            else:
                schema_name = "file_sources" # Default schema

            normalized = f"{schema_name}.{table_name}"
            logging.debug(f"Normalized quoted file path '{name}' to: '{normalized}'")
            return normalized
        except Exception as e:
            logging.error(f"Error parsing quoted file path '{name}': {e}. Returning original name.")
            # Fallback to returning the original name if parsing fails unexpectedly
            return name # Or return "" or raise an error? Returning name might be safer.

    if "." in name:
        # Assume it's already in schema.table format (e.g., "wh_db.DimTime")
        logging.debug(f"Input '{name}' has a dot and is not a quoted file, returning as is.")
        return name
    else:
        # Simple name without dots, assume it's a table in the 'main' schema
        logging.debug(f"Input '{name}' has no dot, prepending 'main.'.")
        return f"main.{name}"


# --- Modified Main Loading Logic ---
def load_lineage_to_memgraph(db, data, script_name, pipeline_name):
    """
    Loads the lineage JSON data into Memgraph, including Pipeline and Script context.
    """
    if not all([script_name, pipeline_name]):
        logging.error(f"Script name ('{script_name}') or Pipeline name ('{pipeline_name}') is missing. Skipping lineage loading for this data.")
        return

    # --- Start: Add Pipeline and Script MERGE ---
    try:
        # 1. Ensure Pipeline node exists
        db.execute(
            "MERGE (p:Pipeline {name: $pipeline_name})",
            {"pipeline_name": pipeline_name}
        )
        logging.debug(f"Merged Pipeline: {pipeline_name}")

        # 2. Ensure Script node exists and link to Pipeline
        db.execute(
            """
            MATCH (p:Pipeline {name: $pipeline_name})
            MERGE (s:Script {name: $script_name})
            ON CREATE SET s.type = 'SQL' // Or derive type if needed
            MERGE (p)-[:CONTAINS_SCRIPT]->(s)
            """,
            {"pipeline_name": pipeline_name, "script_name": script_name}
        )
        logging.debug(f"Merged Script: {script_name} and linked to Pipeline: {pipeline_name}")

    except Exception as e:
        logging.error(f"Failed to merge Pipeline/Script nodes for {pipeline_name}/{script_name}: {e}")
        # Decide if you want to stop processing this file or continue
        return # Stop processing this lineage entry if pipeline/script fails

    # --- End: Add Pipeline and Script MERGE ---

    # --- Start: Pre-process File Sources ---
    processed_file_source_details = None # Store details if a FILE source is found
    try:
        for source_summary in data.get("sources_summary", []):
            if source_summary.get("type") == "FILE":
                raw_file_path = source_summary.get("name")
                if not raw_file_path:
                    logging.warning("Found FILE source in summary but 'name' (path) is missing. Skipping.")
                    continue

                f_schema, f_table, f_full_name = parse_file_path(raw_file_path)

                if not all([f_schema, f_table, f_full_name]):
                     logging.warning(f"Could not parse file path '{raw_file_path}' correctly. Skipping.")
                     continue

                # Store details for lookup during lineage processing
                processed_file_source_details = {
                    "schema": f_schema,
                    "table": f_table,
                    "full_name": f_full_name
                }
                logging.info(f"Found FILE source: Path='{raw_file_path}', Parsed Full Name='{f_full_name}'")

                # Create/Merge Schema and Table nodes for the file source
                db.execute(
                    """
                    MERGE (s:Schema {name: $schema_name})
                    MERGE (t:Table {full_name: $table_full_name})
                    ON CREATE SET t.name = $table_name, t.type = 'FILE' // Add type property
                    ON MATCH SET t.type = 'FILE' // Ensure type is set even if node exists
                    MERGE (t)-[:IN_SCHEMA]->(s)
                    """,
                    {
                        "schema_name": f_schema,
                        "table_full_name": f_full_name,
                        "table_name": f_table,
                    }
                )
                logging.debug(f"  Merged FILE source schema '{f_schema}' and table '{f_full_name}'")
                # Assuming only one file source per JSON for now based on input
                # If multiple are possible, this needs a more robust mapping
                break # Stop after processing the first FILE source found

    except Exception as e:
        logging.error(f"Error pre-processing sources_summary for files: {e}")
        # Decide if you want to continue without file source info or stop
        # return # Or just log and continue

    target_full_table_name_raw = data.get('target_table')
    if not target_full_table_name_raw:
        logging.warning(f"Missing 'target_table' in lineage data for script {script_name}. Skipping.")
        return

    target_full_table_name = normalize_table_name(target_full_table_name_raw)

    if target_full_table_name is None:
        logging.error(f"Normalization of target table '{target_full_table_name_raw}' resulted in None. Skipping processing for script {script_name}.")
        return 

    target_full_table_name = normalize_table_name(target_full_table_name)
    if '.' not in target_full_table_name:
         logging.warning(f"Could not properly parse schema/table from target '{target_full_table_name}' for script {script_name}. Skipping.")
         return
    target_schema_name, target_table_name = target_full_table_name.split('.', 1) # Split only on first dot

    # Use MERGE for idempotency (create if not exists)
    # Create/Merge Target Schema, Table
    db.execute(
        """
        MERGE (s:Schema {name: $schema_name})
        MERGE (t:Table {full_name: $table_full_name})
        ON CREATE SET t.name = $table_name
        MERGE (t)-[:IN_SCHEMA]->(s)
        """,
        {
            "schema_name": target_schema_name,
            "table_full_name": target_full_table_name,
            "table_name": target_table_name,
        }
    )
    logging.debug(f"Merged target schema '{target_schema_name}' and table '{target_full_table_name}'")

    # Process each target column's lineage
    lineage_details = data.get('lineage', {})
    if not lineage_details:
         logging.warning(f"No 'lineage' details found for target table '{target_full_table_name}' in script {script_name}.")
         return

    for target_col_name, lineage_info in lineage_details.items():
        target_col_full_name = f"{target_full_table_name}.{target_col_name}"

        # Create/Merge Target Column and link to its table
        db.execute(
            """
            MATCH (t:Table {full_name: $table_full_name})
            MERGE (c:Column {full_name: $col_full_name})
            ON CREATE SET c.name = $col_name
            MERGE (c)-[:IN_TABLE]->(t)
            """,
            {
                "table_full_name": target_full_table_name,
                "col_full_name": target_col_full_name,
                "col_name": target_col_name,
            }
        )
        logging.debug(f"  Merged target column '{target_col_full_name}'")

        # Process sources for this target column
        for source_info in lineage_info.get('sources', []):
            try:
                source_identifier = source_info.get('source_identifier')
                if not source_identifier:
                    logging.warning("  Skipping source: missing 'source_identifier'")
                    continue

                src_schema_name, src_table_name, src_col_name = parse_identifier(
                    source_identifier,
                    processed_file_source_details # Pass the dict here
                )

                if processed_file_source_details and src_table_name == processed_file_source_details['table'] and src_schema_name == processed_file_source_details['schema']:
                    src_full_table_name = processed_file_source_details['full_name']
                else:
                     # Handle regular schema.table sources
                    src_full_table_name = f"{src_schema_name}.{src_table_name}" if src_schema_name else src_table_name

                src_col_full_name = f"{src_full_table_name}.{src_col_name}"

                # Create/Merge Source Schema, Table, Column and relationships
                db.execute(
                    """
                    MERGE (s_src:Schema {name: $src_schema_name})
                    MERGE (t_src:Table {full_name: $src_full_table_name})
                    // Set name/type only on create, avoid overwriting potentially better info (like FILE type)
                    ON CREATE SET t_src.name = $src_table_name, t_src.type = 'TABLE'
                    MERGE (t_src)-[:IN_SCHEMA]->(s_src)
                    MERGE (c_src:Column {full_name: $src_col_full_name})
                    ON CREATE SET c_src.name = $src_col_name
                    MERGE (c_src)-[:IN_TABLE]->(t_src)
                    """,
                    {
                        "src_schema_name": src_schema_name,
                        "src_full_table_name": src_full_table_name,
                        "src_table_name": src_table_name,
                        "src_col_full_name": src_col_full_name,
                        "src_col_name": src_col_name,
                    }
                )
                logging.debug(f"    Merged source column '{src_col_full_name}' and linked entities")

                # Create the DERIVED_FROM relationship with properties (Using your previous fix)
                rel_props = {
                    # Get transformation details from the specific source_info
                    "transformation_type": source_info.get("transformation_type", lineage_info.get("transformation_type")), # Fallback needed? Check JSON structure consistency
                    "transformation_logic": source_info.get("transformation_logic", lineage_info.get("transformation_logic")), # Fallback needed?
                    "path": str(source_info.get("path")),
                    "role": source_info.get("role"),
                    "join_info": json.dumps(source_info.get("join_info")) if source_info.get("join_info") else None,
                    "notes": source_info.get("notes", lineage_info.get("notes")), # Fallback needed?
                }
                 # Add specific details for FILE copy operations if available
                if source_identifier.startswith("file.") and "COPY from file" in str(rel_props.get("transformation_logic")):
                     rel_props["transformation_type"] = "FILE_LOAD" # Be more specific
                     # transformation_logic could remain "COPY from file" or be enhanced

                rel_props = {k: v for k, v in rel_props.items() if v is not None}

                db.execute(
                    """
                    MATCH (c_src:Column {full_name: $src_col_full_name})
                    MATCH (c_tgt:Column {full_name: $tgt_col_full_name})
                    MERGE (c_tgt)-[r:DERIVED_FROM]->(c_src)
                    ON CREATE SET r = $props
                    ON MATCH SET r += $props
                    """,
                    {
                        "src_col_full_name": src_col_full_name,
                        "tgt_col_full_name": target_col_full_name,
                        "props": rel_props,
                    }
                )
                logging.debug(f"      Merged DERIVED_FROM relationship from '{src_col_full_name}' to '{target_col_full_name}'")

                # --- Start: Add Script -> Column Links ---
                db.execute(
                    """
                    MATCH (s:Script {name: $script_name})
                    MATCH (c_src:Column {full_name: $src_col_full_name})
                    MATCH (c_tgt:Column {full_name: $tgt_col_full_name})
                    // Link script to its inputs/outputs for this specific derivation
                    MERGE (s)-[:READS_FROM]->(c_src)
                    MERGE (s)-[:GENERATES]->(c_tgt)
                    """,
                    {
                        "script_name": script_name,
                        "src_col_full_name": src_col_full_name,
                        "tgt_col_full_name": target_col_full_name,
                    }
                )
                logging.debug(f"      Merged Script links: {script_name} READS_FROM {src_col_full_name}, GENERATES {target_col_full_name}")
                # --- End: Add Script -> Column Links ---

            except ValueError as e:
                logging.warning(f"    Skipping source due to error: {e} (Source Identifier: {source_info.get('source_identifier')})")
            except Exception as e:
                logging.error(f"    An unexpected error occurred processing source {source_info.get('source_identifier', 'UNKNOWN')} for target {target_col_full_name}: {e}", exc_info=True)


# --- Schema Import Functions (Keep yours, added checks for connections) ---
def get_table_schema_duckdb(db_conn: duckdb.DuckDBPyConnection, target_full_table_name: str) -> Optional[Tuple[List[Tuple[str, str, Optional[str], Optional[str]]], Optional[str]]]:
    """Gets table schema and comments from DuckDB."""
    if not db_conn:
        logging.warning("DuckDB connection not available, cannot get schema.")
        return None, None
    if target_full_table_name is None:
        return None, None

    # Normalize before splitting (handle potential CSV paths)
    target_full_table_name = normalize_table_name(target_full_table_name)
    if '.' not in target_full_table_name:
        logging.warning(f"Cannot parse schema/table from '{target_full_table_name}' for schema lookup.")
        return None, None
    target_schema_name, target_table_name = target_full_table_name.split('.', 1)

    try:
        # Use qualified name in PRAGMA
        schema_info = db_conn.execute(f"PRAGMA table_info('{target_full_table_name}');").fetchall()
        if not schema_info:
            logging.warning(f"Could not get schema for table '{target_full_table_name}' or it's empty.")
            return None, None

        fields = [
            (row[1], row[2], f"Primary Key: {row[5]}" if row[5] else None, None) # Add placeholder for comment
            for row in schema_info
        ]

        # DuckDB doesn't directly support COMMENT ON TABLE/COLUMN via SQL standard `obj_description` easily like Postgres
        # Using information_schema is more standard if available, or specific DuckDB catalog functions
        table_comment = None # Placeholder, DuckDB comment retrieval is less straightforward

        # Get column comments using DuckDB's system table (adjust if needed for your DuckDB version)
        column_comments_query = f"""
            SELECT column_name, comment
            FROM duckdb_columns()
            WHERE database_name = current_database() -- or specific DB name if not default
            AND schema_name = ?
            AND table_name = ?;
        """
        # Use parameters to avoid injection issues
        column_comments_result = db_conn.execute(column_comments_query, [target_schema_name, target_table_name]).fetchall()
        column_comments = {row[0]: row[1] for row in column_comments_result}

        # Augment fields with column comments
        augmented_fields = []
        for col_name, col_type, col_desc, _ in fields:
            comment = column_comments.get(col_name)
            augmented_fields.append((col_name, col_type, col_desc, comment))

        logging.debug(f"Retrieved schema for {target_full_table_name} with {len(augmented_fields)} columns.")
        return augmented_fields, table_comment # table_comment is likely None

    except Exception as e:
        logging.error(f"Error getting schema from DuckDB for table {target_full_table_name}: {e}")
        return None, None


def import_schema_to_memgraph(
    memgraph_conn,
    duckdb_conn: duckdb.DuckDBPyConnection,
    target_full_table_name: str,
) -> None:
    """Imports table schema from DuckDB into Memgraph nodes and properties."""
    if not memgraph_conn:
        logging.error("Memgraph connection not available, cannot import schema.")
        return
    if not duckdb_conn:
         logging.warning(f"DuckDB connection not available, cannot import schema for {target_full_table_name}.")
         return

    # Normalize name before getting schema
    target_full_table_name = normalize_table_name(target_full_table_name)
    table_schema, table_comment = get_table_schema_duckdb(duckdb_conn, target_full_table_name)

    if table_schema is None:
        logging.warning(f"Skipping schema import for table '{target_full_table_name}' due to missing schema information.")
        return

    if '.' not in target_full_table_name:
        logging.warning(f"Cannot parse schema/table from '{target_full_table_name}' for schema import.")
        return
    target_schema_name, target_table_name = target_full_table_name.split('.', 1)

    # Merge Schema and Table nodes first
    try:
        merge_table_query = """
        MERGE (s:Schema {name: $schema_name})
        MERGE (t:Table {full_name: $table_full_name})
        ON CREATE SET t.name = $table_name
        MERGE (t)-[:IN_SCHEMA]->(s)
        """
        # Add comment if available (table_comment is often None from DuckDB)
        if table_comment:
            merge_table_query += " SET t.comment = $table_comment"

        params = {
            "schema_name": target_schema_name,
            "table_full_name": target_full_table_name,
            "table_name": target_table_name,
        }
        if table_comment:
            params["table_comment"] = table_comment

        memgraph_conn.execute(merge_table_query, params)
        logging.debug(f"Merged Schema '{target_schema_name}' and Table '{target_full_table_name}' for schema import.")

        # Merge Column nodes with properties and link to table
        for col_name, col_type, col_desc, col_comment in table_schema:
            col_full_name = f"{target_full_table_name}.{col_name}"
            merge_col_query = """
            MATCH (t:Table {full_name: $table_full_name}) // Ensure table exists
            MERGE (c:Column {full_name: $col_full_name})
            ON CREATE SET c.name = $col_name
            SET c.type = $col_type
            """
            col_params = {
                "table_full_name": target_full_table_name,
                "col_full_name": col_full_name,
                "col_name": col_name,
                "col_type": col_type,
            }
            # Add optional properties
            if col_desc:
                merge_col_query += " SET c.description = $col_desc"
                col_params["col_desc"] = col_desc
            if col_comment:
                merge_col_query += " SET c.comment = $col_comment"
                col_params["col_comment"] = col_comment

            # Add merge for relationship
            merge_col_query += " MERGE (c)-[:IN_TABLE]->(t)"

            memgraph_conn.execute(merge_col_query, col_params)
            logging.debug(f"  Merged Column '{col_full_name}' with properties.")

        logging.info(f"Successfully imported schema for table '{target_full_table_name}'")

    except Exception as e:
        logging.error(f"Error importing schema to Memgraph for table {target_full_table_name}: {e}", exc_info=True)
        # Consider whether to raise the exception or just log and continue

def extract_table_names(data: dict) -> list[str]:
    """Extracts source and target table names, handling potential CSVs."""
    all_names = []
    target_table = data.get('target_table')
    if target_table:
        all_names.append(target_table)

    # Get source names from sources_summary (often file paths for CSVs)
    sources_list = data.get("sources_summary", [])
    for source in sources_list:
         name = source.get("name")
         if name:
            all_names.append(name)

    # Normalize all collected names
    normalized_tables = set() # Use set to avoid duplicate schema imports
    for name in all_names:
        normalized = normalize_table_name(name)
        if normalized: # Add only if normalization didn't fail
            normalized_tables.add(normalized)

    logging.debug(f"Extracted and normalized table names: {list(normalized_tables)}")
    return list(normalized_tables)

def load_all_json_files(directory_path: str | Path) -> list[Tuple[Path, dict]]:
    """Loads all JSON files in a directory, returning (filepath, data) tuples."""
    path = Path(directory_path)
    if not path.is_dir():
        logging.error(f"Not a directory: {path}")
        raise NotADirectoryError(f"Not a directory: {path}")

    json_data_list = []
    for json_file in sorted(path.glob("*.json")): # Sort for predictable order
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                json_data_list.append((json_file, data)) # Store path along with data
                logging.debug(f"Successfully loaded JSON: {json_file.name}")
        except (json.JSONDecodeError, OSError) as e:
            logging.warning(f"Skipped loading {json_file.name}: {e}")

    logging.info(f"Loaded {len(json_data_list)} JSON files from {directory_path}")
    return json_data_list


# --- Main Execution Logic ---
def main():
    if not memgraph:
        logging.critical("Cannot proceed without a Memgraph connection.")
        return
    if not duckdb_conn:
        logging.warning("Cannot proceed with schema import without DuckDB connection.")
        # Allow proceeding without schema import if desired

    # --- Define JSON source directory ---
    json_dir = "/app/agentic/Agent_LLM_JSONs"
    #json_dir = "/app/src/LLM_answers/llm_prompt_for_column_level_lineage_hard_w_ex/2"

    try:
        all_json_data = load_all_json_files(json_dir)

        # Process each loaded JSON file
        for json_filepath, lineage_data in all_json_data:
            logging.info(f"--- Processing file: {json_filepath.name} ---")

            # **Determine Script and Pipeline Name**
            # Assumption: script name is the JSON filename without .json
            potential_script_name = json_filepath.stem
            script_name = None
            pipeline_name = None

            # Check if the potential name looks like a script file we know
            if potential_script_name in SCRIPT_TO_PIPELINE_MAP:
                script_name = potential_script_name
                pipeline_name = SCRIPT_TO_PIPELINE_MAP[script_name]
                logging.info(f"Identified Script: '{script_name}', Pipeline: '{pipeline_name}'")
            else:
                # Handle cases where filename doesn't match a known script
                # Maybe try matching based on suffix?
                found = False
                for known_script in SCRIPT_TO_PIPELINE_MAP.keys():
                    if potential_script_name.startswith(known_script.replace('.sql','')):
                         script_name = known_script
                         pipeline_name = SCRIPT_TO_PIPELINE_MAP[script_name]
                         logging.info(f"Matched Script based on prefix: '{script_name}', Pipeline: '{pipeline_name}'")
                         found = True
                         break
                if not found:
                     logging.warning(f"Could not determine Script/Pipeline for JSON file {json_filepath.name}. Skipping Pipeline/Script linking for this file.")
                     # Decide: skip entirely, or load lineage without pipeline/script links?
                     # continue # Option: Skip this file entirely


            # Update CSV source names if needed
            lineage_data = update_function_name(lineage_data)

            # Extract tables and import schema first (idempotent)
            if duckdb_conn: # Only attempt schema import if DuckDB connected
                table_names_for_schema = extract_table_names(lineage_data)
                for table_name in table_names_for_schema:
                    if table_name and '.' in table_name and not table_name.startswith("csv_files."): # Avoid schema import for generic CSVs
                        logging.debug(f"Importing schema for: {table_name}")
                        import_schema_to_memgraph(memgraph, duckdb_conn, table_name)
                    else:
                        logging.debug(f"Skipping schema import for non-standard or CSV table: {table_name}")
            else:
                logging.info("Skipping schema import step as DuckDB connection is not available.")


            # Load the column-level lineage with script/pipeline context
            if script_name and pipeline_name:
                 load_lineage_to_memgraph(memgraph, lineage_data, script_name, pipeline_name)
            else:
                 logging.warning(f"Skipping lineage loading for {json_filepath.name} due to missing script/pipeline context.")
                 # Alternatively, call load_lineage without script/pipeline context if you want partial data
                 # load_lineage_to_memgraph(memgraph, lineage_data, None, None) # Requires adjusting the function to handle None

        # --- Create Constraints/Indexes ---
        logging.info("Ensuring constraints and indexes...")
        constraints_indexes = [
            "CREATE CONSTRAINT ON (s:Schema) ASSERT s.name IS UNIQUE;",
            "CREATE INDEX ON :Schema(name);",
            "CREATE CONSTRAINT ON (t:Table) ASSERT t.full_name IS UNIQUE;",
            "CREATE INDEX ON :Table(full_name);",
            "CREATE CONSTRAINT ON (c:Column) ASSERT c.full_name IS UNIQUE;",
            "CREATE INDEX ON :Column(full_name);",
            # Add constraints/indexes for Pipeline and Script
            "CREATE CONSTRAINT ON (p:Pipeline) ASSERT p.name IS UNIQUE;",
            "CREATE INDEX ON :Pipeline(name);",
            "CREATE CONSTRAINT ON (s:Script) ASSERT s.name IS UNIQUE;",
            "CREATE INDEX ON :Script(name);",
        ]
        for statement in constraints_indexes:
            try:
                memgraph.execute(statement)
            except Exception as e:
                # Ignore errors if they already exist (common for constraints/indexes)
                err_msg = str(e).lower()
                if "already exists" in err_msg or "constraint requires" in err_msg or "index already exists" in err_msg:
                    logging.debug(f"Constraint/Index already exists: {statement.split(' ON ')[0]}")
                else:
                    logging.warning(f"Could not apply constraint/index '{statement}': {e}")
        logging.info("Finished applying constraints and indexes.")

        logging.info("\nLineage loading process complete.")

    except NotADirectoryError as e:
        logging.error(f"JSON source directory error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during the main process: {e}", exc_info=True)
    finally:
        # Clean up connections
        if duckdb_conn:
            try:
                duckdb_conn.close()
                logging.info("Closed DuckDB connection.")
            except Exception as e:
                logging.error(f"Error closing DuckDB connection: {e}")
        # GQLAlchemy connection doesn't usually need explicit close unless using specific drivers/pooling


    # set sql and paths

    sql_files_dict = read_all_sql_files("/app/src/main/sql_for_pipelines")
    update_script_properties(memgraph, sql_files_dict)
    dependencies(memgraph)




if __name__ == "__main__":
    main()
# your_etl_script.py
import json
import duckdb
from lineage_decorator import log_lineage
from openlineage_setup import OPENLINEAGE_NAMESPACE
from lineage_translator import translate_llm_format_to_ol_map

import os
duckdb_path = os.path.abspath("C:\lopu-kg-test\project\initial_db.duckdb")

# Assume conn is an established DuckDB connection
conn = duckdb.connect(duckdb_path) # Or :memory:

# Example SQL script content (can be loaded from a file)
LOAD_BATCH_DATE_SQL = """
INSERT INTO wh_Db.BatchDate (batchdate, batchid)
SELECT
    '2011-01-01'::DATE,
    7 batchid
;
"""
sql_path = "C:\lopu-kg-test\project\src\main\sql_for_pipelines\\3_wh_db.DimCustomer.sql"
with open(sql_path, 'r', encoding='utf-8') as f_sql:
    sql_content = f_sql.read()

LOAD_BATCH_DATE_SQL = sql_content


# Manually define column lineage (Option B example)
# This needs careful construction based on your actual SQL and desired detail
dim_customer_col_lineage = {
    "batchdate": {"input": "file.date"
    },
    "batchid": {
        "input": 
            "HC.string"
    }
    # ... add other columns if needed ...
}
#lineage_path ="C:\lopu-kg-test\project\src\LLM_answers\llm_prompt_for_column_level_lineage_easy\\answer_3_wh_db.DimCustomer_20250411_174942.json"
lineage_path = "C:\lopu-kg-test\project\src\LLM_answers\llm_prompt_for_column_level_lineage_hard_wo_ex\\answer_3_wh_db.DimCustomer_20250411_180418.json"  # -- NENDEGA PRAEGU EI TÖÖTA, sest on liiga detailsed
with open(lineage_path, 'r', encoding='utf-8') as f_lin:
    lin_content_string = f_lin.read()

name_space = "default_ns"
lineage_json = json.loads(lin_content_string)
lineage_for_ol_map = translate_llm_format_to_ol_map(lineage_json, name_space)

@log_lineage(
    job_name="load_dimcustomer",
    input_tables=["customers_final", "wh_Db.TaxRate", "wh_db_stage.ProspectIncremental"],
    output_tables=["wh_db.DimCustomer"],
    sql=LOAD_BATCH_DATE_SQL, # Pass the SQL query text
    column_lineage=lineage_for_ol_map, # Uncomment if using manual lineage map
    script_path=__file__ # Optional: Explicitly pass script path if needed
)
def run_load_dim_customer(db_conn: duckdb.DuckDBPyConnection, some_other_param: str = "default"):
    """Executes the SQL to load the customer dimension."""
    print(f"Running Load Dim Customer with param: {some_other_param}")
    # Execute the actual SQL

    # DISABLE FOR TESTING  ----  HISTORICAL

    # db_conn.execute(LOAD_BATCH_DATE_SQL) 
    print("Dim Customer load finished.")
    # You might return something if needed, e.g., row count affected
    # return db_conn.query("SELECT count(*) FROM dim_customer").fetchone()[0]


# --- How to run it ---
if __name__ == "__main__":

    # Execute the decorated function
    try:
        run_load_dim_customer(conn, some_other_param="run1")
    except Exception as e:
        print(f"ETL script failed: {e}")

    conn.close()




import os
from datetime import datetime
from typing import Any, Dict, List, Optional

# Assuming you have duckdb installed and a connection if you want to fetch schemas
# import duckdb

from openlineage.client.event_v2 import (
    Dataset,
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.facet_v2 import (
    SchemaDatasetFacet, # Renamed from schema_dataset
    SchemaField,       # Renamed from schema_dataset
    ColumnLineageDatasetFacet, # Renamed from column_lineage_dataset
    Fields,            # Renamed from column_lineage_dataset
    InputField,        # Renamed from column_lineage_dataset
    # Other facets you might use
    # nominal_time_run,
    # source_code_location_job,
    # sql_job,
)
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client import OpenLineageClient
from openlineage.client.uuid import generate_new_uuid

# --- Your Provided Input Data ---
lineage_input = {
    "target_table": "wh_db.DimCustomer",
    "sources_summary": {
        "customers_final": [
            "customerid", "taxid", "status", "lastname", "firstname",
            "middleinitial", "gender", "tier", "dob", "addressline1",
            "addressline2", "postalcode", "city", "stateprov", "country",
            "phone1", "phone2", "phone3", "email1", "email2", "lcl_tx_id",
            "nat_tx_id", "iscurrent", "batchid", "effectivedate", "enddate"
        ],
        "wh_db.TaxRate": [
            "TX_ID", "TX_NAME", "TX_RATE"
        ],
        "wh_db_stage.ProspectIncremental": [
            "agencyid", "creditrating", "networth", "marketingnameplate",
            "lastname", "firstname", "addressline1", "addressline2", "postalcode"
        ],
        "wh_db.DimCustomer": [
            "sk_customerid"
        ]
    },
    "lineage": {
        "sk_customerid": {
            "path": ["MaxSK"], "transformation_type": "ROW_NUMBER",
            "transformation_logic": "ROW_NUMBER() OVER () + (SELECT max_sk_customerid FROM MaxSK) + 1",
            "sources": [{"table": "wh_db.DimCustomer", "column": "sk_customerid", "role": "MAX_VALUE", "transformation_type": "AGGREGATION", "transformation_logic": "COALESCE(MAX(sk_customerid), 0)"}]
        },
        "customerid": {
            "path": ["CustomerData"], "transformation_type": "RENAME", "transformation_logic": "c.customerid",
            "sources": [{"table": "customers_final", "column": "customerid", "role": "SOURCE", "transformation_type": "RENAME", "transformation_logic": "c.customerid"}]
        },
        "taxid": {
             "path": ["CustomerData"], "transformation_type": "RENAME", "transformation_logic": "c.taxid",
            "sources": [{"table": "customers_final", "column": "taxid", "role": "SOURCE", "transformation_type": "RENAME", "transformation_logic": "c.taxid"}]
        },
        # ... (rest of your lineage data) ...
         "gender": {
            "path": ["CustomerData"], "transformation_type": "CASE_MAPPING",
            "transformation_logic": "IF(c.gender IN ('M', 'F'), c.gender, 'U')",
            "sources": [{"table": "customers_final", "column": "gender", "role": "CONDITION_INPUT", "transformation_type": "RENAME", "transformation_logic": "c.gender"}]
        },
        "nationaltaxratedesc": {
            "path": ["CustomerData"], "transformation_type": "RENAME", "transformation_logic": "r_nat.TX_NAME",
            "sources": [{"table": "wh_db.TaxRate", "column": "TX_NAME", "role": "SOURCE", "transformation_type": "RENAME", "transformation_logic": "r_nat.TX_NAME"}]
        },
        "nationaltaxrate": {
            "path": ["CustomerData"], "transformation_type": "RENAME", "transformation_logic": "r_nat.TX_RATE",
            "sources": [{"table": "wh_db.TaxRate", "column": "TX_RATE", "role": "SOURCE", "transformation_type": "RENAME", "transformation_logic": "r_nat.TX_RATE"}]
        },
        "localtaxratedesc": {
            "path": ["CustomerData"], "transformation_type": "RENAME", "transformation_logic": "r_lcl.TX_NAME",
            "sources": [{"table": "wh_db.TaxRate", "column": "TX_NAME", "role": "SOURCE", "transformation_type": "RENAME", "transformation_logic": "r_lcl.TX_NAME"}]
        },
         "localtaxrate": {
            "path": ["CustomerData"], "transformation_type": "RENAME", "transformation_logic": "r_lcl.TX_RATE",
            "sources": [{"table": "wh_db.TaxRate", "column": "TX_RATE", "role": "SOURCE", "transformation_type": "RENAME", "transformation_logic": "r_lcl.TX_RATE"}]
        },
        "agencyid": {
            "path": ["CustomerData"], "transformation_type": "RENAME", "transformation_logic": "p.agencyid",
            "sources": [{"table": "wh_db_stage.ProspectIncremental", "column": "agencyid", "role": "SOURCE", "transformation_type": "RENAME", "transformation_logic": "p.agencyid"}]
        },
        # ... (add the rest of your lineage entries here for a complete example)
         "enddate": {
            "path": ["CustomerData"], "transformation_type": "RENAME", "transformation_logic": "c.enddate",
            "sources": [{"table": "customers_final", "column": "enddate", "role": "SOURCE", "transformation_type": "RENAME", "transformation_logic": "c.enddate"}]
        }
    }
}

# --- Helper Function for Column Lineage Facet ---

def create_column_lineage_facet(
    lineage_map: Dict[str, Any],
    namespace: str,
    # Add optional mapping for source table names if they differ in OL namespace
    source_name_mapping: Optional[Dict[str, str]] = None
) -> ColumnLineageDatasetFacet:
    """
    Creates an OpenLineage ColumnLineageDatasetFacet from a lineage map.

    Args:
        lineage_map: The 'lineage' dictionary from your input structure.
        namespace: The OpenLineage namespace for the datasets.
        source_name_mapping: Optional dict to map table names from the input
                             JSON to the actual OpenLineage dataset names
                             (if they differ, e.g., due to prefixes/suffixes).
                             Keys are names from JSON, values are OL dataset names.

    Returns:
        An instance of ColumnLineageDatasetFacet.
    """
    output_fields_map: Dict[str, Fields] = {}
    if source_name_mapping is None:
        source_name_mapping = {} # Use original names if no mapping provided

    for output_column, details in lineage_map.items():
        input_fields_list: List[InputField] = []
        for source in details.get("sources", []):
            source_table = source.get("table")
            source_column = source.get("column")

            if source_table and source_column:
                # Apply mapping if necessary
                ol_source_table_name = source_name_mapping.get(source_table, source_table)

                input_fields_list.append(
                    InputField(
                        namespace=namespace,
                        name=ol_source_table_name, # Use potentially mapped name
                        field=source_column,
                    )
                )

        # Only add the field to the map if it has input fields
        if input_fields_list:
             output_fields_map[output_column] = Fields(
                inputFields=input_fields_list,
                transformationDescription=details.get("transformation_logic"),
                transformationType=details.get("transformation_type"),
            )

    return ColumnLineageDatasetFacet(fields=output_fields_map)

# --- V2 Helper Function for Schema (Optional but Recommended) ---
# Note: This needs a database connection (e.g., duckdb) to actually work.
# It's adapted from your V1 function to use V2 classes.

def get_table_schema_v2(db_conn: Any, table_name: str) -> Optional[SchemaDatasetFacet]:
    """Gets table schema and returns OpenLineage V2 SchemaDatasetFacet."""
    # Replace with your actual DB connection logic (duckdb, sqlalchemy, etc.)
    if db_conn is None:
        print(f"Warning: No DB connection provided for schema lookup of '{table_name}'.")
        return None
    try:
        # Example for DuckDB - adjust query for your database
        # schema_info = db_conn.execute(f"PRAGMA table_info('{table_name}');").fetchall()
        # Example for generic SQL (might need adjustments)
        # schema_info = db_conn.execute(f"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{table_name.split('.')[-1]}' AND table_schema = '{table_name.split('.')[0]}';").fetchall()

        # *** Placeholder - Replace with actual DB interaction ***
        print(f"Placeholder: Would query DB for schema of '{table_name}' here.")
        # Example structure assuming query returns [(name, type, description), ...]
        schema_info = [
             ("col1", "VARCHAR", "Sample Column 1"),
             ("col2", "INTEGER", None),
             ("col3", "DATE", "Sample Date Column")
        ] # Replace with real data

        fields = [
             # Adjust indices based on your actual query results
             # SchemaField(name=row[0], type=row[1], description=row[2])
             SchemaField(name=row[0], type=row[1], description=row[2] if len(row) > 2 and row[2] else None)
             for row in schema_info
        ]
        if not fields:
            print(f"Warning: Could not get schema for table '{table_name}' or it's empty.")
            return None
        return SchemaDatasetFacet(fields=fields)
    except Exception as e:
        print(f"Error getting schema for table {table_name}: {e}")
        return None


# --- Example Usage ---

# 1. Configure OpenLineage Client
# Using FileTransport for demonstration
output_dir = "./openlineage_events"
os.makedirs(output_dir, exist_ok=True)
file_config = FileConfig(openlineage_yml_path=output_dir) # Directory path
client = OpenLineageClient(transport=file_config)
# For HTTP:
# from openlineage.client.transport.http import HttpConfig, HttpTransport
# client = OpenLineageClient(url="http://localhost:5000") # Replace with your OL endpoint


# 2. Define Job and Run Details
namespace = "my_data_warehouse" # Or your specific namespace
job_name = "load_dim_customer"
run_id = str(generate_new_uuid())
start_time = datetime.utcnow()

# --- Mock DB Connection (Replace with your actual connection) ---
db_connection = None # Or: duckdb.connect(':memory:')

# 3. Create Input Datasets
input_datasets: List[InputDataset] = []
source_tables = list(lineage_input["sources_summary"].keys())

# Optional: Define mapping if JSON names differ from OL dataset names
# Example: If 'customers_final' is actually 'staging.customers_final' in OL
source_name_mapping = {
    # "customers_final": "staging.customers_final"
}

for table_name in source_tables:
    ol_table_name = source_name_mapping.get(table_name, table_name)
    schema_facet = get_table_schema_v2(db_connection, table_name) # Fetch schema if possible
    input_facets = {}
    if schema_facet:
        input_facets["schema"] = schema_facet
    input_datasets.append(
        InputDataset(
            namespace=namespace,
            name=ol_table_name, # Use potentially mapped name
            facets=input_facets
        )
    )

# 4. Create the Column Lineage Facet for the Output
column_lineage_facet = create_column_lineage_facet(
    lineage_map=lineage_input["lineage"],
    namespace=namespace,
    source_name_mapping=source_name_mapping # Pass the mapping here too
)

# 5. Create Output Dataset (and add the column lineage facet)
target_table_name = lineage_input["target_table"]
target_schema_facet = get_table_schema_v2(db_connection, target_table_name)
output_facets = {
    "columnLineage": column_lineage_facet # Key name follows convention
}
if target_schema_facet:
    output_facets["schema"] = target_schema_facet


output_dataset = OutputDataset(
    namespace=namespace,
    name=target_table_name,
    facets=output_facets
)

# 6. Create and Emit START Event
start_run_event = RunEvent(
    eventType=RunState.START,
    eventTime=start_time.isoformat() + "Z",
    run=Run(runId=run_id), # Add run facets if needed
    job=Job(namespace=namespace, name=job_name), # Add job facets if needed
    inputs=input_datasets,
    outputs=[], # Typically no outputs on START, unless it's CREATE TABLE AS
    producer="my_custom_script/v1.0"
)
print("Emitting START event...")
client.emit(start_run_event)
print(f"Event written to {output_dir} (or sent via HTTP)")


# --- Simulate Job Execution ---
print(f"\nSimulating execution of job: {job_name} ...\n")
# time.sleep(2) # Simulate work


# 7. Create and Emit COMPLETE Event (including output dataset with lineage)
end_time = datetime.utcnow()

complete_run_event = RunEvent(
    eventType=RunState.COMPLETE,
    eventTime=end_time.isoformat() + "Z",
    run=Run(runId=run_id), # Should match the START event's runId
    job=Job(namespace=namespace, name=job_name), # Should match
    inputs=input_datasets, # Include inputs again for context
    outputs=[output_dataset], # Include the output with column lineage
    producer="my_custom_script/v1.0"
)

print("Emitting COMPLETE event...")
client.emit(complete_run_event)
print(f"Event written to {output_dir} (or sent via HTTP)")
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
#lineage_for_ol_map = translate_llm_format_to_ol_map(lineage_json, name_space)

@log_lineage(
    job_name="load_dimcustomer",
    input_tables=["customers_final", "wh_Db.TaxRate", "wh_db_stage.ProspectIncremental"],
    output_tables=["wh_db.DimCustomer"],
    sql=LOAD_BATCH_DATE_SQL, # Pass the SQL query text
    column_lineage=lineage_json, # Uncomment if using manual lineage map
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
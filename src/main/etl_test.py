# your_etl_script.py
import duckdb
from lineage_decorator import log_lineage
from openlineage_setup import OPENLINEAGE_NAMESPACE

import os
duckdb_path = os.path.abspath("C:\lopu-kg-test\project\initial_db.duckdb")

# Assume conn is an established DuckDB connection
conn = duckdb.connect(duckdb_path) # Or :memory:

# Example SQL script content (can be loaded from a file)
LOAD_DIM_CUSTOMER_SQL = """
INSERT INTO wh_Db.BatchDate (batch_date, batchid)
SELECT
    '2011-01-01'::DATE,
    7 batchid
;
"""

# Manually define column lineage (Option B example)
# This needs careful construction based on your actual SQL and desired detail
dim_customer_col_lineage = {
    "customer_key": {
        "inputFields": [{"namespace": OPENLINEAGE_NAMESPACE, "name": "staging_customer", "field": "customer_id"}],
        "transformationType": "IDENTITY"
    },
    "name": {
        "inputFields": [
            {"namespace": OPENLINEAGE_NAMESPACE, "name": "staging_customer", "field": "first_name"},
            {"namespace": OPENLINEAGE_NAMESPACE, "name": "staging_customer", "field": "last_name"}
        ],
        "transformationType": "TRANSFORMATION", # Or be more specific
        "transformationDescription": "Concatenation"
    },
    "address": {
         "inputFields": [{"namespace": OPENLINEAGE_NAMESPACE, "name": "staging_address", "field": "address"}],
         "transformationType": "IDENTITY"
    },
    "source_id": {
         "inputFields": [{"namespace": OPENLINEAGE_NAMESPACE, "name": "staging_customer", "field": "customer_id"}],
         "transformationType": "IDENTITY"
    }
    # ... add other columns if needed ...
}


@log_lineage(
    job_name="load_dim_customer",
    input_tables=["wh_Db.BatchDate"],
    output_tables=["wh_Db.BatchDate"],
    sql=LOAD_DIM_CUSTOMER_SQL, # Pass the SQL query text
    column_lineage=dim_customer_col_lineage, # Uncomment if using manual lineage map
    script_path=__file__ # Optional: Explicitly pass script path if needed
)
def run_load_dim_customer(db_conn: duckdb.DuckDBPyConnection, some_other_param: str = "default"):
    """Executes the SQL to load the customer dimension."""
    print(f"Running Load Dim Customer with param: {some_other_param}")
    # Execute the actual SQL
    db_conn.execute(LOAD_DIM_CUSTOMER_SQL)
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
# your_etl_script.py
import duckdb
from lineage_decorator import log_lineage
from openlineage_setup import OPENLINEAGE_NAMESPACE

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

""" 
{
  "order_id": {"input": "delivery_7_days.order_id"},
  "order_placed_on": {"input": "delivery_7_days.order_placed_on"},
  "order_delivered_on": {"input": "delivery_7_days.order_delivered_on"},
  "order_delivery_time": {
    "inputs": [
      {
        "column": "delivery_7_days.order_placed_on",
        "type": "DIRECT",
        "subtype": "Transformation",
        "description": "order_placed_on used in DATEDIFF",
        "masking": false
      },
      {
        "column": "delivery_7_days.order_delivered_on",
        "type": "DIRECT",
        "subtype": "Transformation",
        "description": "order_delivered_on used in DATEDIFF",
        "masking": false
      }
    ],
    "transformation": "DATEDIFF(minute, order_placed_on, order_delivered_on)"
  }
}
"""


@log_lineage(
    job_name="load_BatchDate",
    input_tables=[],
    output_tables=["wh_Db.BatchDate"],
    sql=LOAD_BATCH_DATE_SQL, # Pass the SQL query text
    column_lineage=dim_customer_col_lineage, # Uncomment if using manual lineage map
    script_path=__file__ # Optional: Explicitly pass script path if needed
)
def run_load_dim_customer(db_conn: duckdb.DuckDBPyConnection, some_other_param: str = "default"):
    """Executes the SQL to load the customer dimension."""
    print(f"Running Load Dim Customer with param: {some_other_param}")
    # Execute the actual SQL
    db_conn.execute(LOAD_BATCH_DATE_SQL)
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
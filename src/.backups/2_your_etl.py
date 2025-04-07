# your_etl_script.py (or a utility module)
import duckdb
from lineage_decorator_dynamic import log_lineage_dynamic # Import the modified decorator

@log_lineage_dynamic() # Apply the decorator without arguments
def execute_sql_task_dynamically(db_conn: duckdb.DuckDBPyConnection, job_metadata: dict):
    """
    Executes a single SQL task.
    Expects 'sql' key in job_metadata containing the SQL string.
    Relies on the @log_lineage_dynamic decorator to handle OL events based on job_metadata.
    """
    sql_to_run = job_metadata.get('sql')
    job_name = job_metadata.get('job_name', 'unnamed_sql_task') # Get name for logging

    if not sql_to_run:
        raise ValueError(f"No 'sql' key found in job_metadata for job '{job_name}'.")

    print(f"  Executing SQL for job: {job_name}")
    # Here you might load SQL from a file if job_metadata['sql'] is a path
    db_conn.execute(sql_to_run)
    print(f"  Finished SQL for job: {job_name}")
    # You could return row count or other info if needed by the calling loop

    # your_etl_script.py (Main execution part)
    
import duckdb
# Assume get_metadata_for_sql_task exists as defined previously

if __name__ == "__main__":
    conn = duckdb.connect('my_database.db')
    # Example: List of SQL files or commands to process
    sql_tasks_identifiers = ["load_dim_customer.sql", "load_dim_product.sql", "invalid_task.sql"] # Or other identifiers

    for task_id in sql_tasks_identifiers:
        try:
            # 1. Get the metadata for the current task
            current_job_meta = get_metadata_for_sql_task(task_id) # Your logic here
            if not current_job_meta:
                print(f"Skipping task '{task_id}': No metadata found.")
                continue

            # 2. Call the decorated function, passing the connection and the metadata dictionary
            execute_sql_task_dynamically(db_conn=conn, job_metadata=current_job_meta)

        except Exception as e:
            # Log error, decide whether to continue or stop the loop
            print(f"!!! Error processing task '{task_id}': {e}")
            # break # Optional: Stop processing further tasks on error

    conn.close()
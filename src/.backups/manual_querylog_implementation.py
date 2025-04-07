sql = """
CREATE TABLE IF NOT EXISTS querylog (
    log_id BIGINT PRIMARY KEY DEFAULT nextval('querylog_log_id_seq'), -- Auto-incrementing ID
    start_time TIMESTAMP_TZ,       -- Timestamp with timezone when query started
    end_time TIMESTAMP_TZ,         -- Timestamp with timezone when query ended
    query_text TEXT,              -- The actual SQL query executed
    status VARCHAR CHECK (status IN ('PASS', 'FAIL')), -- Success or failure
    rows_affected BIGINT,         -- Rows returned (SELECT) or affected (DML)
    error_message TEXT NULL,      -- Error details if status is 'FAIL'
    input_sources TEXT NULL,      -- Optional: Manual description of input tables/files
    output_destination TEXT NULL  -- Optional: Manual description of output table/file
);

-- Create the sequence for the auto-incrementing ID
CREATE SEQUENCE IF NOT EXISTS querylog_log_id_seq;
"""

import duckdb
import datetime
import time # For calculating duration if needed, though start/end times are stored

def execute_and_log(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: list | tuple | None = None,
    input_sources: str | None = None,
    output_destination: str | None = None
) -> duckdb.DuckDBPyRelation | None:
    """
    Executes a SQL query using the given DuckDB connection, logs the execution
    details to the 'querylog' table, and returns the query result (Relation).

    Args:
        con: An active DuckDB connection object.
        sql: The SQL query string to execute.
        params: Optional parameters for the SQL query (for parameterized queries).
        input_sources: Optional description of input tables/files for logging.
        output_destination: Optional description of output table/file for logging.

    Returns:
        A DuckDB Relation object if the query executes successfully and is
        expected to return results (like SELECT), otherwise None.
        Re-raises the original exception if the query fails after logging.

    Raises:
        duckdb.Error: Re-raises the original DuckDB error after logging the failure.
    """
    start_time = datetime.datetime.now(datetime.timezone.utc)
    status = 'FAIL' # Default to FAIL
    rows_affected = -1 # Default for error or non-applicable
    error_message = None
    result_relation = None
    log_conn = None # Use a separate cursor/conn for logging if needed for atomicity

    try:
        # --- Execute the actual query ---
        if params:
            result_relation = con.execute(sql, params)
        else:
            result_relation = con.execute(sql)

        # --- Try to determine rows affected/returned ---
        # For SELECT statements, fetchall() materializes results.
        # For DML (INSERT/UPDATE/DELETE), DuckDB's execute often doesn't
        # directly return row count on the relation itself easily *before* fetching
        # (which isn't applicable). We'll fetch for SELECTs to get count.
        # For others, rely on potential future enhancements or accept 0.
        # A common pattern is to wrap DML in 'SELECT count(*) FROM (...)'
        # if the count is critical *during* the operation.
        if result_relation and result_relation.description: # Check if it's a SELECT-like query
            try:
                # Be mindful of memory usage for very large results
                results = result_relation.fetchall()
                rows_affected = len(results)
                # Re-create relation if fetchall() consumed it and caller needs it
                # Note: This might re-execute. Depending on DuckDB version/behavior.
                # It might be better to just return the fetched 'results' list instead
                # of the relation if fetchall() consumes it. Let's return the original relation.
                # Caller can decide to fetch. Set rows_affected based on fetch attempt.

            except Exception as fetch_err:
                print(f"Warning: Could not fetch results to determine row count: {fetch_err}")
                rows_affected = -2 # Indicate fetch error maybe
        else:
             # For non-SELECT (INSERT, UPDATE, DELETE, CREATE TABLE AS, etc.)
             # DuckDB Python API doesn't consistently provide row count here easily.
             # You might execute a subsequent COUNT(*) or rely on DuckDB's logs if enabled.
             # Setting to 0 as a placeholder.
             rows_affected = 0 # Placeholder for DML/DDL

        status = 'PASS'
        # --- Execution successful ---

    except duckdb.Error as e:
        error_message = str(e)
        status = 'FAIL'
        rows_affected = -1 # Explicitly -1 for failure
        # The exception will be re-raised in the finally block after logging

    finally:
        end_time = datetime.datetime.now(datetime.timezone.utc)
        log_sql = """
            INSERT INTO querylog (
                start_time, end_time, query_text, status,
                rows_affected, error_message, input_sources, output_destination
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """
        log_params = (
            start_time,
            end_time,
            sql, # Log the original query
            status,
            rows_affected,
            error_message,
            input_sources,
            output_destination
        )

        try:
            # Use the same connection for simplicity, but be aware of transactions.
            # If the main query fails and rolls back, this log might too unless
            # handled carefully (e.g., separate connection, or commit log separately).
            log_conn = con.cursor() # Use a cursor
            log_conn.execute(log_sql, log_params)
            # Decide on commit strategy:
            # 1. Rely on caller's commit: Do nothing here.
            # 2. Auto-commit (if enabled): Do nothing here.
            # 3. Commit log separately (use with caution): con.commit()
            # Let's assume caller manages commits for the main transaction.
            log_conn.close()

        except duckdb.Error as log_e:
            # Critical: Failed to log the query execution!
            print(f"--- CRITICAL: FAILED TO LOG QUERY ---")
            print(f"Log Error: {log_e}")
            print(f"Original Query: {sql}")
            print(f"Original Status: {status}")
            print(f"Original Error: {error_message}")
            print(f"------------------------------------")

        # If the original query failed, re-raise the exception
        # so the calling code knows about it.
        if status == 'FAIL' and 'e' in locals():
             raise e # Re-raise the original exception

    return result_relation # Return the relation object for successful queries

# --- Example Usage ---

# Connect to an in-memory database or a file
db_file = 'my_database.duckdb'
conn = duckdb.connect(database=db_file, read_only=False)

# Create the sequence and table (idempotent using IF NOT EXISTS)
create_sequence_sql = "CREATE SEQUENCE IF NOT EXISTS querylog_log_id_seq;"
create_table_sql = """
CREATE TABLE IF NOT EXISTS querylog (
    log_id BIGINT PRIMARY KEY DEFAULT nextval('querylog_log_id_seq'),
    start_time TIMESTAMP_TZ,
    end_time TIMESTAMP_TZ,
    query_text TEXT,
    status VARCHAR CHECK (status IN ('PASS', 'FAIL')),
    rows_affected BIGINT,
    error_message TEXT NULL,
    input_sources TEXT NULL,
    output_destination TEXT NULL
);
"""
conn.execute(create_sequence_sql)
conn.execute(create_table_sql)
conn.commit() # Commit table creation

# --- Example 1: Successful SELECT query ---
print("Running successful SELECT...")
try:
    # Manually specify potential input source if known
    relation = execute_and_log(
        conn,
        "SELECT ?::INTEGER + ?::INTEGER AS result;", # Parameterized query
        params=[5, 7],
        input_sources="Literal values"
    )
    if relation:
        print("Query Result:")
        print(relation.fetchall()) # Fetch results from the returned relation
    conn.commit() # Commit transaction if needed
except duckdb.Error as e:
    print(f"Query failed: {e}")
    # conn.rollback() # Rollback if needed

# --- Example 2: Successful CREATE TABLE and INSERT ---
print("\nRunning successful CREATE/INSERT...")
try:
    # Create a sample table
    execute_and_log(
        conn,
        "CREATE OR REPLACE TABLE my_table (id INTEGER, name VARCHAR);",
        output_destination="my_table"
    )
    # Insert into the table
    execute_and_log(
        conn,
        "INSERT INTO my_table VALUES (1, 'Alice'), (2, 'Bob');",
        input_sources="Literal values",
        output_destination="my_table"
    )
    # Verify insertion (also gets logged)
    relation_verify = execute_and_log(
        conn,
        "SELECT * FROM my_table;",
        input_sources="my_table"
    )
    if relation_verify:
        print("Table content:")
        print(relation_verify.fetchall())
    conn.commit()
except duckdb.Error as e:
    print(f"Query failed: {e}")
    conn.rollback()

# --- Example 3: Failing query ---
print("\nRunning failing query...")
try:
    execute_and_log(conn, "SELECT * FROM non_existent_table;")
    conn.commit() # This won't be reached if execute_and_log raises error
except duckdb.Error as e:
    print(f"Caught expected query failure: {e}")
    # No commit/rollback needed here usually, as the transaction failed
    # before changes (unless prior operations in the try block succeeded)

# --- Check the log table ---
print("\n--- Query Log Contents ---")
log_contents = conn.execute("SELECT * FROM querylog ORDER BY start_time;").fetchall()
for row in log_contents:
    print(row)

# Close the connection
conn.close()
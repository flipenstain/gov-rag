import duckdb
import logging
from typing import List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the database file path (make sure this matches setup_duckdb.py)
DB_FILE = "C:\lopu-kg-test\project\initial_db.duckdb"

def get_table_columns(table_name: str) -> List[str]:
    """
    Retrieves the list of column names for a given table from the DuckDB database.

    Args:
        table_name: The fully qualified name of the table (e.g., 'schema_name.table_name').

    Returns:
        A list of column names for the specified table.
        Returns an empty list if the table is not found or an error occurs.
    """
    logging.info(f"Attempting to get columns for table: {table_name}")
    columns = []
    try:
        # Connect to DuckDB in read-only mode
        con = duckdb.connect(database=DB_FILE, read_only=True)

        # Use PRAGMA table_info to get schema information
        # Query needs to handle potential SQL injection if table_name were directly formatted,
        # but PRAGMA doesn't support parameters directly. DuckDB's Python client is generally safe.
        # However, validating table_name format beforehand is good practice if input is less trusted.
        # For internal agent use, this is generally okay.
        query = f"PRAGMA table_info('{table_name}');"
        logging.info(f"Executing query: {query}")

        result = con.execute(query).fetchall()

        # The column name is typically in the second position (index 1) of the result tuple
        if result:
            columns = [row[1] for row in result]
            logging.info(f"Found columns for '{table_name}': {columns}")
        else:
            logging.warning(f"No columns found for table '{table_name}'. It might not exist or schema is inaccessible.")

    except duckdb.CatalogException as e:
        logging.error(f"Catalog Error: Table '{table_name}' likely does not exist or schema name is incorrect: {e}")
        # Return empty list as the table wasn't found
        columns = []
    except duckdb.Error as e:
        logging.error(f"DuckDB Error querying table info for '{table_name}': {e}")
        # Return empty list on other DuckDB errors
        columns = []
    except Exception as e:
        logging.error(f"An unexpected error occurred in get_table_columns for '{table_name}': {e}", exc_info=True)
        # Return empty list on unexpected errors
        columns = []
    finally:
        if 'con' in locals() and con:
            con.close()
            logging.debug("DuckDB connection closed.")

    return columns

if __name__ == "__main__":
    print("Testing get_table_columns...")
    get_table_columns("temp_propect")
    import os
    PROMPT_FILE_PATH = os.path.join("C:\lopu-kg-test\project\\agentic\prompts", "copy_prompt.txt")
    print(PROMPT_FILE_PATH)
    
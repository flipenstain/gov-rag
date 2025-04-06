import duckdb
import os
import re

def execute_sql_and_comments(folder_path, con):
    """
    Executes all .sql and .comment files in a folder using DuckDB.

    Args:
        folder_path (str): The path to the folder containing .sql and .comment files.
        con (duckdb.DuckDBPyConnection): The DuckDB connection object.
    """
    if not os.path.isdir(folder_path):
        print(f"Error: {folder_path} is not a directory.")
        return

    sql_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".sql")])
    comment_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".comment")])

    for filename in sql_files:
        sql_file_path = os.path.join(folder_path, filename)
        try:
            with open(sql_file_path, "r") as file:
                sql_content = file.read()
                con.sql(sql_content)
                print(f"SQL script {filename} executed successfully.")
        except FileNotFoundError:
            print(f"Error: File not found at {sql_file_path}")
        except duckdb.Error as e:
            print(f"DuckDB Error in {filename}: {e}")
        except Exception as generic_e:
            print(f"An unexpected error occurred in {filename}: {generic_e}")

    for filename in comment_files:
        comment_file_path = os.path.join(folder_path, filename)
        try:
            with open(comment_file_path, "r") as file:
                comment_content = file.read()
                con.sql(comment_content)
                print(f"Comment script {filename} executed successfully.")
        except FileNotFoundError:
            print(f"Error: File not found at {comment_file_path}")
        except duckdb.Error as e:
            print(f"DuckDB Error in {filename}: {e}")
        except Exception as generic_e:
            print(f"An unexpected error occurred in {filename}: {generic_e}")

# Example Usage:
folder_path = r"src\divider\ddls"  # Replace with your folder path

#con = duckdb.connect()
con_path = r"initial_db.duckdb"
con = duckdb.connect(con_path)
execute_sql_and_comments(folder_path, con)

con.close()
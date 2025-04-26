
import duckdb
con_path = "C:\lopu-kg-test\project\initial_db.duckdb"
con = duckdb.connect(con_path)
con.sql("CALL start_ui();")

# Get table list
tables = con.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_catalog = 'initial_db'
""").fetchall()

# Now query each table dynamically
results = []
for schema, table in tables:
    count = con.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
    results.append((schema, table, count))

# Output
for row in results:
    print(row)

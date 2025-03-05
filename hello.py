import duckdb

con = duckdb.connect()

try:
    con.execute("""
    CREATE TABLE IF NOT EXISTS wh_db_stage.FinWire (
        rectype STRING,
        recdate DATE,
        value STRING
    )
    PARTITION BY (rectype);
    """)

    con.execute("""
    INSERT INTO wh_db_stage.FinWire VALUES
        ('CMP', '2023-01-15', 'Data 1'),
        ('FIN', '2023-01-20', 'Data 2'),
        ('CMP', '2023-02-10', 'Data 3'),
        ('SEC', '2023-02-25', 'Data 4');
    """)

    con.commit()

    result = con.execute("SELECT * FROM wh_db_stage.FinWire").fetchdf()
    print(result)

    result2 = con.execute("SELECT DISTINCT rectype FROM wh_db_stage.FinWire").fetchdf()
    print(result2)

except duckdb.Error as e:
    print(f"DuckDB Error: {e}")
finally:
    con.close()
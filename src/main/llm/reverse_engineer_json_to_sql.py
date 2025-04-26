import json

# Lae lineage JSON failist või defineeri otse
with open('lineage.json', 'r') as f:
    lineage_data = json.load(f)

# Valmistan andmed
fields = []
selects = []
joins = set()

# Eeltöötle vajadusel
for column, details in lineage_data['lineage'].items():
    transformation = details.get('transformation_logic')
    transformation_type = details.get('transformation_type')
    sources = details.get('sources', [])

    if transformation_type == 'WINDOW_FUNCTION':
        selects.append(f"ROW_NUMBER() OVER () + (SELECT max_sk_customerid FROM MaxSK) + 1 AS {column}")
    elif transformation_type == 'CASE_MAPPING':
        selects.append(f"CASE WHEN {transformation} END AS {column}")
    elif transformation_type == 'JOIN_LOOKUP':
        for source in sources:
            join_info = source.get('join_info')
            if join_info:
                join_type = join_info.get('type', 'LEFT JOIN')
                left = join_info['left_source']['identifier']
                right = join_info['right_source']['identifier']
                condition = join_info.get('join_condition')
                if condition:
                    joins.add((join_type, right.split('.')[0], condition))
                else:
                    joins.add((join_type, right.split('.')[0], f"c.{left.split('.')[-1]} = {right}"))
        selects.append(f"{transformation.split(' via ')[0]} AS {column}")
    else:
        selects.append(f"{transformation} AS {column}")

    fields.append(column)

# Genereeri SQL
with_ctes = """
WITH MaxSK AS (
    SELECT MAX(sk_customerid) AS max_sk_customerid
    FROM wh_db.DimCustomer
),
CustomerData AS (
    SELECT
        c.*,
        -- JOIN result fields
"""

# Lisa CustomerData SELECT
for sel in selects:
    with_ctes += f"        {sel},\n"

with_ctes = with_ctes.rstrip(',\n') + "\n    FROM customers_final c\n"

# Lisa JOIN-id
for join_type, table, condition in joins:
    with_ctes += f"    {join_type} {table} p\n        ON {condition}\n"

with_ctes += ")\n"

# Valmista INSERT
insert_stmt = f"INSERT INTO {lineage_data['target_table']} (\n    {', '.join(fields)}\n)\nSELECT\n    {', '.join(f.split(' AS ')[-1] for f in selects)}\nFROM CustomerData c;"

# Koosta kogu SQL
full_sql = with_ctes + "\n" + insert_stmt

# Salvesta või väljasta
with open('generated_insert.sql', 'w') as f:
    f.write(full_sql)

print("SQL genereeritud edukalt!")

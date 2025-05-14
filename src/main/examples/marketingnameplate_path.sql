WITH RECURSIVE derived_path AS (
  SELECT
    cl.from_column_id,
    cl.to_column_id,
    1 AS depth,
    ARRAY[cl.from_column_id, cl.to_column_id] AS path
  FROM column_lineage cl
  JOIN columns c ON cl.to_column_id = c.column_id
  JOIN tables t ON c.table_id = t.table_id
  JOIN schemas s ON t.schema_id = s.schema_id
  WHERE t.name = 'DimCustomer'
    AND s.name = 'wh_db'
    AND c.name = 'marketingnameplate'

  UNION ALL

  SELECT
    cl2.from_column_id,
    cl2.to_column_id,
    dp.depth + 1,
    dp.path || cl2.from_column_id
  FROM column_lineage cl2
  JOIN derived_path dp ON cl2.to_column_id = dp.from_column_id
)
SELECT DISTINCT * FROM derived_path;



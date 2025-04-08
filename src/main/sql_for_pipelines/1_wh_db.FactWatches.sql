 
INSERT INTO wh_db.FactWatches
WITH watchhistory AS (
    SELECT
        *,
        1 AS batchid
    FROM read_csv(
        'src/data/Batch1\WatchHistory.txt',
        HEADER=FALSE,
        columns={'w_c_id': 'BIGINT', 'w_s_symb': 'VARCHAR', 'w_dts': 'TIMESTAMP', 'w_action': 'VARCHAR'}
    )
),
watches AS (
    SELECT
        wh.w_c_id AS customerid,
        wh.w_s_symb AS symbol,
        MIN(w_dts)::DATE AS dateplaced,
        CASE WHEN w_action = 'CNCL' THEN w_dts ELSE NULL END::DATE AS dateremoved,
        MIN(batchid) AS batchid
    FROM watchhistory wh
    GROUP BY ALL
)
select
  c.sk_customerid sk_customerid,
  s.sk_securityid sk_securityid,
  CAST(strftime(dateplaced, '%Y%m%d') AS BIGINT)  sk_dateid_dateplaced,
  CAST(strftime(dateremoved, '%Y%m%d') AS BIGINT) sk_dateid_dateremoved,
  wh.batchid 
from watches wh
JOIN wh_db.DimSecurity s 
  ON 
    s.symbol = wh.symbol
    AND wh.dateplaced >= s.effectivedate 
    AND wh.dateplaced < s.enddate
JOIN wh_db.DimCustomer c 
  ON
    wh.customerid = c.customerid
    AND wh.dateplaced >= c.effectivedate 
    AND wh.dateplaced < c.enddate;

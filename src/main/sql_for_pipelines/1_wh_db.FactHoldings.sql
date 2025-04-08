 
INSERT INTO wh_db.FactHoldings
WITH Holdings AS (
    SELECT
        *,
        1 AS batchid
    FROM read_csv(
        'src/data/Batch1\HoldingHistory.txt',
        HEADER=FALSE,
        columns={'hh_h_t_id': 'INTEGER', 'hh_t_id': 'INTEGER', 'hh_before_qty': 'INTEGER', 'hh_after_qty': 'INTEGER'}
    )
)
SELECT
  hh_h_t_id tradeid,
  hh_t_id currenttradeid,
  sk_customerid,
  sk_accountid,
  sk_securityid,
  sk_companyid,
  sk_closedateid sk_dateid,
  sk_closetimeid sk_timeid,
  tradeprice currentprice,
  hh_after_qty currentholding,
  h.batchid
FROM Holdings h
  JOIN wh_db.DimTrade dt 
    ON tradeid = hh_t_id;
     
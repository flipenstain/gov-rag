 
INSERT INTO wh_db.FactCashBalances
WITH historical AS (
    SELECT
        accountid,
        ct_dts::DATE AS datevalue,
        SUM(ct_amt) AS account_daily_total,
        1 batchid
    FROM read_csv(
        'src/data/Batch1\CashTransaction.txt',
        HEADER=FALSE,
        columns={'accountid': 'BIGINT', 'ct_dts': 'TIMESTAMP', 'ct_amt': 'DOUBLE', 'ct_name': 'VARCHAR'}
    )
    GROUP BY ALL
  )
SELECT
    a.sk_customerid,
    a.sk_accountid,
    CAST(STRFTIME(datevalue, '%Y%m%d') AS BIGINT) AS sk_dateid,
    SUM(account_daily_total) OVER (PARTITION BY c.accountid ORDER BY datevalue) AS cash,
    c.batchid
FROM historical c
JOIN wh_db.DimAccount a
    ON c.accountid = a.accountid
    AND c.datevalue >= a.effectivedate
    AND c.datevalue < a.enddate;

    
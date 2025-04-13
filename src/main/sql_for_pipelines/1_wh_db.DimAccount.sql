INSERT INTO wh_db.DimAccount 
WITH account AS (
    SELECT
    accountid,
    customerid,
    accountdesc,
    taxstatus,
    brokerid,
    status,
    update_ts,
    1 batchid
from wh_db_stage.CustomerMgmt
where ActionType NOT IN ('UPDCUST', 'INACT')
  ),
account_final AS (
  SELECT
    accountid, -- Kept accountid as it's the partitioning key
    customerid,
    COALESCE(
        accountdesc,
        last_value(accountdesc) OVER w -- Retained  for correct logic
    ) AS accountdesc,
    COALESCE(
        taxstatus,
        last_value(taxstatus)  OVER w -- Retained 
    ) AS taxstatus,
    COALESCE(
        brokerid,
        last_value(brokerid)  OVER w -- Retained 
    ) AS brokerid,
    COALESCE(
        status,
        last_value(status)  OVER w -- Retained 
    ) AS status,
    batchid,
    CASE
        WHEN lead(update_ts) OVER w IS NULL THEN 'Y' -- Check if it's the last record in the partition
        ELSE 'N'
    END AS iscurrent,
    update_ts::DATE AS effectivedate, -- Using target format's casting style
    COALESCE(
        lead(update_ts::DATE) OVER w, -- Get next record's date within the partition
        '9999-12-31'::DATE           -- Default for the last record
    ) AS enddate
FROM
    account a -- Using the original table name
WINDOW w AS (
    PARTITION BY accountid -- Partitioning by the key from the original query
    ORDER BY update_ts     -- Ordering by the timestamp from the original query
 )
),
  account_cust_updates AS (
  SELECT
    a.* EXCLUDE (effectivedate, enddate, customerid),
    c.sk_customerid,
    if(
      a.effectivedate < c.effectivedate,
      c.effectivedate,
      a.effectivedate
    ) effectivedate,
    if(a.enddate > c.enddate, c.enddate, a.enddate) enddate
  FROM account_final a
  FULL OUTER JOIN wh_db.DimCustomer c 
    ON a.customerid = c.customerid
    AND c.enddate > a.effectivedate
    AND c.effectivedate < a.enddate
  WHERE a.effectivedate < a.enddate
)
SELECT
    CAST(strftime(a.effectivedate, '%Y%m%d') || a.accountid AS BIGINT),
    a.accountid,
    b.sk_brokerid, 
    a.sk_customerid,
    a.accountdesc,
    a.TaxStatus,
    a.status,
    -- Using standard CASE instead of IF() for iscurrent calculation
    CASE
        WHEN a.enddate = '9999-12-31'::DATE THEN true
        ELSE false
    END AS iscurrent,
    a.batchid,
    a.effectivedate,
    a.enddate
FROM
    account_cust_updates a
JOIN
    wh_db.DimBroker b ON a.brokerid = b.brokerid;

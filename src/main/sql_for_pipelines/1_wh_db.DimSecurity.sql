 
INSERT INTO wh_db.DimSecurity
WITH SEC AS (
    SELECT
        recdate AS effectivedate,
        TRIM(SUBSTR(value, 1, 15)) AS Symbol,
        TRIM(SUBSTR(value, 16, 6)) AS issue,
        TRIM(SUBSTR(value, 22, 4)) AS Status,
        TRIM(SUBSTR(value, 26, 70)) AS Name,
        TRIM(SUBSTR(value, 96, 6)) AS exchangeid,
        TRY_CAST(SUBSTR(value, 102, 13) AS BIGINT) AS sharesoutstanding,
        TRY_CAST(STRPTIME(SUBSTR(value, 115, 8), '%Y%m%d') AS DATE) AS firsttrade,
        TRY_CAST(STRPTIME(SUBSTR(value, 123, 8), '%Y%m%d') AS DATE) AS firsttradeonexchange,
        TRY_CAST(SUBSTR(value, 131, 12) AS DOUBLE) AS Dividend,
        TRIM(CASE WHEN  regexp_matches(SUBSTR(value, -10), '^[0-9]+$') THEN  REGEXP_REPLACE(SUBSTR(value, -10), '^0+', '')
            ELSE SUBSTR(value, -60)
        end) as conameorcik
    FROM wh_db_stage.FinWire
    WHERE rectype = 'SEC'
),
dc AS (
    SELECT
        sk_companyid,
        name AS conameorcik,
        EffectiveDate,
        EndDate
    FROM wh_db.DimCompany
    UNION ALL
    SELECT
        sk_companyid,
        CAST(companyid AS VARCHAR) AS conameorcik,
        EffectiveDate,
        EndDate
    FROM wh_db.DimCompany
),
SEC_prep AS (
    SELECT
        SEC.* EXCLUDE (Status, conameorcik),
        COALESCE(TRY_CAST(conameorcik AS BIGINT)::VARCHAR, conameorcik) AS conameorcik,
        CASE 
            WHEN status = 'ACTV' THEN 'Active'
            WHEN status = 'CMPT' THEN 'Completed'
            WHEN status = 'CNCL' THEN 'Canceled'
            WHEN status = 'PNDG' THEN 'Pending'
            WHEN status = 'SBMT' THEN 'Submitted'
            WHEN status = 'INAC' THEN 'Inactive'
            ELSE NULL -- Or handle other cases
        END AS status,
        COALESCE(
            LEAD(effectivedate) OVER (PARTITION BY Symbol ORDER BY effectivedate),
            ('9999-12-31')::DATE
        ) AS enddate
    FROM SEC
),
        SEC_final AS (
    SELECT
        SEC.Symbol,
        SEC.issue,
        SEC.status,
        SEC.Name,
        SEC.exchangeid,
        dc.sk_companyid,
        SEC.sharesoutstanding,
        SEC.firsttrade,
        SEC.firsttradeonexchange,
        SEC.Dividend,
        CASE WHEN SEC.effectivedate < dc.EffectiveDate THEN dc.EffectiveDate ELSE SEC.effectivedate END AS effectivedate,
        CASE WHEN SEC.enddate > dc.EndDate THEN dc.EndDate ELSE SEC.enddate END AS enddate
    FROM SEC_prep SEC
    JOIN dc
        ON SEC.conameorcik = dc.conameorcik
        AND SEC.effectivedate < dc.EndDate
        AND SEC.enddate > dc.EffectiveDate
)
SELECT
    ROW_NUMBER() OVER () AS sk_securityid,
    Symbol,
    issue,
    status,
    Name,
    exchangeid,
    sk_companyid,
    sharesoutstanding,
    firsttrade,
    firsttradeonexchange,
    Dividend,
    CASE WHEN enddate = '9999-12-31'::DATE THEN TRUE ELSE FALSE END AS iscurrent,
    1 AS batchid,
    effectivedate,
    enddate
FROM SEC_final
WHERE effectivedate < enddate;



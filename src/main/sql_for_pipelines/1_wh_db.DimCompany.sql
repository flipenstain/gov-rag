INSERT INTO wh_db.DimCompany
WITH cmp AS (
    SELECT
        recdate,
        TRIM(SUBSTR(value, 1, 60)) AS CompanyName,
        TRIM(SUBSTR(value, 61, 10)) AS CIK,
        TRIM(SUBSTR(value, 71, 4)) AS Status,
        TRIM(SUBSTR(value, 75, 2)) AS IndustryID,
        TRIM(SUBSTR(value, 77, 4)) AS SPrating,
        TRY_CAST(TRY_CAST(SUBSTRING(value, 81, 8) AS TIMESTAMP) AS DATE) AS FoundingDate,
        TRIM(SUBSTR(value, 89, 80)) AS AddrLine1,
        TRIM(SUBSTR(value, 169, 80)) AS AddrLine2,
        TRIM(SUBSTR(value, 249, 12)) AS PostalCode,
        TRIM(SUBSTR(value, 261, 25)) AS City,
        TRIM(SUBSTR(value, 286, 20)) AS StateProvince,
        TRIM(SUBSTR(value, 306, 24)) AS Country,
        TRIM(SUBSTR(value, 330, 46)) AS CEOname,
        TRIM(SUBSTR(value, 376, 150)) AS Description
    FROM wh_db_stage.FinWire
    WHERE rectype = 'CMP'
)
SELECT
    CAST(strftime(effectivedate, '%Y%m%d') || companyid AS BIGINT) AS sk_companyid,
    companyid,
    status,
    name,
    industry,
    sprating,
    islowgrade,
    ceo,
    addressline1,
    addressline2,
    postalcode,
    city,
    stateprov,
    country,
    description,
    foundingdate,
    CASE WHEN enddate = '9999-12-31'::DATE THEN TRUE ELSE FALSE END AS iscurrent,
    batchid,
    effectivedate,
    enddate
FROM (
    SELECT
        CAST(cik AS BIGINT) AS companyid,
        CASE cmp.status
            WHEN 'ACTV' THEN 'Active'
            WHEN 'CMPT' THEN 'Completed'
            WHEN 'CNCL' THEN 'Canceled'
            WHEN 'PNDG' THEN 'Pending'
            WHEN 'SBMT' THEN 'Submitted'
            WHEN 'INAC' THEN 'Inactive'
            ELSE NULL -- or a default value, if needed
        END AS status,
        CompanyName AS name,
        ind.in_name AS industry,
        CASE
            WHEN SPrating IN ('AAA', 'AA', 'AA+', 'AA-', 'A', 'A+', 'A-', 'BBB', 'BBB+', 'BBB-', 'BB', 'BB+', 'BB-', 'B', 'B+', 'B-', 'CCC', 'CCC+', 'CCC-', 'CC', 'C', 'D') THEN SPrating
            ELSE NULL::VARCHAR
        END AS sprating,
        CASE
            WHEN SPrating IN ('AAA', 'AA', 'A', 'AA+', 'A+', 'AA-', 'A-', 'BBB', 'BBB+', 'BBB-') THEN FALSE
            WHEN SPrating IN ('BB', 'B', 'CCC', 'CC', 'C', 'D', 'BB+', 'B+', 'CCC+', 'BB-', 'B-', 'CCC-') THEN TRUE
            ELSE NULL::BOOLEAN
        END AS islowgrade,
        CEOname AS ceo,
        AddrLine1 AS addressline1,
        AddrLine2 AS addressline2,
        PostalCode AS postalcode,
        City AS city,
        StateProvince AS stateprov,
        Country AS country,
        Description AS description,
        FoundingDate AS foundingdate,
        1 AS batchid,
        recdate AS effectivedate,
        COALESCE(
            LEAD(try_cast(recdate AS DATE)) OVER (PARTITION BY cik ORDER BY recdate),
            try_cast('9999-12-31' AS DATE)
        ) AS enddate
    FROM cmp
    JOIN wh_db.industry ind ON cmp.industryid = ind.in_id
)
WHERE effectivedate < enddate;

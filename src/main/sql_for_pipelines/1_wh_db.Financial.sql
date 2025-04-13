INSERT INTO wh_db.Financial
WITH finwire_parsed AS (
    -- Parse the fixed-width data and extract relevant fields including conditional id/cname
    SELECT
        CAST(SUBSTRING(value FROM 1 FOR 4) AS INTEGER) AS fi_year,
        CAST(SUBSTRING(value FROM 5 FOR 1) AS INTEGER) AS fi_qtr,
        -- Explicitly cast strptime result to DATE if effectivedate/enddate are DATEs
        strptime(SUBSTRING(value FROM 6 FOR 8), '%Y%m%d')::DATE AS fi_qtr_start_date,
        CAST(SUBSTRING(value FROM 22 FOR 17) AS DOUBLE) AS fi_revenue,
        CAST(SUBSTRING(value FROM 39 FOR 17) AS DOUBLE) AS fi_net_earn,
        CAST(SUBSTRING(value FROM 56 FOR 12) AS DOUBLE) AS fi_basic_eps,
        CAST(SUBSTRING(value FROM 68 FOR 12) AS DOUBLE) AS fi_dilut_eps,
        CAST(SUBSTRING(value FROM 80 FOR 12) AS DOUBLE) AS fi_margin,
        CAST(SUBSTRING(value FROM 92 FOR 17) AS DOUBLE) AS fi_inventory,
        CAST(SUBSTRING(value FROM 109 FOR 17) AS DOUBLE) AS fi_assets,
        CAST(SUBSTRING(value FROM 126 FOR 17) AS DOUBLE) AS fi_liability,
        CAST(SUBSTRING(value FROM 143 FOR 13) AS BIGINT) AS fi_out_basic,
        CAST(SUBSTRING(value FROM 156 FOR 13) AS BIGINT) AS fi_out_dilut,
        -- Conditionally extract ID (as TEXT) or Name
        -- Ensure id and cname columns are mutually exclusive (one is NULL if other is not)
        CASE
            WHEN regexp_matches(SUBSTRING(value FROM -10), '^[0-9]+$')
            -- Remove leading zeros and trim; result is TEXT
            THEN trim(regexp_replace(SUBSTRING(value FROM -10), '^0+', ''))
            ELSE NULL
        END AS company_id_text,
        CASE
            WHEN NOT regexp_matches(SUBSTRING(value FROM -10), '^[0-9]+$')
            THEN trim(SUBSTRING(value FROM -60))
            ELSE NULL
        END AS company_name,
        -- Keep recdate for joining condition
        recdate
    FROM wh_db_stage.FinWire
    -- Filter record types early in the CTE
    WHERE rectype IN ('FIN_COMPANYID', 'FIN_NAME')
)
SELECT
    -- Select the SK from the successfully joined DimCompany record
    dc.sk_companyid,
    -- Select all parsed financial columns, excluding intermediate fields
    fp.* EXCLUDE (company_id_text, company_name, recdate)
FROM finwire_parsed AS fp
-- Use a single INNER JOIN (or LEFT JOIN if you need un-matched finwire rows)
JOIN wh_db.DimCompany AS dc
    -- Apply the date range filter first (can help with partitioning/pruning)
    ON fp.recdate >= dc.effectivedate
   AND fp.recdate < dc.enddate
    -- Apply the conditional join logic: match EITHER id OR name
   AND (
        (fp.company_id_text IS NOT NULL AND fp.company_id_text = dc.companyid::TEXT) -- Cast dc.companyid to TEXT if needed
        OR
        (fp.company_name IS NOT NULL AND fp.company_name = dc.name)
   );

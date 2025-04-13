     INSERT INTO wh_db_stage.FinWire
    SELECT
        CASE
            WHEN SUBSTR(column0, 16, 3) = 'FIN' THEN
                CASE
                    WHEN TRY_CAST(TRIM(SUBSTR(column0, 187, 60)) AS BIGINT) IS NOT NULL THEN 'FIN_COMPANYID'
                    ELSE 'FIN_NAME'
                END
            ELSE SUBSTR(column0, 16, 3)
        END AS rectype,
        STRPTIME(SUBSTR(column0, 1, 8), '%Y%m%d') AS recdate,
        SUBSTR(column0, 19) AS value
    FROM read_csv_auto('src/data/Batch1\FINWIRE1967Q2', HEADER=FALSE, filename=false, all_varchar=true)
    
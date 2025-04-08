CREATE OR REPLACE TEMP TABLE customers_final AS
SELECT
    customerid,
    COALESCE(taxid, last_value(taxid ORDER BY update_ts DESC) OVER w) AS taxid,
    status,
    COALESCE(lastname, last_value(lastname ORDER BY update_ts DESC) OVER w) AS lastname,
    COALESCE(firstname, last_value(firstname ORDER BY update_ts DESC) OVER w) AS firstname,
    COALESCE(middleinitial, last_value(middleinitial ORDER BY update_ts DESC) OVER w) AS middleinitial,
    COALESCE(gender, last_value(gender ORDER BY update_ts DESC) OVER w) AS gender,
    COALESCE(tier, last_value(tier ORDER BY update_ts DESC) OVER w) AS tier,
    COALESCE(dob, last_value(dob ORDER BY update_ts DESC) OVER w) AS dob,
    COALESCE(addressline1, last_value(addressline1 ORDER BY update_ts DESC) OVER w) AS addressline1,
    COALESCE(addressline2, last_value(addressline2 ORDER BY update_ts DESC) OVER w) AS addressline2,
    COALESCE(postalcode, last_value(postalcode ORDER BY update_ts DESC) OVER w) AS postalcode,
    COALESCE(CITY, last_value(CITY ORDER BY update_ts DESC) OVER w) AS CITY,
    COALESCE(stateprov, last_value(stateprov ORDER BY update_ts DESC) OVER w) AS stateprov,
    COALESCE(country, last_value(country ORDER BY update_ts DESC) OVER w) AS country,
    COALESCE(phone1, last_value(phone1 ORDER BY update_ts DESC) OVER w) AS phone1,
    COALESCE(phone2, last_value(phone2 ORDER BY update_ts DESC) OVER w) AS phone2,
    COALESCE(phone3, last_value(phone3 ORDER BY update_ts DESC) OVER w) AS phone3,
    COALESCE(email1, last_value(email1 ORDER BY update_ts DESC) OVER w) AS email1,
    COALESCE(email2, last_value(email2 ORDER BY update_ts DESC) OVER w) AS email2,
    COALESCE(LCL_TX_ID, last_value(LCL_TX_ID ORDER BY update_ts DESC) OVER w) AS LCL_TX_ID,
    COALESCE(NAT_TX_ID, last_value(NAT_TX_ID ORDER BY update_ts DESC) OVER w) AS NAT_TX_ID,
    batchid,
    CASE 
        WHEN NULLIF(lead(update_ts) OVER w, NULL) IS NULL THEN 'Y' 
        ELSE 'N' 
    END AS iscurrent,
    update_ts::DATE AS effectivedate,
    COALESCE(lead(update_ts::DATE) OVER w, '9999-12-31'::DATE) AS enddate
FROM
    customers
WINDOW w AS (PARTITION BY customerid ORDER BY update_ts);
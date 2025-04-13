INSERT INTO wh_db.DimCustomer (
    sk_customerid,
    customerid,
    taxid,
    status,
    lastname,
    firstname,
    middleinitial,
    gender,
    tier,
    dob,
    addressline1,
    addressline2,
    postalcode,
    city,
    stateprov,
    country,
    phone1,
    phone2,
    phone3,
    email1,
    email2,
    nationaltaxratedesc,
    nationaltaxrate,
    localtaxratedesc,
    localtaxrate,
    agencyid,
    creditrating,
    networth,
    marketingnameplate,
    iscurrent,
    batchid,
    effectivedate,
    enddate
)
WITH MaxSK AS (
    SELECT COALESCE(MAX(sk_customerid), 0) AS max_sk_customerid
    FROM wh_db.DimCustomer
),
CustomerData AS (
    SELECT 
        c.customerid,
        c.taxid,
        c.status,
        c.lastname,
        c.firstname,
        c.middleinitial,
        c.gender,
        c.tier,
        c.dob,
        c.addressline1,
        c.addressline2,
        c.postalcode,
        c.city,
        c.stateprov,
        c.country,
        c.phone1,
        c.phone2,
        c.phone3,
        c.email1, 
        c.email2,
        r_nat.TX_NAME as nationaltaxratedesc,
        r_nat.TX_RATE as nationaltaxrate,
        r_lcl.TX_NAME as localtaxratedesc,
        r_lcl.TX_RATE as localtaxrate,
        p.agencyid,
        p.creditrating,
        p.networth,
        p.marketingnameplate,
        c.iscurrent,
        c.batchid,
        c.effectivedate,
        c.enddate 
    FROM customers_final c
    JOIN wh_db.TaxRate r_lcl 
        ON c.lcl_tx_id = r_lcl.TX_ID
    JOIN wh_db.TaxRate r_nat 
        ON c.nat_tx_id = r_nat.TX_ID
    LEFT JOIN wh_db_stage.ProspectIncremental p 
        ON 
            UPPER(p.lastname) = UPPER(c.lastname)
            AND UPPER(p.firstname) = UPPER(c.firstname)
            AND UPPER(p.addressline1) = UPPER(c.addressline1)
            AND UPPER(NULLIF(p.addressline2, '')) = UPPER(NULLIF(c.addressline2, ''))
            AND UPPER(p.postalcode) = UPPER(c.postalcode)
    WHERE c.effectivedate < c.enddate
)
SELECT 
    ROW_NUMBER() OVER () + (SELECT max_sk_customerid FROM MaxSK) + 1 AS sk_customerid,
    c.customerid,
    c.taxid,
    c.status,
    c.lastname,
    c.firstname,
    c.middleinitial,
    IF(c.gender IN ('M', 'F'), c.gender, 'U') AS gender,
    c.tier,
    c.dob,
    c.addressline1,
    c.addressline2,
    c.postalcode,
    c.city,
    c.stateprov,
    c.country,
    c.phone1,
    c.phone2,
    c.phone3,
    c.email1, 
    c.email2,
    nationaltaxratedesc,
    nationaltaxrate,
    localtaxratedesc,
    localtaxrate,
    agencyid,
    creditrating,
    networth,
    marketingnameplate,
    iscurrent,
    batchid,
    effectivedate,
    enddate 
FROM CustomerData c;
 
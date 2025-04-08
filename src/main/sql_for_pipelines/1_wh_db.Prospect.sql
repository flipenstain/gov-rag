
INSERT INTO wh_db.Prospect
WITH cust AS (
    SELECT
        lastname,
        firstname,
        addressline1,
        addressline2,
        postalcode
    FROM wh_db.DimCustomer
    WHERE iscurrent = 'Y'
)
SELECT
    p.agencyid,
    CAST(STRFTIME(recdate.batchdate, '%Y%m%d') AS BIGINT) AS sk_recorddateid,
    CAST(STRFTIME(origdate.batchdate, '%Y%m%d') AS BIGINT) AS sk_updatedateid,
    p.batchid,
    CASE WHEN c.LastName IS NOT NULL THEN TRUE ELSE FALSE END AS iscustomer,
    p.lastname,
    p.firstname,
    p.middleinitial,
    p.gender,
    p.addressline1,
    p.addressline2,
    p.postalcode,
    city,
    state,
    country,
    phone,
    income,
    numbercars,
    numberchildren,
    maritalstatus,
    age,
    creditrating,
    ownorrentflag,
    employer,
    numbercreditcards,
    networth,
    marketingnameplate
FROM wh_db_stage.ProspectIncremental p
JOIN wh_db.BatchDate recdate
    ON p.recordbatchid = recdate.batchid
JOIN wh_db.BatchDate origdate
    ON p.batchid = origdate.batchid
LEFT JOIN cust c
    ON
         UPPER(p.LastName) = UPPER(c.lastname)
        AND UPPER(p.FirstName) = UPPER(c.firstname)
        AND UPPER(p.AddressLine1) = UPPER(c.addressline1)
        AND UPPER(COALESCE(p.addressline2, '')) = UPPER(COALESCE(c.addressline2, ''))
        AND UPPER(p.PostalCode) = UPPER(c.postalcode)
         ;

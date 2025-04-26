CREATE OR REPLACE TEMP TABLE customers AS
  SELECT
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
    lcl_tx_id,
    nat_tx_id,
    1 batchid,
    update_ts
  FROM
    wh_db_stage.CustomerMgmt c
  WHERE
    ActionType in ('NEW', 'INACT', 'UPDCUST')
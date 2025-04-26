CREATE OR REPLACE TABLE wh_db_stage.CustomerMgmt  AS  
        SELECT
        try_cast(C_ID as BIGINT) customerid,
        try_cast(CA_ID as BIGINT) accountid,
        try_cast(CA_B_ID as BIGINT) brokerid,
        nullif(C_TAX_ID, '') taxid,
        nullif(CA_NAME, '') accountdesc,
        try_cast(CA_TAX_ST as TINYINT) taxstatus,
        CASE
            WHEN ActionType IN ('NEW', 'ADDACCT', 'UPDACCT', 'UPDCUST') THEN 'Active'
            WHEN ActionType IN ('CLOSEACCT', 'INACT') THEN 'Inactive'
            ELSE NULL
        END AS status,
        nullif(C_L_NAME, '') lastname,
        nullif(C_F_NAME, '') firstname,
        nullif(C_M_NAME, '') middleinitial,
        nullif(upper(C_GNDR), '') gender,
        try_cast(C_TIER as TINYINT) tier,
        try_cast(C_DOB as DATE) dob,
        nullif(C_ADLINE1, '') addressline1,
        nullif(C_ADLINE2, '') addressline2,
        nullif(C_ZIPCODE, '') postalcode,
        nullif(C_CITY, '') city,
        nullif(C_STATE_PROV, '') stateprov,
        nullif(C_CTRY, '') country,
        CASE
            WHEN nullif(C_LOCAL, '') IS NOT NULL THEN
                concat(
                    CASE WHEN nullif(C_CTRY_CODE, '') IS NOT NULL THEN '+' || C_CTRY_CODE || ' ' ELSE '' END,
                    CASE WHEN nullif(C_AREA_CODE, '') IS NOT NULL THEN '(' || C_AREA_CODE || ') ' ELSE '' END,
                    C_LOCAL,
                    COALESCE(C_EXT, '')
                )
            ELSE NULL
        END AS phone1,
        CASE
            WHEN nullif(C_LOCAL_1, '') IS NOT NULL THEN
                concat(
                    CASE WHEN nullif(C_CTRY_CODE_1, '') IS NOT NULL THEN '+' || C_CTRY_CODE_1 || ' ' ELSE '' END,
                    CASE WHEN nullif(C_AREA_CODE_1, '') IS NOT NULL THEN '(' || C_AREA_CODE_1 || ') ' ELSE '' END,
                    C_LOCAL_1,
                    COALESCE(C_EXT_1, '')
                )
            ELSE NULL
        END AS phone2,
        CASE
            WHEN nullif(C_LOCAL_2, '') IS NOT NULL THEN
                concat(
                    CASE WHEN nullif(C_CTRY_CODE_2, '') IS NOT NULL THEN '+' || C_CTRY_CODE_2 || ' ' ELSE '' END,
                    CASE WHEN nullif(C_AREA_CODE_2, '') IS NOT NULL THEN '(' || C_AREA_CODE_2 || ') ' ELSE '' END,
                    C_LOCAL_2,
                    COALESCE(C_EXT_2, '')
                )
            ELSE NULL
        END AS phone3,
        nullif(C_PRIM_EMAIL, '') email1,
        nullif(C_ALT_EMAIL, '') email2,
        nullif(C_LCL_TX_ID, '') lcl_tx_id,
        nullif(C_NAT_TX_ID, '') nat_tx_id,
        try_cast(ActionTS as TIMESTAMP) update_ts,
        ActionType
           
            FROM xml_dataframe
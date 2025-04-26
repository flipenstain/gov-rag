INSERT INTO wh_db.DimBroker
SELECT
    employeeid sk_brokerid,
    employeeid brokerid,
    managerid,
    employeefirstname firstname,
    employeelastname lastname,
    employeemi middleinitial,
    employeebranch branch,
    employeeoffice office,
    employeephone phone,
    true iscurrent,
    1 batchid, --temp, later read from db
    (SELECT min(datevalue::DATE) as effectivedate FROM wh_db.DimDate) effectivedate,
    '9999-12-31'::DATE enddate
FROM temp_broker;
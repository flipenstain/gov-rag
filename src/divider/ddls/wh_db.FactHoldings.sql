 

 CREATE TABLE IF NOT EXISTS wh_db.FactHoldings (
   tradeid INT,
   currenttradeid INT NOT NULL,
   sk_customerid BIGINT,
   sk_accountid BIGINT,
   sk_securityid BIGINT,
   sk_companyid BIGINT,
   sk_dateid BIGINT,
   sk_timeid BIGINT,
   currentprice DOUBLE,
   currentholding INT,
   batchid INT)
 

 

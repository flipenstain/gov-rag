 

 CREATE TABLE IF NOT EXISTS wh_db.FactMarketHistory (
   sk_securityid BIGINT NOT NULL,
   sk_companyid BIGINT,
   sk_dateid BIGINT NOT NULL,
   peratio DOUBLE,
   yield DOUBLE,
   fiftytwoweekhigh DOUBLE,
   sk_fiftytwoweekhighdate BIGINT,
   fiftytwoweeklow DOUBLE,
   sk_fiftytwoweeklowdate BIGINT,
   closeprice DOUBLE,
   dayhigh DOUBLE,
   daylow DOUBLE,
   volume INT,
   batchid INT)
 

 

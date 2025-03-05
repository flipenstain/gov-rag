 

 CREATE TABLE IF NOT EXISTS wh_db.DimTrade (
   tradeid INT NOT NULL,
   sk_brokerid BIGINT,
   sk_createdateid BIGINT,
   sk_createtimeid BIGINT,
   sk_closedateid BIGINT,
   sk_closetimeid BIGINT,
   status STRING,
   type STRING,
   cashflag BOOLEAN,
   sk_securityid BIGINT,
   sk_companyid BIGINT,
   quantity INT,
   bidprice DOUBLE,
   sk_customerid BIGINT,
   sk_accountid BIGINT,
   executedby STRING,
   tradeprice DOUBLE,
   fee DOUBLE,
   commission DOUBLE,
   tax DOUBLE,
   batchid INT) 
 

 

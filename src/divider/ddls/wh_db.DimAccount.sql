 

 CREATE TABLE IF NOT EXISTS wh_db.DimAccount (
   sk_accountid BIGINT NOT NULL,
   accountid BIGINT,
   sk_brokerid BIGINT,
   sk_customerid BIGINT,
   accountdesc STRING,
   taxstatus TINYINT,
   status STRING,
   iscurrent BOOLEAN,
   batchid INT,
   effectivedate DATE,
   enddate DATE) 
 

 

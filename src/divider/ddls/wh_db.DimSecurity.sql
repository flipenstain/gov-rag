 

 CREATE TABLE IF NOT EXISTS wh_db.DimSecurity (
   sk_securityid BIGINT NOT NULL,
   symbol STRING,
   issue STRING,
   status STRING,
   name STRING,
   exchangeid STRING,
   sk_companyid BIGINT,
   sharesoutstanding BIGINT,
   firsttrade DATE,
   firsttradeonexchange DATE,
   dividend DOUBLE,
   iscurrent BOOLEAN,
   batchid INT,
   effectivedate DATE,
   enddate DATE) 
 

 

 

 CREATE TABLE IF NOT EXISTS wh_db.DimCompany (
   sk_companyid BIGINT NOT NULL,
   companyid BIGINT,
   status STRING,
   name STRING,
   industry STRING,
   sprating STRING,
   islowgrade BOOLEAN,
   ceo STRING,
   addressline1 STRING,
   addressline2 STRING,
   postalcode STRING,
   city STRING,
   stateprov STRING,
   country STRING,
   description STRING,
   foundingdate DATE,
   iscurrent BOOLEAN,
   batchid INT,
   effectivedate DATE,
   enddate DATE)
 

 

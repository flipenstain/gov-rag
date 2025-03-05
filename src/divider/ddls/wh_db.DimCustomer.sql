 

 CREATE TABLE IF NOT EXISTS wh_db.DimCustomer (
   sk_customerid BIGINT NOT NULL,
   customerid BIGINT,
   taxid STRING,
   status STRING,
   lastname STRING,
   firstname STRING,
   middleinitial STRING,
   gender STRING,
   tier TINYINT,
   dob DATE,
   addressline1 STRING,
   addressline2 STRING,
   postalcode STRING,
   city STRING,
   stateprov STRING,
   country STRING,
   phone1 STRING,
   phone2 STRING,
   phone3 STRING,
   email1 STRING,
   email2 STRING,
   nationaltaxratedesc STRING,
   nationaltaxrate FLOAT,
   localtaxratedesc STRING,
   localtaxrate FLOAT,
   agencyid STRING,
   creditrating INT,
   networth INT,
   marketingnameplate STRING,
   iscurrent BOOLEAN,
   batchid INT,
   effectivedate DATE,
   enddate DATE) 
 

 

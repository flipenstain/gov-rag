 

 CREATE TABLE IF NOT EXISTS wh_db.DimBroker (
   sk_brokerid BIGINT NOT NULL,
   brokerid BIGINT,
   managerid BIGINT,
   firstname STRING,
   lastname STRING,
   middleinitial STRING,
   branch STRING,
   office STRING,
   phone STRING,
   iscurrent BOOLEAN,
   batchid INT,
   effectivedate DATE,
   enddate DATE)
 

 

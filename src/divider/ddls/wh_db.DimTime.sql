 

 CREATE TABLE IF NOT EXISTS wh_db.DimTime (
   sk_timeid BIGINT NOT NULL,
   timevalue STRING,
   hourid INT,
   hourdesc STRING,
   minuteid INT,
   minutedesc STRING,
   secondid INT,
   seconddesc STRING,
   markethoursflag BOOLEAN,
   officehoursflag BOOLEAN)
 

 

 

 CREATE TABLE IF NOT EXISTS wh_db.DimDate (
   sk_dateid BIGINT NOT NULL,
   datevalue DATE,
   datedesc STRING,
   calendaryearid INT,
   calendaryeardesc STRING,
   calendarqtrid INT,
   calendarqtrdesc STRING,
   calendarmonthid INT,
   calendarmonthdesc STRING,
   calendarweekid INT,
   calendarweekdesc STRING,
   dayofweeknum INT,
   dayofweekdesc STRING,
   fiscalyearid INT,
   fiscalyeardesc STRING,
   fiscalqtrid INT,
   fiscalqtrdesc STRING,
   holidayflag BOOLEAN)
 

 

 

 CREATE TABLE IF NOT EXISTS wh_db.Financial (
   sk_companyid BIGINT NOT NULL,
   fi_year INT NOT NULL,
   fi_qtr INT NOT NULL,
   fi_qtr_start_date DATE,
   fi_revenue DOUBLE,
   fi_net_earn DOUBLE,
   fi_basic_eps DOUBLE,
   fi_dilut_eps DOUBLE,
   fi_margin DOUBLE,
   fi_inventory DOUBLE,
   fi_assets DOUBLE,
   fi_liability DOUBLE,
   fi_out_basic BIGINT,
   fi_out_dilut BIGINT)
 

 

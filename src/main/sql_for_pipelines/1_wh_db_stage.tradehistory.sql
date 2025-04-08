 

CREATE OR REPLACE TABLE wh_db_stage.tradetxt AS 
SELECT * from  read_csv('src/data/Batch1\Trade.txt', HEADER=FALSE, filename=false, all_varchar=true, delim='|',strict_mode=false , columns = {
    "tradeid": "BIGINT", 
    "t_dts": "TIMESTAMP",
    "status": "STRING", 
    "t_tt_id": "STRING",
    "cashflag": "TINYINT",
    "t_s_symb": "STRING",
    "quantity": "INT",
    "bidprice": "DOUBLE",
    "t_ca_id": "BIGINT",
    "executedby": "STRING",
    "tradeprice": "DOUBLE",
    "fee": "DOUBLE",
    "commission": "DOUBLE",
    "tax": "DOUBLE"});


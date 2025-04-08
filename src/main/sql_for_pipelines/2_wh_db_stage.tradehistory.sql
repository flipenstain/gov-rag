 
CREATE OR REPLACE TABLE wh_db_stage.tradehistory AS 
select * FROM read_csv_auto('src/data/Batch1\TradeHistory.txt', HEADER=FALSE, filename=false, all_varchar=true, columns = {
    "tradeid": "BIGINT", --TH_T_ID
    "th_dts": "TIMESTAMP", --TH_DTS
    "status": "STRING"}); --TH_ST_ID


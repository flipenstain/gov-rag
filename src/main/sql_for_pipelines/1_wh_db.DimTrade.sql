INSERT INTO wh_db.DimTrade
WITH tradehistorical AS (
select 
    a.tradeid,
    --brokerid
    CASE
    		WHEN b.status = 'SBMT' AND a.t_tt_id IN ( 'TMB', 'TMS' ) OR b.status = 'PNDG' THEN b.TH_DTS
			WHEN b.status IN ( 'CMPT', 'CNCL' ) THEN NULL
		END AS SK_CreateDateID
		, CASE 
			WHEN b.status = 'SBMT' AND a.t_tt_id IN ( 'TMB', 'TMS' ) OR b.status = 'PNDG' THEN b.TH_DTS
			WHEN b.status IN ( 'CMPT', 'CNCL' ) THEN NULL
		END AS SK_CreateTimeID
		, CASE 
			WHEN b.status = 'SBMT' AND a.t_tt_id IN ( 'TMB', 'TMS' ) OR b.status = 'PNDG' THEN NULL
			WHEN b.status IN ( 'CMPT', 'CNCL' ) THEN b.TH_DTS
		END AS SK_CloseDateID
		, CASE 
			WHEN b.status = 'SBMT' AND a.t_tt_id IN ( 'TMB', 'TMS' ) OR b.status = 'PNDG' THEN NULL
			WHEN b.status IN ( 'CMPT', 'CNCL' ) THEN b.TH_DTS
    END AS SK_CloseTimeID,
    c.st_name,
    CASE t_tt_id
      WHEN 'TMB' THEN 'Market Buy'
      WHEN 'TMS' THEN 'Market Sell'
      WHEN 'TSL' THEN 'Stop Loss'
      WHEN 'TLS' THEN 'Limit Sell'
      WHEN 'TLB' THEN 'Limit Buy'
    ELSE NULL -- Or some default value if needed
END AS type,
  a.cashflag,
  --ds.sk_securityid
  --ds.sk_companyid
  a.quantity,
  a.bidprice,
   -- sk_customerid
  -- sk_accountid
  a.executedby,
  a.tradeprice,
  a.fee,
  a.commission,
  a.tax,
  1 batchid,
  a.t_s_symb,
  a.t_ca_id
  from wh_db_stage.tradetxt a
  join wh_db_stage.tradehistory b
on a.tradeid = b.tradeid
  join wh_db.StatusType c -- ainult �ks praegu
    ON a.status = c.st_id
)
select 
  trade.tradeid
  ,da.sk_brokerid
  ,CAST(strftime(trade.SK_CreateDateID, '%Y%m%d') || da.accountid || da.sk_brokerid AS BIGINT)
  ,CAST(strftime(trade.SK_CreateTimeID, '%Y%m%d') || da.accountid || da.sk_brokerid AS BIGINT)
  ,CAST(strftime(trade.SK_CloseDateID, '%Y%m%d') || da.accountid || da.sk_brokerid AS BIGINT)
  ,CAST(strftime(trade.SK_CloseTimeID, '%Y%m%d') || da.accountid || da.sk_brokerid AS BIGINT)
  ,trade.st_name
  ,trade.type
  ,trade.cashflag
  ,ds.sk_securityid
  ,ds.sk_companyid
  ,trade.quantity
  ,trade.bidprice
  ,da.sk_customerid
  ,da.sk_accountid
  ,trade.executedby
  ,trade.tradeprice
  ,trade.fee
  ,trade.commission
  ,trade.tax
  ,1 batchid
  
from tradehistorical trade
JOIN wh_db.DimSecurity ds
  ON 
    ds.symbol = trade.t_s_symb
    AND SK_CreateDateID::DATE >= ds.effectivedate 
    AND SK_CreateDateID::DATE < ds.enddate
JOIN wh_db.DimAccount da
  ON 
    trade.t_ca_id = da.accountid 
    AND SK_CreateDateID::DATE >= da.effectivedate 
    AND SK_CreateDateID::DATE < da.enddate;
        
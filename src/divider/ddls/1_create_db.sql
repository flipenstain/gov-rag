CREATE OR REPLACE SCHEMA wh_db;
CREATE OR REPLACE SCHEMA wh_db_stage;

DROP TABLE IF EXISTS wh_db_stage.ProspectIncremental;
DROP TABLE IF EXISTS wh_db_stage.finwire;
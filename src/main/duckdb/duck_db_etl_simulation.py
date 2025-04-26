import duckdb
import os
import re
import logging
from pathlib import Path
import xml_to_CustomerMgmt

# --- Configuration ---
# Use Path objects for better path handling across OS
#BASE_DIR = Path(__file__).resolve().parent # Assumes the script is run from its location
BASE_DIR = Path("C:/lopu-kg-test/project/") # Assumes the script is run from its location
# Define paths relative to the script location or use absolute paths if necessary
DB_PATH = BASE_DIR / 'initial_db.duckdb'
SRC_FOLDER_BATCH1 = BASE_DIR / 'src' / 'data' / 'Batch1'
SQL_OUTPUT_FOLDER = Path("C:/lopu-kg-test/project/src/main/duck_db/sql_for_pipelines_v2") # Keep absolute path if required, or make relative

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- SQL Definitions ---
# Store multi-line SQL queries as constants for clarity
# (Keeping your original SQL strings)
INSERT_DIMBROKER_SQL = """
INSERT INTO wh_db.DimBroker
SELECT
    employeeid sk_brokerid,
    employeeid brokerid,
    managerid,
    employeefirstname firstname,
    employeelastname lastname,
    employeemi middleinitial,
    employeebranch branch,
    employeeoffice office,
    employeephone phone,
    true iscurrent,
    1 batchid, --temp, later read from db
    (SELECT min(datevalue::DATE) as effectivedate FROM wh_db.DimDate) effectivedate,
    '9999-12-31'::DATE enddate
FROM temp_broker;
"""

INSERT_DIMCOMPANY_SQL = """
INSERT INTO wh_db.DimCompany
WITH cmp AS (
    SELECT
        recdate,
        TRIM(SUBSTR(value, 1, 60)) AS CompanyName,
        TRIM(SUBSTR(value, 61, 10)) AS CIK,
        TRIM(SUBSTR(value, 71, 4)) AS Status,
        TRIM(SUBSTR(value, 75, 2)) AS IndustryID,
        TRIM(SUBSTR(value, 77, 4)) AS SPrating,
        TRY_CAST(TRY_CAST(SUBSTRING(value, 81, 8) AS TIMESTAMP) AS DATE) AS FoundingDate,
        TRIM(SUBSTR(value, 89, 80)) AS AddrLine1,
        TRIM(SUBSTR(value, 169, 80)) AS AddrLine2,
        TRIM(SUBSTR(value, 249, 12)) AS PostalCode,
        TRIM(SUBSTR(value, 261, 25)) AS City,
        TRIM(SUBSTR(value, 286, 20)) AS StateProvince,
        TRIM(SUBSTR(value, 306, 24)) AS Country,
        TRIM(SUBSTR(value, 330, 46)) AS CEOname,
        TRIM(SUBSTR(value, 376, 150)) AS Description
    FROM wh_db_stage.FinWire
    WHERE rectype = 'CMP'
)
SELECT
    CAST(strftime(effectivedate, '%Y%m%d') || companyid AS BIGINT) AS sk_companyid,
    companyid,
    status,
    name,
    industry,
    sprating,
    islowgrade,
    ceo,
    addressline1,
    addressline2,
    postalcode,
    city,
    stateprov,
    country,
    description,
    foundingdate,
    CASE WHEN enddate = '9999-12-31'::DATE THEN TRUE ELSE FALSE END AS iscurrent,
    batchid,
    effectivedate,
    enddate
FROM (
    SELECT
        CAST(cik AS BIGINT) AS companyid,
        CASE cmp.status
            WHEN 'ACTV' THEN 'Active'
            WHEN 'CMPT' THEN 'Completed'
            WHEN 'CNCL' THEN 'Canceled'
            WHEN 'PNDG' THEN 'Pending'
            WHEN 'SBMT' THEN 'Submitted'
            WHEN 'INAC' THEN 'Inactive'
            ELSE NULL -- or a default value, if needed
        END AS status,
        CompanyName AS name,
        ind.in_name AS industry,
        CASE
            WHEN SPrating IN ('AAA', 'AA', 'AA+', 'AA-', 'A', 'A+', 'A-', 'BBB', 'BBB+', 'BBB-', 'BB', 'BB+', 'BB-', 'B', 'B+', 'B-', 'CCC', 'CCC+', 'CCC-', 'CC', 'C', 'D') THEN SPrating
            ELSE NULL::VARCHAR
        END AS sprating,
        CASE
            WHEN SPrating IN ('AAA', 'AA', 'A', 'AA+', 'A+', 'AA-', 'A-', 'BBB', 'BBB+', 'BBB-') THEN FALSE
            WHEN SPrating IN ('BB', 'B', 'CCC', 'CC', 'C', 'D', 'BB+', 'B+', 'CCC+', 'BB-', 'B-', 'CCC-') THEN TRUE
            ELSE NULL::BOOLEAN
        END AS islowgrade,
        CEOname AS ceo,
        AddrLine1 AS addressline1,
        AddrLine2 AS addressline2,
        PostalCode AS postalcode,
        City AS city,
        StateProvince AS stateprov,
        Country AS country,
        Description AS description,
        FoundingDate AS foundingdate,
        1 AS batchid,
        recdate AS effectivedate,
        COALESCE(
            LEAD(try_cast(recdate AS DATE)) OVER (PARTITION BY cik ORDER BY recdate),
            try_cast('9999-12-31' AS DATE)
        ) AS enddate
    FROM cmp
    JOIN wh_db.industry ind ON cmp.industryid = ind.in_id
)
WHERE effectivedate < enddate;
"""

# --- Helper Functions ---

def save_sql_to_file(sql_string: str, script_name: str, output_folder: Path):
    """
    Creates the output folder (if it doesn't exist) and writes the SQL string
    to a .sql file, handling potential filename collisions with numbering.

    Args:
        sql_string: The SQL query string to save.
        script_name: The base name for the SQL file (e.g., table name).
        output_folder: The Path object representing the directory to save files in.
    """
    if not sql_string:
        logging.warning(f"Skipping save for {script_name} due to empty SQL string.")
        return

    try:
        output_folder.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logging.error(f"Error creating folder '{output_folder}': {e}")
        return

    # Sanitize script_name for use as a filename (e.g., replace dots)
    safe_script_name = script_name.replace('.', '_')
    base_filename = f"{safe_script_name}.sql"
    counter = 1
    file_path = output_folder / f"{counter}_{base_filename}"

    # Find the next available numbered filename
    while file_path.exists():
        counter += 1
        file_path = output_folder / f"{counter}_{base_filename}"

    try:
        with open(file_path, "w", encoding='utf-8') as f:
            f.write(sql_string.strip()) # Write trimmed SQL
        logging.info(f"SQL query successfully written to '{file_path}'.")
    except IOError as e:
        logging.error(f"Error writing to file '{file_path}': {e}")

def execute_and_save_sql(con: duckdb.DuckDBPyConnection, sql_string: str, script_name: str, output_folder: Path):
    """
    Executes a SQL query using the given connection and saves it to a file.

    Args:
        con: The DuckDB database connection.
        sql_string: The SQL query string to execute and save.
        script_name: The base name for the SQL file.
        output_folder: The Path object for the output directory.

    Raises:
        duckdb.Error: If the SQL execution fails.
    """
    try:
        logging.info(f"Executing SQL for: {script_name}")
        con.sql(sql_string)
        logging.info(f"Successfully executed SQL for: {script_name}")
        save_sql_to_file(sql_string, script_name, output_folder)
    except duckdb.Error as e:
        logging.error(f"DuckDB error executing SQL for {script_name}: {e}")
        logging.error(f"Failed SQL:\n{sql_string}")
        # Optionally re-raise the exception if you want the script to stop on error
        raise
    except Exception as e:
        logging.error(f"Unexpected error during execute/save for {script_name}: {e}")
        raise # Re-raise unexpected errors

def find_finwire_files(folder_path: Path) -> list[Path]:
    """
    Recursively finds files matching the pattern FINWIRE<YYYY>Q<Q>.txt/.csv etc.
    in the specified folder.

    Args:
        folder_path: The Path object of the folder to search.

    Returns:
        A list of Path objects for the matching files.
    """
    matching_files = []
    # Pattern: Starts with FINWIRE, 4 digits (year), Q, 1 digit (quarter), ends before extension.
    pattern = re.compile(r"^FINWIRE\d{4}Q\d$", re.IGNORECASE) # Case-insensitive match

    if not folder_path.is_dir():
        logging.warning(f"Source data folder not found at {folder_path}")
        return matching_files

    for item in folder_path.rglob('*'): # rglob searches recursively
        if item.is_file():
            base_name = item.stem # Filename without extension
            if pattern.match(base_name):
                matching_files.append(item)
                logging.debug(f"Found FinWire file: {item}")

    if not matching_files:
        logging.warning(f"No FinWire files found in {folder_path}")

    return matching_files

# --- Main Data Loading Logic ---

def load_initial_data(con: duckdb.DuckDBPyConnection, src_folder: Path, output_folder: Path):
    """
    Loads initial dimension and reference data from text/csv files.
    """
    logging.info(f"--- Starting Initial Data Load from {src_folder} ---")
    # Mapping from source filename to target table name
    data_map = {
        "Date.txt": "wh_db.DimDate",
        "Time.txt": "wh_db.DimTime",
        "StatusType.txt": "wh_db.StatusType",
        "TaxRate.txt": "wh_db.TaxRate",
        "TradeType.txt": "wh_db.TradeType",
        "Industry.txt": "wh_db.industry",
        # HR.csv handled separately
    }

    for file_name, table_name in data_map.items():
        file_path = src_folder / file_name
        if file_path.exists():
            logging.info(f"Loading {file_name} into {table_name}...")
            # Use COPY for simple delimited files
            query = f"COPY {table_name} FROM '{file_path.as_posix()}' (DELIMITER '|');"
            execute_and_save_sql(con, query, table_name, output_folder)
        else:
            logging.warning(f"File not found, skipping: {file_path}")

    # Special handling for HR.csv -> DimBroker
    hr_file = src_folder / "HR.csv"
    if hr_file.exists():
        logging.info("Processing HR.csv for DimBroker...")
        # 1. Create temporary staging table from HR.csv (only job code 314)
        stage_hr_sql = f"""
        CREATE OR REPLACE TABLE temp_broker AS
        SELECT * FROM read_csv('{hr_file.as_posix()}', delim=',', columns={{
            'employeeid': 'BIGINT',
            'managerid': 'BIGINT',
            'employeefirstname': 'STRING',
            'employeelastname': 'STRING',
            'employeemi': 'STRING',
            'employeejobcode': 'STRING',
            'employeebranch': 'STRING',
            'employeeoffice': 'STRING',
            'employeephone': 'STRING'
        }}, header=False, auto_detect=false) -- Set auto_detect=false when specifying columns
        WHERE employeejobcode = '314';
        """
        execute_and_save_sql(con, stage_hr_sql, "temp_broker_creation", output_folder)

        # 2. Insert into the target DimBroker table from the temp table
        execute_and_save_sql(con, INSERT_DIMBROKER_SQL, "wh_db.DimBroker_insert", output_folder)
        logging.info("Finished processing HR.csv.")
    else:
        logging.warning(f"File not found, skipping HR processing: {hr_file}")

    logging.info("--- Initial Data Load Complete ---")


def load_finwire_data(con: duckdb.DuckDBPyConnection, src_folder: Path, output_folder: Path):
    """
    Finds FinWire files, loads them into a staging table, then populates DimCompany.
    """
    logging.info(f"--- Starting FinWire Data Load from {src_folder} ---")
    finwire_files = find_finwire_files(src_folder)

    if not finwire_files:
        logging.warning("No FinWire files found to process.")
        return # Exit if no files

    # Ensure staging table exists (optional, can be done once)
    # Consider creating wh_db_stage schema if it doesn't exist
    # con.sql("CREATE SCHEMA IF NOT EXISTS wh_db_stage;")
    # con.sql("CREATE TABLE IF NOT EXISTS wh_db_stage.FinWire (rectype VARCHAR, recdate DATE, value VARCHAR);")

    # Load all found FinWire files into the staging table
    for file_path in finwire_files:
        logging.info(f"Loading FinWire file: {file_path.name}")
        # Parameterize file path in SQL for clarity and safety
        fin_wire_stage_sql = f"""
        INSERT INTO wh_db_stage.FinWire
        SELECT
            CASE
                WHEN SUBSTR(column0, 16, 3) = 'FIN' THEN
                    CASE
                        WHEN TRY_CAST(TRIM(SUBSTR(column0, 187, 60)) AS BIGINT) IS NOT NULL THEN 'FIN_COMPANYID'
                        ELSE 'FIN_NAME'
                    END
                ELSE SUBSTR(column0, 16, 3)
            END AS rectype,
            STRPTIME(SUBSTR(column0, 1, 8), '%Y%m%d') AS recdate,
            SUBSTR(column0, 19) AS value
        FROM read_csv_auto('{file_path.as_posix()}', HEADER=FALSE, filename=false, all_varchar=true);
        """
        # Save the specific SQL executed for this file
        script_name = f"wh_db_stage.FinWire_insert_{file_path.stem}"
        execute_and_save_sql(con, fin_wire_stage_sql, script_name, output_folder)

    # After loading all files, populate the final DimCompany table
    logging.info("Populating DimCompany from staged FinWire data...")
    execute_and_save_sql(con, INSERT_DIMCOMPANY_SQL, "wh_db.DimCompany_insert", output_folder)

    logging.info("--- FinWire Data Load Complete ---")


def load_prospect_staging_data(con: duckdb.DuckDBPyConnection, src_folder: Path, output_folder: Path):
    """
    Loads data from Prospect.csv in the source folder into temporary staging tables,
    calculating batchid and marketingnameplate.
    """
    logging.info(f"--- Starting Prospect Staging Data Load from {src_folder} ---")

    prospect_file_path = src_folder / "Prospect.csv"

    if not prospect_file_path.exists():
        logging.warning(f"Prospect file not found, skipping: {prospect_file_path}")
        logging.info("--- Prospect Staging Data Load Complete (Skipped) ---")
        return

    # Extract batch number from folder name (e.g., "Batch1" -> 1)
    batch_str = ''.join(filter(str.isdigit, src_folder.name))
    if not batch_str:
        logging.error(f"Could not extract batch number from folder name: {src_folder.name}. Aborting Prospect staging.")
        logging.info("--- Prospect Staging Data Load Complete (Error) ---")
        return
    try:
        batch_number = int(batch_str)
        logging.info(f"Using Batch ID: {batch_number} for Prospect data.")
    except ValueError:
        logging.error(f"Invalid batch number '{batch_str}' extracted from folder name: {src_folder.name}. Aborting Prospect staging.")
        logging.info("--- Prospect Staging Data Load Complete (Error) ---")
        return

    # --- SQL Definition for temp_propect (using f-string) ---
    temp_propect_sql = f"""
CREATE OR REPLACE TABLE temp_propect AS 
SELECT     *, 
    {batch_number} AS batchid 
    FROM read_csv_auto('{prospect_file_path}', columns={{
    "agencyid": "STRING",
    "lastname": "STRING",
    "firstname": "STRING",
    "middleinitial": "STRING",
    "gender": "STRING",
    "addressline1": "STRING",
    "addressline2": "STRING",
    "postalcode": "STRING",
    "city": "STRING",
    "state": "STRING",
    "country": "STRING",
    "phone": "STRING",
    "income": "INT",
    "numbercars": "INT",
    "numberchildren": "INT",
    "maritalstatus": "STRING",
    "age": "INT",
    "creditrating": "INT",
    "ownorrentflag": "STRING",
    "employer": "STRING",
    "numbercreditcards": "INT",
    "networth": "INT",
}}, header=False);
"""
    # --- SQL Definition for temp_propect_marketingnameplate ---
    temp_propect_marketingnameplate_sql = """
CREATE OR REPLACE TABLE temp_propect_marketingnameplate AS
SELECT
    *,
    CASE
        WHEN LENGTH(
            CONCAT(
                CASE WHEN networth > 1000000 OR income > 200000 THEN 'HighValue+' ELSE '' END,
                CASE WHEN numberchildren > 3 OR numbercreditcards > 5 THEN 'Expenses+' ELSE '' END,
                CASE WHEN age > 45 THEN 'Boomer+' ELSE '' END,
                CASE WHEN income < 50000 OR creditrating < 600 OR networth < 100000 THEN 'MoneyAlert+' ELSE '' END,
                CASE WHEN numbercars > 3 OR numbercreditcards > 7 THEN 'Spender+' ELSE '' END,
                CASE WHEN age < 25 AND networth > 1000000 THEN 'Inherited+' ELSE '' END
            )
        ) > 0
        THEN LEFT(
            CONCAT(
                CASE WHEN networth > 1000000 OR income > 200000 THEN 'HighValue+' ELSE '' END,
                CASE WHEN numberchildren > 3 OR numbercreditcards > 5 THEN 'Expenses+' ELSE '' END,
                CASE WHEN age > 45 THEN 'Boomer+' ELSE '' END,
                CASE WHEN income < 50000 OR creditrating < 600 OR networth < 100000 THEN 'MoneyAlert+' ELSE '' END,
                CASE WHEN numbercars > 3 OR numbercreditcards > 7 THEN 'Spender+' ELSE '' END,
                CASE WHEN age < 25 AND networth > 1000000 THEN 'Inherited+' ELSE '' END
            ),
            LENGTH(
                CONCAT(
                    CASE WHEN networth > 1000000 OR income > 200000 THEN 'HighValue+' ELSE '' END,
                    CASE WHEN numberchildren > 3 OR numbercreditcards > 5 THEN 'Expenses+' ELSE '' END,
                    CASE WHEN age > 45 THEN 'Boomer+' ELSE '' END,
                    CASE WHEN income < 50000 OR creditrating < 600 OR networth < 100000 THEN 'MoneyAlert+' ELSE '' END,
                    CASE WHEN numbercars > 3 OR numbercreditcards > 7 THEN 'Spender+' ELSE '' END,
                    CASE WHEN age < 25 AND networth > 1000000 THEN 'Inherited+' ELSE '' END
                )
            ) - 1
        )
        ELSE NULL
    END AS marketingnameplate
FROM temp_propect;
"""

    ProspectIncremental = f"""
INSERT INTO wh_db_stage.ProspectIncremental (
    agencyid, lastname, firstname, middleinitial, gender, addressline1, 
    addressline2, postalcode, city, state, country, phone, income, 
    numbercars, numberchildren, maritalstatus, age, creditrating, 
    ownorrentflag, employer, numbercreditcards, networth, 
    marketingnameplate, recordbatchid, batchid
)
SELECT
    tp.agencyid, tp.lastname, tp.firstname, tp.middleinitial, tp.gender, tp.addressline1, 
    tp.addressline2, tp.postalcode, tp.city, tp.state, tp.country, tp.phone, tp.income, 
    tp.numbercars, tp.numberchildren, tp.maritalstatus, tp.age, tp.creditrating, 
    tp.ownorrentflag, tp.employer, tp.numbercreditcards, tp.networth, 
    tp.marketingnameplate, tp.batchid, tp.batchid
FROM temp_propect_marketingnameplate AS tp
ON CONFLICT (agencyid, lastname, firstname) DO UPDATE SET
    middleinitial = excluded.middleinitial,
    gender = excluded.gender,
    addressline1 = excluded.addressline1,
    addressline2 = excluded.addressline2,
    postalcode = excluded.postalcode,
    city = excluded.city,
    state = excluded.state,
    country = excluded.country,
    phone = excluded.phone,
    income = excluded.income,
    numbercars = excluded.numbercars,
    numberchildren = excluded.numberchildren,
    maritalstatus = excluded.maritalstatus,
    age = excluded.age,
    creditrating = excluded.creditrating,
    ownorrentflag = excluded.ownorrentflag,
    employer = excluded.employer,
    numbercreditcards = excluded.numbercreditcards,
    networth = excluded.networth,
    marketingnameplate = excluded.marketingnameplate,
    recordbatchid = excluded.batchid;
"""


    try:
        # Execute and save the first temp table creation
        script_name_1 = "wh_db_stage.ProspectIncremental"
        # Note: Your original snippet used "wh_db_stage.ProspectIncremental" here.
        logging.info(f"Creating temporary table temp_propect (Batch ID: {batch_number})")
        con.sql(temp_propect_sql)
        save_sql_to_file(temp_propect_sql, script_name_1, output_folder)
        logging.info("Successfully created temp_propect.")

        logging.info("Creating temporary table temp_propect_marketingnameplate")
        con.sql(temp_propect_marketingnameplate_sql)
        save_sql_to_file(temp_propect_marketingnameplate_sql, script_name_1, output_folder)
        logging.info("Successfully created temp_propect_marketingnameplate.")

        con.sql("""
        ALTER TABLE wh_db_stage.ProspectIncremental
            ADD CONSTRAINT ProspectIncremental_pk PRIMARY KEY (agencyid, lastname, firstname);
        """)
        
        save_sql_to_file(ProspectIncremental, script_name_1, output_folder)

    except duckdb.Error as e:
        logging.error(f"DuckDB error during Prospect staging: {e}")
        # Optionally log the specific SQL that failed
        # Decide if you want to re-raise or just log and continue/stop
    except Exception as e:
        logging.error(f"Unexpected error during Prospect staging: {e}", exc_info=True)
        # Re-raise unexpected errors if needed
        # raise

    logging.info("--- Prospect Staging Data Load Complete ---")

def create_temp_customer_table(con: duckdb.DuckDBPyConnection, output_folder: Path):
    """
    Creates a temporary table 'customers' by selecting relevant records
    from the persistent 'wh_db_stage.CustomerMgmt' table.
    """
    customers =  """ 
CREATE OR REPLACE TEMP TABLE customers AS
  SELECT
    customerid,
    taxid,
    status,
    lastname,
    firstname,
    middleinitial,
    gender,
    tier,
    dob,
    addressline1,
    addressline2,
    postalcode,
    city,
    stateprov,
    country,
    phone1,
    phone2,
    phone3,
    email1,
    email2,
    lcl_tx_id,
    nat_tx_id,
    1 batchid,
    update_ts
  FROM
    wh_db_stage.CustomerMgmt c
  WHERE
    ActionType in ('NEW', 'INACT', 'UPDCUST')
"""

    customers_final = """CREATE OR REPLACE TABLE customers_final AS
SELECT
    customerid,
    COALESCE(taxid, last_value(taxid ORDER BY update_ts DESC) OVER w) AS taxid,
    status,
    COALESCE(lastname, last_value(lastname ORDER BY update_ts DESC) OVER w) AS lastname,
    COALESCE(firstname, last_value(firstname ORDER BY update_ts DESC) OVER w) AS firstname,
    COALESCE(middleinitial, last_value(middleinitial ORDER BY update_ts DESC) OVER w) AS middleinitial,
    COALESCE(gender, last_value(gender ORDER BY update_ts DESC) OVER w) AS gender,
    COALESCE(tier, last_value(tier ORDER BY update_ts DESC) OVER w) AS tier,
    COALESCE(dob, last_value(dob ORDER BY update_ts DESC) OVER w) AS dob,
    COALESCE(addressline1, last_value(addressline1 ORDER BY update_ts DESC) OVER w) AS addressline1,
    COALESCE(addressline2, last_value(addressline2 ORDER BY update_ts DESC) OVER w) AS addressline2,
    COALESCE(postalcode, last_value(postalcode ORDER BY update_ts DESC) OVER w) AS postalcode,
    COALESCE(CITY, last_value(CITY ORDER BY update_ts DESC) OVER w) AS CITY,
    COALESCE(stateprov, last_value(stateprov ORDER BY update_ts DESC) OVER w) AS stateprov,
    COALESCE(country, last_value(country ORDER BY update_ts DESC) OVER w) AS country,
    COALESCE(phone1, last_value(phone1 ORDER BY update_ts DESC) OVER w) AS phone1,
    COALESCE(phone2, last_value(phone2 ORDER BY update_ts DESC) OVER w) AS phone2,
    COALESCE(phone3, last_value(phone3 ORDER BY update_ts DESC) OVER w) AS phone3,
    COALESCE(email1, last_value(email1 ORDER BY update_ts DESC) OVER w) AS email1,
    COALESCE(email2, last_value(email2 ORDER BY update_ts DESC) OVER w) AS email2,
    COALESCE(LCL_TX_ID, last_value(LCL_TX_ID ORDER BY update_ts DESC) OVER w) AS LCL_TX_ID,
    COALESCE(NAT_TX_ID, last_value(NAT_TX_ID ORDER BY update_ts DESC) OVER w) AS NAT_TX_ID,
    batchid,
    CASE 
        WHEN NULLIF(lead(update_ts) OVER w, NULL) IS NULL THEN 'Y' 
        ELSE 'N' 
    END AS iscurrent,
    update_ts::DATE AS effectivedate,
    COALESCE(lead(update_ts::DATE) OVER w, '9999-12-31'::DATE) AS enddate
FROM
    customers
WINDOW w AS (PARTITION BY customerid ORDER BY update_ts);"""

    dimcustomer = """
INSERT INTO wh_db.DimCustomer (
    sk_customerid,
    customerid,
    taxid,
    status,
    lastname,
    firstname,
    middleinitial,
    gender,
    tier,
    dob,
    addressline1,
    addressline2,
    postalcode,
    city,
    stateprov,
    country,
    phone1,
    phone2,
    phone3,
    email1,
    email2,
    nationaltaxratedesc,
    nationaltaxrate,
    localtaxratedesc,
    localtaxrate,
    agencyid,
    creditrating,
    networth,
    marketingnameplate,
    iscurrent,
    batchid,
    effectivedate,
    enddate
)
WITH MaxSK AS (
    SELECT COALESCE(MAX(sk_customerid), 0) AS max_sk_customerid
    FROM wh_db.DimCustomer
),
CustomerData AS (
    SELECT 
        c.customerid,
        c.taxid,
        c.status,
        c.lastname,
        c.firstname,
        c.middleinitial,
        c.gender,
        c.tier,
        c.dob,
        c.addressline1,
        c.addressline2,
        c.postalcode,
        c.city,
        c.stateprov,
        c.country,
        c.phone1,
        c.phone2,
        c.phone3,
        c.email1, 
        c.email2,
        r_nat.TX_NAME as nationaltaxratedesc,
        r_nat.TX_RATE as nationaltaxrate,
        r_lcl.TX_NAME as localtaxratedesc,
        r_lcl.TX_RATE as localtaxrate,
        p.agencyid,
        p.creditrating,
        p.networth,
        p.marketingnameplate,
        c.iscurrent,
        c.batchid,
        c.effectivedate,
        c.enddate 
    FROM customers_final c
    JOIN wh_db.TaxRate r_lcl 
        ON c.lcl_tx_id = r_lcl.TX_ID
    JOIN wh_db.TaxRate r_nat 
        ON c.nat_tx_id = r_nat.TX_ID
    LEFT JOIN wh_db_stage.ProspectIncremental p 
        ON 
            UPPER(p.lastname) = UPPER(c.lastname)
            AND UPPER(p.firstname) = UPPER(c.firstname)
            AND UPPER(p.addressline1) = UPPER(c.addressline1)
            AND UPPER(NULLIF(p.addressline2, '')) = UPPER(NULLIF(c.addressline2, ''))
            AND UPPER(p.postalcode) = UPPER(c.postalcode)
    WHERE c.effectivedate < c.enddate
)
SELECT 
    ROW_NUMBER() OVER () + (SELECT max_sk_customerid FROM MaxSK) + 1 AS sk_customerid,
    c.customerid,
    c.taxid,
    c.status,
    c.lastname,
    c.firstname,
    c.middleinitial,
    IF(c.gender IN ('M', 'F'), c.gender, 'U') AS gender,
    c.tier,
    c.dob,
    c.addressline1,
    c.addressline2,
    c.postalcode,
    c.city,
    c.stateprov,
    c.country,
    c.phone1,
    c.phone2,
    c.phone3,
    c.email1, 
    c.email2,
    nationaltaxratedesc,
    nationaltaxrate,
    localtaxratedesc,
    localtaxrate,
    agencyid,
    creditrating,
    networth,
    marketingnameplate,
    iscurrent,
    batchid,
    effectivedate,
    enddate 
FROM CustomerData c;
 """

    logging.info("--- Starting Temp Customer Table Creation ---")

    # Define the script name for saving the SQL file
    # Note: User requested "wh_db.DimCustomer", which might be confusing as this
    # SQL creates a TEMP table named 'customers', not the final dimension table.
    script_name = "wh_db.DimCustomer"
    # A potentially clearer alternative: script_name = "temp_customers_creation"

    try:
        logging.info(f"Executing SQL to create temporary table 'customers' from 'wh_db_stage.CustomerMgmt'.")
        con.sql(customers)
        logging.info("Successfully created temporary table 'customers'.")

        # Save the SQL that was just executed
        save_sql_to_file(customers, script_name, output_folder)
        save_sql_to_file(customers_final, script_name, output_folder)
        save_sql_to_file(dimcustomer, script_name, output_folder)

    except duckdb.Error as e:
        logging.error(f"DuckDB error during temp customer table creation: {e}")
        logging.error(f"Failed SQL:\n{customers.strip()}")
        logging.error(f"Failed SQL:\n{customers_final.strip()}")
        logging.error(f"Failed SQL:\n{dimcustomer.strip()}")
        # Decide whether to re-raise or just log and continue/stop
        # raise
    except Exception as e:
        logging.error(f"Unexpected error during temp customer table creation: {e}", exc_info=True)
        # Re-raise unexpected errors if needed
        # raise

    logging.info("--- Temp Customer Table Creation Complete ---")

# --- Main Execution ---

def main():
    """
    Main function to orchestrate the data loading process.
    """
    logging.info("Script started.")
    logging.info(f"Using Database: {DB_PATH}")
    logging.info(f"Source Folder: {SRC_FOLDER_BATCH1}")
    logging.info(f"SQL Output Folder: {SQL_OUTPUT_FOLDER}")

    # Ensure the SQL output directory exists (save_sql_to_file will also check)
    try:
        SQL_OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logging.error(f"Could not create SQL output directory '{SQL_OUTPUT_FOLDER}': {e}")
        return # Exit if we can't create the output folder

    try:
        # Use a context manager for the database connection
        with duckdb.connect(database=str(DB_PATH), read_only=False) as con:
            # Optional: Create necessary schemas if they might not exist
            con.sql("CREATE SCHEMA IF NOT EXISTS wh_db;")
            con.sql("CREATE SCHEMA IF NOT EXISTS wh_db_stage;")
            # Optional: Add CREATE TABLE statements here if tables might not exist,
            # otherwise assume they are pre-created.
            # e.g., con.sql("CREATE TABLE IF NOT EXISTS wh_db.DimDate (...)")

            # Load initial dimension/reference data
            load_initial_data(con, SRC_FOLDER_BATCH1, SQL_OUTPUT_FOLDER)


            xml_to_CustomerMgmt.main(con)
            # Load FinWire data (staging and final insert)
            load_finwire_data(con, SRC_FOLDER_BATCH1, SQL_OUTPUT_FOLDER)
            load_prospect_staging_data(con, SRC_FOLDER_BATCH1, SQL_OUTPUT_FOLDER)
            create_temp_customer_table(con, SQL_OUTPUT_FOLDER)

    except duckdb.Error as e:
        logging.error(f"A database error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True) # Log traceback for unexpected errors
    finally:
        logging.info("Script finished.")


if __name__ == "__main__":
    main()
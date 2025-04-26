import xml.etree.ElementTree as ET
import pandas as pd
import os

def xml_to_dataframe(xml_file_path):
    """
    Parses an XML file and converts it into a pandas DataFrame.

    Args:
        xml_file_path (str): The path to the XML file.

    Returns:
        pandas.DataFrame: The DataFrame containing the XML data, or None if an error occurs.
    """
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        data = []
        for action in root:
            action_data = {}
            # Extract Action attributes
            for key, value in action.attrib.items():
                action_data["Action." + key.split("}")[-1]] = value

            for element in action:
                for key, column0 in element.attrib.items():
                    action_data["Customer." + key.split("}")[-1]] = column0 # value, oli siin enne 04.04 muudatus
                if len(element) == 0:  # Simple element
                    action_data[element.tag.split("}")[-1]] = element.text
                else:  # Nested element
                    for sub_element in element:
                        for key, value in sub_element.attrib.items():
                            action_data["Account." + key.split("}")[-1]] = value
                        if len(sub_element) == 0:
                            action_data[element.tag.split("}")[-1] + "." + sub_element.tag.split("}")[-1]] = sub_element.text
                        else:
                            for sub_sub_element in sub_element:
                                if len(sub_sub_element) == 0:
                                    action_data[element.tag.split("}")[-1] + "." + sub_element.tag.split("}")[-1] + "." + sub_sub_element.tag.split("}")[-1]] = sub_sub_element.text
                                else:
                                    for sub_sub_sub_element in sub_sub_element:
                                        action_data[element.tag.split("}")[-1] + "." + sub_element.tag.split("}")[-1] + "." + sub_sub_element.tag.split("}")[-1] + "." + sub_sub_sub_element.tag.split("}")[-1]] = sub_sub_sub_element.text
            data.append(action_data)

        df = pd.DataFrame(data)
        return df

    except Exception as e:
        print(f"Error processing XML file: {e}")
        return None


def rename_columns(df):
    """
    Renames DataFrame columns by extracting the last part after '.', 
    and appends a counter if duplicates are found.

    Args:
        df (pd.DataFrame): The DataFrame to rename columns.

    Returns:
        pd.DataFrame: The DataFrame with renamed columns.
    """
    new_columns = []
    seen_columns = {}  # Track seen column names and their counts

    for col in df.columns:
        parts = col.split('.')
        new_col = parts[-1]  # Extract the last part

        if new_col in seen_columns:
            seen_columns[new_col] += 1
            new_col = f"{new_col}_{seen_columns[new_col]}"  # Append a counter
        else:
            seen_columns[new_col] = 0

        new_columns.append(new_col)

    df.columns = new_columns
    return df

path_to_xml = "C:\lopu-kg-test\project\src\data\Batch1\CustomerMgmt.xml"

xml_dataframe = xml_to_dataframe(path_to_xml)

xml_dataframe = rename_columns(xml_dataframe)

stage_CustomerMgmt = """ 
        CREATE OR REPLACE TABLE wh_db_stage.CustomerMgmt  AS  
        SELECT
        try_cast(C_ID as BIGINT) customerid,
        try_cast(CA_ID as BIGINT) accountid,
        try_cast(CA_B_ID as BIGINT) brokerid,
        nullif(C_TAX_ID, '') taxid,
        nullif(CA_NAME, '') accountdesc,
        try_cast(CA_TAX_ST as TINYINT) taxstatus,
        CASE
            WHEN ActionType IN ('NEW', 'ADDACCT', 'UPDACCT', 'UPDCUST') THEN 'Active'
            WHEN ActionType IN ('CLOSEACCT', 'INACT') THEN 'Inactive'
            ELSE NULL
        END AS status,
        nullif(C_L_NAME, '') lastname,
        nullif(C_F_NAME, '') firstname,
        nullif(C_M_NAME, '') middleinitial,
        nullif(upper(C_GNDR), '') gender,
        try_cast(C_TIER as TINYINT) tier,
        try_cast(C_DOB as DATE) dob,
        nullif(C_ADLINE1, '') addressline1,
        nullif(C_ADLINE2, '') addressline2,
        nullif(C_ZIPCODE, '') postalcode,
        nullif(C_CITY, '') city,
        nullif(C_STATE_PROV, '') stateprov,
        nullif(C_CTRY, '') country,
        CASE
            WHEN nullif(C_LOCAL, '') IS NOT NULL THEN
                concat(
                    CASE WHEN nullif(C_CTRY_CODE, '') IS NOT NULL THEN '+' || C_CTRY_CODE || ' ' ELSE '' END,
                    CASE WHEN nullif(C_AREA_CODE, '') IS NOT NULL THEN '(' || C_AREA_CODE || ') ' ELSE '' END,
                    C_LOCAL,
                    COALESCE(C_EXT, '')
                )
            ELSE NULL
        END AS phone1,
        CASE
            WHEN nullif(C_LOCAL_1, '') IS NOT NULL THEN
                concat(
                    CASE WHEN nullif(C_CTRY_CODE_1, '') IS NOT NULL THEN '+' || C_CTRY_CODE_1 || ' ' ELSE '' END,
                    CASE WHEN nullif(C_AREA_CODE_1, '') IS NOT NULL THEN '(' || C_AREA_CODE_1 || ') ' ELSE '' END,
                    C_LOCAL_1,
                    COALESCE(C_EXT_1, '')
                )
            ELSE NULL
        END AS phone2,
        CASE
            WHEN nullif(C_LOCAL_2, '') IS NOT NULL THEN
                concat(
                    CASE WHEN nullif(C_CTRY_CODE_2, '') IS NOT NULL THEN '+' || C_CTRY_CODE_2 || ' ' ELSE '' END,
                    CASE WHEN nullif(C_AREA_CODE_2, '') IS NOT NULL THEN '(' || C_AREA_CODE_2 || ') ' ELSE '' END,
                    C_LOCAL_2,
                    COALESCE(C_EXT_2, '')
                )
            ELSE NULL
        END AS phone3,
        nullif(C_PRIM_EMAIL, '') email1,
        nullif(C_ALT_EMAIL, '') email2,
        nullif(C_LCL_TX_ID, '') lcl_tx_id,
        nullif(C_NAT_TX_ID, '') nat_tx_id,
        try_cast(ActionTS as TIMESTAMP) update_ts,
        ActionType
           
            FROM xml_dataframe"""

con.sql(stage_CustomerMgmt)

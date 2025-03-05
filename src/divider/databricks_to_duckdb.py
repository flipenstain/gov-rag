import re
import os

def process_sql_file(file_path):
    output_dir = "src/divider/ddls"
    os.makedirs(output_dir, exist_ok=True)
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split by -- COMMAND ----------
    sections = content.split("COMMAND ----------")
    
    pattern = re.compile(r"\$\{catalog}\.\$\{wh_db}_\$\{scale_factor}([A-Za-z0-9_\.]+)\s*\(")
    
    for i, section in enumerate(sections):
        match = pattern.search(section)
        if match:

            if match.group(1).startswith("_"):
                file_name = match.group(1).replace("_stage", "wh_db_stage") + ".sql"

            else:
                file_name = match.group(1).replace(".", "wh_db.") + ".sql"

            
            updated_section = re.sub(r"^\s*CONSTRAINT.*$", "", section, flags=re.MULTILINE)
            file_path = os.path.join(output_dir, file_name)
            updated_section = re.sub(r"\$\{catalog}\.\$\{wh_db}_\$\{scale_factor}", "wh_db", updated_section)
            #updated_section = re.sub(r"\$\{([^.}]+)\.\w+_${scale_factor}\}", "wh_db", section)
            
            # Convert SQL to DuckDB-compatible format
            updated_section = re.sub(r"CREATE OR REPLACE TABLE", "CREATE TABLE IF NOT EXISTS", updated_section)
        
            comment_pattern = re.compile(r"\s*COMMENT\s+'[^']*'")
            comments = comment_pattern.findall(updated_section)
            #column_pattern = re.compile(r"\s*(\w+)\s+\w+\s+COMMENT")
            column_pattern = re.compile(r"\s*(?!NOT\b)(\w+)\s+\w+(?:\s+NOT NULL)?(?:,|$)")
            columns = column_pattern.findall(updated_section)
            comments = [comment.replace(" COMMENT ", "") for comment in comments]


            updated_section = comment_pattern.sub("", updated_section).replace(",,",",")
            columns = column_pattern.findall(updated_section.strip())

            # Remove trailing constraints and table properties
            updated_section = re.sub(r"CONSTRAINT .*?\)|TBLPROPERTIES \(.*?\);", "", updated_section, flags=re.DOTALL)

            # Append COMMENT ON COLUMN statements
            for column, comment in zip(columns, comments):
                updated_section += f"\nCOMMENT ON COLUMN {file_name.replace('.sql', '')}.{column} IS {comment};"
            
            updated_section = re.sub(r",\s*\)", ")", updated_section)

            with open(file_path, 'w', encoding='utf-8') as out_file:
                out_file.write(updated_section)

if __name__ == "__main__":
    file_path = "src\divider\dw_init with DDL.sql"  # Replace with your actual .sql file path
    process_sql_file(file_path)
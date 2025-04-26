import os
import json
def read_all_sql_files(directory: str) -> dict:
    """
    Recursively finds all SQL files in a directory and returns their contents and paths.
    
    Args:
        directory: Path to the directory to search
        
    Returns:
        dict: {
            'file1.sql': {
                'content': 'SELECT * FROM...',
                'path': '/full/path/to/file1.sql'
            },
            'file2.sql': {
                'content': 'CREATE TABLE...',
                'path': '/full/path/to/file2.sql'
            }
        }
        
    Raises:
        ValueError: If the directory doesn't exist
    """
    if not os.path.isdir(directory):
        raise ValueError(f"Directory does not exist: {directory}")
    
    sql_files = {}
    
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.sql'):
                full_path = os.path.join(root, file)
                try:
                    with open(full_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    sql_files[file] = {
                        'content': content,
                        'path': full_path
                    }
                except IOError as e:
                    print(f"Warning: Could not read file {full_path} - {str(e)}")
                    continue
    
    return sql_files

def dependencies(db):
    # --- Reading in sql pipeline to process mapping ---
    json_file_pipe_dep = "/app/src/main/pipeline_dependency.json"
    with open(json_file_pipe_dep, "r", encoding="utf-8") as f:
                    PIPELINE_DEPENDENCY = json.load(f)

    json_file_script_dep = "/app/src/main/script_dependency.json"
    with open(json_file_pipe_dep, "r", encoding="utf-8") as f:
                    SCRIPT_DEPENDENCY = json.load(f)

    pipe_deps_dict = {
    pipeline: dependant
    for pipeline, dependant in SCRIPT_DEPENDENCY.items()
    for dependant in dependant
    }

    script_depts_dict = {
    script: dependant
    for script, dependant in PIPELINE_DEPENDENCY.items()
    for dependant in dependant
    }

    #return pipe_deps_dict, script_depts_dict

    for pipe, dependant in pipe_deps_dict.items():

        try:
            db.execute(
                """
                MATCH (p:Pipeline {name: $pipe})
                MATCH (p2:Pipeline {name: $dependant})
                MERGE (p)-[:DEPENDS_ON]->(p2)
                """,
                {"pipe": pipe, "dependant": dependant} 
            )
        except Exception as e:
            logging.error(f"Failed create dependency '{pipe}' > '{dependant}': {e}")

    for script, dependant in script_depts_dict.items():

        try:
            db.execute(
                """
                MATCH (s:Script {name: $script})
                MATCH (s2:Script {name: $dependant})
                MERGE (s)-[:DEPENDS_ON]->(s2)
                """,
                {"script": script, "dependant": dependant} 
            )
        except Exception as e:
            logging.error(f"Failed create dependency '{script}' > '{dependant}': {e}")    


import logging

def update_script_properties(db, script_details):
    """
    Updates existing Script nodes in Neo4j with their path and SQL content.

    Finds Script nodes by their name and updates the 'path' and
    'sql_content' properties based on the provided dictionary.

    Args:
        db: The database connection/session object with an 'execute' method.
        script_details (dict): A dictionary where keys are script names (str)
                               and values are dictionaries containing 'path' (str)
                               and 'content' (str).
                               Example:
                               {
                                   'file1.sql': {
                                       'content': 'SELECT * FROM...',
                                       'path': '/full/path/to/file1.sql'
                                   },
                                   'file2.sql': {
                                       'content': 'CREATE TABLE...',
                                       'path': '/full/path/to/file2.sql'
                                   }
                               }
    """
    updated_count = 0
    not_found_count = 0

    for script_name, details in script_details.items():
        if 'path' not in details or 'content' not in details:
            logging.warning(f"Skipping script '{script_name}': Missing 'path' or 'content'.")
            continue

        try:
            # MATCH the existing script node by name
            # SET the properties using parameters
            # Use OPTIONAL MATCH if you want to handle non-existent scripts gracefully
            # or RETURN s to check if the node was found.
            # This version uses RETURN count(s) to check if the node was found.
            result = db.execute(
                """
                MATCH (s:Script {name: $script_name})
                SET s.path_to_sql = $path, s.sql_content = $content
                RETURN count(s) as update_count
                """,
                {
                    "script_name": script_name,
                    "path": details['path'],
                    "content": details['content'] # Renamed parameter to avoid conflict
                }
            )

            # Assuming execute returns results allowing extraction,
            # Check if the node was found and updated. Adjust based on your db driver.
            # Example: if using neo4j driver, result might be a Result object.
            # record = result.single() # Get the single summary record
            # update_count = record["update_count"] if record else 0

            # Simplified check (assumes execute might return None or empty if no match)
            # Adapt this check based on your specific db.execute behavior
            update_check = result # Placeholder: Adapt based on actual return type
            if update_check: # Needs adjustment based on actual result format
                 logging.debug(f"Updated properties for Script: {script_name}")
                 updated_count += 1
            else:
                 logging.warning(f"Script node not found, could not update: {script_name}")
                 not_found_count += 1

        except Exception as e:
            logging.error(f"Failed to update Script node '{script_name}': {e}")

    logging.info(f"Script property update complete. Updated: {updated_count}, Not Found: {not_found_count}")

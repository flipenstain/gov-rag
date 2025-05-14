import os
import json
from llama_index.llms.openai import OpenAI
from llama_index.graph_stores.memgraph import MemgraphPropertyGraphStore # For direct client access

# --- Configuration ---
# Ensure your OPENAI_API_KEY is set as an environment variable
OPENAI_API_KEY = os.getenv("OPENAI_API")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable not set. Please set it before running the script.")

# Memgraph connection details (replace with your actual credentials)
MEMGRAPH_URL = "bolt://localhost:7687"
MEMGRAPH_USER = ""
MEMGRAPH_PASSWORD = ""

# LLM Configuration
LLM_MODEL = "gpt-4.1-mini" # Or "gpt-4.1-mini", "gpt-3.5-turbo", etc. GPT-4 is recommended for complex evaluation.
LLM_TEMPERATURE = 0.0 # For deterministic and factual evaluation

# --- Helper Functions ---
print("asd")
def get_all_script_names(graph_store: MemgraphPropertyGraphStore) -> list[str]:
    """
    Fetches all distinct script names from Memgraph.
    """
    query = "MATCH (s:Script) WHERE s.name IS NOT NULL RETURN DISTINCT s.name AS script_name"
    script_names = []
    try:
        with graph_store.client.session() as session:
            result = session.run(query)
            script_names = [record["script_name"] for record in result]
        print(f"Successfully fetched script names: {script_names}")
    except Exception as e:
        print(f"Error fetching script names from Memgraph: {e}")
    return script_names

def get_data_for_script(graph_store: MemgraphPropertyGraphStore, script_name: str) -> list[dict]:
    """
    Fetches SQL content and transformation logic for a given script name from Memgraph.
    Uses the user's provided Cypher query structure.
    """
    query_template = """
    // First row with only the SQL content
    MATCH (s:Script {name: $script_name})
    RETURN s.sql_content AS sql_content,
           NULL AS loeb_veerust,
           NULL AS kirjutab_veergu,
           NULL AS transformatsiooni_tyyp,
           NULL AS transformatsiooni_loogika

    UNION ALL // Using UNION ALL as the two parts of the query return distinct structures

    // All the actual transformation rows
    MATCH (s:Script {name: $script_name})
    MATCH (s)-[:READS_FROM]->(input:Column)
    MATCH (s)-[:GENERATES]->(output:Column)
    MATCH (output)-[l:DERIVED_FROM]->(input)
    WHERE l.transformation_type <> 'DIRECT INPUT'
    RETURN
      NULL AS sql_content,
      input.full_name AS loeb_veerust,
      output.full_name AS kirjutab_veergu,
      l.transformation_type AS transformatsiooni_tyyp,
      l.transformation_logic AS transformatsiooni_loogika
    """
    data = []
    try:
        with graph_store.client.session() as session:
            result = session.run(query_template, {"script_name": script_name})
            data = [dict(record) for record in result] # Convert records to a list of dictionaries
        print(f"Successfully fetched data for script: {script_name}")
    except Exception as e:
        print(f"Error fetching data for script {script_name} from Memgraph: {e}")
    return data

def format_data_and_create_prompt(script_name: str, script_data: list[dict]) -> str | None:
    """
    Formats the fetched SQL and transformation data and creates a detailed prompt for LLM evaluation.
    """
    if not script_data:
        print(f"No data provided for script: {script_name} to format prompt.")
        return None

    sql_content = "SQL content not found or not in the expected format."
    transformations_list = []

    # Separate SQL content (expected in the first record from the first part of UNION ALL)
    if script_data[0].get("sql_content") is not None:
        sql_content = script_data[0]["sql_content"]
        # Transformation data starts from the second record if SQL content was present
        transformation_records = script_data[1:]
    else:
        # This case might occur if the first part of the UNION ALL returned no results,
        # or if the order is not as expected.
        # We assume all records might be transformations if sql_content is not in the first row.
        print(f"Warning: SQL content not found in the first record for script {script_name}. Processing all records for transformations.")
        transformation_records = script_data


    for row in transformation_records:
        # Check if it's a transformation row (loeb_veerust should be present)
        if row.get("loeb_veerust"):
            transformations_list.append(
                f"  - Reads From: {row.get('loeb_veerust', 'N/A')}\n"
                f"    Writes To: {row.get('kirjutab_veergu', 'N/A')}\n"
                f"    Transformation Type: {row.get('transformatsiooni_tyyp', 'N/A')}\n"
                f"    Transformation Logic: {row.get('transformatsiooni_loogika', 'N/A')}"
            )

    transformations_string = "\n\n".join(transformations_list) if transformations_list else "No column-level transformation details extracted or found."

    prompt = f"""
You are an expert data model analyst and SQL reviewer. Your task is to meticulously evaluate the consistency, accuracy, and completeness of a data model (represented by column-level transformations extracted from a graph database) against its corresponding SQL script.

Script Name: {script_name}


IMPORTANT INSTRUCTION ON FILTERING: For this evaluation, you MUST disregard and explicitly NOT report discrepancies

Provided SQL Content:
```sql
{sql_content if sql_content else "No SQL content provided."}

Extracted Column-Level Data Model (Transformations from Graph Database):
{transformations_string}

**Output Format:**
  "column_name": ["list_of_notes"]

If no issues are found, return an empty list for that script. Use concise, factual language and focus on deviations from expected lineage behavior.
"""
    return prompt


def asd():
    print("Initializing LLM...")
    evaluation_result_total = ""
    try:
        llm = OpenAI(model=LLM_MODEL, api_key=OPENAI_API_KEY, temperature=LLM_TEMPERATURE)
    except Exception as e:
        print(f"Failed to initialize LLM: {e}")
        return

    print(f"Connecting to Memgraph at {MEMGRAPH_URL}...")
    try:
        graph_store = MemgraphPropertyGraphStore(
            url=MEMGRAPH_URL,
            username=MEMGRAPH_USER,
            password=MEMGRAPH_PASSWORD
        )
        # Test connection
        with graph_store.client.session() as session:
            session.run("RETURN 1")
        print("Successfully connected to Memgraph.")
    except Exception as e:
        print(f"Failed to connect to Memgraph: {e}")
        return

    print("\n--- Starting Data Model Evaluation Process ---")
    all_script_names = get_all_script_names(graph_store)

    if not all_script_names:
        print("No script names found in Memgraph. Exiting.")
        return

    for script_name in all_script_names:
        print(f"\n--- Processing Script: {script_name} ---")

        script_data = get_data_for_script(graph_store, script_name)
        if not script_data:
            print(f"No data retrieved for script: {script_name}. Skipping.")
            continue

        # For debugging: print the raw data fetched from Memgraph
        # print(f"Raw data for {script_name}:\n{json.dumps(script_data, indent=2)}")

        evaluation_prompt = format_data_and_create_prompt(script_name, script_data)
        if not evaluation_prompt:
            print(f"Could not generate evaluation prompt for {script_name}. Skipping.")
            continue

        # For debugging: print the prompt being sent to the LLM
        # print(f"\nEvaluation prompt for {script_name}:\n{evaluation_prompt}\n")

        print(f"Sending request to LLM for evaluation of {script_name}...")
        try:
            response = llm.complete(evaluation_prompt)
            evaluation_result = response.text.strip()
            print(f"\nLLM Evaluation Result for {script_name}:\n{evaluation_result}")
            evaluation_result_total += f"{script_name}:\n{evaluation_result}"
        except Exception as e:
            print(f"Error during LLM evaluation for {script_name}: {e}")
            # Consider logging the prompt that caused the error for more detailed debugging
            # with open(f"error_prompt_{script_name}.txt", "w") as f:
            #     f.write(evaluation_prompt)

    print("\n--- Data Model Evaluation Process Complete ---")
    with open(f"EVALUATOR_OUTPUT.txt", "w") as f:
                 f.write(evaluation_result_total)

if __name__ == "__main__":
    asd()
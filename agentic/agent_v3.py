import google.generativeai as genai
from google.generativeai.types import GenerationConfig, HarmCategory, HarmBlockThreshold, FunctionDeclaration, Tool
import duckdb
import logging
import os
import json
from dotenv import load_dotenv
import re
from typing import List, Dict, Any, Optional
import time

# Import the tool function
from tools import get_table_columns
start = time.time()
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables (for API key)
load_dotenv()

# --- Configuration ---
API_KEY = os.getenv("GOOGLE_API_KEY")
if not API_KEY:
    raise ValueError("GOOGLE_API_KEY environment variable not set.")

# Define prompt file paths
PROMPT_DIR = "C:\lopu-kg-test\project\\agentic\prompts"
IDENTIFIER_PROMPT_FILE = os.path.join(PROMPT_DIR, "identifier_prompt_v2.txt") # Use new identifier prompt
COPY_ANALYZER_PROMPT_FILE = os.path.join(PROMPT_DIR, "general_lineage_prompt.txt") # Use new analyzer prompt
# Add paths for other analyzer prompts as you create them
# SELECT_ANALYZER_PROMPT_FILE = os.path.join(PROMPT_DIR, "select_analyzer_prompt.txt")
# INSERT_ANALYZER_PROMPT_FILE = os.path.join(PROMPT_DIR, "insert_analyzer_prompt.txt")
# CTAS_ANALYZER_PROMPT_FILE = os.path.join(PROMPT_DIR, "ctas_analyzer_prompt.txt")

# Configure the Gemini client
genai.configure(api_key=API_KEY)

# Define the Gemini model to use
MODEL_NAME = "gemini-1.5-flash-latest" # Or "gemini-1.5-pro-latest"

# --- Tool Definition (Only needed for context gathering stage) ---
# No tools needed directly by the analyzer agents in this model if context is pre-fetched
# Tool definition is kept here for the context gathering stage
duckdb_tool = Tool(
    function_declarations=[
        FunctionDeclaration(
            name='get_table_columns',
            description="Retrieves the list of column names for a given database table.",
            parameters={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "The fully qualified name of the table (e.g., 'schema_name.table_name')."
                    }
                },
                "required": ["table_name"]
            }
        )
    ]
)

# Map function names to actual Python functions
available_tools = {
    "get_table_columns": get_table_columns,
}

# --- Helper Function to Load Prompt ---
def load_prompt(file_path: str, **kwargs) -> str:
    """Loads prompt from a file and formats it with provided arguments."""
    try:
        with open(file_path, 'r') as f:
            prompt_template = f.read()
        # Basic formatting - replace placeholders like {sql_content}, {context_json}
        for key, value in kwargs.items():
             # Ensure value is a string for replacement
             str_value = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
             prompt_template = prompt_template.replace(f"{{{key}}}", str_value)
        return prompt_template
    except FileNotFoundError:
        logging.error(f"Prompt file not found: {file_path}")
        raise
    except Exception as e:
        logging.error(f"Error loading/formatting prompt file {file_path}: {e}", exc_info=True)
        raise

# --- Stage 1: Identifier Agent (V2) ---
def identify_sql_and_context_needs(sql_content: str) -> Optional[Dict[str, Any]]:
    """
    Uses Gemini to identify SQL type, context need, reason, and specific tables.
    """
    logging.info("Stage 1: Identifying SQL type and specific context needs...")
    try:
        prompt = load_prompt(IDENTIFIER_PROMPT_FILE, sql_content=sql_content)
        identifier_model = genai.GenerativeModel(MODEL_NAME)
        # Increased retries slightly for potentially more complex identification
        response = identifier_model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(temperature=0.1) # Lower temp for structured output
        )

        # Robust JSON parsing
        text_content = response.text.strip()
        logging.debug(f"Identifier raw response: {text_content}")
        match = re.search(r"```json\s*([\s\S]*?)\s*```", text_content, re.IGNORECASE)
        json_string = match.group(1).strip() if match else text_content
        if not json_string:
             logging.error("Identifier agent returned empty JSON string.")
             return None

        result = json.loads(json_string)
        logging.info(f"Stage 1 Result: {result}")

        # Basic validation
        if "statement_type" in result and "context_needed" in result:
             if result["context_needed"] and "tables_requiring_context" not in result:
                 logging.error("Identifier response indicates context needed but missing 'tables_requiring_context' key.")
                 # Attempt to proceed but flag the issue
                 result["tables_requiring_context"] = [] # Add empty list to avoid downstream errors
                 result["error_flag"] = "Missing tables list from identifier"
             return result
        else:
            logging.error(f"Identifier agent response missing required keys: {result}")
            return None

    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse identifier agent JSON response: {e}. Response: {text_content}")
        return None
    except Exception as e:
        # Catch potential Google API errors (e.g., blocked content)
        logging.error(f"Error in Stage 1 (identify_sql_and_context_needs): {e}", exc_info=True)
        # Log response details if available
        try:
            logging.error(f"Identifier response details on error: {response.prompt_feedback}")
        except Exception:
            pass # Ignore if feedback isn't available
        return None

# --- Stage 2: Context Gathering ---
def gather_required_context(tables_to_query: List[str]) -> Dict[str, Any]:
    """
    Calls the get_table_columns tool for each table identified in Stage 1.
    """
    if not tables_to_query:
        logging.info("Stage 2: No tables require context gathering.")
        return {"schemas": {}, "errors": {}}

    logging.info(f"Stage 2: Gathering context for tables: {tables_to_query}")
    gathered_context: Dict[str, List[str]] = {}
    errors: Dict[str, str] = {}
    tool_function = available_tools.get("get_table_columns")

    if not tool_function:
        logging.error("Tool 'get_table_columns' not found in available_tools.")
        return {"schemas": {}, "errors": {"general": "Tool function not configured."}}

    for table_name in tables_to_query:
        try:
            logging.info(f"Executing tool get_table_columns(table_name='{table_name}')")
            columns = tool_function(table_name=table_name)
            if columns: # Only add if columns were found
                 gathered_context[table_name] = columns
                 logging.info(f"Successfully retrieved columns for {table_name}")
            else:
                 logging.warning(f"Tool returned no columns for {table_name}. It might not exist or be accessible.")
                 errors[table_name] = "No columns found (table might not exist or is inaccessible)"
        except Exception as e:
            logging.error(f"Error executing tool get_table_columns for '{table_name}': {e}", exc_info=True)
            errors[table_name] = str(e)

    logging.info("Stage 2: Context gathering complete.")
    return {"schemas": gathered_context, "errors": errors}

# --- Stage 3: Specialized Analysis Agent (with Context) ---
def run_specialized_analysis_with_context(
    sql_content: str,
    prompt_file: str,
    gathered_context: Dict[str, Any] # Expects {"schemas": {...}, "errors": {...}}
    ) -> Dict[str, Any]:
    """
    Runs the detailed analysis using the appropriate prompt, injecting pre-fetched context.
    No tool calls are expected from the LLM in this stage for this model.
    """
    logging.info(f"Stage 3: Running specialized analysis using prompt: {prompt_file}")
    analysis_result: Dict[str, Any] = {"error": "Analysis not completed"}

    # Prepare context string for the prompt
    context_for_prompt = gathered_context.get("schemas", {})
    context_errors = gathered_context.get("errors", {})
    if context_errors:
        # Optionally include context errors in the prompt for the LLM to know
        # context_for_prompt["_context_errors"] = context_errors
        logging.warning(f"Context gathering encountered errors: {context_errors}")


    try:
        # Inject SQL content and the gathered context (as JSON string) into the prompt
        prompt = load_prompt(prompt_file, sql_content=sql_content, context_json=context_for_prompt)
    except Exception as e:
         return {"error": f"Failed to load or format analysis prompt {prompt_file}: {e}"}

    try:
        # Initialize model - NO TOOLS needed for this stage in this design
        analysis_model = genai.GenerativeModel(MODEL_NAME)
        # Use generate_content as we don't expect function calls back
        logging.info("Sending analysis prompt with context to Gemini...")
        response = analysis_model.generate_content(
            prompt,
             generation_config=genai.types.GenerationConfig(temperature=0.1) # Lower temp for structured JSON
        )
        logging.debug("Received final analysis response.")


        # --- Process Final Response ---
        if not response.candidates:
             logging.error("Received empty candidates list in final analysis response.")
             # Log feedback if available
             try:
                 logging.error(f"Analysis response feedback: {response.prompt_feedback}")
             except Exception: pass
             return {"error": "Received no candidates in final analysis response."}

        # Check for blocked content
        if response.candidates[0].finish_reason.name != "STOP":
             logging.error(f"Analysis generation stopped for reason: {response.candidates[0].finish_reason.name}")
             # Log feedback if available
             try:
                 logging.error(f"Analysis response feedback: {response.prompt_feedback}")
             except Exception: pass
             return {"error": f"Analysis generation stopped unexpectedly: {response.candidates[0].finish_reason.name}"}


        if response.candidates[0].content and response.candidates[0].content.parts:
            final_response_part = response.candidates[0].content.parts[0]
            if final_response_part.text:
                try:
                    text_content = final_response_part.text
                    logging.debug(f"Raw final analysis text response:\n{text_content}")
                    match = re.search(r"```json\s*([\s\S]*?)\s*```", text_content, re.IGNORECASE)
                    json_string = match.group(1).strip() if match else text_content.strip()

                    if not json_string:
                        logging.error("Final analysis extracted JSON string is empty.")
                        return {"error": "Final analysis extracted JSON string is empty", "raw_response": text_content}

                    analysis_result = json.loads(json_string)
                    logging.info("Successfully parsed final analysis JSON.")

                except json.JSONDecodeError as e:
                    logging.error(f"Failed to parse final analysis JSON response: {e}")
                    analysis_result = {"error": "Failed to parse final analysis response as JSON", "raw_response": text_content}
                except Exception as e:
                     logging.error(f"Unexpected error processing final analysis response: {e}", exc_info=True)
                     analysis_result = {"error": f"Unexpected error processing final analysis response: {e}", "raw_response": final_response_part.text}
            else:
                logging.warning("Analysis agent finished without providing a text response part.")
                analysis_result = {"error": "Analysis agent did not provide a final text response part."}
        else:
             logging.warning("Analysis agent finished without providing content or parts.")
             analysis_result = {"error": "Analysis agent did not provide content or parts in final response."}


    except Exception as e:
        logging.error(f"An error occurred during the specialized analysis stage (Stage 3): {e}", exc_info=True)
        error_details = str(e)
        if hasattr(e, 'message'): error_details = e.message
        # Log feedback if available and possible
        try:
             if 'response' in locals() and response and response.prompt_feedback:
                 logging.error(f"Analysis response feedback on error: {response.prompt_feedback}")
        except Exception: pass
        analysis_result = {"error": f"Analysis agent API or processing error: {error_details}"}

    return analysis_result


# --- Main Orchestrator Logic (V2) ---
def process_sql_file_orchestrated(sql_file_path: str) -> dict:
    """
    Orchestrates the multi-stage process (V2) for analyzing a SQL file.
    Identify -> Gather Context -> Analyze with Context
    """
    logging.info(f"--- Starting Orchestrated Processing V2 for: {sql_file_path} ---")
    final_result: Dict[str, Any] = {}

    # 0. Read SQL File
    try:
        with open(sql_file_path, 'r') as f:
            sql_content = f.read().strip()
        if not sql_content:
            return {"error": "SQL file is empty", "file_path": sql_file_path}
        logging.debug(f"Read SQL content ({len(sql_content)} chars).")
    except FileNotFoundError:
        logging.error(f"SQL file not found: {sql_file_path}")
        return {"error": "SQL file not found", "file_path": sql_file_path}
    except Exception as e:
        logging.error(f"Error reading SQL file {sql_file_path}: {e}", exc_info=True)
        return {"error": f"Failed to read SQL file: {e}", "file_path": sql_file_path}

    # 1. Identify Type and Context Needs
    identification_result = identify_sql_and_context_needs(sql_content)
    if not identification_result:
        return {"error": "Stage 1 Failed: Could not identify SQL type or context needs."}
    final_result["identification"] = identification_result # Store stage 1 result

    statement_type = identification_result.get("statement_type", "OTHER").upper()
    context_needed = identification_result.get("context_needed", False)
    tables_for_context = identification_result.get("tables_requiring_context", [])

    # 2. Gather Required Context (if needed)
    gathered_context: Dict[str, Any] = {"schemas": {}, "errors": {}}
    if context_needed:
        if tables_for_context:
            gathered_context = gather_required_context(tables_for_context)
            final_result["context_gathering"] = gathered_context # Store stage 2 result
            # Handle critical context errors if necessary
            # if gathered_context["errors"] and not gathered_context["schemas"]:
            #     return {"error": "Stage 2 Failed: Could not gather required context.", **final_result}
        else:
            logging.warning("Stage 1 indicated context needed, but no tables were provided.")
            final_result["context_gathering"] = {"warning": "Context needed but no tables identified by Stage 1."}
            # Decide if we can proceed without context or should error out
            # For COPY, we absolutely need the target columns, so error out if table list was empty
            if statement_type == "COPY":
                 return {"error": "Stage 1 failed to identify target table for COPY context.", **final_result}


    # 3. Route to appropriate analysis prompt
    target_analyzer_prompt_file: Optional[str] = None

    logging.info(f"Routing based on type: {statement_type}")

    target_analyzer_prompt_file = COPY_ANALYZER_PROMPT_FILE
        
    # Check if the required prompt file exists
    if not os.path.exists(target_analyzer_prompt_file):
         logging.error(f"Required analyzer prompt file not found: {target_analyzer_prompt_file}")
         return {"error": f"Analyzer prompt file missing for type {statement_type}", "prompt_path": target_analyzer_prompt_file, **final_result}

    # 4. Run Specialized Analysis with Context
    analysis_result = run_specialized_analysis_with_context(
        sql_content,
        target_analyzer_prompt_file,
        gathered_context
    )
    final_result["analysis"] = analysis_result # Store stage 3 result

    logging.info(f"--- Finished Orchestrated Processing V2 for: {sql_file_path} ---")
    # Return the final analysis, potentially nested within the full results
    # return final_result # Return everything
    return analysis_result # Or just return the final analysis JSON


def run_orchestrator(sql_file_path: str):
    """Run the orchestrated process and return the result."""
    print(f"\n--- Running Orchestrator on ({sql_file_path}) ---")
    return process_sql_file_orchestrated(sql_file_path)

def save_result_to_json(result: dict, sql_file_path: str, output_dir: str = "Agent_LLM_JSONs"):
    """
    Save the orchestrated result to a JSON file named after the SQL file.
    
    Args:
        result: The JSON-serializable result to save.
        sql_file_path: The path to the SQL file that generated the result.
        output_dir: Subfolder to save JSON files in (created if needed).
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Extract SQL base name and create JSON filename
    base_name = os.path.basename(sql_file_path)
    name_without_ext = os.path.splitext(base_name)[0]
    json_filename = f"{name_without_ext}.json"
    output_path = os.path.join(output_dir, json_filename)

    # Save JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    
    print(f"\n✅ Result saved to: {output_path}")

def process_all_sql_files(sql_dir: str, output_dir: str):
    for filename in os.listdir(sql_dir):
        time.sleep(5)
        print(f"Sleeping for 5 seconds...\n")
        if filename.endswith(".sql"):
            sql_file_path = os.path.join(sql_dir, filename)
            logging.info(f"Working on file: {filename}")
            result = run_orchestrator(sql_file_path)
            save_result_to_json(result, sql_file_path, output_dir)

if __name__ == "__main__":
    sql_dir = r"C:\lopu-kg-test\project\src\main\sql_for_pipelines"
    output_dir = r"C:\lopu-kg-test\project\agentic\Agent_LLM_JSONs"

    process_all_sql_files(sql_dir, output_dir)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    end = time.time()
    duration = end - start
    minutes = int(duration // 60)
    seconds = duration % 60

    print(f"Process took {minutes} minutes and {seconds:.2f} seconds")


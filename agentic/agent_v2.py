import google.generativeai as genai
from google.generativeai.types import GenerationConfig, HarmCategory, HarmBlockThreshold, FunctionDeclaration, Tool
import duckdb
import logging
import os
import json
from dotenv import load_dotenv
import re
from typing import List, Dict, Any, Optional

# Import the tool function
from tools import get_table_columns

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables (for API key)
load_dotenv()

# --- Configuration ---
API_KEY = os.getenv("GOOGLE_API_KEY")
if not API_KEY:
    raise ValueError("GOOGLE_API_KEY environment variable not set.")

# Define prompt file paths
PROMPT_DIR = "prompts"
IDENTIFIER_PROMPT_FILE = os.path.join(PROMPT_DIR, "identifier_prompt.txt")
COPY_PROMPT_FILE = os.path.join(PROMPT_DIR, "copy_prompt.txt")
# Add paths for other prompts as you create them
# SELECT_PROMPT_FILE = os.path.join(PROMPT_DIR, "select_prompt.txt")
# INSERT_PROMPT_FILE = os.path.join(PROMPT_DIR, "insert_prompt.txt")
# CTAS_PROMPT_FILE = os.path.join(PROMPT_DIR, "ctas_prompt.txt")

# Configure the Gemini client
genai.configure(api_key=API_KEY)

# Define the Gemini model to use
MODEL_NAME = "gemini-1.5-flash-latest" # Or "gemini-1.5-pro-latest"

# --- Tool Definition (Only needed for analysis stage) ---
duckdb_tool = Tool(
    function_declarations=[
        FunctionDeclaration(
            name='get_table_columns',
            description="Retrieves the list of column names for a given database table. Use this when analyzing SELECT *, COPY target, or implicit INSERT columns.",
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
        # Basic formatting - replace placeholders like {sql_content}
        for key, value in kwargs.items():
             prompt_template = prompt_template.replace(f"{{{key}}}", str(value))
        return prompt_template
    except FileNotFoundError:
        logging.error(f"Prompt file not found: {file_path}")
        raise
    except Exception as e:
        logging.error(f"Error loading/formatting prompt file {file_path}: {e}", exc_info=True)
        raise

# --- Stage 1: Identifier Agent ---
def identify_sql_type_and_context(sql_content: str) -> Optional[Dict[str, Any]]:
    """
    Uses Gemini to identify the SQL statement type and context requirement.
    """
    logging.info("Stage 1: Identifying SQL type and context needs...")
    try:
        prompt = load_prompt(IDENTIFIER_PROMPT_FILE, sql_content=sql_content)
        # Use a model instance specifically for this simple task (no tools needed)
        identifier_model = genai.GenerativeModel(MODEL_NAME)
        response = identifier_model.generate_content(prompt)

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
        if "statement_type" in result and "requires_context" in result:
            return result
        else:
            logging.error(f"Identifier agent response missing required keys: {result}")
            return None

    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse identifier agent JSON response: {e}. Response: {text_content}")
        return None
    except Exception as e:
        logging.error(f"Error in Stage 1 (identify_sql_type_and_context): {e}", exc_info=True)
        return None

# --- Stage 3: Specialized Analysis Agent ---
def run_specialized_analysis(sql_content: str, prompt_file: str, tools_list: Optional[List[Tool]] = None) -> Dict[str, Any]:
    """
    Runs the detailed analysis using the appropriate prompt and tools.
    (This is similar to the previous process_sql_file, but generalized)
    """
    logging.info(f"Stage 3: Running specialized analysis using prompt: {prompt_file}")
    analysis_result: Dict[str, Any] = {"error": "Analysis not completed"} # Default error

    try:
        prompt = load_prompt(prompt_file, sql_content=sql_content)
    except Exception as e:
         return {"error": f"Failed to load or format analysis prompt {prompt_file}: {e}"}

    try:
        # Initialize model with tools if provided
        model_tools = tools_list if tools_list else []
        analysis_model = genai.GenerativeModel(MODEL_NAME, tools=model_tools)
        chat = analysis_model.start_chat(enable_automatic_function_calling=False) # Manual control

        # --- Interaction Loop (Initial Prompt + Potential Tool Calls) ---
        logging.info("Sending analysis prompt to Gemini...")
        response = chat.send_message(prompt)
        logging.debug("Received initial analysis response.")

        while response.candidates and response.candidates[0].content.parts and response.candidates[0].content.parts[0].function_call.name:
            function_call = response.candidates[0].content.parts[0].function_call
            function_name = function_call.name
            args = dict(function_call.args) # Convert to dict

            logging.info(f"Analysis agent requested function call: {function_name} with args: {args}")

            if function_name in available_tools:
                function_to_call = available_tools[function_name]
                try:
                    logging.info(f"Executing tool: {function_name}({args})")
                    tool_result = function_to_call(**args)
                    logging.info(f"Tool '{function_name}' executed.")
                    logging.debug(f"Tool result: {tool_result}")

                    function_response_part = genai.protos.Part(
                        function_response=genai.protos.FunctionResponse(
                            name=function_name,
                            response={"result": tool_result}
                        )
                    )
                except Exception as e:
                    logging.error(f"Error executing tool '{function_name}': {e}", exc_info=True)
                    function_response_part = genai.protos.Part(
                        function_response=genai.protos.FunctionResponse(
                            name=function_name,
                            response={"error": f"Failed to execute tool: {e}"}
                        )
                    )
                    logging.warning("Sent tool error information back to Gemini.")
                    # Decide if we should stop or let the LLM try to continue
                    # For lineage, tool failure might be critical, let's stop here
                    return {"error": f"Tool execution failed: {e}", "function_name": function_name, "args": args}

                # Send tool response back
                logging.info("Sending function response back to Gemini...")
                response = chat.send_message(function_response_part)
                logging.debug("Received response after function call.")

            else:
                logging.error(f"Analysis agent requested unknown function: {function_name}")
                return {"error": f"Model requested unknown function: {function_name}"}

        # --- Process Final Response ---
        logging.info("Processing final analysis response from Gemini.")
        if not response.candidates or not response.candidates[0].content or not response.candidates[0].content.parts:
             logging.error("Received an empty or invalid final analysis response.")
             return {"error": "Received an empty or invalid final analysis response."}

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
            logging.warning("Analysis agent finished without providing a text response.")
            analysis_result = {"error": "Analysis agent did not provide a final text response."}

    except Exception as e:
        logging.error(f"An error occurred during the specialized analysis stage: {e}", exc_info=True)
        error_details = str(e)
        if hasattr(e, 'message'): error_details = e.message
        analysis_result = {"error": f"Analysis agent API or processing error: {error_details}"}

    return analysis_result


# --- Main Orchestrator Logic ---
def process_sql_file_orchestrated(sql_file_path: str) -> dict:
    """
    Orchestrates the multi-stage process for analyzing a SQL file.
    """
    logging.info(f"--- Starting Orchestrated Processing for: {sql_file_path} ---")

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
    identification_result = identify_sql_type_and_context(sql_content)
    if not identification_result:
        return {"error": "Failed to identify SQL type or context needs."}

    statement_type = identification_result.get("statement_type", "OTHER").upper()
    requires_context = identification_result.get("requires_context", False)

    # 2. Route to appropriate analysis prompt and tools
    target_prompt_file: Optional[str] = None
    required_tools: Optional[List[Tool]] = None

    logging.info(f"Routing based on type: {statement_type}, requires_context: {requires_context}")

    if statement_type == "COPY":
        target_prompt_file = COPY_PROMPT_FILE
        # COPY always needs context for target columns via tool
        if not requires_context:
             logging.warning("Identifier classified COPY as not needing context, but overriding - tool is required.")
        required_tools = [duckdb_tool] # Requires get_table_columns
    # elif statement_type == "SELECT":
    #     target_prompt_file = SELECT_PROMPT_FILE
    #     if requires_context: # e.g., SELECT *
    #          required_tools = [duckdb_tool]
    # elif statement_type == "INSERT":
    #     target_prompt_file = INSERT_PROMPT_FILE
    #     if requires_context: # e.g., INSERT INTO tbl VALUES (...)
    #          required_tools = [duckdb_tool]
    # elif statement_type == "CREATE_TABLE_AS":
    #     target_prompt_file = CTAS_PROMPT_FILE
    #     # CTAS might need context if the underlying SELECT uses *
    #     if requires_context:
    #          required_tools = [duckdb_tool]
    # Add more elif blocks for other statement types...
    else:
        logging.warning(f"No specialized handler defined for statement type: {statement_type}")
        # Decide how to handle: return error, use a generic prompt, etc.
        return {"error": f"Unsupported statement type for detailed analysis: {statement_type}", "identification": identification_result}

    # Check if the required prompt file exists
    if not os.path.exists(target_prompt_file):
         logging.error(f"Required prompt file not found: {target_prompt_file}")
         return {"error": f"Prompt file missing for type {statement_type}", "prompt_path": target_prompt_file}

    # 3. Run Specialized Analysis
    final_result = run_specialized_analysis(sql_content, target_prompt_file, required_tools)

    logging.info(f"--- Finished Orchestrated Processing for: {sql_file_path} ---")
    return final_result


if __name__ == "__main__":
    # Ensure the DB exists (needed for the tool call)
    db_file = "C:\lopu-kg-test\project\initial_db.duckdb"
    if not os.path.exists(db_file):
        print(f"\nError: Database file '{db_file}' not found.")
        print("Please run 'python setup_duckdb.py' first to create the DB and tables.")
    else:
        # Create/get the test COPY SQL file
        sql_pipeline = os.path.join("C:\lopu-kg-test\project\src\main\sql_for_pipelines", "1_wh_db.DimTime.sql")
        # --- Test Orchestrated Processing ---
        print(f"\n--- Running Orchestrator on ({sql_pipeline}) ---")
        orchestrated_result = process_sql_file_orchestrated(sql_pipeline)
        print("\n--- Orchestrator Result ---")
        print(json.dumps(orchestrated_result, indent=2))
        output_path = os.path.join("C:\lopu-kg-test\project\\agentic", "copy_result.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(orchestrated_result, f, indent=2)



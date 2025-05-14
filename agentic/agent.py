import google.generativeai as genai
from google.generativeai.types import GenerationConfig, HarmCategory, HarmBlockThreshold, FunctionDeclaration, Tool
import duckdb
import logging
import os
import json
from dotenv import load_dotenv
import re # Import regular expressions module
from typing import List, Dict, Any # Added for type hinting

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

PROMPT_FILE_PATH = os.path.join("C:\lopu-kg-test\project\\agentic\prompts", "copy_prompt.txt")

# Configure the Gemini client
genai.configure(api_key=API_KEY)

# Define the Gemini model to use (function calling works well with 1.5 Pro/Flash)
MODEL_NAME = "gemini-1.5-flash-latest" # Or "gemini-1.5-pro-latest"

# --- Tool Definition for Gemini ---
# Describe the function(s) the model can call
duckdb_tool = Tool(
    function_declarations=[
        FunctionDeclaration(
            name='get_table_columns',
            # Updated description to reflect usage for COPY target table
            description="Retrieves the list of column names for a given database table. Use this to find the columns of the target table specified in a COPY FROM statement.",
            parameters={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "The fully qualified name of the target table (e.g., 'schema_name.table_name') from the COPY statement."
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
        return prompt_template.format(**kwargs)
    except FileNotFoundError:
        logging.error(f"Prompt file not found: {file_path}")
        raise
    except KeyError as e:
        logging.error(f"Missing key for prompt formatting: {e}")
        raise
    except Exception as e:
        logging.error(f"Error loading prompt file {file_path}: {e}", exc_info=True)
        raise

# --- Agent Core Logic ---
def process_sql_file(sql_file_path: str) -> dict:
    """
    Processes a SQL file containing a COPY statement using the Gemini agent.
    1. Reads the SQL content.
    2. Sends it to Gemini, instructing it to use the DuckDB tool to get target columns.
    3. Handles the function call requested by Gemini.
    4. Sends tool results back and gets the final JSON analysis.

    Args:
        sql_file_path: Path to the .sql file (expected to contain a COPY statement).

    Returns:
        A dictionary representing the JSON analysis from Gemini, or an error dictionary.
    """
    logging.info(f"Processing SQL file: {sql_file_path}")

    try:
        with open(sql_file_path, 'r') as f:
            sql_content = f.read().strip() # Read and strip whitespace
        logging.debug(f"Read SQL content:\n{sql_content}")
        # Basic check if it looks like a COPY command before sending to LLM
        if not sql_content.upper().startswith("COPY"):
             logging.warning(f"File {sql_file_path} does not appear to start with COPY. Agent might not produce expected results.")
             # Depending on desired behavior, you could return an error here directly
             # return {"error": "File content does not start with COPY", "file_path": sql_file_path}

    except FileNotFoundError:
        logging.error(f"SQL file not found: {sql_file_path}")
        return {"error": "SQL file not found", "file_path": sql_file_path}
    except Exception as e:
        logging.error(f"Error reading SQL file {sql_file_path}: {e}", exc_info=True)
        return {"error": f"Failed to read SQL file: {e}", "file_path": sql_file_path}

    # Initialize the Gemini model
    model = genai.GenerativeModel(
        MODEL_NAME,
        tools=[duckdb_tool],
        # Optional: Adjust safety settings if needed
        # safety_settings={ ... }
    )
    # Use manual function calling control
    chat = model.start_chat(enable_automatic_function_calling=False)

    # Load and format the prompt from the file
    try:
        prompt = load_prompt(PROMPT_FILE_PATH, sql_content=sql_content)
    except Exception as e:
         return {"error": f"Failed to load or format prompt: {e}"}

    logging.info("Sending initial prompt to Gemini...")
    try:
        # --- First Interaction: Expecting a function call ---
        response = chat.send_message(prompt)
        logging.debug(f"Initial Gemini response received.")

        # Check if the model immediately returned text instead of a function call
        if not response.candidates or not response.candidates[0].content.parts or not response.candidates[0].content.parts[0].function_call.name:
            logging.warning("Gemini did not request a function call as expected.")
            # Try to parse the response as JSON (maybe it generated the error JSON?)
            if response.candidates and response.candidates[0].content.parts and response.candidates[0].content.parts[0].text:
                try:
                    text_content = response.candidates[0].content.parts[0].text
                    logging.debug(f"Raw unexpected text response: {text_content}")
                    # Attempt to parse potential error JSON from prompt instructions
                    parsed_json = json.loads(text_content.strip())
                    return parsed_json # Return the JSON Gemini provided
                except json.JSONDecodeError:
                     logging.error("Gemini returned text, but it wasn't the expected function call or valid JSON.")
                     return {"error": "Unexpected initial response from Gemini (not a function call or valid JSON)", "raw_response": text_content}
                except Exception as e:
                     logging.error(f"Error processing unexpected initial text response: {e}")
                     return {"error": "Error processing unexpected initial text response", "raw_response": text_content}

            else:
                 logging.error("Received an empty or invalid initial response from Gemini.")
                 return {"error": "Received an empty or invalid initial response from Gemini."}


        # --- Process Expected Function Call ---
        function_call = response.candidates[0].content.parts[0].function_call
        function_name = function_call.name
        args = function_call.args

        logging.info(f"Gemini requested function call: {function_name} with args: {args}")

        tool_result: Any = None # Variable to store the result from the tool

        if function_name == 'get_table_columns' and function_name in available_tools:
            # Execute the local function
            function_to_call = available_tools[function_name]
            function_args = dict(args)
            try:
                logging.info(f"Executing tool: {function_name}({function_args})")
                # ** Execute the actual tool function **
                tool_result = function_to_call(**function_args) # Store the result
                logging.info(f"Tool '{function_name}' executed. Result type: {type(tool_result)}")
                logging.debug(f"Tool result: {tool_result}")

                # Prepare the response part for Gemini
                function_response_part = genai.protos.Part(
                    function_response=genai.protos.FunctionResponse(
                        name=function_name,
                        response={"result": tool_result} # Package response correctly
                    )
                )

            except Exception as e:
                logging.error(f"Error executing tool '{function_name}': {e}", exc_info=True)
                # Send error back to Gemini
                function_response_part = genai.protos.Part(
                    function_response=genai.protos.FunctionResponse(
                        name=function_name,
                        response={"error": f"Failed to execute tool: {e}"}
                    )
                )
                logging.warning("Sent error information back to Gemini.")
                # Stop processing if the tool fails critically, as we need the columns
                return {"error": f"Tool execution failed: {e}", "function_name": function_name, "args": function_args}

        else:
            logging.error(f"Unknown or unexpected function requested by Gemini: {function_name}")
            return {"error": f"Model requested invalid function: {function_name}"}


        # --- Second Interaction: Send tool result, expect final JSON ---
        logging.info("Sending function response back to Gemini, expecting final JSON...")
        response = chat.send_message(function_response_part)


        # --- Final Response Processing ---
        logging.info("Received final response from Gemini.")

        # Check if response is valid before accessing parts
        if not response.candidates or not response.candidates[0].content or not response.candidates[0].content.parts:
             logging.error("Received an empty or invalid final response from Gemini.")
             try:
                 logging.debug(f"Invalid Gemini response object: {response}")
             except Exception:
                 logging.debug("Could not log the full invalid response object.")
             return {"error": "Received an empty or invalid final response from Gemini."}

        final_response_part = response.candidates[0].content.parts[0]

        if final_response_part.text:
            # Attempt to parse the final text response as JSON
            try:
                text_content = final_response_part.text
                logging.debug(f"Raw final Gemini text response:\n{text_content}")
                # Use regex for more robust extraction of potential markdown code blocks
                match = re.search(r"```json\s*([\s\S]*?)\s*```", text_content, re.IGNORECASE)
                if match:
                    json_string = match.group(1).strip()
                    logging.debug("Extracted JSON content using regex.")
                else:
                    # Assume the whole text is JSON if no markdown block found
                    json_string = text_content.strip()
                    logging.debug("No JSON markdown block found, attempting to parse entire text.")

                if not json_string:
                    logging.error("Extracted JSON string is empty.")
                    return {"error": "Extracted JSON string is empty", "raw_response": text_content}

                final_json = json.loads(json_string)
                logging.info("Successfully parsed final JSON response from Gemini.")

                # --- Optional Validation ---
                # Check if the returned JSON structure seems correct based on the tool result
                if isinstance(tool_result, list) and 'lineage' in final_json:
                    lineage_keys = list(final_json['lineage'].keys())
                    if set(lineage_keys) != set(tool_result):
                         logging.warning(f"Mismatch between tool result columns {tool_result} and lineage keys {lineage_keys} in final JSON.")
                # --- End Optional Validation ---

                return final_json
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse final Gemini response as JSON: {e}")
                logging.debug(f"Attempted to parse: {json_string}")
                return {"error": "Failed to parse final response as JSON", "raw_response": text_content}
            except Exception as e:
                 logging.error(f"Unexpected error processing final Gemini response: {e}", exc_info=True)
                 return {"error": f"Unexpected error processing final response: {e}", "raw_response": final_response_part.text}
        else:
            logging.warning("Gemini finished without providing a text response.")
            logging.debug(f"Last response details: {response.candidates[0].content}")
            return {"error": "Gemini did not provide a final text response."}

    except Exception as e:
        logging.error(f"An error occurred during the Gemini interaction: {e}", exc_info=True)
        error_details = str(e)
        if hasattr(e, 'message'): # Try to get more specific Google API error message
            error_details = e.message
        return {"error": f"Gemini API or processing error: {error_details}"}


if __name__ == "__main__":
    # Ensure the DB exists (needed for the tool call)
    db_file = "C:\lopu-kg-test\project\initial_db.duckdb"
    if not os.path.exists(db_file):
        print(f"\nError: Database file '{db_file}' not found.")

    else:
        # Create the test COPY SQL file using an existing table
        select_sql_file = os.path.join("C:\lopu-kg-test\project\src\main\sql_for_pipelines", "1_wh_db.DimTime.sql")

        # --- Test COPY Statement ---
        print(f"\n--- Running Agent on COPY statement ({select_sql_file}) ---")
        copy_result_json = process_sql_file(select_sql_file)
        print("\n--- Agent Result (COPY) ---")
        print(json.dumps(copy_result_json, indent=2))
        output_path = os.path.join(os.getcwd(), "copy_result.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(copy_result_json, f, indent=2)

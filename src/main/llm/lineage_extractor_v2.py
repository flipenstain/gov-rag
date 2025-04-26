import os
import json
from datetime import datetime
# Make sure you have the library installed: pip install google-generativeai
import google.generativeai as genai
# Assuming types is needed for config, keep it. If not strictly needed, could remove.
from google.generativeai import types
import logging
import time

# --- Configuration ---
# Use os.path.join for better cross-platform compatibility
BASE_OUTPUT_DIR = os.path.join("C:", os.sep, "lopu-kg-test", "project", "src", "LLM_answers")
# Define paths using os.path.join
SQL_FILES_DIR = os.path.join("C:", os.sep, "lopu-kg-test", "project", "src", "main", "sql_for_pipelines")
TEMPLATES_DIR = os.path.join("C:", os.sep, "lopu-kg-test", "project", "src", "templates")
LOG_FILE_PATH = os.path.join(BASE_OUTPUT_DIR, 'llm_processing_log.log') # Central log file

# Configure logging
# Ensure the base output directory exists for the log file
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, filename=LOG_FILE_PATH, filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Function Definitions ---

def get_next_execution_number(template_base_output_dir: str) -> int:
    """
    Determines the next execution number for a template by checking existing
    numbered subdirectories.

    Args:
        template_base_output_dir: The base directory for the template's output
                                  (e.g., C:\...\LLM_answers\template_A).

    Returns:
        The next integer execution number (starts from 1).
    """
    os.makedirs(template_base_output_dir, exist_ok=True) # Ensure base exists
    existing_nums = []
    try:
        for item in os.listdir(template_base_output_dir):
            item_path = os.path.join(template_base_output_dir, item)
            if os.path.isdir(item_path) and item.isdigit():
                existing_nums.append(int(item))
    except OSError as e:
        logging.error(f"Error listing directory {template_base_output_dir}: {e}")
        # Default to 1 if listing fails, or handle differently
        return 1

    if not existing_nums:
        return 1
    else:
        return max(existing_nums) + 1


def process_llm_answer(response_text: str, output_dir: str, sql_file_name: str, timestamp: str):
    """
    Processes the LLM's answer text, saves it as JSON in the specified output directory.

    Args:
        response_text (str): The raw text response from the LLM.
        output_dir (str): The full path to the directory where the answer should be saved
                          (e.g., C:\...\LLM_answers\template_A\1).
        sql_file_name (str): The base name of the SQL file used (without extension).
        timestamp (str): The timestamp string for unique filenames.

    Returns:
        dict or None: The parsed JSON data from the response, or None if parsing fails.
    """
    # Clean up the response text
    data = response_text.strip()
    if data.startswith("```json"):
        data = data[len("```json"):].strip()
    if data.endswith("```"):
        data = data[:-len("```")].strip()

    # Convert string to JSON object
    try:
        json_data = json.loads(data)
    except json.JSONDecodeError as e:
        logging.error(f"Error: Invalid JSON format for {sql_file_name} in {output_dir}. Error: {e}. Raw Response: {response_text}")
        return None # Indicate failure

    # Ensure the specific output directory exists (should be created by main loop already, but double-check)
    os.makedirs(output_dir, exist_ok=True)

    # Generate the unique filename for the answer
    json_filename = os.path.join(output_dir, f"answer_{sql_file_name}_{timestamp}.json")

    # Save individual answer JSON
    try:
        with open(json_filename, "w", encoding="utf-8") as json_file:
            json.dump(json_data, json_file, indent=4, ensure_ascii=False)
        logging.info(f"Saved LLM answer to: {json_filename}")
        return json_data # Return parsed data on success
    except IOError as e:
        logging.error(f"Error saving LLM answer JSON to {json_filename}: {e}")
        return None # Indicate failure


def create_prompt_from_files(template_path: str, sql_path: str) -> str:
    """
    Reads a prompt template and an SQL query from files, replaces a placeholder
    in the template with the SQL query, and returns the resulting prompt string.

    Args:
        template_path: The absolute path to the text file containing the
                       prompt template with 'YOUR SQL QUERY HERE' placeholder.
        sql_path: The absolute path to the text file containing the SQL query.

    Returns:
        A string containing the final prompt.

    Raises:
        FileNotFoundError: If either file cannot be found.
        Exception: For other file reading errors.
    """
    placeholder = "YOUR SQL QUERY HERE"
    template_content = ""
    sql_content = ""

    try:
        with open(template_path, 'r', encoding='utf-8') as f_template:
            template_content = f_template.read()
    except FileNotFoundError:
        logging.error(f"Template file not found at '{template_path}'")
        raise
    except Exception as e:
        logging.error(f"Error reading template file '{template_path}': {e}")
        raise

    try:
        with open(sql_path, 'r', encoding='utf-8') as f_sql:
            sql_content = f_sql.read()
    except FileNotFoundError:
        logging.error(f"SQL file not found at '{sql_path}'")
        raise
    except Exception as e:
        logging.error(f"Error reading SQL file '{sql_path}': {e}")
        raise

    if placeholder not in template_content:
        logging.warning(f"Placeholder '{placeholder}' not found in template file: {template_path}")

    final_prompt = template_content.replace(placeholder, sql_content)
    return final_prompt


def generate_llm_response(prompt_for_api: str, template_name: str, sql_file_name_base: str, current_run_output_dir: str) -> tuple:
    """
    Sends a prompt to Gemini API, processes the answer, and returns the answer
    and metadata information.

    Args:
        prompt_for_api (str): The complete prompt string.
        template_name (str): The name of the template file (without path).
        sql_file_name_base (str): The base name of the SQL file (e.g., "1_wh_db.DimAccount").
        current_run_output_dir (str): The full path to the output directory for this execution run
                                      (e.g., C:\...\LLM_answers\template_A\1).

    Returns:
        tuple: (parsed_json_answer, metadata_dict)
               parsed_json_answer is the dict from the LLM response (or None on failure).
               metadata_dict contains prompt, tokens, etc. (or None on major failure).
    """
    # Generate a single timestamp for files related to this call
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    parsed_json_answer = None
    metadata_dict = None
    usage_info = None

    try:
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            logging.error("GOOGLE_API_KEY environment variable not set.")
            raise ValueError("Missing Google API Key")

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash') # Or your preferred model

        generation_config = types.GenerationConfig(
            temperature=0.1,
            candidate_count=1
        )

        logging.info(f"Sending prompt for template '{template_name}', SQL '{sql_file_name_base}' to {current_run_output_dir}")
        response = model.generate_content(
            contents=prompt_for_api,
            generation_config=generation_config,
        )
        logging.info(f"Received response for template '{template_name}', SQL '{sql_file_name_base}'")

        # --- Process the answer ---
        llm_answer_text = ""
        try:
            if response.parts:
                 llm_answer_text = response.parts[0].text
            elif hasattr(response, 'text'):
                 llm_answer_text = response.text
            else:
                 logging.warning(f"Could not extract text from response for {sql_file_name_base}, template {template_name}. Response: {response}")
        except Exception as e:
             logging.error(f"Error extracting text content from response: {e}. Response object: {response}")

        if llm_answer_text:
            parsed_json_answer = process_llm_answer(
                llm_answer_text,
                current_run_output_dir, # Pass the specific run directory
                sql_file_name_base,
                timestamp
            )
        else:
             logging.error(f"LLM response text is empty for {sql_file_name_base}, template {template_name}. Skipping answer processing.")

        # --- Prepare metadata ---
        usage_info = getattr(response, 'usage_metadata', None) # Safely get usage metadata
        prompt_tokens = None
        answer_tokens = None

        if usage_info:
            prompt_tokens = getattr(usage_info, 'prompt_token_count', None)
            answer_tokens = getattr(usage_info, 'candidates_token_count', None)
            if prompt_tokens is None or answer_tokens is None:
                 logging.warning(f"Could not extract full token counts from usage metadata for {sql_file_name_base}, template {template_name}. Metadata: {usage_info}")
        else:
             logging.warning(f"No usage metadata found in response for {sql_file_name_base}, template {template_name}.")

        metadata_dict = {
            "prompt_details": { # Nest prompt to avoid huge top-level string in combined file
                 "sql_file_name": sql_file_name_base,
                 "timestamp": timestamp,
                 "prompt_text_hash": hash(prompt_for_api), # Hash of prompt instead of full text for brevity in combined file
                 # "prompt_text": prompt_for_api # Uncomment if you need the full prompt here
            },
            "usage":{
                "query_tokens_used": prompt_tokens,
                "answer_tokens_used": answer_tokens,
            },
            "model_used": model.model_name, # Get model name dynamically
            "response_summary":{
                 "answer_saved": parsed_json_answer is not None,
                 "answer_file": f"answer_{sql_file_name_base}_{timestamp}.json" if parsed_json_answer is not None else None
            }
        }
        return parsed_json_answer, metadata_dict

    except ValueError as ve: # Catch the API key error
        logging.error(f"Configuration error: {ve}")
        raise # Re-raise to stop execution
    except Exception as e:
        logging.exception(f"Error during API call or processing for template '{template_name}', SQL '{sql_file_name_base}': {e}")
        # Still try to return a minimal metadata dict indicating failure if possible
        metadata_dict = {
            "prompt_details": {
                 "sql_file_name": sql_file_name_base,
                 "timestamp": timestamp,
                 "error": f"Processing failed: {type(e).__name__}"
            },
             "usage": None,
             "model_used": None,
             "response_summary": {
                 "answer_saved": False,
                 "answer_file": None
             }
        }
        return None, metadata_dict # Indicate failure for both


def save_consolidated_metadata(metadata_list: list, output_dir: str, template_name: str, execution_number: int):
    """
    Saves the collected list of metadata items into a single JSON file for the run.

    Args:
        metadata_list (list): List of metadata dictionaries collected during the run.
        output_dir (str): The specific output directory for this execution run.
        template_name (str): The name of the template.
        execution_number (int): The current execution number.
    """
    if not metadata_list:
        logging.warning(f"No metadata collected for template '{template_name}', execution #{execution_number}. Skipping metadata file creation.")
        return

    metadata_filename = os.path.join(output_dir, "_metadata_run.json")
    consolidated_data = {
        "template_name": template_name,
        "execution_number": execution_number,
        "run_timestamp": datetime.now().isoformat(),
        "individual_prompts_metadata": metadata_list
    }

    try:
        with open(metadata_filename, "w", encoding="utf-8") as meta_file:
            json.dump(consolidated_data, meta_file, indent=4, ensure_ascii=False)
        logging.info(f"Saved consolidated metadata for run {execution_number} to: {metadata_filename}")
    except IOError as e:
        logging.error(f"Error saving consolidated metadata JSON to {metadata_filename}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during consolidated metadata saving: {e}")


# --- Main Execution Logic ---

if __name__ == "__main__":
    start_time = datetime.now()
    logging.info(f"Script started at {start_time.isoformat()}")

    if not os.path.exists(SQL_FILES_DIR):
        logging.error(f"SQL files directory not found: {SQL_FILES_DIR}")
        exit(1)
    if not os.path.exists(TEMPLATES_DIR):
        logging.error(f"Templates directory not found: {TEMPLATES_DIR}")
        exit(1)

    # Iterate through each template file in the templates directory
    for template_filename in os.listdir(TEMPLATES_DIR):
        template_full_path = os.path.join(TEMPLATES_DIR, template_filename)

        if os.path.isfile(template_full_path):
            template_base_name = os.path.splitext(template_filename)[0]
            logging.info(f"--- Processing Template: {template_base_name} ---")

            # Determine base output dir for this template
            template_base_output_dir = os.path.join(BASE_OUTPUT_DIR, template_base_name)

            # Determine the execution number for THIS run
            current_exec_number = get_next_execution_number(template_base_output_dir)
            current_run_output_dir = os.path.join(template_base_output_dir, str(current_exec_number))

            # Create the output directory for this specific execution run *before* processing files
            try:
                os.makedirs(current_run_output_dir, exist_ok=True)
                logging.info(f"Output directory for this run: {current_run_output_dir}")
            except OSError as e:
                logging.error(f"Could not create output directory {current_run_output_dir}. Skipping template {template_base_name}. Error: {e}")
                continue # Skip to the next template

            # List to hold metadata for all SQL files processed in this run for this template
            run_metadata_list = []

            # Iterate through each SQL file for the current template
            sql_files_processed_count = 0
            for sql_filename in os.listdir(SQL_FILES_DIR):
                if sql_filename.lower().endswith(".sql"):
                    sql_full_path = os.path.join(SQL_FILES_DIR, sql_filename)
                    sql_file_name_base = os.path.splitext(sql_filename)[0]

                    try:
                        # 1. Create the prompt
                        prompt = create_prompt_from_files(template_full_path, sql_full_path)

                        # 2. Call the API, get answer and metadata item
                        parsed_answer, metadata_item = generate_llm_response(
                            prompt,
                            template_base_name,
                            sql_file_name_base,
                            current_run_output_dir # Pass the specific dir for this run/exec number
                        )

                        # 3. Collect metadata for this SQL file
                        if metadata_item:
                             # Add execution context to the item if needed (already in structure)
                             # metadata_item['execution_number'] = current_exec_number
                             run_metadata_list.append(metadata_item)
                        else:
                            logging.warning(f"No metadata item returned for {sql_filename}")


                        if parsed_answer is None:
                            logging.warning(f"Processing failed or returned no answer JSON for SQL: {sql_filename}")
                        # else: parsed_answer contains the dict if needed downstream

                        sql_files_processed_count += 1

                        # Optional: Add a delay between API calls
                        logging.debug("Sleeping for 5 seconds before next API call...")
                        time.sleep(5)

                    except FileNotFoundError as fnf_error:
                        logging.error(f"Skipping pair due to FileNotFoundError: {fnf_error}")
                        continue
                    except Exception as e:
                        logging.exception(f"Unhandled error processing Template: {template_base_name}, SQL: {sql_filename}. Error: {e}")
                        # Optionally add error info to metadata list
                        error_meta = {"prompt_details": {"sql_file_name": sql_file_name_base, "error": f"Unhandled processing error: {type(e).__name__}"}}
                        run_metadata_list.append(error_meta)
                        continue # Continue with the next SQL file
                else:
                     logging.debug(f"Skipping non-SQL file: {sql_filename}")

            # --- After processing all SQL files for the template ---
            # Save the consolidated metadata for this execution run
            if sql_files_processed_count > 0:
                 save_consolidated_metadata(run_metadata_list, current_run_output_dir, template_base_name, current_exec_number)
            else:
                 logging.info(f"No SQL files processed for template {template_base_name} in this run. No metadata file created.")

        else:
            logging.debug(f"Skipping non-file item in templates directory: {template_filename}")

    end_time = datetime.now()
    logging.info(f"Script finished at {end_time.isoformat()}. Total duration: {end_time - start_time}")
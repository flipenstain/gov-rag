import os
import json
from datetime import datetime
from google import genai
from google.genai import types
import logging
import time

def process_llm_response(response_text, template_name, sql_file_name):
    """
    Processes an LLM response, extracts the question-answer pair, 
    saves each answer as a separate JSON file, and logs all Q&A pairs in qa_log.json.
    """
    # Clean up the response text
    data = response_text.replace("```json", "").replace("```", "").strip()

    # Convert string to JSON object
    try:
        json_data = json.loads(data)
    except json.JSONDecodeError:
        print("Error: Invalid JSON format.")
        return

    # Ensure the LLM_answers directory exists
    llm_answers_dir = f"C:\lopu-kg-test\project\src\LLM_answers/{template_name}"
    os.makedirs(llm_answers_dir, exist_ok=True)

    # Generate a unique filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_filename = f"{llm_answers_dir}/answer_{sql_file_name}_{timestamp}.json"

    # Save individual answer JSON
    with open(json_filename, "w", encoding="utf-8") as json_file:
        json.dump(json_data, json_file, indent=4)


def create_prompt_from_files(template_path: str, sql_path: str) -> str:
    """
    Reads a prompt template and an SQL query from files, replaces a placeholder
    in the template with the SQL query, and returns the resulting prompt string.

    Args:
        template_path: The absolute path to the text file containing the
                       prompt template. This template should contain the
                       placeholder 'YOUR SQL QUERY HERE'.
        sql_path: The absolute path to the text file containing the SQL query.

    Returns:
        A string containing the prompt template with the placeholder replaced
        by the content of the SQL file.

    Raises:
        FileNotFoundError: If either the template file or the SQL file
                           cannot be found at the specified paths.
        Exception: For other potential file reading errors.
    """
    placeholder = "YOUR SQL QUERY HERE"
    template_content = ""
    sql_content = ""

    # Validate paths (optional but good practice)
    if not os.path.isabs(template_path):
        print(f"Warning: Provided template path '{template_path}' is not absolute.")
        # You could choose to raise an error here if absolute paths are mandatory
        # raise ValueError("Template path must be absolute.")
    if not os.path.isabs(sql_path):
        print(f"Warning: Provided SQL path '{sql_path}' is not absolute.")
        # raise ValueError("SQL path must be absolute.")

    # --- Read the prompt template file ---
    try:
        # Use encoding='utf-8' as it's a safe default for text files
        with open(template_path, 'r', encoding='utf-8') as f_template:
            template_content = f_template.read()
    except FileNotFoundError:
        print(f"Error: Template file not found at '{template_path}'")
        raise FileNotFoundError(f"Template file not found at '{template_path}'")
    except Exception as e:
        print(f"Error reading template file '{template_path}': {e}")
        raise # Re-raise the original exception

    # --- Read the SQL query file ---
    try:
        with open(sql_path, 'r', encoding='utf-8') as f_sql:
            sql_content = f_sql.read()
    except FileNotFoundError:
        print(f"Error: SQL file not found at '{sql_path}'")
        raise FileNotFoundError(f"SQL file not found at '{sql_path}'")
    except Exception as e:
        print(f"Error reading SQL file '{sql_path}': {e}")
        raise

    # --- Replace the placeholder ---
    # Check if the placeholder exists before replacing (optional, for warning)
    if placeholder not in template_content:
        print(f"Warning: Placeholder '{placeholder}' not found in template file: {template_path}")

    final_prompt = template_content.replace(placeholder, sql_content)

    return final_prompt



# Configure logging
logging.basicConfig(level=logging.INFO, filename=f'C:\lopu-kg-test\project\src\LLM_answers/llm_prompt_for_column_level_lineage_easy.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def generate_and_log_json_response(prompt_for_api, template_path, sql_full_path):
    """Sends a prompt to Gemini API, expects a JSON response, and logs it."""
    try:
        client = genai.Client(api_key=os.environ["GOOGLE_API_KEY"])

        response = client.models.generate_content(
        model="gemini-2.0-flash",
        config=types.GenerateContentConfig(
        # max_output_tokens=500,
            temperature=0.1
            #system_instruction=sys_instruct
            ),
        contents=prompt_for_api)

        sql_file_name = os.path.splitext(os.path.basename(sql_full_path))[0]
        template_name = os.path.basename(template_path)
        process_llm_response(response.text, template_name , sql_file_name)

        # Try to parse the text response as JSON
        json_response = json.loads(response.text)

        # Log the full JSON response
        logging.info("Prompt: %s", prompt_for_api)
        logging.info("Response JSON: %s", json.dumps(json_response, indent=2))

        return json_response

    except json.JSONDecodeError:
        logging.error("Failed to decode JSON from response: %s", response.text)
        return {"error": "Invalid JSON in response"}
    except Exception as e:
        logging.error("Error during model call: %s", str(e))
        return {"error": str(e)}


# dummy_template_path = "C:\lopu-kg-test\project\src\\templates\llm_prompt_for_column_level_lineage_easy"
# dummy_sql_path = "C:\lopu-kg-test\project\src\main\sql_for_pipelines\\1_wh_db.DimAccount.sql"

# prompt_for_api = create_prompt_from_files(dummy_template_path, dummy_sql_path)

# generate_and_log_json_response(prompt_for_api, dummy_template_path, dummy_sql_path)



sql_path = "C:\lopu-kg-test\project\src\main\sql_for_pipelines\\"
template_path = "C:\lopu-kg-test\project\src\\templates\\"

for template in os.listdir(template_path):
    template_file = os.path.join(template_path, template)

    for filename in os.listdir(sql_path):
            time.sleep(5)
            if filename.endswith(".sql"):
                sql_file_path = os.path.join(sql_path, filename)

                if not sql_file_path.endswith(".sql"):
                    print(f"Error: {sql_file_path} is not a .sql file.")

                prompt_for_api = create_prompt_from_files(template_file, sql_file_path)
                generate_and_log_json_response(prompt_for_api, template_file, sql_file_path)
        

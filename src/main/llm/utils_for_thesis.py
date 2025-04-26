import json

def calculate_token_usage(json_file_path):
    """
    Calculates and prints token usage statistics from a JSON file.

    Args:
        json_file_path (str): The path to the JSON file.
    """
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)

        # Extract usage data
        usage_data = [item["usage"] for item in data["individual_prompts_metadata"]]

        query_tokens = [usage["query_tokens_used"] for usage in usage_data]
        answer_tokens = [usage["answer_tokens_used"] for usage in usage_data]

        # Calculate statistics for query tokens
        if query_tokens:  # Check if the list is not empty
            min_query_tokens = min(query_tokens)
            max_query_tokens = max(query_tokens)
            avg_query_tokens = sum(query_tokens) / len(query_tokens)
            sum_query_tokens = sum(query_tokens)

             # Find the sql_file_name for min and max query tokens
            min_query_file_name = data["individual_prompts_metadata"][query_tokens.index(min_query_tokens)]["prompt_details"]["sql_file_name"]
            max_query_file_name = data["individual_prompts_metadata"][query_tokens.index(max_query_tokens)]["prompt_details"]["sql_file_name"]
        else:
            min_query_tokens = 0
            max_query_tokens = 0
            avg_query_tokens = 0
            sum_query_tokens = 0
            min_query_file_name = "N/A"
            max_query_file_name = "N/A"

        # Calculate statistics for answer tokens
        if answer_tokens: # Check if the list is not empty.
            min_answer_tokens = min(answer_tokens)
            max_answer_tokens = max(answer_tokens)
            avg_answer_tokens = sum(answer_tokens) / len(answer_tokens)
            sum_answer_tokens = sum(answer_tokens)
             # Find the sql_file_name for min and max answer tokens.
            min_answer_file_name = data["individual_prompts_metadata"][answer_tokens.index(min_answer_tokens)]["prompt_details"]["sql_file_name"]
            max_answer_file_name = data["individual_prompts_metadata"][answer_tokens.index(max_answer_tokens)]["prompt_details"]["sql_file_name"]
        else:
            min_answer_tokens = 0
            max_answer_tokens = 0
            avg_answer_tokens = 0
            sum_answer_tokens = 0
            min_answer_file_name = "N/A"
            max_answer_file_name = "N/A"
        # Print the results
        print("Query Token Usage:")
        print(f"  Min: {min_query_tokens} (File: {min_query_file_name})")
        print(f"  Max: {max_query_tokens} (File: {max_query_file_name})")
        print(f"  Avg: {avg_query_tokens:.2f}")
        print(f"  Sum: {sum_query_tokens}")

        print("\nAnswer Token Usage:")
        print(f"  Min: {min_answer_tokens} (File: {min_answer_file_name})")
        print(f"  Max: {max_answer_tokens} (File: {max_answer_file_name})")
        print(f"  Avg: {avg_answer_tokens:.2f}")
        print(f"  Sum: {sum_answer_tokens}")

    except FileNotFoundError:
        print(f"Error: File not found at {json_file_path}")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {json_file_path}")
    except KeyError as e:
        print(f"Error: Key not found in JSON data: {e}")
        print("  Please ensure the JSON file has the expected structure.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


json_file_path_easy = "LLM_answers\llm_prompt_for_column_level_lineage_easy\\1\_metadata_run.json"
json_file_path_wo = "LLM_answers\llm_prompt_for_column_level_lineage_hard_wo_ex\\1\_metadata_run.json"
json_file_path_w = "LLM_answers\llm_prompt_for_column_level_lineage_hard_w_ex\\1\_metadata_run.json"
print("#################Easy \n")
calculate_token_usage(json_file_path_easy)
print("#################Without \n")
calculate_token_usage(json_file_path_wo)
print("#################With \n")
calculate_token_usage(json_file_path_w)

from typing import Dict, Any, Optional, Tuple, List

# Assume OPENLINEAGE_NAMESPACE is defined elsewhere or passed in
# from common_setup import OPENLINEAGE_NAMESPACE


def _parse_input_column_string(
    column_string: str, default_namespace: str
) -> Tuple[str, str, str]:
    """
    Parses a string like 'table.column' or potentially 'schema.table.column'
    into namespace, table name, and column name.

    Args:
        column_string: The input string.
        default_namespace: The namespace to use if not specified.

    Returns:
        A tuple (namespace, table_name, column_name).
        Returns ('unknown_namespace', 'unknown_table', 'unknown_column') on error.
    """
    if not isinstance(column_string, str) or not column_string:
        print("Warning: Invalid input column string received.")
        return default_namespace, "unknown_table", "unknown_column" # Or raise error

    parts = column_string.split('.')
    if len(parts) == 2:
        # Assumes format 'table.column'
        table_name = parts[0]
        column_name = parts[1]
        namespace = default_namespace
    elif len(parts) == 1:
        # Assumes format 'column', assigns default table name - less ideal
        print(f"Warning: Input column string '{column_string}' contains only one part. Assuming column name only.")
        table_name = "unknown_source_table"
        column_name = parts[0]
        namespace = default_namespace
    elif len(parts) >= 3:
         # Assumes format 'schema.table.column' or 'db.schema.table.column', etc.
         # Use the first part as namespace ONLY if it makes sense, otherwise use default?
         # Option 1: Treat first part as schema, combine schema.table as name
         # namespace = default_namespace
         # table_name = f"{parts[0]}.{parts[1]}"
         # column_name = parts[2] # Assumes exactly 3 parts for this example
         # Option 2: Simplest for now - join all but last as table, use default ns
         namespace = default_namespace
         table_name = ".".join(parts[:-1])
         column_name = parts[-1]
         print(f"Warning: Input column string '{column_string}' has >= 3 parts. Interpreted as Table: '{table_name}', Column: '{column_name}' in Namespace: '{namespace}'. Adjust parsing if needed.")
    else: # Empty string after split? Should not happen if input is valid str
        print(f"Warning: Could not parse input column string '{column_string}'.")
        return default_namespace, "unknown_table", "unknown_column"

    return namespace, table_name, column_name


def translate_llm_format_to_ol_map(
    llm_lineage_data: Dict[str, Any],
    default_namespace: str,
) -> Dict[str, Any]:
    """
    Translates lineage data from a custom "LLM-extracted" format
    into the dictionary format expected by the get_column_lineage_facet function.

    Args:
        llm_lineage_data: Dictionary representing lineage in the custom format.
            Example:
            {
              "output_col": {"input": "table.column"},
              "output_col2": {
                "inputs": [ {"column": "t1.c1", "type": "T", ...}, ...],
                "transformation": "SQL Expression"
              }
            }
        default_namespace: The default OpenLineage namespace to use.

    Returns:
        A dictionary structured for the get_column_lineage_facet function.
    """
    ol_lineage_map = {}

    if not isinstance(llm_lineage_data, dict):
        print("Error: Input llm_lineage_data is not a dictionary.")
        return ol_lineage_map

    for output_field_name, lineage_info in llm_lineage_data.items():
        output_field_details = {}
        input_field_list_for_output = []

        if not isinstance(lineage_info, dict):
            print(f"Warning: Skipping output field '{output_field_name}', lineage info is not a dictionary.")
            continue

        # --- Case 1: Simple Direct Input ---
        if "input" in lineage_info and isinstance(lineage_info["input"], str):
            input_col_str = lineage_info["input"]
            ns, table, field = _parse_input_column_string(input_col_str, default_namespace)

            # Create the single InputField structure
            input_field_dict = {
                "namespace": ns,
                "name": table,
                "field": field,
                "transformations": [{
                                    "type": "DIRECT",
                                    "subtype": "IDENTITY",
                                    "description": "",
                                    "masking": False
                                        }] # No specific transformations listed for simple input
            }
            input_field_list_for_output.append(input_field_dict)

            # Set overall transformation type for the output field
            output_field_details["transformationType"] = "IDENTITY"
            output_field_details["transformationDescription"] = "Direct copy"

        # --- Case 2: Complex Multiple Inputs ---
        elif "inputs" in lineage_info and isinstance(lineage_info["inputs"], list):
            for input_detail in lineage_info["inputs"]:
                if not isinstance(input_detail, dict) or "column" not in input_detail:
                    print(f"Warning: Skipping invalid input detail item for output field '{output_field_name}'.")
                    continue

                input_col_str = input_detail["column"]
                ns, table, field = _parse_input_column_string(input_col_str, default_namespace)

                # Extract transformation details for *this specific input*
                input_transformations = []
                # Map keys from LLM format to OL Transformation format if possible
                transformation_detail = {}
                if "type" in input_detail:
                    transformation_detail["type"] = input_detail["type"] # Or map to known OL types?
                if "description" in input_detail:
                    transformation_detail["description"] = input_detail["description"]
                if "masking" in input_detail:
                    transformation_detail["masking"] = input_detail.get("masking", False) # Default if missing?
                # Add subtype if needed: transformation_detail["subtype"] = input_detail.get("subtype")

                if transformation_detail: # Only add if details exist
                    input_transformations.append(transformation_detail)

                # Create the InputField structure
                input_field_dict = {
                    "namespace": ns,
                    "name": table,
                    "field": field,
                    "transformations": input_transformations if input_transformations else None
                }
                input_field_list_for_output.append(input_field_dict)

            # Set overall transformation details for the output field
            output_field_details["transformationType"] = "TRANSFORMATION" # Generic type
            if "transformation" in lineage_info and isinstance(lineage_info["transformation"], str):
                output_field_details["transformationDescription"] = lineage_info["transformation"]
            else:
                 output_field_details["transformationDescription"] = "Complex transformation involving multiple inputs"

        # --- Handle unknown format ---
        else:
            print(f"Warning: Skipping output field '{output_field_name}', unknown lineage info format: {lineage_info}")
            continue

        # --- Finalize the entry for this output field ---
        if input_field_list_for_output:
             output_field_details["inputFields"] = input_field_list_for_output
             ol_lineage_map[output_field_name] = output_field_details
        else:
             print(f"Warning: No valid input fields could be parsed for output field '{output_field_name}'.")


    return ol_lineage_map

# --- Example Usage ---
if __name__ == '__main__':
# next step, break the lineage really down according to https://openlineage.io/docs/spec/facets/dataset-facets/column_lineage_facet
    llm_extracted_data = {
  "order_id": {"input": "delivery_7_days.order_id"},
  "order_placed_on": {"input": "delivery_7_days.order_placed_on"},
  "order_delivered_on": {"input": "delivery_7_days.order_delivered_on"},
  "order_delivery_time": {
    "inputs": [
      {
        "column": "delivery_7_days.order_placed_on",
        "type": "DIRECT",
        "subtype": "Transformation",
        "description": "order_placed_on used in DATEDIFF",
        "masking": False
      },
      {
        "column": "delivery_7_days.order_delivered_on",
        "type": "DIRECT",
        "subtype": "Transformation",
        "description": "order_delivered_on used in DATEDIFF",
        "masking": False
      }
    ],
    "transformation": "DATEDIFF(minute, order_placed_on, order_delivered_on)"
  }
}

    default_ns = "my_etl_namespace"
    translated_map = translate_llm_format_to_ol_map(llm_extracted_data, default_ns)

    print("\n--- LLM Extracted Data ---")
    import json
    print(json.dumps(llm_extracted_data, indent=2))

    print("\n--- Translated Map (for get_column_lineage_facet) ---")
    print(json.dumps(translated_map, indent=2))

    # You would now pass 'translated_map' to your 'get_column_lineage_facet' function
    # from db_utils import get_column_lineage_facet
    # facet = get_column_lineage_facet(translated_map)
    # print("\n--- Generated Facet Object (Representation) ---")
    # print(facet)
import duckdb
import traceback
from typing import List, Dict, Optional, Any # Added Any for lineage_map flexibility
from openlineage.client.facet import (
    SchemaField,
    SchemaDatasetFacet,
    OutputStatisticsOutputDatasetFacet,
    ColumnLineageDatasetFacet, # Import the main facet
   # column_lineage_dataset # Import the submodule for nested types
)

from openlineage.client.facet_v2 import (
#    nominal_time_run,
#    schema_dataset,
#    source_code_location_job,
#    sql_job,
    column_lineage_dataset,
)

from lineage_translator import translate_llm_format_to_ol_map

# --- Existing Helper Functions ---

def get_table_schema(db_conn: duckdb.DuckDBPyConnection, table_name: str) -> Optional[SchemaDatasetFacet]:
    """Gets table schema from DuckDB and returns OpenLineage SchemaDatasetFacet."""
    try:
        schema_info = db_conn.execute(f"PRAGMA table_info('{table_name}');").fetchall()
        fields = [
            SchemaField(name=row[1], type=row[2], description=f"Primary Key: {row[5]}" if row[5] else None)
            for row in schema_info
        ]
        if not fields:
            print(f"Warning: Could not get schema for table '{table_name}' or it's empty.")
            return None
        return SchemaDatasetFacet(fields=fields)
    except Exception as e:
        print(f"Error getting schema for table {table_name}: {e}")
        return None

def get_table_row_count(db_conn: duckdb.DuckDBPyConnection, table_name: str) -> Optional[OutputStatisticsOutputDatasetFacet]:
    """Gets row count and returns OpenLineage OutputStatisticsOutputDatasetFacet."""
    try:
        row_count = db_conn.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
        return OutputStatisticsOutputDatasetFacet(rowCount=row_count, size=None)
    except Exception as e:
        print(f"Error getting row count for table {table_name}: {e}")
        return None

# --- NEW: User's Helper Functions for Column Lineage ---
# (Assuming these are correctly defined based on openlineage-python client models)

def create_transformation(
    type: str,
    subtype: Optional[str] = None,
    description: Optional[str] = None,
    masking: Optional[bool] = None
) -> column_lineage_dataset.Transformation:
    """Creates a Transformation object."""
    # Ensure optional arguments are only passed if not None
    kwargs = {k: v for k, v in locals().items() if v is not None and k != 'type'}
    return column_lineage_dataset.Transformation(type=type, **kwargs)


def create_input_field(
    namespace: str,
    name: str,
    field: str,
    transformations: Optional[List[column_lineage_dataset.Transformation]] = None
) -> column_lineage_dataset.InputField:
    """Creates an InputField object."""
    return column_lineage_dataset.InputField(
        namespace=namespace,
        name=name,
        field=field,
        transformations=transformations
    )


def create_fields(
    input_fields: List[column_lineage_dataset.InputField],
    transformation_description: Optional[str] = None,
    transformation_type: Optional[str] = None
) -> column_lineage_dataset.Fields:
    """Creates a Fields object (represents lineage for one output field)."""
    return column_lineage_dataset.Fields(
        inputFields=input_fields,
        transformationDescription=transformation_description,
        transformationType=transformation_type
    )

# Note: Renamed user's function to avoid conflict and match naming convention
def create_column_lineage_facet_from_map(
    fields_map: Dict[str, column_lineage_dataset.Fields]
) -> ColumnLineageDatasetFacet:
    """
    Creates a ColumnLineageDatasetFacet object from a pre-built
    dictionary mapping output field names to Fields objects.
    """
    # The user's original `create_column_lineage_dataset_facet` seems to just wrap this.
    return ColumnLineageDatasetFacet(fields=fields_map)


# --- UPDATED: get_column_lineage_facet implementation ---

def get_column_lineage_facet(lineage_map: Optional[Dict[str, Any]] = None) -> Optional[ColumnLineageDatasetFacet]:
    """
    Constructs the OpenLineage ColumnLineageDatasetFacet based on a
    provided lineage map dictionary.

    Args:
        lineage_map: A dictionary where keys are output field names and
                     values are dictionaries describing the lineage for that field.
                     Example structure:
                     {
                         "output_column_A": {
                             "inputFields": [
                                 {
                                     "namespace": "ns1", "name": "input_table1", "field": "input_col_x",
                                     "transformations": [ # Optional
                                         {"type": "MASKING", "description": "Masked"}
                                     ]
                                 },
                                 # ... more input fields for output_column_A
                             ],
                             "transformationType": "PROJECTION", # Optional
                             "transformationDescription": "Description" # Optional
                         },
                         "output_column_B": { ... }
                     }

    Returns:
        An Optional ColumnLineageDatasetFacet object.
    """
    if not lineage_map:
        return None

    try:
        # print(lineage_map) # debug
        lineage_map = translate_llm_format_to_ol_map(lineage_map, "TAKE NAME FROM FIRST LINE OF SQL SCRIPT?")
        facet_fields: Dict[str, column_lineage_dataset.Fields] = {}

        for output_field_name, field_details in lineage_map.items():
            if not isinstance(field_details, dict) or 'inputFields' not in field_details:
                print(f"Warning: Invalid structure for output field '{output_field_name}' in lineage map. Skipping.")
                continue

            input_fields_data = field_details.get('inputFields', [])
            if not isinstance(input_fields_data, list):
                 print(f"Warning: 'inputFields' for '{output_field_name}' is not a list. Skipping.")
                 continue

            processed_input_fields: List[column_lineage_dataset.InputField] = []
            for input_field_data in input_fields_data:
                if not isinstance(input_field_data, dict):
                    print(f"Warning: Invalid input field data structure for '{output_field_name}'. Skipping input field.")
                    continue

                # Create transformations if they exist
                transformations_data = input_field_data.get('transformations')
                processed_transformations: Optional[List[column_lineage_dataset.Transformation]] = None
                if isinstance(transformations_data, list):
                    processed_transformations = []
                    for trans_data in transformations_data:
                         if isinstance(trans_data, dict) and 'type' in trans_data:
                              processed_transformations.append(create_transformation(**trans_data))
                         else:
                             print(f"Warning: Invalid transformation data for input field '{input_field_data.get('field')}'. Skipping transformation.")

                # Create the input field object
                try:
                    input_field = create_input_field(
                        namespace=input_field_data.get('namespace', 'default_ns'), # Add defaults if needed
                        name=input_field_data.get('name', 'default_table'),
                        field=input_field_data.get('field', 'default_field'),
                        transformations=processed_transformations
                    )
                    processed_input_fields.append(input_field)
                except TypeError as te:
                    print(f"Error creating InputField for {output_field_name} from data {input_field_data}: {te}")
                except Exception as ie:
                    print(f"Unexpected error creating InputField for {output_field_name}: {ie}")


            if not processed_input_fields:
                 print(f"Warning: No valid input fields processed for output field '{output_field_name}'. Skipping this output field.")
                 continue

            # Create the Fields object for this output field
            fields_object = create_fields(
                input_fields=processed_input_fields,
                transformation_description=field_details.get('transformationDescription'),
                transformation_type=field_details.get('transformationType')
            )
            facet_fields[output_field_name] = fields_object

        if not facet_fields:
             print("Warning: Column lineage map processed, but resulted in no valid fields for the facet.")
             return None

        # Create the final facet using the constructed fields map
        return create_column_lineage_facet_from_map(fields_map=facet_fields)

    except Exception as e:
        print(f"Error constructing column lineage facet: {e}")
        traceback.print_exc() # Print detailed traceback for debugging
        return None

# --- Example Usage (within the decorator or calling code) ---
# lineage_map_example = {
#      "user_id_out": {
#           "inputFields": [
#               {"namespace": "input_ns", "name": "input_dataset", "field": "user_id",
#                "transformations": [{"type": "FILTER", "description": "Filter out invalid users"}]}
#           ],
#           "transformationType": "FILTER"
#      },
#      "product_name_out": {
#           "inputFields": [
#                {"namespace": "input_ns", "name": "input_dataset", "field": "product_name"}
#           ],
#           "transformationType": "IDENTITY"
#      }
# }
#
# column_lineage_facet = get_column_lineage_facet(lineage_map_example)
#
# if column_lineage_facet:
#     print("\n--- Generated Column Lineage Facet ---")
#     import json
#     # Print as JSON for readability (requires OL models to be serializable or use a helper)
#     # print(json.dumps(column_lineage_facet.to_dict(), indent=2)) # Assuming a to_dict method or similar
#     print(column_lineage_facet) # Print the object representation
# else:
#     print("\n--- Failed to generate Column Lineage Facet ---")
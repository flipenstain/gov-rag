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

def get_column_lineage_facet(lineage_map: Optional[Dict[str, Dict]] = None
)-> Optional[ColumnLineageDatasetFacet]:
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
    lineage = lineage_map["lineage"]
    source_name_mapping = lineage_map["sources_summary"]
    if not lineage:
        return None

    try:
        output_fields_map: Dict[str, column_lineage_dataset.Fields] = {}
        if source_name_mapping is None:
            source_name_mapping = {} # Use original names if no mapping provided

        for output_column, details in lineage.items():
            input_fields_list: List[column_lineage_dataset.InputField] = []
            for source in details.get("sources", []):
                source_table = source.get("table")
                source_column = source.get("column")
#TO DO transormatsiooni loogika ja vaadata üle miks INPUTE nii palju tuleb, vast see source mapping on jura ja saab eemaldada
#
                if source_table and source_column:
                    # Apply mapping if necessary
                    ol_source_table_name = source_name_mapping.get(source_table, source_table)

                    input_fields_list.append(
                        column_lineage_dataset.InputField(
                            namespace="default_ns",
                            name=ol_source_table_name, # Use potentially mapped name
                            field=source_column,
                        )
                    )

            # Only add the field to the map if it has input fields
            if input_fields_list:
                output_fields_map[output_column] = column_lineage_dataset.Fields(
                    inputFields=input_fields_list,
                    transformationDescription=details.get("transformation_logic"),
                    transformationType=details.get("transformation_type"),
                )
        print(output_fields_map)
        #return create_column_lineage_facet_from_map(fields=output_fields_map)
        return column_lineage_dataset.ColumnLineageDatasetFacet(fields=output_fields_map)


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
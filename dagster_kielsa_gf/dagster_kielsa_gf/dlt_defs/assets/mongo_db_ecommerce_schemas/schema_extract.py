import json
from pathlib import Path
from dlt.common.normalizers.naming.snake_case import NamingConvention

def extract_schema(json_file_path):
    snake_case_normalizer = NamingConvention()

    # Load the JSON schema from file
    with open(json_file_path, "r", encoding="utf-8") as file:
        json_schema = json.load(file)

    # Initialize dictionaries
    field_type_dict = {}
    field_types_probabilities_dict = {}

    # Iterate over the fields in the JSON schema
    for field in json_schema["fields"]:
        field_name = field["name"] #snake_case_normalizer.normalize_identifier(field["name"])
        types = field["types"]

        # Find the type with the maximum probability
        max_type = max(types, key=lambda t: t["probability"])
        field_type_dict[field_name] = max_type["name"]

        # Store all types and their probabilities
        field_types_probabilities_dict[field_name] = {t["name"]: t["probability"] for t in types}

    return field_type_dict, field_types_probabilities_dict

if __name__ == "__main__":
    # Define the path to the JSON schema file
    json_path = Path(__file__).parent / "orders.json"
    
    # Extract the schema
    field_type_dict, field_types_probabilities_dict = extract_schema(json_path)

    # Print the dictionaries
    print("Field Type Dictionary:")
    print(json.dumps(field_type_dict, indent=4))

    print("\nField Types and Probabilities Dictionary:")
    print(json.dumps(field_types_probabilities_dict, indent=4))
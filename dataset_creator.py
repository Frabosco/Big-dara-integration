import os
import re
import json

cur_path = os.getcwd()
MEDIATED_SCHEMA = "/Mediated_schema/mediated_schema.json"
folder_path = "Monitor_specs"

# Read all the json files and convert them
def read_json_files_and_create(folder_path):

    with open(cur_path + MEDIATED_SCHEMA, 'r') as mediated_schema:
        mediated_schema = json.load(mediated_schema)
    
    schema_mapping = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as json_file:
                        data = json.load(json_file)
                        decoded_data = decode_unicode_escapes(data)
                        schema_mapping.append(create_schema_mapping_from_data(mediated_schema, decoded_data))

    return schema_mapping

def create_schema_mapping_from_data(mediated_schema, data):

    converted_dict = {}
        
    for key, value in data.items():
            
        # if the key as already the same name in the mediated schema
        if key in mediated_schema:
            converted_dict[key] = value
        # if the key is different from the mediated one but inside its values than convert it
        else:
            for mediated_key, mediated_value in mediated_schema.items():
                if key in mediated_value:
                    # Sometimes happen that we have a list instead of a single item
                    if type(value) == list:
                        converted_dict[mediated_key] = value[0] # Take the first elem. of the list
                    else:
                        converted_dict[mediated_key] = value
            
    return converted_dict

# Function to replace Unicode escape sequences
def decode_unicode_escapes(data):
    # Regular expression to find Unicode escape sequences
    unicode_escape_pattern = re.compile(r'\\u([0-9a-fA-F]{4})')

    # Function to replace the matched escape sequence with the corresponding character
    def replace_match(match):
        return chr(int(match.group(1), 16))

    # Recursively replace Unicode escapes in strings, dictionaries, and lists
    if isinstance(data, str):
        return unicode_escape_pattern.sub(replace_match, data)
    elif isinstance(data, dict):
        return {key: decode_unicode_escapes(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [decode_unicode_escapes(element) for element in data]
    else:
        return data

def value_exist_in_json(data, value):
    for key in data.keys():
        for val in key:
            if value in val:
                return True
    return False

def main():
    # Read all json files
    schema_mapping = read_json_files_and_create(folder_path)
    
    output_path = os.path.join(cur_path, "schema_mapping.json")
    with open(output_path, 'w', encoding='utf-8') as output_file:
        json.dump(schema_mapping, output_file, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    main()

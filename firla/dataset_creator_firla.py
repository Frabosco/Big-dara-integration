import os
import re
import json

CUR_PATH = os.getcwd()
MEDIATED_SCHEMA = "/mediated_schema/mediated_schema.json"
DATASET_PATH = "monitor_specs"

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
         return decode_unicode_escapes(data[0]) if data else None
    else:
        return data

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

# Read all the json files and convert them
def create_mapped_dataset(dataset_path):

    with open(CUR_PATH + MEDIATED_SCHEMA, 'r') as mediated_schema:
        mediated_schema = json.load(mediated_schema)
    
    for root, _, files in os.walk(dataset_path):
        for file in files:
            
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)
                    decoded_data = decode_unicode_escapes(data)
                    new_data = create_schema_mapping_from_data(mediated_schema, decoded_data)
        
                with open('firla\\monitor_specs_mediated\\' + os.path.basename(root) + '\\' + file, 'w', encoding='utf-8') as output_file:
                    json.dump(new_data, output_file, indent=4, ensure_ascii=False)

def create_blocks(record, attribute, l):
    blocks = []
    
    field_value = record[attribute].lower()
    blocks = [field_value[i:i+l] for i in range(len(field_value) - l + 1)]

    return blocks

def create_mapped_schema(dataset_path):

    with open(CUR_PATH + MEDIATED_SCHEMA, 'r') as mediated_schema:
        mediated_schema = json.load(mediated_schema)
    
    schema_mapping = []

    for root, _, files in os.walk(dataset_path):
        for file in files:
            
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)
                    decoded_data = decode_unicode_escapes(data)
                    
                    new_data = create_schema_mapping_from_data(mediated_schema, decoded_data)
                    new_data['record_ID'] = os.path.basename(root) + '\\' + file
                    new_data['record_blocks'] = create_blocks(new_data, 'name', 10)

                    schema_mapping.append(new_data)
    
    # avg_len = 0
    # for record in schema_mapping:
    #     avg_len += len(record['name'])

    # print('Dataset lenght = ', len(schema_mapping))
    # print('Avarage Name Lenght = ', avg_len/len(schema_mapping))

    with open('firla\\schema_mapping_firla.json', 'w', encoding='utf-8') as output_file:
        json.dump(schema_mapping, output_file, indent=4, ensure_ascii=False)

def main():
    # create_mapped_dataset(DATASET_PATH)
    create_mapped_schema(DATASET_PATH)


if __name__ == "__main__":
    main()

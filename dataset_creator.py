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
                with open(file_path, 'r') as json_file:
                        data = json.load(json_file)
                        schema_mapping.append(create_schema_mapping_from_data(mediated_schema, data))

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

def value_exist_in_json(data, value):
    for key in data.keys():
        for val in key:
            if value in val:
                return True
    return False

def main():
    # Read all json files
    schema_mapping = read_json_files_and_create(folder_path)
    
    output_path = os.path.join(cur_path, "prova.json")
    with open(output_path, 'w') as output_file:
        json.dump(schema_mapping, output_file, indent=4)

if __name__ == "__main__":
    main()

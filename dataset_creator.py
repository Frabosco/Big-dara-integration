import os
import json

cur_path = os.getcwd()

def read_json_files_and_create(folder_path):

    with open(cur_path + "/Mediated_schema/mediated_schema.json", 'r') as mediated_schema:
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
                    converted_dict[mediated_key] = value
            
    return converted_dict

def value_exist_in_json(data, value):
    for key in data.keys():
        for val in key:
            if value in val:
                return True
    return False

def main():
    folder_path = "Monitor_specs"
    # Read all json files
    schema_mapping = read_json_files_and_create(folder_path)
    # Convert every attribute of every json file in Monitor_specs into the mediated schema
    
    output_path = os.path.join(cur_path, "prova.json")
    with open(output_path, 'w') as output_file:
        json.dump(schema_mapping, output_file, indent=4)

if __name__ == "__main__":
    main()

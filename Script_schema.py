import os
import json

def extract_keys(folder_path):
    keys = set()
    
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                
                with open(file_path, 'r') as json_file:
                    data = json.load(json_file)
                    keys.update(data.keys())
    return keys

def write_keys_to_file(folder_path, keys):
    folder_name = os.path.basename(folder_path)
    output_file_path = os.path.join(folder_path, f"{folder_name}.txt")
    
    with open(output_file_path, 'w') as output_file:
        for key in sorted(keys):
            output_file.write(f"{key}\n")
            
def process_folders(base_path):
    for root, dirs, _ in os.walk(base_path):
        for dir in dirs:
            folder_path = os.path.join(root, dir)
            keys = extract_keys(folder_path)
            write_keys_to_file(folder_path, keys)
            
            
if __name__ == "__main__":
    base_path = os.path.join(os.getcwd(), "Monitor_specs")
    process_folders(base_path)
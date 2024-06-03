import os
import json
from collections import defaultdict

def extract_keys_from_json_files(folder_path):
    """
    Extract keys and their counts from all JSON files in the given folder.
    """
    key_counts = defaultdict(int)
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as json_file:
                    try:
                        data = json.load(json_file)
                        for key in data.keys():
                            key_counts[key] += 1
                    except json.JSONDecodeError:
                        print(f"Error decoding JSON from file: {file_path}")
    return key_counts

def write_keys_to_file(folder_path, key_counts):
    
    folder_name = os.path.basename(folder_path)
    output_file_path = os.path.join(os.getcwd(), "Attributes\\" + f"{folder_name}.txt")
    sorted_key_counts = sorted(key_counts.items(), key=lambda item: item[1], reverse=True)
    with open(output_file_path, 'w') as output_file:
        for key, count in sorted_key_counts:
            output_file.write(f"{key}: {count}\n")
            
            
def process_folders(base_path):
    for root, dirs, _ in os.walk(base_path):
        for dir in dirs:
            folder_path = os.path.join(root, dir)
            keys = extract_keys_from_json_files(folder_path)
            write_keys_to_file(folder_path, keys)
            
            
if __name__ == "__main__":
    base_path = os.path.join(os.getcwd(), "Monitor_specs")
    process_folders(base_path)
import os
import json
from collections import defaultdict

output_file_path ="attributes/all_in_one.txt"

if os.path. exists(output_file_path):
    os.remove(output_file_path)

def write_keys_to_file(folder_path, key_counts, c):
    
    folder_name = os.path.basename(folder_path)
    sorted_key_counts = sorted(key_counts.items(), key=lambda item: item[1], reverse=True)
    with open(output_file_path, 'a+') as output_file:
        output_file.write(f"{folder_name}({c}):[")
        for key, count in sorted_key_counts:
            if count>=c/10:
                output_file.write(f" {key},")
        output_file.write("]\n")
        

def extract_keys_from_json_files(folder_path):
    """
    Extract keys and their counts from all JSON files in the given folder.
    """
    key_counts = defaultdict(int)
    for root, _, files in os.walk(folder_path):
        c=0
        for file in files:
            c+=1
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as json_file:
                    try:
                        data = json.load(json_file)
                        for key in data.keys():
                            key_counts[key] += 1
                    except json.JSONDecodeError:
                        print(f"Error decoding JSON from file: {file_path}")
        write_keys_to_file(folder_path, key_counts, c)  
            
def process_folders(base_path):
    for root, dirs, _ in os.walk(base_path):
        for dir in dirs:
            folder_path = os.path.join(root, dir)
            extract_keys_from_json_files(folder_path)

def main():
    base_path = os.path.join(os.getcwd(), "monitor_specs")
    process_folders(base_path)           
            
if __name__ == "__main__":
    main()
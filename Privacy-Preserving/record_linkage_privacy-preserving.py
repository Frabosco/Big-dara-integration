import os
import re
import json
import time
import numpy as np
import pandas as pd
import anonypy
import phonetics
from pprl.embedder import features
from pprl.embedder.embedder import Embedder
from functools import partial
from numpy.linalg import norm

DATASET_PATH = "firla\\monitor_specs_mediated"
phonetic_attribute=["display_type", "backlight_technology", "audio_input", "audio_output", "usb_ports", "builtin_speakers", "builtin_camera", 
                    "height_adjustable", "certifications", "wall_mountable", "vesa_mount_compatible", "security_lock_slot", "hdcp_supported",
                    "plug_and_play", "source_datasheet", "manual", "led_indicators", "osd_languages", "sync_hv_separated", "wifi", "bluetooth", 
                    "ethernet", "subwoofer", "display_surface","manufacturer", "ean", "upc", "sku", "description", "label", "name", "category", 
                    "condition", "country", "payment_method", "warranty", "hardware_platform", "operating_system", "record_ID", "part_number"]

clusters={}

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

def preprocessing(table):
    for column in table:
        
        if column in phonetic_attribute:
            table[column]=[phonetics.dmetaphone(str(i))  for i in table[column]]
        else:
            table[column]=[''.join(c for c in str(i) if c.isdigit()) for i in table[column]]
            if column == 'brightness':
                table[column]=[i.replace('²', '') for i in table[column]]
            table[column]=[float(i) if i !="" else None for i in table[column]]
            
            table[column]=[np.log10(i) if i and not np.isnan(i) else 0 for i in table[column]]
            table[column]=[10**(int(i)) if i != float('inf') else float('inf') for i in table[column]]
        table[column]=[str(i) for i in table[column]]
    return table
     
     
def clustering(table, file):
    factory={}
    spec1={}
    for key in mediated_schema:
        factory[key]=partial(features.gen_misc_shingled_features, label=key)
        if(key in table.keys()):
            spec1[key]=key
    embedder = Embedder(factory, bf_size=1024, num_hashes=2)
    embedded_table=embedder.embed(table, colspec=spec1 ,update_thresholds=True) 
    embedded_table=[i for i in embedded_table.loc[:,"bf_indices"]]
    for i in range(len(embedded_table)):
        embedded_table[i]=[1 if j in  embedded_table[i] else 0 for j in range(1024)]
    embedded_table=[(file[i], embedded_table[i])for i in range(len(embedded_table))]
    if not clusters:
        for record in embedded_table:
            if str(record[1]) not in clusters.keys():
                clusters["cn-"+str(len(clusters.keys())+1)]=[record]
    else:
        records=[]
        for record in embedded_table:
            flag=True
            if str(record) in records:
                break
            records.append(str(record))
            for cluster in clusters.keys():
                if np.dot(record[1],clusters[cluster][0][1])/(norm(record[1])*norm(clusters[cluster][0][1]))>0.9:
                    flag=False
                    clusters[cluster].append(record)
                    break
            if flag:
                clusters["cn-"+str(len(clusters.keys())+1)]=[record]
    return clusters

def pprl(table, file):
    source_df=pd.DataFrame.from_records(table)
    processed_table=preprocessing(source_df)
    return clustering(processed_table, file)
    

def read_dataset_sources(dataset_path):
    iter = 1
    total_time = 0
    
    for root, _, files in os.walk(dataset_path):
        
        cur_table = []
        ids=[]
        for file in files:
            if file.endswith(".json"):
                
                file_path = os.path.join(root, file)
                
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)
                    data['record_ID'] = file_path
                    decoded_data = decode_unicode_escapes(data)
                    cur_table.append(decoded_data)
                ids.append(root.split("\\")[-1]+"\\"+file.split(".")[0])
        
        if len(files) > 0:
            print(f'\n--------------------- ITERAZIONE {iter} ---------------------\n')
            start_time = time.time()
            
            result = pprl(cur_table, ids)
            result2={}
            for cluster in result.keys():
                result2[cluster]=[result[cluster][i][0] for i in range(len(result[cluster]))]
        
            end_time = time.time()
            execution_time = end_time - start_time
            total_time += execution_time
            
            print(f'Iteration time Time: {execution_time:.2f} s')
            print(f'Execution Time: {total_time:.2f} s')
            print(f'n° of cluster: {len(result.keys())}')
            with open(f'Privacy-Preserving\clusters\iteration{iter}.json', 'w', encoding='utf-8') as res_file:
                json.dump(result2, res_file, indent=4, ensure_ascii=False)
            iter += 1
            

if __name__ == "__main__":
    offset = 0
    clusters = {}
    clusters_blocks = {}

    with open("Mediated_schema\mediated_schema.json", 'r', encoding='utf-8') as json_file:
        mediated_schema = json.load(json_file)
    
    read_dataset_sources(DATASET_PATH)
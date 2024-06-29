import os
import json
import numpy as np
import Levenshtein as lev
from dataset_creator_firla import decode_unicode_escapes

CUR_PATH = os.getcwd()
DATASET_PATH = "firla\\monitor_specs_mediated"
THETA = 4

def concatenate_attributes(record):
    attr_string = ''
    attributes = record.values()

    for attr in attributes:
        attr_string += attr.lower()
        attr_string += '#'

    res = attr_string[:-1]
    
    return res

def deduplication(records):
    global offset
    dup = False
    idx = 0

    for record in records.values():
        for k, v in clusters.items():
            if record in v:
                # clusters[k].append(record)
                dup = True
                break
        if not dup:
            clusters['C'+ str(idx + offset)] = [record]
            idx += 1
    
    offset = idx

def blocking():
    global clusters_blocks
    global clusters

    if clusters_blocks:
        
        new_clusters = {k: v for k, v in clusters.items() if k not in clusters_blocks}
        
        for key, values in new_clusters.items():
            if key not in clusters_blocks.keys():
                clusters_blocks[key] = []
            
            for v in values:
                b = v[:5]
                if b not in clusters_blocks[key]:
                    clusters_blocks[key].append(b)
    
    else:
        for key, values in clusters.items():
            if key not in clusters_blocks.keys():
                clusters_blocks[key] = []
            
            for v in values:
                b = v[:5]
                if b not in clusters_blocks[key]:
                    clusters_blocks[key].append(b)

def calculate_signature(field):
    field = field.lower()
    field = field.replace(' ', '')
    
    signature = np.zeros(26, dtype=int)
    
    for char in field:
        if 'a' <= char <= 'z':
            signature[ord(char) - ord('a')] = 1
    
    return signature

def can_skip_comparison(record1, record2):

    if len(record1) != len(record2): return False
    if set(record1.keys()) != set(record2.keys()): return False

    hamming_dist = []

    for k in record1.keys():
        sig1 = calculate_signature(record1[k])
        sig2 = calculate_signature(record2[k])

        hamming_dist.append(np.sum(np.bitwise_xor(sig1, sig2)))

    return all(dist > 2 * THETA for dist in hamming_dist)

def edit_distance(record1, record2):
    edit_distance = []
    r2keys = list(record2.keys())

    for k1 in record1.keys():
        for k2 in r2keys:
            if k1 == k2:
                edit_distance.append(lev.distance(record1[k1], record2[k2]))
                r2keys.remove(k2)
                break
    
    return all(dist <= THETA for dist in edit_distance)

def firla(dataset):
    global clusters_blocks
    global clusters

    concatenated_records = {f'{i + offset}': concatenate_attributes(dataset[i]) for i in range(len(dataset))}
    sorted_records = { k: v for k,v in sorted(concatenated_records.items(), key=lambda item: item[1])}
    deduplication(sorted_records)
    
    blocking()

    for cluster in list(clusters.keys()):
        
        is_clustered = False
        
        for rep_record in clusters[cluster]:
            
            with open(rep_record[rep_record.rfind('#') + 1:], 'r', encoding='utf-8') as rep_file:
                rep_record_json = json.load(rep_file)
                rep_record_json = decode_unicode_escapes(rep_record_json)
            
            block = rep_record[:5]
            candidate_clusters = [k for k, v in clusters_blocks.items() if block in v]
            
            for candidate_cluster in candidate_clusters:
                
                if candidate_cluster == cluster:
                    continue
                
                for candidate_record in clusters[candidate_cluster]:
                    with open(candidate_record[candidate_record.rfind('#') + 1:], 'r', encoding='utf-8') as candidate_file:
                        candidate_record_json = json.load(candidate_file)
                        candidate_record_json = decode_unicode_escapes(candidate_record_json)
                    
                    if can_skip_comparison(rep_record_json, candidate_record_json):
                        is_clustered = True
                        continue
                    
                    if  edit_distance(rep_record_json, candidate_record_json):
                        clusters[cluster].extend(clusters[candidate_cluster])
                        clusters[candidate_cluster] = []
                        is_clustered = True
                        break
                if is_clustered:
                    break
            if is_clustered:
                break
    
    return clusters


def read_dataset_sources(dataset_path):
    iter = 1
    
    for root, _, files in os.walk(dataset_path):
        
        cur_table = []
        
        for file in files:
            if file.endswith(".json"):
                
                file_path = os.path.join(root, file)
                
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    data = json.load(json_file)
                    data['record_ID'] = file_path
                    decoded_data = decode_unicode_escapes(data)
                    cur_table.append(decoded_data)
        
        if len(files) > 0:
            print(f'\n--------------------- ITERAZIONE {iter} ---------------------\n')
            result = firla(cur_table)

            with open(f'firla\\clusters\\iteration{iter}.json', 'w', encoding='utf-8') as res_file:
                json.dump(result, res_file, indent=4, ensure_ascii=False)
            
            iter += 1

if __name__ == "__main__":
    offset = 0
    clusters = {}
    clusters_blocks = {}

    with open('mediated_schema\\mediated_schema.json', 'r', encoding='utf-8') as json_file:
        mediated_schema = json.load(json_file)
    
    read_dataset_sources(DATASET_PATH)
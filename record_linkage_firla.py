from collections import defaultdict
import os
import re
import json
import numpy as np
from dataset_creator import decode_unicode_escapes

CUR_PATH = os.getcwd()
DATASET_PATH = "monitor_specs"

def concatenate_attributes(record: dict):
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
    signature = np.zeros(26, dtype=int)
    for char in field:
        if 'a' <= char <= 'z':
            signature[ord(char) - ord('a')] = 1
    return signature

def hamming_distance(sig1, sig2):
    return np.sum(np.bitwise_xor(sig1, sig2))

def can_skip_comparison(sig1, sig2, theta):
    hamming_dist = hamming_distance(sig1, sig2)
    return hamming_dist > 2 * theta

def edit_distance(str1, str2):
    len1, len2 = len(str1), len(str2)
    dp = np.zeros((len1 + 1, len2 + 1), dtype=int)
    
    for i in range(len1 + 1):
        for j in range(len2 + 1):
            if i == 0:
                dp[i][j] = j
            elif j == 0:
                dp[i][j] = i
            elif str1[i-1] == str2[j-1]:
                dp[i][j] = dp[i-1][j-1]
            else:
                dp[i][j] = 1 + min(dp[i][j-1], dp[i-1][j], dp[i-1][j-1])
    
    return dp[len1][len2]

def firla(dataset, theta):
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
                    
                    if can_skip_comparison(rep_record_json, candidate_record_json, theta):
                        continue
                    
                    if edit_distance(rep_record, candidate_record) <= theta:
                        clusters[candidate_cluster].extend(clusters[cluster])
                        del clusters[cluster]
                        is_clustered = True
                        break
                if is_clustered:
                    break
            if is_clustered:
                break
        if not is_clustered:
            clusters[rep_record] = [rep_record]
    
    print(clusters)
    print('\n\n')


def read_dataset_sources(dataset_path, output_path):
    
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
        
        # if len(files) > 0:
        #     with open(output_path + os.path.basename(root) + '.json', 'w', encoding='utf-8') as output_file:
        #         json.dump(cur_table, output_file, indent=4, ensure_ascii=False)
        
        firla(cur_table, theta)

if __name__ == "__main__":
    theta = 2
    offset = 0
    clusters = {}
    clusters_blocks = {}
    linked_records = []
    
    read_dataset_sources(DATASET_PATH, 'monitor_specs_merged\\')
import os
import json
import time
import pstats
import cProfile
import numpy as np
import Levenshtein as lev
from dataset_creator_firla import decode_unicode_escapes

THETA = 6
BLOCK_VALUE = 10
CUR_PATH = os.getcwd()
DATASET_PATH = "firla\\monitor_specs_mediated"

def concatenate_attributes(record):
    attr_string = '#'.join(attr.lower() for k, attr in record.items() if k != 'record_blocks')
    return attr_string

def deduplication(records):
    global offset
    idx = 0

    for record in records.values():
        if not any(record in cluster for cluster in clusters.values()):
            clusters['C'+ str(idx + offset)] = [record]
            idx += 1
    
    offset = idx

def blocking(dataset):
    global clusters_blocks
    global clusters

    if clusters_blocks:
        
        new_clusters = {k: v for k, v in clusters.items() if k not in clusters_blocks}
        
        for key, values in new_clusters.items():
            clusters_blocks.setdefault(key, set())
            
            for v in values:
                record_blocks = list(filter(lambda record: record['record_ID'] == v[v.rfind('#') + 1:], dataset))
                clusters_blocks[key].update(record_blocks[0]['record_blocks'])
    
    else:
        for key, values in clusters.items():
            clusters_blocks.setdefault(key, set())

            for v in values:
                record = list(filter(lambda record: record['record_ID'] == v[v.rfind('#') + 1:], dataset))
                clusters_blocks[key].update(record[0]['record_blocks'])

def calculate_signature(field):
    field = field.lower().replace(' ', '')
    
    signature = np.zeros(26, dtype=int)
    
    for char in field:
        if 'a' <= char <= 'z':
            signature[ord(char) - ord('a')] = 1
    
    return signature

def can_skip_comparison(record1, record2, r1keys, r2keys):

    if len(record1) != len(record2): return False
    if set(r1keys) != set(r2keys): return False

    hamming_dist = []

    for k in r1keys:
        sig1 = calculate_signature(record1[k])
        sig2 = calculate_signature(record2[k])

        hamming_dist.append(np.sum(np.bitwise_xor(sig1, sig2)))

    return all(dist > 2 * THETA for dist in hamming_dist)

def edit_distance(record1, record2, r1keys, r2keys):

    edit_distance = [lev.distance(record1[k], record2[k]) for k in r1keys if k in r2keys]
    
    return all(dist <= THETA for dist in edit_distance)

def firla(dataset):
    global clusters_blocks
    global clusters

    concatenated_records = {f'{i + offset}': concatenate_attributes(dataset[i]) for i in range(len(dataset))}
    sorted_records = { k: v for k,v in sorted(concatenated_records.items(), key=lambda item: item[1])}
    deduplication(sorted_records)
    blocking(dataset)

    for cluster in list(clusters.keys()):
        
        is_clustered = False
        
        for rep_record in clusters[cluster]:
            
            record_data = list(filter(lambda record: record['record_ID'] == rep_record[rep_record.rfind('#') + 1:], dataset))
            r1keys = list(filter(lambda k: k != 'record_ID' and k != 'record_blocks', record_data[0].keys()))
            
            for block in record_data[0]['record_blocks']:
                
                candidate_clusters = [k for k, v in clusters_blocks.items() if block in v and k != cluster]
                
                for candidate_cluster in candidate_clusters:

                    for candidate_record in clusters[candidate_cluster]:
                        
                        candidate_record_data = list(filter(lambda record: record['record_ID'] == candidate_record[candidate_record.rfind('#') + 1:], dataset))
                        r2keys = list(filter(lambda k: k != 'record_ID' and k != 'record_blocks', candidate_record_data[0].keys()))
                        
                        if can_skip_comparison(record_data[0], candidate_record_data[0], r1keys, r2keys):
                            is_clustered = True
                            continue
                        
                        if  edit_distance(record_data[0], candidate_record_data[0], r1keys, r2keys):
                            clusters[cluster].extend(clusters[candidate_cluster])
                            clusters[candidate_cluster] = []
                            is_clustered = True
                            break

                    if is_clustered:
                        break
                if is_clustered:
                    break
            if is_clustered:
                break
    
    return clusters


def read_dataset_sources(dataset_path):
    iter = 1
    total_time = 0

    for subdir in os.listdir(dataset_path):
        cur_table = list(filter(lambda record: subdir in record['record_ID'], schema_mapping))
        
        print(f'\n--------------------- ITERATION {iter} ---------------------')    
        start_time = time.time()
        
        result = firla(cur_table)
        
        end_time = time.time()
        execution_time = end_time - start_time
        total_time += execution_time

        print(f'Iteration time Time: {execution_time:.2f} s')
        print(f'Execution Time: {total_time:.2f} s')

        with open(f'firla\\clusters\\iteration{iter}.json', 'w', encoding='utf-8') as res_file:
            json.dump(result, res_file, indent=4, ensure_ascii=False)
        
        iter += 1
        break

if __name__ == "__main__":
    offset = 0
    clusters = {}
    clusters_blocks = {}

    with open('firla\\schema_mapping_firla.json', 'r', encoding='utf-8') as schema_file:
        schema_mapping = json.load(schema_file)
    
    profiler = cProfile.Profile()
    profiler.enable()
    read_dataset_sources(DATASET_PATH)
    profiler.disable()
    profiler.dump_stats("profile_output.prof")

    # Load the profiling results
    stats = pstats.Stats("profile_output.prof")
    stats.sort_stats(pstats.SortKey.TIME)
    stats.print_stats(100)
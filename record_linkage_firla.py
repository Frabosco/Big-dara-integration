# Da rivedere
import numpy as np

def concatenate_attributes(record: dict):
    attr_string = ''
    attributes = record.values()

    for attr in attributes:
        attr_string += attr.lower()
    
    return attr_string

def deduplication(records):
    temp = []
    res = dict()
 
    for key, val in records.items():
    
        if val not in temp:
            temp.append(val)
            res[key] = val
    
    return list(res.items())

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
    concatenated_records = {f'{i}': concatenate_attributes(dataset[i]) for i in range(len(dataset))}
    sorted_records = { k: v for k,v in sorted(concatenated_records.items(), key=lambda item: item[1])}
    unique_records = deduplication(sorted_records)
    
    clusters = {}
    for idx in range(len(unique_records)):
        clusters['C'+ str(idx)] = [unique_records[idx]]
    
    for cluster in list(clusters.keys()):
        is_clustered = False
        for rep_record in clusters[cluster]:
            for block in rep_record:
                for candidate_cluster in list(clusters.keys()):
                    if candidate_cluster == cluster:
                        continue
                    for candidate_record in clusters[candidate_cluster]:
                        sig1 = calculate_signature(rep_record)
                        sig2 = calculate_signature(candidate_record)
                        
                        if can_skip_comparison(sig1, sig2, theta):
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
            if is_clustered:
                break
        
        if not is_clustered:
            clusters[rep_record] = [rep_record]
    
    return clusters

# Example usage:
dataset = [
    {"name": "John Doe", "address": "123 Elm St"},
    {"name": "Jane Doe", "address": "123 Elm St"},
    {"name": "Jon Doe", "address": "123 Elm Street"},
    {"name": "J. Doe", "address": "123 Elm St."}
]
theta = 2

result_clusters = firla(dataset, theta)
print(result_clusters)

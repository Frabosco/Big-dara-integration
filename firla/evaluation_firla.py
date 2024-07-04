import re
import json
import pandas as pd
from sklearn.metrics import precision_score, recall_score, f1_score

def parse_labeled_dataset(file_path):
    df = pd.read_csv(file_path)
    matches = set()
    non_matches = set()
    
    for index, row in df.iterrows():
        
        if row['label'] == 1:
            matches.add((row['left_spec_id'], row['right_spec_id']))
        else:
            non_matches.add((row['left_spec_id'], row['right_spec_id']))
    
    return matches, non_matches

def extract_website_and_number(record_string):
    # Define the regular expression pattern to match the desired part
    pattern = r'(.+?\\\d+)\.json$'
    
    # Search for the pattern in the string
    match = re.search(pattern, record_string)
    
    # If a match is found, return the captured group
    if match:
        return match.group(1)
    else:
        return None

def extract_pairs_from_clusters(clusters):
    predicted_pairs = set()
    
    for cluster in clusters.values():
        
        for i in range(len(cluster)):
            
            for j in range(i + 1, len(cluster)):
                
                record1 = extract_website_and_number(cluster[i])
                record2 = extract_website_and_number(cluster[j])
                predicted_pairs.add((record1, record2))

    return predicted_pairs

def evaluate(true_pairs, pred_pairs):
    y_true = []
    y_pred = []
    
    all_pairs = list(true_pairs | pred_pairs)

    for pair in all_pairs:
        y_true.append(1 if pair in true_pairs else 0)
        y_pred.append(1 if pair in pred_pairs else 0)
    
    precision = precision_score(y_true, y_pred)
    recall = recall_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred)

    print(f"Precision: {precision}")
    print(f"Recall: {recall}")
    print(f"F1 Score: {f1}")

def main():
    with open('firla\\clusters\\iteration26.json', 'r', encoding='utf-8') as f:
        firla_clusters = json.load(f)
    
    true_pairs, false_pairs = parse_labeled_dataset('firla\\record_linkage_labelled_data.csv')
    pred_pairs = extract_pairs_from_clusters(firla_clusters)

    evaluate(true_pairs, pred_pairs)

if __name__ == '__main__':
    main()
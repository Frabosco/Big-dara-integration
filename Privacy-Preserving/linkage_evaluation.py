import json
import pandas as pd


with open("Privacy-Preserving\clusters\iteration26.json", 'r', encoding='utf-8') as json_file:
    clusters = json.load(json_file)
        
labeled_linkage = pd.read_csv("firla\\record_linkage_labelled_data.csv")

c=0
for i, pair in labeled_linkage.iterrows():
    falg=0
    for cluster in clusters:
        list=[i.strip("\\") for i in clusters[cluster]]
        if(pair[0].strip("\\") and pair[1].strip("\\") in list):
                flag=1
                break
    
    if flag==pair[2]:
        c+=1
        
print(c)
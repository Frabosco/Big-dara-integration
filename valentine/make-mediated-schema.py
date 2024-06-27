import os
import pickle
import multiprocessing
from multiprocessing import Process
from time import time
from valentine import valentine_match
import pandas as pd
from valentine.algorithms import Coma
import shutil
import json

ABS_PATH = os.path.dirname(os.path.abspath(__file__))
MATCHES_DIRECTORY = os.path.join(ABS_PATH, "matches")

def parseChunk(chunk, schemaList, schemaNames, processNumber):
    matcher = Coma(use_instances=True, java_xmx="2048m")
    matches = dict()
    n = 0
    for comparison in chunk:
        n += 1
        print(n)
        i = comparison[0]
        j = comparison[1]
        result = valentine_match(schemaList[i], schemaList[j], matcher, schemaNames[i], schemaNames[j])
        for key in result:
            score = result[key]
            matches[key] = score
    print(f"finished {processNumber} process")
    pickle.dump(matches, open(os.path.join(MATCHES_DIRECTORY, "matches" + str(processNumber)), 'wb'))

def load_schemas(dataSource):
    schemaList = []
    schemaNames = []
    source_folders = [f for f in os.listdir(dataSource) if os.path.isdir(os.path.join(dataSource, f))]
    
    for folder in source_folders:
        folderPath = os.path.join(dataSource, folder)
        json_files = [f for f in os.listdir(folderPath) if f.endswith('.json')]
        
        for json_file in json_files:
            filePath = os.path.join(folderPath, json_file)
            with open(filePath, 'r') as f:
                data = json.load(f)
                schemaList.append(pd.DataFrame([data]))  # Assuming each JSON file contains a single object
                schemaNames.append(folder)
                
    return schemaList, schemaNames

if __name__ == "__main__":
    # making matches
    start = time()
    numProcs = multiprocessing.cpu_count()
    procs = []
    parsers = []
    dataSource = os.path.join(ABS_PATH, "sources-json")
    
    schemaList, schemaNames = load_schemas(dataSource)
    
    totalSchemas = len(schemaList)
    totalComparisons = totalSchemas * (totalSchemas - 1) // 2
    comparisons = []
    chunk_size = totalComparisons // numProcs
    remainder = totalComparisons - chunk_size * numProcs
    
    for i in range(totalSchemas - 1):
        for j in range(i + 1, totalSchemas):
            comparisons.append((i, j))
    
    chunks = [comparisons[i: i + chunk_size] for i in range(0, len(comparisons) - remainder, chunk_size)]
    
    if remainder != 0:
        for i, elem in enumerate(comparisons[-remainder:]):
            chunks[i].append(elem)
    
    if os.path.exists(MATCHES_DIRECTORY):
        shutil.rmtree(MATCHES_DIRECTORY)
    os.mkdir(MATCHES_DIRECTORY)
    
    try:
        for i in range(numProcs):
            proc = Process(target=parseChunk, args=(chunks[i], schemaList, schemaNames, i))
            procs.append(proc)
            proc.start()
        for proc in procs:
            proc.join()
    except KeyboardInterrupt:
        print("\nclosing all processes")
        for proc in procs:
            proc.terminate()
        print("closed all processes")
        quit()
    
    # processing matches to create mediated schema
    allMatches = os.listdir(MATCHES_DIRECTORY)
    matches = dict()
    
    for fileName in allMatches:
        filePath = os.path.join(MATCHES_DIRECTORY, fileName)
        with open(filePath, 'rb') as pickleFile:
            m = pickle.load(pickleFile)
        for key in m:
            score = m[key]
            if score > 0.33:
                matches[key] = score
    
    sets = []
    
    for key in matches:
        dataset0 = key[0]
        dataset1 = key[1]
        combined = set()
        combined.add(dataset0)
        combined.add(dataset1)
        j = 0
        for s in sets:
            if dataset0 in s:
                temp = sets.pop(j)
                combined = combined.union(temp)
            j += 1
        j = 0
        for s in sets:
            if dataset0 in s:
                temp = sets.pop(j)
                combined = combined.union(temp)
            j += 1
        sets.append(combined)

    PROCESSED_MATCHES_DIRECTORY = os.path.join(ABS_PATH, "processed-matches")
    
    if os.path.exists(PROCESSED_MATCHES_DIRECTORY):
        shutil.rmtree(PROCESSED_MATCHES_DIRECTORY)
    os.mkdir(PROCESSED_MATCHES_DIRECTORY)
    
    for i, s in enumerate(sets):
        fileName = "matches" + str(i) + ".txt"
        filePath = os.path.join(PROCESSED_MATCHES_DIRECTORY, fileName)
        with open(filePath, 'w') as f:
            for elem in s:
                f.write(str(elem) + '\n')

    totalTime = str(time() - start)
    print(f"total time automatic process: {totalTime}")

    start = time()
    labelsMediatedSchema = []
    
    for s in sets:
        print(s)
        print("what name do you want to give to this set of attributes?")
        name = input()
        print()
        labelsMediatedSchema.append(name)

    schemas = []
    
    for folder in os.listdir(dataSource):
        folderPath = os.path.join(dataSource, folder)
        json_files = [f for f in os.listdir(folderPath) if f.endswith('.json')]
        
        for json_file in json_files:
            filePath = os.path.join(folderPath, json_file)
            with open(filePath, 'r') as jsonfile:
                data = json.load(jsonfile)
            for key in data:
                label = None
                for i, s in enumerate(sets):
                    for elem in s:
                        dataset = elem[0]
                        column = elem[1]
                        if dataset != folder:
                            continue
                        if key != column:
                            continue
                        label = labelsMediatedSchema[i]
                        break
                    if label:
                        break
                if not label:
                    print(f"{key} from {folder} needs a label, input wanted label:")
                    label = input()
                data[key] = label
            data["dataset"] = folder
            schemas.append(data)

    totalTime = str(time() - start)
    print(f"total time manual process: {totalTime}")
    
    mediatedSchemaPath = os.path.join(ABS_PATH, "mediated-schema.json")
    
    if os.path.exists(mediatedSchemaPath):
        os.remove(mediatedSchemaPath)
    
    with open(mediatedSchemaPath, "w") as json_file:
        json.dump(schemas, json_file, indent=4)

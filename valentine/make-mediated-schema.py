import os
import pickle
from multiprocessing import Process
from time import time
from valentine import valentine_match
from valentine.algorithms import Coma
import pandas as pd
import shutil
import json

ABS_PATH = os.path.dirname(os.path.abspath(__file__))
MATCHES_DIRECTORY = ABS_PATH + "/matches"

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
    pickle.dump(matches, open(MATCHES_DIRECTORY+"/matches"+str(processNumber), 'wb'))

if __name__ == "__main__":
    # making matches
    start = time()
    numProcs = 2 # numProcs = multiprocessing.cpu_count()
    procs = []
    parsers = []
    schemaList = []
    schemaNames = []
    dataSource = ABS_PATH + "\sources-json"
    
    for source in os.listdir(dataSource):
        sourcePath = os.path.join(dataSource, source)
        if os.path.isdir(sourcePath):
            dfs = []
            for file in os.listdir(sourcePath):
                filePath = os.path.join(sourcePath, file)
                if os.path.isfile(filePath):
                    with open(filePath) as f:
                        data_dict = json.load(f)
                    df = pd.DataFrame([data_dict])
                    dfs.append(df)
            combined_df = pd.concat(dfs)
            schemaList.append(combined_df)
            schemaNames.append(source)
                        
    totalSchemas = len(schemaList)
    totalComparisons = totalSchemas*(totalSchemas-1)//2
    comparisons = []
    chunk_size = totalComparisons // numProcs
    remainder = totalComparisons - chunk_size*numProcs
    for i in range(totalSchemas-1):
        for j in range(i+1, totalSchemas):
            comparisons.append((i,j))
    chunks = [comparisons[i : i + chunk_size] for i in range(0, len(comparisons)-remainder, chunk_size)]
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
        filePath = MATCHES_DIRECTORY + "/" + fileName
        with open(filePath, 'rb') as pickleFile:
            m = pickle.load(pickleFile)
        for key in m:
            score = m[key]
            if score>0.33: matches[key] = score
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

    PROCESSED_MATCHES_DIRECTORY = ABS_PATH + "/processed-matches"
    if os.path.exists(PROCESSED_MATCHES_DIRECTORY):
        shutil.rmtree(PROCESSED_MATCHES_DIRECTORY)
    os.mkdir(PROCESSED_MATCHES_DIRECTORY)
    for i, s in enumerate(sets):
        fileName = "matches"+str(i)+".txt"
        filePath = PROCESSED_MATCHES_DIRECTORY + "/" + fileName
        with open(filePath, 'w') as f:
            for elem in s:
                f.write(str(elem) + '\n')

    totalTime = str(time() - start)
    print(f"total time automatic process: {totalTime}")

    start = time()
    labelsMediatedSchema = []
    for s in sets:
        print(s)
        print("what name do you want to give to this set of attibutes?")
        name = input()
        print()
        labelsMediatedSchema.append(name)

    SOURCES_DIRECTORY = ABS_PATH + "/sources-json"
    files = os.listdir(SOURCES_DIRECTORY)
    schemas = []
    for source in os.listdir(dataSource):
        i = 0
        sourcePath = os.path.join(dataSource, source)
        if os.path.isdir(sourcePath):
            for file in os.listdir(sourcePath):
                if i == 1: break
                filePath = os.path.join(sourcePath, file)
                with open(filePath, 'r') as jsonfile:
                    data = json.load(jsonfile)
                for key in data:
                    label = None
                    for i, s in enumerate(sets):
                        for elem in s:
                            dataset = elem[0]
                            column = elem[1]
                            if dataset != file: continue
                            if key != column: continue
                            label = labelsMediatedSchema[i]
                            break
                        if label: break
                    if not label:
                        print(f"{key} from {file} needs a label, input wanted label:")
                        label = input()
                    data[key] = label
                data["dataset"] = file
                schemas.append(data)
                i = i + 1

    totalTime = str(time() - start)
    print(f"total time manual process: {totalTime}")
    mediatedSchemaPath = ABS_PATH + "/mediated-schema.json"
    if os.path.exists(mediatedSchemaPath):
        os.remove(mediatedSchemaPath)
    with open(mediatedSchemaPath, "w") as json_file:
        json.dump(schemas, json_file, indent=4)
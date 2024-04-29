import pandas as pd
import re
import html
import tarfile
import sys
from collections import defaultdict
import tarfile
import json
import logging
from concurrent.futures import ProcessPoolExecutor,wait,ALL_COMPLETED


file_path = 'ss_not_open.txt'

with open(file_path, 'r') as file:
    missing = [line.strip() for line in file]
    
    
type_field =["book-section", "book", "book-chapter", "book-part", "book-series", "book-set", "book-track", "edited-book", "reference-book", "monograph", "journal-article", "dissertation", "other", "peer-review", "proceedings", "proceedings-article", "reference-entry", "report", "report-series", "standard", "standard-series", "posted-content", "dataset"]
publishers = ["Test accounts", "CrossRef Test Account"]
author_names = [",", "none none", "none, none", "none &na;", "(:null)", "test test test", "test test", "test", "&na; &na;"]



def do(name, df):
    try: 
        print("processing file:" + name)
        
        authors = []
        for i in range(len(df)):
            if 'author'  in df['items'][i] and 'title' in  df['items'][i]:
                if len(df['items'][i]['title'])>0 and df['items'][i]['publisher'] not in publishers:
                    authors.append(i)

        crossref_auth = df.iloc[authors].copy()

        crossref_auth.reset_index(inplace= True)
        crossref_auth.drop(columns = ['index'], inplace = True)

        crossref_auth.loc[:, 'DOI'] = crossref_auth['items'].apply(lambda x: x['DOI'])
        crossref_auth.loc[:,'authors'] = crossref_auth['items'].apply(lambda x: x['author'])
        crossref_auth.loc[:,'type'] = crossref_auth['items'].apply(lambda x: x['type'])

        def get_name(k):
            return list(set([crossref_auth['authors'][k][i]['given'] + " " + crossref_auth['authors'][k][i]['family'] if 'given'in crossref_auth['authors'][k][i] else '' for i in range(len(crossref_auth['authors'][k]))]))
            
        names = [get_name(k) if get_name(k)[0] not in author_names else '' for k in range(len(crossref_auth))]

        crossref_auth['names'] = names

        remove_rows = [i for i in range(len(crossref_auth)) if len(crossref_auth['names'][i]) == 0 or (crossref_auth['names'][i][0] == 'Addie Jackson' and crossref_auth['items'][i]['publisher'] =="Elsevier BV") or crossref_auth['type'][i] not in type_field]
        crossref_auth_final= crossref_auth.drop(remove_rows)
        crossref_auth_final.reset_index(inplace = True)
        dois = list(crossref_auth_final['DOI'])
        intersection = sorted(set(dois).intersection(missing))

        with open('missing_dois/'+str(name)+".txt", "w") as file:
            for item in intersection:
                file.write(item + "\n")

        
    except Exception as Argument:
        logging.exception("Error in thred code for file: " + name)

if __name__ == "__main__":
    
    
    i = 1
    data = []
    numberOfThreads = int(sys.argv[2])
    executor = ProcessPoolExecutor(max_workers=numberOfThreads)
    
    with tarfile.open(sys.argv[1], "r:gz") as tar:
        while True:
            member = tar.next()
            # returns None if end of tar
            if not member:
                break
            if member.isfile():
                
                print("reading file: " + member.name)
    
                current_file = tar.extractfile(member)
    
                df = pd.read_json(current_file, orient='records')
                # print(crossref_df)
                data.append((member.name, df))
                i += 1
    
                if (i > numberOfThreads):
                    print("execute batch: " + str([name for (name, d) in data]))
                    futures = [executor.submit(do, name, d) for (name, d) in data]
                    done, not_done = wait(futures)
                    
                    # print(done)
                    print(not_done)
    
                    data = []
                    i = 1
    
        futures = [executor.submit(do, name, d) for (name, d) in data]
        done, not_done = wait(futures)
        print(not_done)
    
        print("Done")
    

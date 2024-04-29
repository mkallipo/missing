import pandas as pd
import json
from itertools import chain
import os
import sys



folder_path = '/data/crossref/datacite_dump'


with open('/data/crossref/missing_dois/in_crossref/not_in_crossref.txt', 'r') as file:
    not_in_crossref = [line.strip().lower() for line in file]
    

df_list = [ folder_path + "/" + filename for filename in os.listdir(folder_path)]



def in_datacite(p):
    datacite = pd.read_parquet(p)

    dois = [x.lower() for x in list(datacite['doi'])]
    intersection = sorted(set(dois).intersection(not_in_crossref))
    print('done!')
    return intersection
   

    

for i, p in enumerate(df_list):
    print(i)
    try:
        with open("in_datacite/"+str(i)+".txt", "w") as file:
            for item in  in_datacite(p):

            # Write each item to the file
                file.write(item + "\n")
    except Exception as e:
        print(f"An error occurred: {e}") 
import pandas as pd
import tarfile
import sys
import tarfile
import logging
from concurrent.futures import ProcessPoolExecutor,wait,ALL_COMPLETED


file_path = '/data/crossref/ss_not_open.txt'

with open(file_path, 'r') as file:
    missing = [line.strip() for line in file]
    


def do(name, df):
    try: 
        print("processing file:" + name)
        

        df.loc[:, 'DOI'] = df['items'].apply(lambda x: x['DOI'])
       
        dois1 = list(df['DOI'])
        dois = list(map(lambda x: x.lower().strip(), dois1))     
        
        intersection = sorted(set(dois).intersection(missing))
        #not_in_crossref = set(missing).difference(set(dois))


        with open('in_crossref/'+str(name[:-5])+".txt", "w") as file:
        # Iterate through the list
            for item in intersection:
                # Write each item to the file
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
    

This repository contains:

**ss_not_open.ipynb**: A Jupyter notebook that creates the `ss_not_open.txt` file of DOIs found in Semantic Scholar but not in OpenAIRE. 
  - Input 1: `all_dois.csv`: a CSV with all DOIs in OpenAIRE.
  - Input 2:  DOIs in Semantic Scholar

**in_crossref.py**: A python script to obtain the missing dois that are in Crossref. 
  - Input 1: the file `ss_not_open.txt` that contains the DOIs of Semantic Scholar that do not appear in OpenAIRE (output of `ss_not_open.ipynb`).
  - Input 2: latest Crossref's dump (a tar.gz of JSON files). 

**in_crossref_criteria.py**: A python script to obtain the missing dois that are in Crossref and satisfy the [inclusion criteria](https://graph.openaire.eu/docs/graph-production-workflow/aggregation/non-compatible-sources/doiboost?_highlight=crossref#crossref-filtering). 
  - Input 1: `in_crossref.txt`: the txt output of `in_crossref.py` (or the `ss_not_open.txt` file).
  - Input 2: latest Crossref's dump (tar.gz of JSON files).
    
**in_datacite.py**: A python script to obtain the missing dois that are in DataCite. 
  - Input 1: the file `ss_not_open.txt`.
  - Input 2: latest DataCite dump (Parquet files).

**not_open.ipynb** A Jupyter notebook for analyzing the collected data.

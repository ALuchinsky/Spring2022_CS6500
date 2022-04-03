## [AL]

### Data collection

In the directory `data/papers/` we have collected three json files (300Mb in total), that contain information about 3000 papers published in 2021. These files were downloaded using commands like

    wget "https://inspirehep.net/api/literature?sort=mostrecent&size=1000&page=1&q=date%202021&subject=Phenomenology-HEP" -O papers_1000_1.json

the corresponding INSPIRES (https://inspirehep.net/) request is

    date 2021

Because of large size these files are not included in the repository

### Data Analysis

Downloaded data was analyzed in two python notebooks.

First, in file `notebooks\notebooks/AL_read_papers.ipynb` we are reading raw JSON files, extracting for each paper such information as titles, number of citations, assigned by authors and editors keyword, etc. After that top 20 keywords are transformed into dummy variables and the resulting table is saved into directory `data/processed/dummy.json/`. The size of this directory is much smaller, about 1.5MB

The resulting table is analyzed in the notebook ` notebooks/AL_kmeans_clustering.ipynb`. It makes a first attempt to do KMeans clustering. Some results are produced, but right now no analysis or interpretation of this clustering is done.

## TODO

It seems to me that next steps to be done are:

- Figure out, how to analyze and interpret the clustering result
- What other fields should we include in the analysis?
- What is the optimal number of keywords?
- Use also keywords generated automatically from titles and abstracts of the papers.
- PCA

## JM

### abstractWordExtraction

Read in JSON files which was adapted from Aleksei's branch.
Created a new Dataframe that has the count of each word in the abstract ordered by ID of paper.
The ID is a unique number given to each paper so that the words can later be added to the paper they belong to.

## Old READMEs

In files `README_scala.md` and `README_pyspark.md` you can find information about scala scripts and CLI pyspark scripts that were used earlier to study the data set. This information is not very interesting now.

Clustering High Energy Physics Papers using keywords assigned by the authors/editors or extracted automatically from the abstracts. KMeans and Bisected KMeans methods are used.

The general data flow is shown in figure below

![plot](./data_flow.pdf)

Data files are not included in the repository, so you'll have to run all the steps on your computer. Let us discuss each of them separately

## Data collection

Raw data can be downloaded from INSPIRES web site. In this project we are analyzing papers added to the database in year 2000, the corresponding data files should be located in directory `data/papers_2000/`. There are 8 files in this directory, that can be downlaoded with commands like

    wget "https://inspirehep.net/api/literature?sort=mostrecent&size=1000&page=1&q=date%202000&subject=Phenomenology-HEP" -O papers_1000_1.json

(you should change numbers in `page=?` and `papers_1000_?` parts to download the other pages)

These files contain lots of unnecessary information, so the data should be cleaned. This is done in notebook `notebooks/AL_read_papers.ipynb`. The resulting dataset is saved into directory `data/processed/papers_2000/short_papers/`.

## Keywords Extraction

For each of the papers in this short dataset we can extract keywords either from the list of the assigned keywords or from the abstracts. This is done in the notebooks `notebooks/kws_assigned.ipynb` and `notebooks/kws_abstracts.ipynb` respectively. The outputs are save to the directories `data/processed/papers_2000/kws/dummy/` and `data/processed/papers_2000/abs/dummy/`

## Clustering

We are using KMeans and BisectKMeans approaches to do the clustering. It is clear, that the algorithm does not depend on the keywords source (preassigned of extracted from abstracts), so only two Jupyter notebook are used: `notebooks/KMeans.ipynb` and `notebooks/BKeans.ipynb`. In each notebook you can change the data path (`papers_2000/kws/` or `papers_2000/abs`) by specifying it in
data_path = "/papers_2000/kws/"
line. As a result the notebook will read all dummy files, make the clustering and save some results in the corresponding directory.

## Results Comparison

The final comparison of the results is done by the `notebooks/Compare.ipynb` file.

## old README

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

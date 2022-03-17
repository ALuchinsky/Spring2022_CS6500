# Project Proposal

## 1. Introduction

A vast number of papers devoted to theoretical, experimental, and phenomenological studies in high energy physics can be found online. Examples of the well-known databases are www.arxiv.org, https://inspirehep.net, as well as others. With the help of these databases, it is easy to find a paper using some known data, such as names of the authors, title of the article, publication information, etc. It could be difficult, however, to start digging into the field without such knowledge. 

One of the possible solutions is to use data analysis methods to perform a classification of the text corpus, performing keyword extraction, or doing article suggestions. Up to our knowledge the only existing project that tries to solve the mentioned task is https://www.semanticscholar.org/. In our project we will try to reproduce these results using all available data science methods.

## 2. Methodology

As a first step of our work, we are going to download all the required data and automatically extract some keywords from titles and abstracts. After that, the obtained features (as well as such properties as author list, publication type, etc.) will be used for further analysis.

We would consider cluster analysis with Hierarchical, K-means or Model based clustering algorithms as a priority. If one or more methods of clustering process goes well, that will be the whole focus of our analysis. However, if all three of the algorithms do not turn out to have useful or acceptable output, the backup plan is to apply other analytics as substitutions: Classification Analysis (Decision trees, KNN or Neural network) or Segmentation Analysis (Factor Segmentation, Latent Class Clustering). Since we have prepared a large variety of methods to apply in our project, the risk of our project would not be extremely high.

It should also be noted that in the beginning we are going to analyze some small subset of the available data. The developed methods will be used later for larger datasets.

## 3. Timeline

* Project proposal.	Due by 3/18
* Methodology examination.	Due by 3/25
* Code/product development	Due by 4/10
* Experiment, Result analysis.	Due by 4/17
* Conclusion and future work, Compile a report/manuscript.	Due by 4/25
* PPT and Presentation Preparation	

## 4. Reference

* R Ivanov and L Raae, “INSPIRE: A new scientific information system for HEP”, J. Phys.: Conf. Ser. (2010) 219 082010

* Simeon Warner, “Open Archives Initiative protocol development and implementation at arXiv”, arXiv:cs/0101027

* Waleed Ammar et al. “Construction of the Literature Graph in Semantic Scholar”, arXiv:1805.02262 [cs.CL]
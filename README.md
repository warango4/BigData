# Project 3: Text Mining

## Team members:

* Wendy Maria Arango warango4@eafit.edu.co
* Manuela Carrasco mcarras1@eafit.edu.co

## Development Environment

To access to the project in Databricks Community enter here:

[Project 3: Text mining](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/7331743577824378/4013226145831667/8239666868831222/latest.html)

### Cluster

In order to create the cluster use Databricks Community with the following configuration:

* Databricks Runtime Version 5.2 (includes Apache Spark 2.4.0, Scala 2.11)
* Python 3

### Data

For testing, use the following news datasets.

[News Datasets](https://www.kaggle.com/snapcrack/all-the-news)

## Metodology

### CRISP-DM (Cross Industry Standard Process for Data Mining)

CRISP-DM [1] is an open standard process model that describes common approaches used by data mining experts. It is the most widely-used analytics model.

![alt text](https://www.researchgate.net/profile/Vernon_Dsouza/publication/326235288/figure/fig1/AS:645518493495296@1530915010595/CRISP-DM-Model-Taylor-2017.png)

### Phases

1. **Bussiness understanding:** Objectives and requirements from a non-technical perspective.
2. **Data understanding:** Become familiar with the data keeping in mind the business objectives.
3. **Data preparation:** Get the minable view or dataset.
4. **Modeling:** Apply data mining techniques to dataset.
5. **Evaluation:** From the previous phase models to determine if they are useful for business needs.
6. **Deployement:** Exploit the usefulness of the models, integrating them into the decision-making tasks of the organization.

### Business understanding

Mining or text analytics are a set of models, techniques, algorithms and technologies that allow the processing of text of a NON-STRUCTURED nature.

Text mining allows the text to be transformed into a structured form, in such a way that it facilitates a series of applications such as text search, relevance of documents, natural understanding of the language, automatic translation between languages, analysis of feelings, topic detection among many other applications. Perhaps the simplest processing of all, be the wordcount, which is to determine the frequency of the word per document or the entire dataset.

### Data Understanding

The work data for the project is a set of news published in kaggle.

### Data preparation

The news was pre-processed to prepare the data for analytics, with the following rules:

* Remove special characters (.,%()'"...)
* Remove stop words
* Remove words of longitude 1

### Modeling

In this case we used an inverted index where for each word that is entered by keyboard in the Notebook, list in descending order by word frequency in the content "title" of the news, the most relevant news.

<Frec, news_id, title>

* Frec: Frequency of the word.
* news_id: New's id in the dataset.
* title: New's title.

### Evaluation

Carry out grouping of news using one of different algorithms and models of clustering or similarity, in such a way that allows any news to identify that other news is similar.

A metric/similarity function was used based on the intersection of the most common common words for each news item, it is suggested to have 10 most frequent words for news whose frequency is greater than 1.

| News  | Top 10 most frequently words by news |
| :---: | :----------------------------------: |
| new1  | [(word1, f1) ... (wordn, fn)]        |
| ...   | ....                                 |
| newM  | [(word1, f1) ... (wordn, fn)]        |

Where for each news_id passed by the notebook, list in descending order by similitary the 5 news more related with the id.

## References

[1] Wikipedia. (2019). Cross-industry standard process for data mining. [online] Available at: https://en.wikipedia.org/wiki/Cross-industry_standard_process_for_data_mining [Accessed 26 April 2019].
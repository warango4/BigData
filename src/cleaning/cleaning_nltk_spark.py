"""This is meant to be used in Databricks notebook. To use this code you need to add nltk.whl to
the cluster you have your project attached to"""

import glob
import os
import nltk
nltk.download('punkt')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
nltk.download('stopwords')
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

stop_words = set(stopwords.words('english'))
quotation = [".", "?", "!", ",", ";", ":", "-", "_", "[", "]", "{", "}", "(", ")", "...", "\'", "\"", '“', '’', '”', "$", "%", "^", "&", "*", "-","\\", "/", "@", "!", "—"]
for i in quotation: stop_words.add(i)
used_columns = ['id', 'title', 'content']
  
def cleaning_content(content):  
  words = word_tokenize(content)
  cleaned_str = []
  for word in words:
      word = word.lower()
      if word not in stop_words:
          cleaned_str.append(word)
    
  return ' '.join(cleaned_str)

infer_schema = "true"
first_row_is_header = "true"
delimiter = ","
path_to_file = "dbfs:///FileStore/tables/all-news/*.csv"
df_file = spark.read \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .csv(path_to_file) \
    .select('id', 'title', 'content') 

cleaning_udf = udf(cleaning_content)
content_df = df_file.withColumn("content", cleaning_udf(df_file.content)).na.fill("null")
title_df = content_df.withColumn('title', cleaning_udf(content_df.title)).na.fill("null")

display(title_df)
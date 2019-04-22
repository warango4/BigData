"""This is meant to be used in Databricks notebook. To use this code you need to add nltk.whl to
the cluster you have your project attached to"""

import pandas
import glob 
import os
import nltk
nltk.download('punkt')
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
nltk.download('stopwords')

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
  

folder = "/dbfs/FileStore/tables/all-news/"
dataset = glob.glob(folder + "*.csv")
file_word = 'file'
i = 0

for file_name in dataset:
    print(file_name)
    data_frame = pandas.read_csv(file_name, skipinitialspace=True, usecols=used_columns)
    data_frame['content'].dropna(inplace=True)
    data_frame['content'] = data_frame['content'].apply(cleaning_content)
    i += 1
    data_frame.to_csv(folder + file_word + str(i) + ".csv", sep='\t')
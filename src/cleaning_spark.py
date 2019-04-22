from pyspark.sql.functions import udf, col, lower, regexp_replace
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from nltk.stem.snowball import SnowballStemmer
from pyspark.sql.types import ArrayType, StringType

file_location = "dbfs:///FileStore/tables/all-news/*.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_file = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location) \
  .select('id', 'title', 'content') 

# Delete punctuation
df_cleaned_title = df_file.select('id', (lower(regexp_replace('title', "[^a-zA-Z\\s]", "")).alias('title')), 'content')
df_cleaned = df_cleaned_title.select('id', 'title', (lower(regexp_replace('content', "[^a-zA-Z\\s]", "")).alias('content')))

# Tokenize title
title_tokenizer = Tokenizer(inputCol='title', outputCol='tokenized_title')
df_tokenized_title = title_tokenizer.transform(df_cleaned).select('id', 'tokenized_title', 'content')

# Remove stopwords from title
stopwords_title_remover = StopWordsRemover(inputCol='tokenized_title', outputCol='cleaned_title')
df_title_removed_stopwords = stopwords_title_remover.transform(df_tokenized_title).select('id', 'cleaned_title', 'content')

# Tokenize content
content_tokenizer = Tokenizer(inputCol='content', outputCol='tokenized_content')
df_tokenized_content = content_tokenizer.transform(df_title_removed_stopwords).select('id', 'cleaned_title', 'tokenized_content')

# Remove stopwords from content
stopwords_remover = StopWordsRemover(inputCol='tokenized_content', outputCol='cleaned_content')
df_removed_stopwords = stopwords_remover.transform(df_tokenized_content).select('id', 'cleaned_title', 'cleaned_content')

display(df_removed_stopwords)
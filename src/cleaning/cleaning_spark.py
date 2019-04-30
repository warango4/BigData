from pyspark.sql.functions import udf, col, lower, regexp_replace, concat_ws
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.types import ArrayType, StringType, IntegerType

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
  .select('id', 'title', 'content') \
  .na.drop()

# Delete punctuation
df_cleaned = df_file.select('id', (lower(regexp_replace('title', "[^a-zA-Z\\s]", "")).alias('title')), \
                                     (lower(regexp_replace('content', "[^a-zA-Z\\s]", "")).alias('content')))

# Tokenize title
title_tokenizer = Tokenizer(inputCol='title', outputCol='tokenized_title')
df_tokenized_title = title_tokenizer.transform(df_cleaned).select('id', 'tokenized_title', 'content')

# Remove stopwords from title
stopwords_title_remover = StopWordsRemover(inputCol='tokenized_title', outputCol='cleaned_title')
df_title_removed_stopwords = stopwords_title_remover.transform(df_tokenized_title).select('id', 'cleaned_title', 'content')

# Clean words whose lenght is less than 1
filter_length_udf = udf(lambda row: [x for x in row if len(x) > 1], ArrayType(StringType()))
df_final_title = df_title_removed_stopwords.withColumn('cleaned_title', filter_length_udf(col('cleaned_title')))

# Tokenize content
content_tokenizer = Tokenizer(inputCol='content', outputCol='tokenized_content')
df_tokenized_content = content_tokenizer.transform(df_final_title).select('id', 'cleaned_title', 'tokenized_content')

# Remove stopwords from content
stopwords_remover = StopWordsRemover(inputCol='tokenized_content', outputCol='cleaned_content')
df_removed_stopwords = stopwords_remover.transform(df_tokenized_content).select('id', 'cleaned_title', 'cleaned_content')

# Filter length in content
df_final = df_removed_stopwords.withColumn('cleaned_content', filter_length_udf(col('cleaned_content')))

# Make title and content strings and id an integer
df_final = df_final.withColumn('cleaned_title', concat_ws(" ", 'cleaned_title')) \
           .withColumn('cleaned_content', concat_ws(" ", 'cleaned_content')) \
           .withColumn('id', df_final['id'].cast(IntegerType())) \
           .select('id', col('cleaned_title').alias('title'), col('cleaned_content').alias('content')) 
           
display(df_final)
#df_final.count()
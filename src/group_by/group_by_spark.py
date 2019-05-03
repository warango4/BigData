# Databricks notebook source
from pyspark.sql.functions import udf, col, lower, regexp_replace, concat_ws, trim
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.types import ArrayType, StringType, IntegerType

# Online

new_df = df_file.withColumn('id', df_file['id'].cast(IntegerType())) \
                .select('id', 'title', 'content') 

file_rdd = new_df.rdd.map(lambda x: (x['id'], x['title']))
file_map = file_rdd.collectAsMap()

# COMMAND ----------

dbutils.widgets.text("word", "Please enter word to search")
dbutils.widgets.text("search", "Please enter id to search")

# COMMAND ----------

toSearch = str(dbutils.widgets.get("word"))
final_result = inverted_index_rdd.filter(lambda x, toSearch=toSearch: x[0] == toSearch) \
                                 .flatMap(lambda result: result[1])

final_result_list = final_result.collect()
print(final_result_list)

# COMMAND ----------

def printing_result(): 
  cont = 0
  maximum = 5
  for i in final_result_list:
    if cont == maximum: break
    if i[0] != None:
      cont += 1
      yield i[1], list(((k, v) for k, v in file_map.items() if k == i[0]))

print(list(printing_result()))

# COMMAND ----------

from collections import defaultdict

def accumulate2(l):
  d = defaultdict(list)
  for k, *v in l:
    d[k].append(sum(v))
  for k in d.keys():
    yield k, len(d[k])

news_rdd = articles_rdd.flatMap(lambda line: [(line[0] , (word, 1)) for word in (line[1] + " " + line[2]).split(" ")]) \
                       .groupByKey() \
                       .map(lambda word: (word[0], list(word[1]))) \
                       .filter(lambda x: x[0] != None) \
                       .map(lambda lista: (lista[0], sorted(list(accumulate2(lista[1])), key = lambda x: -x[1]))) \
                       .cache()

news_rdd.count()

# COMMAND ----------

from functools import reduce

id_search = int(dbutils.widgets.get("search"))
if not id_search in file_map or id_search == None: print("Not found")
else:
  new_title = file_map[id_search]
  in_new_rdd = news_rdd.filter(lambda x, id_search=id_search: x[0] == id_search)  
  in_new_list = in_new_rdd.collect()
  
  def news_similarity2(rdd_other_news):
    list1 = in_new_list[0][1]
    list2 = rdd_other_news[1]
    list3 = []
    for value in list1:
      for v in list2:
        if value[0] == v[0]: list3.append(value[0]) 
    union = list1 + list2
    distance_list = list(filter((lambda x, list3=list3: x[0] in list3), union))
    if len(distance_list) != 0:
      last_result = reduce(lambda a, b: (a[0], a[1] + b[1]) if a[0] != "" and b[0] != "" else 0, distance_list)[1]
      result = [rdd_other_news[0], last_result, len(list3)]
    else:
      result = [rdd_other_news[0], 0, 0]
    return result

  other_news = news_rdd.filter(lambda x, id_search=id_search: x[0] != id_search) 
  sim_news = other_news.map(news_similarity2) \
                          .sortBy(lambda x: -x[1])
                          
  sim_news_df = sim_news.toDF(["id","similarity","words"])
  display(sim_news_df)

# COMMAND ----------

aux = sim_news.take(5)
news_result_final = []
for i in aux:
  news_result_final.append(i[0])
print(id_search, new_title, news_result_final)

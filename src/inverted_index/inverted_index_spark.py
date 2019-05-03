import itertools
import operator

articles_rdd = df_final.rdd.map(lambda x: (x['id'], x['title'], x['content']))

# Inverted index

def accumulate(l):
  it = itertools.groupby(l, operator.itemgetter(0))
  for key, subiter in it:
     yield key, sum(item[1] for item in subiter)


inverted_index_rdd = articles_rdd.flatMap(lambda line: [(word , (line[0], 1)) for word in (line[1] + " " + line[2]).split(" ")]) \
                                 .groupByKey() \
                                 .map(lambda word: (word[0], list(word[1]))) \
                                 .map(lambda lista: (lista[0], sorted(list(accumulate(lista[1])), key = lambda x: -x[1]))) \
                                 .cache()


inverted_index_rdd.take(5)


# Online

new_df = df_file.withColumn('id', df_file['id'].cast(IntegerType())) \
                .select('id', 'title', 'content') 

file_rdd = new_df.rdd.map(lambda x: (x['id'], x['title']))
file_map = file_rdd.collectAsMap()

# Search

dbutils.widgets.text("word", "Please enter word to search")
toSearch = str(dbutils.widgets.get("word"))

final_result = inverted_index_rdd.filter(lambda x, toSearch=toSearch: x[0] == toSearch) \
                                 .flatMap(lambda result: result[1])

final_result_list = final_result.collect()
print(final_result_list)

def printing_result(): 
  cont = 0
  maximum = 5
  for i in final_result_list:
    if cont == maximum: break
    if i[0] != None:
      cont += 1
      yield i[1], list(((k, v) for k, v in file_map.items() if k == i[0]))

# Final result - Top 5 news by largest frequency
print(list(printing_result()))
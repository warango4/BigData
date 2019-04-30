import itertools
import operator

if __name__ == '__main__':
  articles_rdd = df_final.rdd.map(lambda x: (x['id'], x['title'], x['content']))
  print(articles_rdd.take(5))


# Inverted index

import itertools
import operator

inverted_index_rdd = articles_rdd.flatMap(lambda line: [(word , (line[0], 1)) for word in (line[1] + " " + line[2]).split(" ")]) \
                                 .groupByKey() \
                                 .map(lambda word: (word[0], list(word[1]))) \
                                 .map(lambda lista: (lista[0], sorted(list(accumulate(lista[1])), key = lambda x: -x[1])))

def accumulate(l):
  it = itertools.groupby(l, operator.itemgetter(0))
  for key, subiter in it:
     yield key, sum(item[1] for item in subiter)
inverted_index_rdd.take(5)
import re

from pyspark import SparkConf, SparkContext


def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("file:///Users/sgardziewicz/Documents/SparkCourse/Book")
words = input.flatMap(normalize_words)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda xy: (xy[1], xy[0])).sortByKey(ascending=False)

print(wordCountsSorted.collect())
# for word, count in wordCounts.items():
#     cleanWord = word.encode('ascii', 'ignore')
#     if cleanWord:
#         print(cleanWord, count)

### using dataframes


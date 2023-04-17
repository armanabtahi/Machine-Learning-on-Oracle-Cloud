import re
from operator import add
from pyspark import SparkContext

namespace_name=
bucket_name=
sc = SparkContext(appName="WordCount")

words=sc.textFile("oci://{}@{}/WordCount/input/pg2701.txt".format(bucket_name,namespace_name)).flatMap(lambda line: re.findall('[a-z]+',line.lower()))
word_counts = words.map(lambda word:(word, 1)).reduceByKey(add)

word_counts.saveAsTextFile("oci://{}@{}/WordCount/output/word_counts".format(bucket_name,namespace_name))

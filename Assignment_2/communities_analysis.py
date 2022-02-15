import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, desc, split, explode
from graphframes import *
import re
from operator import add

spark = SparkSession.builder.appName('sg.edu.smu.is459.assignment2').getOrCreate()

# Load data
posts_df = spark.read.load('/user/bluesky3/hardwarezone.parquet')

# Clean the dataframe by removing rows with any null value
posts_df = posts_df.na.drop()
# posts_df.show()

# Statistical information of the posts
author_count = posts_df.select('author','topic').distinct().groupBy('author').count()
# author_count.sort(desc('count')).show()

# print('# of topics: ' + str(posts_df.select('topic').distinct().count()))

# Find distinct users
author_df = posts_df.select('author').distinct()

# print('Author number :' + str(author_df.count()))

# Assign ID to the users
author_id = author_df.withColumn('id', monotonically_increasing_id())
# author_id.show()

# Construct connection between post and author
left_df = posts_df.select('topic', 'author') \
    .withColumnRenamed("topic","ltopic") \
    .withColumnRenamed("author","src_author")

right_df =  left_df.withColumnRenamed('ltopic', 'rtopic') \
    .withColumnRenamed('src_author', 'dst_author')

# Self join on topic to build connection between authors
author_to_author = left_df \
    .join(right_df, left_df.ltopic == right_df.rtopic) \
    .select(left_df.src_author, right_df.dst_author)

# edge_num = author_to_author.count()
# print('Number of edges with duplicate : ' + str(edge_num))

# Convert it into ids
id_to_author = author_to_author \
    .join(author_id, author_to_author.src_author == author_id.author) \
    .select(author_to_author.dst_author, author_id.id) \
    .withColumnRenamed('id','src')

id_to_id = id_to_author \
    .join(author_id, id_to_author.dst_author == author_id.author) \
    .select(id_to_author.src, author_id.id) \
    .withColumnRenamed('id', 'dst')

id_to_id = id_to_id.filter(id_to_id.src > id_to_id.dst) \
    .groupBy('src','dst') \
    .count() \
    .withColumnRenamed('count', 'n')

id_to_id = id_to_id.filter(id_to_id.n >= 5)

id_to_id.cache()

# print("Number of edges without duplicate :" + str(id_to_id.count()))

# Build graph with RDDs
graph = GraphFrame(author_id, id_to_id)

# For complex graph queries, e.g., connected components, you need to set
# the checkpoint directory on HDFS, so Spark can handle failures.
# Remember to change to a valid directory in your HDFS
spark.sparkContext.setCheckpointDir('/user/bluesky3/spark-checkpoint')

# Connected components of the communities
cc_graph = graph.connectedComponents()
grouped_cc_graph = cc_graph.groupBy('component') \
    .count() \
    .withColumnRenamed('count', 'n')

sorted_cc_graph = grouped_cc_graph.orderBy(grouped_cc_graph.n.desc())

print("The size of the largest community in Hardware Zone is: " + str(sorted_cc_graph.head()[1]))

# Most frequent keywords of the community (frequent words)
topic_df = sorted_cc_graph \
    .join(cc_graph, sorted_cc_graph.component == cc_graph.component) \
    .join(left_df, cc_graph.author == left_df.src_author) \
    .select(cc_graph.author, left_df.ltopic, sorted_cc_graph.n) \
    .orderBy(sorted_cc_graph.n.desc()) \
    .withColumnRenamed('ltopic', 'topic')

topic_df = topic_df \
    .withColumn('topic', explode(split('topic', ' ')))

topic_rdd = topic_df.rdd

topic_rdd = topic_rdd.map(lambda x: re.sub('[^a-zA-Z0-9 \n\.]', '', str(x[1])).lower()) \
    .map(lambda x: x.replace('rowtopic', '')) \
    .filter(lambda x: x != '')

topic_rdd = topic_rdd.map(lambda x: (x, 1)) \
    .reduceByKey(add) \
    .sortBy(lambda x: x[1], ascending=False)

topic_rdd = topic_rdd.map(lambda x: (x[0]))

print("The most frequent words in the largest community are: ")
print(str(topic_rdd.take(10)))

# Cohesiveness of the communities
triangle_graph = graph.triangleCount() \
    .withColumnRenamed('count', 'num_triangles') # count, author, id

cc_triangle = sorted_cc_graph \
    .join(cc_graph, sorted_cc_graph.component == cc_graph.component) \
    .join(triangle_graph, cc_graph.author == triangle_graph.author) \
    .select(cc_graph.author, triangle_graph.num_triangles)

cc_triangle_rdd = cc_triangle.rdd

sum_triangle_rdd = cc_triangle_rdd.map(lambda x: (1, x[1])) \
    .reduceByKey(add)

avg_triangle = sum_triangle_rdd.collect()[0][1] / cc_triangle_rdd.count()

print("The cohesiveness of the largest community is: " + str(avg_triangle))

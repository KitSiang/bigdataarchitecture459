from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split, concat_ws, col, lower, regexp_replace, window, current_timestamp, desc, from_json
from pyspark.ml.feature import StopWordsRemover

def parse_data_from_kafka_message(sdf, schema):
    from pyspark.sql.functions import split
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    col = split(sdf['value'], ',')

    # split attributes to nested array in one Column
    # now expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return sdf

if __name__ == "__main__":

    spark = SparkSession.builder \
               .appName("HardwarezoneStreaming") \
               .getOrCreate()

    # Read from Kafka's topic scrapy-output
    df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "scrapy-output") \
            .option("startingOffsets", "earliest") \
            .load()

    # Parse the fields in the value column of the message
    lines = df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Specify the schema of the fields
    hardwarezoneSchema = StructType([ \
        StructField("topic", StringType()), \
        StructField("author", StringType()), \
        StructField("content", StringType()) \
        ])

    # Use the function to parse the fields
    lines = parse_data_from_kafka_message(lines, hardwarezoneSchema) \
        .select("topic","author","content","timestamp")
    # lines = lines.withColumn('data', from_json(col("value"), schema=hardwarezoneSchema)).select('timestamp', 'data.*')

    # Top 10 users with most posts in 2 minutes
    top_10_users = lines.select("timestamp", "author") \
        .groupBy(window("timestamp", "2 minutes", "1 minute"), "author").count() \
        .withColumn("end", col("window")["end"]) \
        .withColumn("current_timestamp", current_timestamp())

    top_10_users = top_10_users \
        .filter(top_10_users.end < top_10_users.current_timestamp) \
        .orderBy(desc('window'), desc("count")).limit(10)

    # Remove stop words and clean contents
    content_df = lines.select('timestamp', split(lower(regexp_replace('content', r'[^\w\s]',''))," ").alias('content'))
    stop_words_list = (["\n","\t", "", " ", "..."])
    remover = StopWordsRemover(inputCol='content', outputCol='final_content', stopWords=stop_words_list)
    final_content = remover.transform(content_df)

    # Top 10 words in the posts in 2 minutes
    top_10_words = final_content.select("timestamp", concat_ws(" ", "final_content").alias("words")) \
        .select("timestamp", explode(split("words", " ")).alias("final_words")) \
        .groupBy(window("timestamp", "2 minutes", "1 minute"), "final_words").count() \
        .withColumn("end", col('window')['end']) \
        .withColumn("current_timestamp", current_timestamp())

    top_10_words = top_10_words \
        .filter(top_10_words.end < top_10_words.current_timestamp) \
        .orderBy(desc('window'), desc("count")).limit(10)

    # Select the content field and output
    users = top_10_users \
        .writeStream \
        .queryName("WriteContent1") \
        .trigger(processingTime="1 minute") \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", "/user/bluesky3/spark-checkpoint") \
        .start()

    words = top_10_words \
        .writeStream \
        .queryName("WriteContent2") \
        .trigger(processingTime="1 minute") \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", "/user/bluesky3/spark-checkpoint2") \
        .start()

    # Start the job and wait for the incoming messages
    words.awaitTermination()
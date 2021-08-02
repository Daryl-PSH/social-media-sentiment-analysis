from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark as spark

sc = SparkContext.getOrCreate()
sc.addPyFile("src/data_pipeline/tweets_preprocessing.py")

from tweets_preprocessing import *


def write_to_cassandra(df, epoch_id):
    print(f"Writing to Cassandra {epoch_id}")
    df.write.format("org.apache.spark.sql.cassandra").options(
        table="tweets", keyspace="social_media"
    ).mode("append").save()


if __name__ == "__main__":

    spark = setup_spark("Tweet Data")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "twitter_data")
        .load()
        .selectExpr(["CAST(value AS STRING)"])
    )

    processed_df = preprocess_data(df)

    query = (
        processed_df.writeStream.trigger(processingTime="5 seconds")
        .outputMode("update")
        .foreachBatch(write_to_cassandra)
        .start()
        .awaitTermination()
    )
    # query = (
    #     processed_df.writeStream.format("json")
    #     .option("checkpointLocation", "./checkpoint")
    #     .option("path", "./json")
    #     .start()
    #     .awaitTermination()
    # )

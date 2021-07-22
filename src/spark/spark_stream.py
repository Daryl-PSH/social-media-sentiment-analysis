from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark as spark

sc = SparkContext.getOrCreate()
sc.addPyFile("src/data_pipeline/tweets_preprocessing.py")

from tweets_preprocessing import *

if __name__ == "__main__":

    spark = setup_spark("Tweet Data")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "twitter_data")
        .load()
        .selectExpr(["CAST(value AS STRING)", "CAST(timestamp AS STRING)"])
    )

    processed_df = preprocess_data(df)

    write_df = (
        processed_df.writeStream.format("json")
        .queryName("tweet_queries")
        .option("path", "./new_json")
        .option("checkpointLocation", "./new_check")
        .start()
        .awaitTermination()
    )

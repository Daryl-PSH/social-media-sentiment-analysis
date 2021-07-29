from nltk import sentiment
from nltk.sentiment import vader
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark as spark
import emoji
import preprocessor as p
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

from typing import List
import string
import uuid


def setup_spark(app_name: str) -> SparkSession:
    """
    Setup Spark Session

    Args:
        app_name: Name of the app for the Spark Session

    Returns:
        spark: SparkSession that is created
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    return spark


def preprocess_data(df: DataFrame) -> DataFrame:
    """
    Function that integrates all the preprocessing step

    Args:
        df (DataFrame): Raw dataframe that is streamed

    Returns:
        processed_df: Processed dataframe
    """
    nltk.download("vader_lexicon")

    schema = generate_schema()

    processed_df = expand_column(df, schema)
    processed_df = convert_emoji_to_text(processed_df)
    processed_df = create_ticker_column(processed_df)
    processed_df = preprocess_tweets(processed_df)  # tweets specific preprocesing
    processed_df = clean_punctuations_digits(processed_df)
    processed_df = explode_ticker_column(processed_df)
    processed_df = generate_uuid(processed_df)
    processed_df = vader_prediction(processed_df)

    return processed_df


def generate_schema() -> StructType:
    """
    Generate the schema for the twitter topic

    Args:
        None

    Returns:
        schema: Schema for the dataframe columns
    """

    schema = StructType([StructField("text", StringType())])

    return schema


def generate_uuid(df: DataFrame) -> DataFrame:
    """
    Generate an Universally Unique Identifier (UUID) for each row of data
    in the dataframe

    Args:
        df (DataFrame): Dataframe to generate a UUID for

    Returns:
        DataFrame: Dataframe with a new UUID column
    """
    uuid_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
    uuid_df = df.withColumn("id", uuid_udf())
    return uuid_df


def expand_column(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Convert and expand the json data in the "value" column to its respective column
    in the dataframe and convert timestamp column to date column

    Args:
        df (DataFrame): Dataframe to be processed
        schema (StructType): Schema of of the preprocessed dataframe

    Returns:
        processed_df: Dataframe that has been converted and expanded
    """
    processed_df = df.withColumn("value", F.from_json("value", schema)).select(
        [F.col("value.*"), "timestamp"]
    )
    processed_df = processed_df.withColumn("timestamp", df["timestamp"].cast("date"))

    return processed_df


def create_ticker_column(df: DataFrame) -> DataFrame:
    """
    Create a ticker column, read extract_ticker func for more info

    Args:
        df (DataFrame): Dataframe to be modified

    Returns:
        processed_df (DataFrame): Dataframe with a new ticker column
    """

    def extract_ticker(row: Row) -> List[str]:
        """
        Extract all tickers mentioned in the list where a ticker is defined as any
        word with a $ as its prefix

        Args:
            row (Row): Pyspark row class where this function will be mapped to

        Returns:
            tickers (List[str]): List of ticker that has been mentioned in the tweet
        """
        tickers = [
            word[1:]
            for word in row.split()
            if word.startswith("$") and word[1:].isalpha()
        ]
        return tickers

    mapped_function = F.udf(extract_ticker, ArrayType(StringType()))
    processed_df = df.withColumn("ticker", mapped_function("text"))

    return processed_df


def vader_prediction(df: DataFrame) -> DataFrame:
    sid = SentimentIntensityAnalyzer()

    def analyze_sentiment(row: Row) -> float:

        ss = sid.polarity_scores(row)
        compound_score = ss["compound"]

        return compound_score

    sentiment_function = F.udf(analyze_sentiment, FloatType())
    processed_df = df.withColumn("sentiment_score", sentiment_function("cleaned_tweet"))

    return processed_df


def convert_emoji_to_text(df: DataFrame) -> DataFrame:
    """
    Convert emojis in tweet to text

    Args:
        df (DataFrame): Dataframe to be processed

    Returns:
        processed_df (DataFrame): Dataframe where emoji have been converted
    """
    emoji_func = F.udf(emoji.demojize, StringType())
    processed_df = df.withColumn("cleaned_tweet", emoji_func("text"))

    return processed_df


def preprocess_tweets(df: DataFrame) -> DataFrame:
    """
    Use the tweet-preprocessor library to preprocess tweets where it
    helps to remove URL, reserved words such as @RT, hashtags, mentions

    Args:
        df (DataFrame): DataFrame to be preprocessed

    Returns:
        processed_df (DataFrame): Preprocessed Dataframe
    """
    preprocess_func = F.udf(p.clean, StringType())
    processed_df = df.withColumn("cleaned_tweet", preprocess_func("cleaned_tweet"))

    return processed_df


def clean_punctuations_digits(df: DataFrame) -> DataFrame:
    """
    Remove punctuations and digits and replace "_" with space where could _ arise
    from processing emojis

    Args:
        df (DataFrame): DataFrame to be preprocessed

    Returns:
        processed_df (DataFrame): Preprocessed Dataframe
    """

    def remove_punctuation_digits(row: Row) -> Row:
        """
        Create a translation table where punctuations are replaced by white spaces
        and stripping excessive white spaces at the end

        Args:
            row (Row): Row of a DataFrame

        Returns:
            (Row): Preprocessed row
        """
        remove_list = string.punctuation + string.digits
        mapping = {k: " " for k in remove_list}
        map_table = str.maketrans(mapping)
        row = row.translate(map_table)
        return " ".join(row.split())

    df = df.withColumn("cleaned_tweet", F.regexp_replace("cleaned_tweet", "_", " "))

    remove_punc = F.udf(remove_punctuation_digits, StringType())
    processed_df = df.withColumn("cleaned_tweet", remove_punc("cleaned_tweet"))

    return processed_df


def explode_ticker_column(df: DataFrame) -> DataFrame:
    """
    For tweets that mention multiple relevant ticker, explode the ticker list

    Args:
        df (DataFrame): Dataframe to be processed

    Returns:
        processed_df (DataFrame): Processed dataframe where ticker is no longer a list
    """

    processed_df = df.withColumn("ticker", F.explode("ticker"))
    return processed_df

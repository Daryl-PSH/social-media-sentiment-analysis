from pyspark.sql import SparkSession, DataFrame, Row
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark as spark
import emoji
import preprocessor as p

from typing import List
import string


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
    schema = generate_schema()

    processed_df = expand_column(df, schema)
    processed_df = convert_emoji_to_text(processed_df)
    processed_df = preprocess_tweets(processed_df)  # tweets specific preprocesing
    processed_df = create_ticker_column(processed_df)
    processed_df = clean_punctuations_digits(processed_df)

    return processed_df


def generate_schema() -> StructType:
    """
    Generate the schema for the twitter topic

    Args:
        None

    Returns:
        schema: Schema for the dataframe columns
    """

    schema = StructType(
        [StructField("created_at", StringType()), StructField("text", StringType())]
    )

    return schema


def expand_column(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Convert and expand the json data in the "value" column to its respective column
    in the dataframe

    Args:
        df (DataFrame): Dataframe to be processed
        schema (StructType): Schema of of the preprocessed dataframe

    Returns:
        processed_df: Dataframe that has been converted and expanded
    """
    processed_df = df.withColumn("value", F.from_json("value", schema)).select(
        F.col("value.*")
    )

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
        tickers = [word[1:] for word in row.split() if word.startswith("$")]
        return tickers

    mapped_function = F.udf(extract_ticker, ArrayType(StringType()))
    processed_df = df.withColumn("ticker", mapped_function("text"))

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
    processed_df = df.withColumn("cleaned_text", emoji_func("text"))

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
    processed_df = df.withColumn("cleaned_text", preprocess_func("cleaned_text"))

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

    def remove_punctuation_digits(row: Row):
        remove_list = string.punctuation + string.digits
        map_table = str.maketrans("", "", remove_list)

        return row.translate(map_table)

    df = df.withColumn("cleaned_text", F.regexp_replace("cleaned_text", "_", " "))

    remove_punc = F.udf(remove_punctuation_digits, StringType())
    processed_df = df.withColumn("cleaned_text", remove_punc("cleaned_text"))

    return processed_df

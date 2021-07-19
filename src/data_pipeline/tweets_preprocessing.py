from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark as spark


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
        F.col("value")
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

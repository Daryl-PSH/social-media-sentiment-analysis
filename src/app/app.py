from flask import Flask, render_template, request, Response, jsonify
import pandas as pd
import numpy as np
import json
from cassandra.query import dict_factory
from cassandra.cluster import Cluster, Session

import time
from datetime import datetime
from typing import List, Tuple

app = Flask(__name__)

# Connect to database upon loading
cluster = Cluster()
session = cluster.connect("social_media")

@app.route("/stream_tweets", methods=["GET"])
def stream_tweets_sentiment():

    data = get_sentiment_data()

    return jsonify(data)


@app.route("/", methods=["GET"])
def homepage():

    data = get_sentiment_data()
    return render_template(
        "main.html",
        dataset=jsonify(data),
    )


def pandas_factory(colnames, rows):
    """
    Return Cassandra query as Pandas DataFrame
    """
    return pd.DataFrame(rows, columns=colnames)


def get_sentiment_data():
    current_date = datetime.now()

    # Monday is first day of the week
    year, month, day = current_date.year, current_date.month, current_date.weekday() + 1

    query = (
        f"""SELECT * FROM tweets WHERE year={year} AND month={month} AND day={day}"""
    )
    session.row_factory = pandas_factory
    session.default_fetch_size = None
    tweets = session.execute(query)._current_rows

    sentiment = tweets.groupby(["ticker"]).agg(
        {"sentiment_score": ["mean"], "cleaned_tweet": ["count"]}
    )
    sentiment.columns = sentiment.columns.droplevel(0)
    top_10 = sentiment.reset_index().nlargest(10, "count")[["ticker", "mean", "count"]]

    ticker_list = list(top_10["ticker"])
    sentiment_score = list(top_10["mean"].round(3))
    count = list(top_10["count"])

    border_colour, background_colour = classify_sentiment_scores(sentiment_score)

    return {
        "ticker": ticker_list,
        "sentiment_score": sentiment_score,
        "count": count,
        "border_colour": border_colour,
        "background_colour": background_colour,
    }


def classify_sentiment_scores(
    sentiment_score: List[float],
) -> Tuple[List[str], List[str]]:
    """
    Classify the sentiment score to three bucket where a sentiment score of above 0.3
    is green, below -0.3 is red and anything in between is light brown

    Args:
        sentiment_score (List[float]): Sentiment score for the respective tickers

    Returns:
        border_colour (List[str]) : colour in rgba format string
        background_colour (List[str]) colour in rgba format string
    """
    background_colour = []
    border_colour = []

    for sentiment in sentiment_score:
        if sentiment >= 0.3:
            border_colour.append("rgba(0, 255, 0, 1)")
            background_colour.append("rgba(0, 255, 0, 0.2)")
        elif sentiment <= -0.3:
            border_colour.append("rgba(255, 0, 0, 1)")
            background_colour.append("rgba(255, 0, 0, 0.2)")
        else:
            border_colour.append("rgba(202, 179, 136, 1)")
            background_colour.append("rgba(202, 179, 136, 0.2)")

    return (background_colour, border_colour)


if __name__ == "__main__":
    app.run(debug=True)

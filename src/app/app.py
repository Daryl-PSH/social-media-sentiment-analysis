from flask import Flask, render_template, request, Response, jsonify
import pandas as pd
import numpy as np
import json
from src.connect_cassandra import *
from cassandra.query import dict_factory

import time
from datetime import datetime
from typing import List, Tuple

app = Flask(__name__)

# Connect to database upon loading
session = connect_to_database("social_media")


@app.route("/stream_tweets", methods=["GET"])
def stream_tweets_sentiment():

    data = get_sentiment_data()

    return jsonify(data)


@app.route("/", methods=["GET"])
def homepage():

    # ticker_list, sentiment_score, count = get_sentiment_data()

    data = get_sentiment_data()
    return render_template(
        "main.html",
        dataset=jsonify(data),
    )


def get_sentiment_data():
    current_date = datetime.now()

    # Monday is first day of the week
    year, month, day = current_date.year, current_date.month, current_date.day - 2

    query = (
        f"""SELECT * FROM tweets WHERE year={year} AND month={month} AND day={day}"""
    )

    tweets = extract_data(session, query)

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

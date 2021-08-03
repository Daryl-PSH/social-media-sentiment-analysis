from flask import Flask, render_template, request, Response, jsonify
import pandas as pd
import numpy as np
import plotly.graph_objs as go
import plotly.express as px
import plotly
import json
from src.connect_cassandra import *
from streamz.dataframe import PeriodicDataFrame
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from cassandra.query import dict_factory

import time
from datetime import datetime

app = Flask(__name__)

# Connect to database upon loading
session = connect_to_database("social_media")


@app.route("/stream_tweets", methods=["GET"])
def stream_tweets_sentiment():

    ticker_list, sentiment_score, count = get_sentiment_data()

    return jsonify(ticker_list)


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
    year, month, day = current_date.year, current_date.month, current_date.day - 1

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

    return {"ticker": ticker_list, "sentiment_score": sentiment_score, "count": count}


if __name__ == "__main__":
    app.run(debug=True)

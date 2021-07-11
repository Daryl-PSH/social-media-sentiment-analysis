import requests
from dotenv import load_dotenv, find_dotenv
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
import tweepy
import pandas as pd

import os
from typing import List

load_dotenv(find_dotenv())  # Load secret keys and bearer token

BEARER_TOKEN = os.environ.get("BEARER_TOKEN")
CONSUMER_KEY = os.environ.get("CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")


class TwitterAuth:
    """
    Sets up the authetication to access Twitter's API
    """

    def setup_auth(self):
        auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        return auth


class StockStreamer:
    """
    Generates stream of data based on tracked ticker
    """

    auth: TwitterAuth
    tickers: List[str]

    def __init__(self, tickers):
        self.auth = TwitterAuth().setup_auth()
        self.tickers = tickers

    def stream_tweets(self):
        listener = TweetsListener()
        stock_stream = Stream(self.auth, listener)
        stock_stream.filter(track=self.tickers)


class TweetsListener(StreamListener):
    """
    Listens for new tweets
    """

    def on_data(self, data):
        print(data)

    def on_error(self, status):
        if status == 420:  # in the event of rate limiting
            return False
        print(status)


def get_top_n_ticker(csv_file: str, n: int) -> List[str]:
    """
    Get top n market cap of tickers in a particular listing

    Args:
        csv_file (str): CSV file to extract the tickers
        n (int): Top n ticker based on market cap

    Returns
        tickers (list): List of top n tickers based on market cap
    """
    df = pd.read_csv(os.path.join(os.path.dirname(__file__), csv_file))
    tickers = list(df.nlargest(n, ["Market Cap"])["Symbol"])
    tickers = ["$" + ticker for ticker in tickers]

    return tickers


def main():
    tickers = get_top_n_ticker("nasdaq_listing.csv", 250)
    streamer = StockStreamer(tickers)
    streamer.stream_tweets()


if __name__ == "__main__":
    main()
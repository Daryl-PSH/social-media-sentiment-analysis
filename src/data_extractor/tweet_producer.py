import requests
from dotenv import load_dotenv, find_dotenv
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
import tweepy

import os


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

    def __init__(self):
        self.auth = TwitterAuth().setup_auth()

    def stream_tweets(self):
        listener = TweetsListener()
        stock_stream = Stream(self.auth, listener)
        stock_stream.filter(track=["tesla", "microsoft"])


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


def main():
    streamer = StockStreamer()
    streamer.stream_tweets()


if __name__ == "__main__":
    main()
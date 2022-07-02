from __future__ import print_function

import json

import pymongo
import tweepy
from pymongo import MongoClient

import Keys

client = MongoClient('mongodb://localhost:27017/')
db = client["twitterdb"]  # use or create a database named demo
tweets = db["tweets"]  # use or create a collection named tweet_collection
tweets.create_index([("tweet_id", pymongo.ASCENDING)], unique=True)  # make sure the collected tweets are unique


class Listener(tweepy.Stream):
    row_count = 0

    def on_data(self, raw_data):
        print(raw_data)

        # Load the Tweet into the variable "data"
        data = json.loads(raw_data)

        tweet_id = data['id_str']  # The Tweet ID from Twitter in string format
        user_id = data['user']['id_str']
        created_at = data['created_at']
        text = data['text']
        user_name = data['user']['screen_name']
        followers_count = data['user']['followers_count']
        friends_count = data['user']['friends_count']
        status_count = data['user']['statuses_count']
        favorite_count = data['user']['friends_count']
        lang = data['user']['lang']
        quotecount = data['quote_count']
        replycount = data['reply_count']
        retweetcount = data['retweet_count']
        favoritecount = data['favorite_count']
        favorited = data['favorited']
        retweeted = data['retweeted']
        location = data['user']['location']

        # Load all of the extracted Tweet data into the variable "tweet" that will be stored into the database
        tweet = {'tweet_id': tweet_id, 'user_id': user_id, 'created_at': created_at, 'text': text,
                 'user_name': user_name,
                 'followers_count': followers_count, 'friends_count': friends_count, 'status_count': status_count,
                 'favorite_count': favorite_count,
                 'lang': lang, 'quotecount': quotecount, 'replycount': replycount,
                 'retweetcount': retweetcount, 'avoritecount': favoritecount,
                 'location': location, 'retweeted': retweeted, 'favorited': favorited}

        # Save the refined Tweet data to MongoDB
        # for tweet in tweepy.Cursor(api.search_tweets, q=WORDS, count=20, result_type="recent").items(200):
        tweets.save(tweet)
        Listener.row_count = Listener.row_count + 1
        if Listener.row_count == 1000:
            exit()
            #  print(tweet.text)

    def on_connection_error(self, status_code):
        if status_code == 420:
            return False


if __name__ == '__main__':
    auth = tweepy.OAuthHandler(Keys.CONSUMER_KEY, Keys.CONSUMER_SECRET)
    auth.set_access_token(Keys.ACCESS_TOKEN, Keys.ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    WORDS = ['#bigdata', '#AI', '#datascience', '#machinelearning', '#ml', '#iot']
    stream_tweet = Listener(Keys.CONSUMER_KEY, Keys.CONSUMER_SECRET, Keys.ACCESS_TOKEN, Keys.ACCESS_TOKEN_SECRET)
    stream_tweet.filter(track=WORDS)

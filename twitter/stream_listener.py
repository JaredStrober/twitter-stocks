import sys
sys.path.insert(0,'..')
import argparse
import json
import yaml
import time
from datetime import datetime
import random

from db import Db
import tweepy

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# constants and paths
STOCKS_LIST = '../data/all_stocks.yml'
MAX_TRACKED_PER_API = 400
TWEET_ATTRS = ['id', 'text', 'created_at', 'in_reply_to_status_id',
                          'in_reply_to_user_id', 'retweeted_status_id', 'quoted_status_id', 'quote_count',
                          'reply_count', 'retweet_count', 'favorite_count']
USER_ATTRS = ['id', 'name', 'screen_name', 'verified', 'followers_count', 'friends_count',
                         'listed_count', 'favourites_count', 'statuses_count', 'created_at']

class TweetStreamListener(tweepy.StreamListener):
    def __init__(self, api_config, db, penny_stocks):
        super(TweetStreamListener, self).__init__()
        self.api = self.create_api(api_config)
        self.db = db
        # create you Stream object with authentication
        stream = tweepy.Stream(self.api.auth, self)

        # filter Twitter Streams to capture data by the symbol name
        stream.filter(track=penny_stocks, languages=["en"], is_async=True)
        logger.info("Penny stocks on the radar: %s" % (" ".join(penny_stocks)))
        logger.info("Streamer launched!")

    def create_api(self, config_path):
        with open(config_path) as file:
            config = yaml.load(file, Loader=yaml.FullLoader)

        # pass OAuth details to tweepy's OAuth handler
        auth = tweepy.OAuthHandler(config['consumer_key'], config['consumer_secret'])
        auth.set_access_token(config['access_token'], config['access_token_secret'])
        api = tweepy.API(auth, wait_on_rate_limit = True,
                               wait_on_rate_limit_notify = True)
        try:
            api.verify_credentials()
        except Exception as e:
            logger.error("Error creating API", exc_info=True)
            raise e
        logger.info("Connected to API.")
        return api

    def insert_hashtag(self, tweet):
        for hashtag in tweet['entities']['hashtags']:
            # insert the new hashtag into db.
            hashtag = "'%s'"%(hashtag['text'].upper())
            self.db.insert('hashtags', {'hashtag': hashtag})
            id = self.db.select('hashtags', ['id'], ['hashtag=%s'%(hashtag)])[0][0]
            self.db.insert('tweets_hashtags', {'tweet_id':tweet['id'], 'hashtag_id': id})

    def insert_symbol(self, tweet):
        for symbol in tweet['entities']['symbols']:
            # insert the new symbol into db.
            symbol = "'%s'"%(symbol['text'].upper())
            self.db.insert('symbols', {'symbol': symbol})
            id = self.db.select('symbols', ['id'], ['symbol=%s'%(symbol)])[0][0]
            self.db.insert('tweets_symbols', {'tweet_id':tweet['id'], 'symbol_id': id})

    def insert_tweet(self, tweet):
        # filter and clean the tweet's attributes.
        tweet['created_at'] = datetime.strftime(datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S')
        tweet['created_at'] = "'%s'"%(tweet['created_at'])
        tweet['text'] = "'%s'"%(tweet['text'].replace("'",''))
        row = dict()
        for attr in tweet:
            if attr in TWEET_ATTRS:
                row[attr] = tweet[attr] if tweet[attr] != None else -1
        row['user_id'] = tweet['user']['id']

        # insert the new tweet into db.
        self.db.insert('tweets', row)

    def insert_user(self, user):
        # filter and clean the user's attributes.
        user['created_at'] = datetime.strftime(datetime.strptime(user['created_at'],'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S')
        user['created_at'] = "'%s'"%(user['created_at'])
        user['name'] = "'%s'"%(user['name'].replace("'",''))
        user['screen_name'] = "'%s'"%(user['screen_name'].replace("'",''))
        row = dict()
        for attr in user:
            if attr in USER_ATTRS:
                row[attr] = user[attr] if user[attr] != None else -1

        # insert the new user into db.
        self.db.insert('users', row)

    def on_status(self, status):
        t1 = time.time()
        tweet = status._json
        user = tweet['user']
        self.insert_user(user)
        self.insert_tweet(tweet)
        # store the retweeted tweet too
        for type in ['quoted_status', 'retweeted_status']:
            if type in tweet:
                self.insert_user(tweet[type]['user'])
                self.insert_tweet(tweet[type])
        if len(tweet['entities']['hashtags'])>0:
            self.insert_hashtag(tweet)
        if len(tweet['entities']['symbols'])>0:
            self.insert_symbol(tweet)
        logger.info("A new tweet with id=%s processed in %s seconds." % (tweet['id'], time.time()-t1))
        return True

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--db', type=str, help='The path to the SQLite db')
parser.add_argument('--api', type=str, help='Twitter API credentials.')
args = parser.parse_args()

db = Db(args.db)
db.reset_factory_tweets()

with open(STOCKS_LIST) as file:
    all_stocks = yaml.load(file, Loader=yaml.FullLoader)

# Sort the stocks based on their market cap.
all_stocks = ['$'+k for k, v in sorted(all_stocks.items(), key=lambda item: item[1])]

# Select the least valued stocks.
penny_stocks = all_stocks[0:MAX_TRACKED_PER_API]

# Initialize and run the stream listener
listener = TweetStreamListener(args.api, db, penny_stocks)

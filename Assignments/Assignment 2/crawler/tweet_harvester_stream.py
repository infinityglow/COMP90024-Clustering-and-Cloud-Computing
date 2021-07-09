import json
import tweepy
import argparse
import couchdb
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from utils import *

class StdOutListener(tweepy.StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, tweet):
        # create database if not exist, or switch to it otherwise
        if 'twitter' in server:
            db = server['twitter']
        else:
            db = server.create('twitter')
        locator = Nominatim(user_agent="application")
        # reverse = RateLimiter(locator.reverse, min_delay_seconds=1)

        tweet = json.loads(tweet)
        if 'text' in tweet:
            dic = tweet

            if dic['lang'] == 'en':
                dic['score'] = sentiment_analysis(tweet['text'])
            else:
                dic['score'] = None

            if tweet['coordinates']:
                long = tweet['coordinates']['coordinates'][0]
                lat = tweet['coordinates']['coordinates'][1]
                if 'suburb' in reverse((lat, long), language='en', exactly_one=True).raw['address']:
                    dic['suburb'] = reverse((lat, long), language='en', exactly_one=True).raw['address']['suburb']
                else:
                    dic['suburb'] = None
            elif tweet['place']:
                dic['coordinates'] = extract_coord(tweet['place'])
                long = dic['coordinates'][0]
                lat = dic['coordinates'][1]
                if 'suburb' in reverse((lat, long), language='en', exactly_one=True).raw['address']:
                    dic['suburb'] = reverse((lat, long), language='en', exactly_one=True).raw['address']['suburb']
                else:
                    dic['suburb'] = None
            else:
                dic['coordinates'] = None
                dic['suburb'] = None
            db.save(dic)

        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--city', type=str, help='which city you want')

    args = parser.parse_args()

    city = args.city

    l = StdOutListener()

    # read configuration
    f = open("config.json", 'r')
    configuration = json.loads(f.read())

    # api configuration
    api_config = configuration['API']

    # city configuration
    city_config = configuration['City']

    # retrieve secret keys and tokens
    consumer_key = api_config['consumer_key']
    consumer_secret = api_config['consumer_secret']

    access_token = api_config['access_token']
    access_token_secret = api_config['access_token_secret']

    # authentication
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # bind to api
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True,
                     retry_errors=set([401, 404, 443, 500, 502, 503, 504, 517]))

    # start server
    server = couchdb.Server("http://admin:admin@172.26.130.24:5984/")
    locator = Nominatim(user_agent="application")
    reverse = RateLimiter(locator.reverse, min_delay_seconds=1)
    print(reverse((-33.70846480580034, 150.520929)).raw)

    # stream = tweepy.Stream(auth, l)
    # stream.filter(locations=city_config[city]['bounding_box'],)

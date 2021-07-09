import tweepy
import json
import time
import couchdb
import argparse
from shapely.geometry import shape, Point
from utils import *

class TweetsHavester(object):

    def __init__(self, server, api, city_config):
        self.server = server  # couch DB server
        self.api = api  # twitter API
        self.city_config = city_config  # city configuration


    def harvest(self, city, period, use_coord, max_enties=1000):

        # retrieve location
        lat = self.city_config[city]['coordinates'][0]
        long = self.city_config[city]['coordinates'][1]
        scope = self.city_config[city]['scope']
        geo_str = str(lat) + ',' + str(long) + ',' + scope

        # retrieve start and end date
        since = period[0]
        until = period[1]

        # search tweets

        tweets = tweepy.Cursor(method=api.search,
                               geocode=geo_str,
                               since=since,
                               until=until).items(max_enties)

        # create database if not exist, or switch to it otherwise
        if 'twitter' in self.server:
            db = self.server['twitter']
        else:
            db = self.server.create('twitter')

        with open('./suburbData.json', 'r') as f:
            suburb_list = json.loads(f.read())

        # select proper fields and save to database
        for i, tweet in enumerate(tweets):
            tweet = tweet._json
            if 'text' in tweet:
                dic = tweet
                date = datetime.datetime.strftime(datetime.datetime.strptime(dic['created_at'],'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d')
                dic['date'] = date
                dic['city'] = city

                if dic['lang'] == 'en':
                    dic['score'] = sentiment_analysis(tweet['text'])
                else:
                    dic['score'] = None

                if tweet['place']:
                    dic['coordinates'] = extract_coord(tweet['place'])
                    for city in suburb_list.keys():
                        for suburb in suburb_list[city]:
                            polygon = shape(suburb_list[city][suburb])
                            point = Point(dic['coordinates'])
                            contained = polygon.contains(point)
                            if contained:
                                dic['suburb'] = suburb
                                break
                        else:
                            continue
                        break
                    else:
                        dic['suburb'] = None
                else:
                    dic['coordinates'] = None
                    dic['suburb'] = None

                db.save(dic)

            if (i + 1) % 100 == 0:
                print("Process: %d/%d" % (i+1, max_enties))

        print("Already saved %d records from %s into database." % (max_enties, city))


start = time.time()

parser = argparse.ArgumentParser()

parser.add_argument('--since', type=str, default='2021-05-17', help='begin date')
parser.add_argument('--until', type=str, default='2021-05-24', help='end date')
parser.add_argument('--city', type=str, help='which city you want')
parser.add_argument('--max_tweets', type=int, default=2500, help='how many tweets for a day')

args = parser.parse_args()

date_list = get_everyday(args.since, args.until)
city = args.city
max_tweets = args.max_tweets

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
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, retry_errors=set([401, 404, 443, 500, 502, 503, 504, 517]))

# start server
server = couchdb.Server("http://admin:admin@172.26.130.24:5984/")


harvester = TweetsHavester(server, api, city_config)
for i in range(len(date_list)-1):
    since = date_list[i]
    until = date_list[i+1]

    period = [since, until]
    print("From " , since, " to ", until)
    harvester.harvest(city, period, False, max_tweets)

f.close()
end = time.time()

print("Cost of time: %.2f s" % (end - start))

import datetime
import nltk
import random
from geopy.geocoders import Nominatim
from nltk.sentiment.vader import SentimentIntensityAnalyzer
# nltk.download('vader_lexicon')

def get_everyday(since, until):
    date_list = []
    since = datetime.datetime.strptime(since, "%Y-%m-%d")
    until = datetime.datetime.strptime(until, "%Y-%m-%d")

    while since <= until:
        since_str = since.strftime("%Y-%m-%d")
        date_list.append(since_str)
        since += datetime.timedelta(days=1)

    return date_list

def sentiment_analysis(text):
    scorer = SentimentIntensityAnalyzer()
    sentiments = scorer.polarity_scores(text)
    score = sentiments['neg'] * -100 + sentiments['neu'] * 50 + sentiments['pos'] * 100
    return score

def extract_coord(place):
    # get bounding box
    bounding_box = place['bounding_box']['coordinates'][0]

    # min and max longitude and latitude
    min_long, max_long = min(bounding_box[0][0], bounding_box[1][0]), \
                         max(bounding_box[0][0], bounding_box[1][0])
    min_lat, max_lat = min(bounding_box[0][1], bounding_box[2][1]), \
                         max(bounding_box[0][1], bounding_box[2][1])

    # randomly select the longitude and latitude in the box
    long = random.random() * (max_long - min_long) + min_long
    lat = random.random() * (max_lat - min_lat) + min_lat

    return [long, lat]


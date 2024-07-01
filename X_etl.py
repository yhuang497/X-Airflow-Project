import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_X_etl():

    access_key = "104shZsF4DofmaI4Yg95klyb1"
    access_secret = "8deuHn6fs5ZpRwqK8AHupw230PHfXj2GciwDc8iKIjFm9itiCh"
    consumer_key = "1395057406112014341-0aQ6DTdtzzQcfKwbUckNVsR8dW1kBA"
    consumer_secret = "WSuAmDFi5N5v8ydfSmrSQ1XgyplegpQfl2otFEuL5qaeq"

    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name = '@elonmusk',
                            # 200 is the maximum allowed count
                            count = 200,
                            include_rts = False,
                            # Necessary to keep full_text
                            # Otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user" : tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}

        tweet_list.append(refined_tweet) 

    df = pd.DataFrame(tweet_list)
    df.to_csv('s3://yuchen-X-airflow-bucket/refined_tweets.csv')
from sa_config import ConfigLocal, ConfigDev, ConfigProd
from sa_models import SocialPosts, sess, engine
# For sending GET requests from the API
import requests
# For saving access tokens and for file management when creating and adding to the dataset
import os
import json
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
from datetime import datetime


if os.environ.get('CONFIG_TYPE')=='local':
    config = ConfigLocal()
elif os.environ.get('CONFIG_TYPE')=='dev':
    config = ConfigDev()
elif os.environ.get('CONFIG_TYPE')=='prod':
    config = ConfigProd()


if not os.path.exists(os.path.join(config.PROJ_ROOT_PATH,'logs')):
    os.mkdir(os.path.join(config.PROJ_ROOT_PATH,'logs'))

#Setting up Logger
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
formatter_terminal = logging.Formatter('%(asctime)s:%(filename)s:%(name)s:%(message)s')

#initialize a logger
logger_twitter = logging.getLogger(__name__)
logger_twitter.setLevel(logging.DEBUG)
# logger_terminal = logging.getLogger('terminal logger')
# logger_terminal.setLevel(logging.DEBUG)

#where do we store logging information
file_handler = RotatingFileHandler(os.path.join(config.PROJ_ROOT_PATH,'logs','twitter.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler.setFormatter(formatter)

#where the stream_handler will print
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter_terminal)

logger_twitter.addHandler(file_handler)
logger_twitter.addHandler(stream_handler)

# config=ConfigLocal()


def create_url():
    usernames = "usernames=EvryMnRodriguez"
    user_fields = "user.fields=description,created_at,id"
    url = "https://api.twitter.com/2/users/by?{}&{}".format(usernames, user_fields)
    return url

# bearer_token = config.TWITTER_BEARER_TOKEN
def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {config.TWITTER_BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2UserLookupPython"
    return r

# def bearer_oauth2():
#     headers = {}
#     headers["Authorization"] = f"Bearer {config.TWITTER_BEARER_TOKEN}"
#     headers["User-Agent"] = "v2UserLookupPython"
#     return headers

def get_twitter_user_id():
    logger_twitter.info('- Running Twitter calls -')
    r_user_info = requests.request("GET", create_url(), auth=bearer_oauth)
    twitter_user_id = r_user_info.json().get('data')[0].get('id')
    return twitter_user_id

# endpoint_tweets = "https://api.twitter.com/2/users/138768918/tweets"
# endpoint_tweets_with_date = "https://api.twitter.com/2/users/138768918/tweets?tweet.fields=created_at%2Cgeo"

def call_twitter_api_to_tweet_dict():
    logger_twitter.info(f"- calling Twitter api -")
    endpoint_tweets_w_date_old = f"https://api.twitter.com/2/users/{config.TWITTER_ID}/tweets?tweet.fields=created_at%2Cgeo&start_time=2016-01-01T01%3A01%3A01Z&max_results=30"
    headers = {"Authorization": "Bearer " + config.TWITTER_BEARER_TOKEN}
    r_tweets = requests.get(endpoint_tweets_w_date_old, headers=headers)
    logger_twitter.info(f"- Twitter response status_code: {r_tweets.status_code} -")
    tweet_response_dict = r_tweets.json()
    logger_twitter.info(f"- tweet count: {len(tweet_response_dict.get('data'))} -")
    return tweet_response_dict


def new_tweets_to_df(tweet_response_dict):
    tweet_dict = {}
    counter= 0 
    for tweet in tweet_response_dict.get('data'):
        tweet_dict['username'] = 'EvryMnRodriguez'
        tweet_dict['title'] = tweet.get('text')
        tweet_dict['network_post_id'] = tweet.get('id')
        tweet_dict['social_name'] = 'Twitter'
        tweet_dict['social_icon'] = 'twitter.png'
        tweet_dict['post_date'] = tweet.get('created_at')[:10]
        tweet_dict['url'] = config.TWITTER_URL_BASE + tweet.get('id')
        
        if counter == 0:
            df_tweets = pd.DataFrame(tweet_dict, index=[str(counter),])
        else:
            df_tweets = pd.concat([df_tweets, pd.DataFrame(tweet_dict, index=[str(counter),])], ignore_index = True)
        
        counter += 1
    
    return df_tweets


def get_existing_tweets():
    # Check for duplicate tweets to remove

    base_query = sess.query(SocialPosts).filter(SocialPosts.social_name =='Twitter')
    df_existing = pd.read_sql(str(base_query)[:-2] + str('= "Twitter"'), sess.bind)

    table_name = 'social_posts_'
    cols = list(df_existing.columns)
    for col in cols:
        if col[:len(table_name)] == table_name:
            df_existing = df_existing.rename(columns=({col: col[len(table_name):]}))

    return df_existing


def add_new_tweets_to_db(df_to_add):
    df_to_add['time_stamp_utc'] = datetime.utcnow()
    rows_added = df_to_add.to_sql('social_posts', con=engine, if_exists='append', index=False)
    logger_twitter.info(f"- Successfully added {rows_added} tweets to db! -")


def twitter_scheduler_update():
    logger_twitter.info(f"- in twitter_scheduler_update -")
    tweet_response_dict = call_twitter_api_to_tweet_dict()
    df_new = new_tweets_to_df(tweet_response_dict)

    # Check for existing tweets in db and remove from new set
    df_existing = get_existing_tweets()
    logger_twitter.info(f"- {len(df_existing)} existing tweets in db -")
    if len(df_existing) == 0:
        df_to_add = df_new
    elif len(df_existing) > 0:
        df_to_add = df_new[~df_new.network_post_id.isin(df_existing.network_post_id)]
    
    logger_twitter.info(f"- Adding {len(df_to_add)} tweets to db -")
    
    add_new_tweets_to_db(df_to_add)

    logger_twitter.info("- Finished running Twitter calls -")
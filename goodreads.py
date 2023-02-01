from sa_config import ConfigLocal, ConfigDev, ConfigProd
from sa_models import SocialPosts, sess, engine
# For sending GET requests from the API
# import requests
# For saving access tokens and for file management when creating and adding to the dataset
import os
import json
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
from datetime import datetime, timedelta
# from goodreads_scraper.user import get_user_info
from goodreads_scraper.shelves import get_all_shelves
# from argparse import Namespace
from subprocess import Popen


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
logger_goodreads = logging.getLogger(__name__)
logger_goodreads.setLevel(logging.DEBUG)

#where do we store logging information
file_handler = RotatingFileHandler(os.path.join(config.PROJ_ROOT_PATH,'logs','sa_goodreads.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler.setFormatter(formatter)

#where the stream_handler will print
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter_terminal)

logger_goodreads.addHandler(file_handler)
logger_goodreads.addHandler(stream_handler)



def get_data_from_goodreads():

    #Get User info
    goodreads_scraped_user_path = os.path.join(config.PROJ_DB_PATH,'goodreads-data','user.json')
    json_file = open(goodreads_scraped_user_path)
    user_dict = json.load(json_file)
    json_file.close()

    ##############################################################
    #TODO: check shelves for read/ want to read/ soemthing else
    ##############################################################
    goodreads_scraped_books_path = os.path.join(config.PROJ_DB_PATH,'books')
    book_dict = {}
    books_list_for_df = []
    counter = 1
    for book_read in os.listdir(goodreads_scraped_books_path):
        if book_read != '.DS_Store':
            json_file = open(os.path.join(goodreads_scraped_books_path, book_read))
            book_dict = json.load(json_file)
            json_file.close()

            if len(book_dict.get('dates_read')) > 0:
                sa_dict = {}

                sa_dict['username'] = user_dict.get('user_name')
                sa_dict['title'] = book_dict.get('book_title')
                sa_dict['description'] = book_dict.get('author').get('author_name')
                sa_dict['social_name'] = 'Goodreads'
                sa_dict['social_icon'] = 'goodreads_misc.png'
                sa_dict['url'] = config.GOODREADS_URL
                # sa_dict['post_date'] = book_dict.get('dates_read')[0]
                sa_dict['post_date'] = datetime.strptime(book_dict.get('dates_read')[0], "%b %d, %Y").strftime("%Y-%m-%d")
                sa_dict['notes'] = f"Nick scored it: {book_dict.get('rating')} stars"
                books_list_for_df.append(sa_dict)

                logger_goodreads.info(f"book_read: {book_read} ---- title: {book_dict.get('book_title')}")
            counter += 1
        else:
            counter += 1  

    df_new = pd.DataFrame(books_list_for_df)

    return df_new


def get_existing_reads():
    # Check for duplicate tweets to remove

    base_query = sess.query(SocialPosts).filter(SocialPosts.social_name =='Goodreads')
    df_existing = pd.read_sql(str(base_query)[:-2] + str('= "Goodreads"'), sess.bind)

    table_name = 'social_posts_'
    cols = list(df_existing.columns)
    for col in cols:
        if col[:len(table_name)] == table_name:
            df_existing = df_existing.rename(columns=({col: col[len(table_name):]}))

    return df_existing


def add_new_goodreads_to_db(df_to_add):
    df_to_add['time_stamp_utc'] = datetime.utcnow()
    rows_added = df_to_add.to_sql('social_posts', con=engine, if_exists='append', index=False)
    logger_goodreads.info(f"- Successfully added {rows_added} commits to db! -")


def goodreads_update():
    logger_goodreads.info(f"--- inspected by nick jan31 ---")
    # call_goodreads_scraper()
    # Popen(['goodreads-user-scraper', '--user_id', '149832357'])
    df_new = get_data_from_goodreads()
    df_existing = get_existing_reads()

    if len(df_existing) == 0:
        df_to_add = df_new
    elif len(df_existing) > 0:
        df_to_add = df_new[~df_new.title.isin(df_existing.title)]
    
    logger_goodreads.info(f"- Adding {len(df_to_add)} commits to db -")
    logger_goodreads.info(f"- github_scheduler_update completed -")
    add_new_goodreads_to_db(df_to_add)

    # # Create a df that mirrors what will be in personal website
    # df_mirror = get_existing_reads()
    # df_mirror.to_pickle(os.path.join(config.PROJ_DB_PATH,'df_mirror.pkl'))
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
logger_stack = logging.getLogger(__name__)
logger_stack.setLevel(logging.DEBUG)
# logger_terminal = logging.getLogger('terminal logger')
# logger_terminal.setLevel(logging.DEBUG)

#where do we store logging information
file_handler = RotatingFileHandler(os.path.join(config.PROJ_ROOT_PATH,'logs','sa_stack_overflow.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler.setFormatter(formatter)

#where the stream_handler will print
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter_terminal)

logger_stack.addHandler(file_handler)
logger_stack.addHandler(stream_handler)

config=ConfigLocal()

def call_stack_overflow_api():
    stack_response = requests.get(config.STACK_OVERFLOW_URL)
    stack_dict = stack_response.json()
    logger_stack.info(f"- Stackoverflow returned {len(stack_dict.get('items'))} questions -")
    return stack_dict


def new_questions_to_df(stack_overflow_response_dict):
    stack_dict = {}
    counter = 0
    for question in stack_overflow_response_dict.get('items'):
        stack_dict['username'] = question.get('owner').get('display_name')
        stack_dict['title'] = question.get('title')
    #     stack_dict['description'] = question.get('title')
        stack_dict['url'] = question.get('link')
        stack_dict['social_name'] = 'Stack Overflow'
        stack_dict['social_icon'] = 'stackoverflow.png'
        stack_dict['post_date'] = datetime.fromtimestamp(question.get('creation_date')).strftime("%Y-%m-%d")
        stack_dict['network_post_id'] = str(question.get('question_id'))
        stack_dict['view_count'] = question.get('view_count')
        stack_dict['like_count'] = question.get('score')
        stack_dict['answered'] = question.get('is_answered')
        stack_dict['user_reputation'] = question.get('owner').get('reputation')

        if counter == 0:
            df_quest = pd.DataFrame(stack_dict, index=[str(counter),])
        else:
            df_quest = pd.concat([df_quest, pd.DataFrame(stack_dict, index=[str(counter),])], ignore_index = True)

        counter += 1

    return df_quest


def get_existing_questions():
    base_query = sess.query(SocialPosts).filter(SocialPosts.social_name =='Stack Overflow')
    df_existing = pd.read_sql(str(base_query)[:-2] + str('= "Stack Overflow"'), sess.bind)

    table_name = 'social_posts_'
    cols = list(df_existing.columns)
    for col in cols:
        if col[:len(table_name)] == table_name:
            df_existing = df_existing.rename(columns=({col: col[len(table_name):]}))

    return df_existing


def add_new_questions_to_db(df_to_add):
    df_to_add['time_stamp_utc'] = datetime.utcnow()
    rows_added = df_to_add.to_sql('social_posts', con=engine, if_exists='append', index=False)
    logger_stack.info(f"- Successfully added {rows_added} stack overflow questions to db! -")


def stackoverflow_scheduler_update():
    logger_stack.info(f"- in stackoverflow_scheduler_update -")
    stack_dict = call_stack_overflow_api()
    df_new = new_questions_to_df(stack_dict)

    # Check for existing tweets in db and remove from new set
    df_existing = get_existing_questions()
    logger_stack.info(f"- {len(df_existing)} existing Stack Overflow questions in db -")
    if len(df_existing) == 0:
        df_to_add = df_new
    elif len(df_existing) > 0:
        df_to_add = df_new[~df_new.network_post_id.isin(df_existing.network_post_id)]
    
    logger_stack.info(f"- Adding {len(df_to_add)} questions to db -")
    
    add_new_questions_to_db(df_to_add)

    logger_stack.info("- Finished running Twitter calls -")
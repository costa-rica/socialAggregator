from sa_config import ConfigDev, ConfigProd, ConfigLocal
from sa_models import sess, SocialPosts
from datetime import date, datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import os
import pandas as pd

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
logger_data_service = logging.getLogger(__name__)
logger_data_service.setLevel(logging.DEBUG)
# logger_terminal = logging.getLogger('terminal logger')
# logger_terminal.setLevel(logging.DEBUG)

#where do we store logging information
file_handler = RotatingFileHandler(os.path.join(config.PROJ_ROOT_PATH,'logs','social_agg_data_service.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler.setFormatter(formatter)

#where the stream_handler will print
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter_terminal)

logger_data_service.addHandler(file_handler)
logger_data_service.addHandler(stream_handler)


def get_social_activity_for_df():
    logger_data_service.info(f"- Inside get_social_activity_for_df  -")

    if os.path.exists(os.path.join(config.PROJ_DB_PATH,'df_mirror.pkl')):
        logger_data_service.info(f"- df_mirror Exists  -")
        df_from_db = get_db_social_activity()
        df_existing = pd.read_pickle(os.path.join(config.PROJ_DB_PATH,'df_mirror.pkl'))
        ### make unique index from network_post_id, social_name, title
        df_from_db.set_index(['network_post_id', 'social_name','title'], inplace=True)
        df_existing.set_index(['network_post_id', 'social_name','title'], inplace=True)

        df_to_add = df_from_db[~df_from_db.index.isin(df_existing.index)]
        

        #Append to df_exisitng
        df_mirror = pd.concat([df_existing, df_to_add]).reset_index()
        #df_existing to pickle
        df_mirror.to_pickle(os.path.join(config.PROJ_DB_PATH,'df_mirror.pkl'))

        df_to_add.reset_index(inplace=True)
    
    else:# - All data is new
        logger_data_service.info(f"- df_mirror NOT exists  -")
        df_to_add = get_db_social_activity()
        df_to_add.to_pickle(os.path.join(config.PROJ_DB_PATH,'df_mirror.pkl'))

    logger_data_service.info(f"- df_to_add.columns:   -")
    logger_data_service.info(df_to_add.columns)
    new_dict = df_to_add.to_dict('records')

    return new_dict


def get_db_social_activity():
    # Check for duplicate tweets to remove

    base_query = sess.query(SocialPosts)
    df_existing = pd.read_sql(str(base_query), sess.bind)

    table_name = 'social_posts_'
    cols = list(df_existing.columns)
    for col in cols:
        if col[:len(table_name)] == table_name:
            df_existing = df_existing.rename(columns=({col: col[len(table_name):]}))

    return df_existing
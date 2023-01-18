from apscheduler.schedulers.background import BackgroundScheduler
import json
import requests
from datetime import datetime, timedelta
import os
from sa_config import ConfigLocal, ConfigDev, ConfigProd
import logging
from logging.handlers import RotatingFileHandler
# import pandas as pd
from twitter import twitter_scheduler_update
from stack_overflow import stackoverflow_scheduler_update
from github import github_scheduler_update
from goodreads import goodreads_update
import subprocess
import time
from data_service import get_social_activity_for_df
from sa_models import SocialPosts, sess, engine


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
logger_init = logging.getLogger(__name__)
logger_init.setLevel(logging.DEBUG)
# logger_terminal = logging.getLogger('terminal logger')
# logger_terminal.setLevel(logging.DEBUG)

#where do we store logging information
file_handler = RotatingFileHandler(os.path.join(config.PROJ_ROOT_PATH,'logs','social_agg_schduler.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler.setFormatter(formatter)

#where the stream_handler will print
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter_terminal)

logger_init.addHandler(file_handler)
logger_init.addHandler(stream_handler)


def scheduler_funct():
    logger_init.info(f"- Started Scheduler on {datetime.today().strftime('%Y-%m-%d %H:%M')}-")
 
    scheduler = BackgroundScheduler()

    #job_call_get_locations = scheduler.add_job(get_locations, 'cron', day='*', hour='23', minute='01', second='05')#Production
    #job_call_get_locations = scheduler.add_job(get_locations, 'cron', hour='*', minute='07', second='05')#Testing
    #job_call_harmless = scheduler.add_job(harmless, 'cron',  hour='*', minute='19', second='25')#Testing
    job_collect_socials = scheduler.add_job(collect_socials,'cron', hour='*', minute='04', second='39')#Testing
    # job_collect_socials = scheduler.add_job(collect_socials,'cron', day='*', hour='06', minute='01', second='05')#Production

    scheduler.start()

    while True:
        pass


def social_agg_process():
    logger_init.info(f"--- Start social_agg_process ---")
    collect_socials()
    collect_goodreads()
    logger_init.info(f"- finishe processing soicals at {datetime.today().strftime('%Y-%m-%d %H:%M')}-")
    sending_to_dest()
    logger_init.info(f"--- Scheduler Complete ---")


def check_status():
    logger_init.info(f'- START: schduler.py def check_status() -')
    logger_init.info(f"- CONFIG_TYPE: {os.environ.get('CONFIG_TYPE')} -")
    output_dir = os.path.join(config.PROJ_DB_PATH) 
    logger_init.info(f"- goodreads-user-scraper output_dir: {output_dir} -")

    # get existing db count
    base_query = sess.query(SocialPosts)
    df_existing = pd.read_sql(str(base_query), sess.bind)

    table_name = 'social_posts_'
    cols = list(df_existing.columns)
    for col in cols:
        if col[:len(table_name)] == table_name:
            df_existing = df_existing.rename(columns=({col: col[len(table_name):]}))
    logger_init.info(f"- Database has: {len(df_existing)} rows -")

    # get existing df mirror count

    df_mirror = pd.read_pickle(os.path.join(os.path.join(config.PROJ_DB_PATH),'df_mirror.pkl'))
    logger_init.info(f"- df_mirror has: {len(df_mirror)} rows -")

    logger_init.info(f'- END: check_status() -')


def collect_socials():

    logger_init.info(f'- in collect_socials -')
    twitter_scheduler_update()
    stackoverflow_scheduler_update()
    github_scheduler_update()
    

def collect_goodreads():
    logger_init.info('- in collect_goodreads -')
    startTime_goodreads = time.time()
    # run goodreads
    output_dir = os.path.join(config.PROJ_DB_PATH) 
    goodreads_process = subprocess.Popen(['goodreads-user-scraper', '--user_id', config.GOODREADS_ID,'--output_dir', output_dir])
    
    _, _ = goodreads_process.communicate()
    
    executionTime = (time.time() - startTime_goodreads)
    logger_init.info(f"- Goodreads scraping took: {executionTime} seconds-")
    goodreads_update()


    

def sending_to_dest():
    logger_init.info(f"- Sending updated social activity to destination: {config.API_URL}-")

    new_dict = get_social_activity_for_df()
    headers = {'password':config.DESTINATION_PASSWORD,'Content-Type': 'application/json'}
    payload = {'new_activity':new_dict}
    response = requests.request("POST", config.API_URL, headers=headers, data=str(json.dumps(payload)))

    logger_init.info(f"- Sent updated social data to destination. Status code: {response.status_code} -")
    






if __name__ == '__main__':  
    scheduler_funct()
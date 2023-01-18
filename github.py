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
from datetime import datetime, timedelta


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
logger_github = logging.getLogger(__name__)
logger_github.setLevel(logging.DEBUG)

#where do we store logging information
file_handler = RotatingFileHandler(os.path.join(config.PROJ_ROOT_PATH,'logs','sa_github.log'), mode='a', maxBytes=5*1024*1024,backupCount=2)
file_handler.setFormatter(formatter)

#where the stream_handler will print
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter_terminal)

logger_github.addHandler(file_handler)
logger_github.addHandler(stream_handler)


def call_github_api_repos(headers):
    logger_github.info(f"- calling github repo api -")
    repos_list =[]
    repo_response_list = [1]
    counter = 1
    while len(repo_response_list) > 0:

        git_repos_url = f"https://api.github.com/user/repos?page={counter}"
        git_repo_response = requests.get(git_repos_url,headers=headers)
        if git_repo_response.status_code != 200:
            print('status_code: ',git_repo_response.status_code)
            break
        repo_response_list = git_repo_response.json()

        for repo in repo_response_list:
            repo_dict = {}
            repo_dict['name'] = repo.get('name')
            repo_dict['description'] = repo.get('description')
            repo_dict['pushed_at'] = datetime.strptime(repo.get('pushed_at'), "%Y-%m-%dT%H:%M:%SZ")

            repos_list.append(repo_dict)
        
        counter += 1
    
    logger_github.info(f"- {config.GITHUB_USERNAME} has {len(repos_list)} repos  -")
    recent_repo_list=[]
    for repo in repos_list:
        if timedelta(30) > (datetime.now() - repo.get('pushed_at')):
            recent_repo_list.append(repo)

    logger_github.info(f"- {config.GITHUB_USERNAME} has {len(recent_repo_list)} repos that have been updated in last 60 days  -")

    return recent_repo_list


def call_github_api_commits(headers, recent_repo_list):
    socialc_repo_list_for_df =[]
    for repo in recent_repo_list:
        logger_github.info(f"- Searching {repo.get('name')} repo for commits  -")
        git_commits_url = f"https://api.github.com/repos/{config.GITHUB_USERNAME}/{repo.get('name')}/commits"
        git_commit_response = requests.get(git_commits_url,headers=headers)
        if git_commit_response.status_code == 200:
            
            # for each repo get list of commits in dict
            
            for commit in git_commit_response.json():
                temp_dict = {}
                temp_dict['username'] = commit.get('commit').get('committer').get('name')
                temp_dict['title'] = repo.get('name')
                temp_dict['description'] = commit.get('commit').get('message')
                temp_dict['social_name'] = 'Github'
                temp_dict['social_icon'] = 'github-mark.png'
                temp_dict['post_date'] = commit.get('commit').get('committer').get('date')[:10]
                temp_dict['network_post_id'] = commit.get('sha')
                try:
                    temp_dict['url'] = commit.get('parents')[0].get('html_url')
                except:
                    temp_dict['url'] = f"https://github.com/{config.GITHUB_USERNAME}/{repo.get('name')}"

                socialc_repo_list_for_df.append(temp_dict)
            logger_github.info(f"- Successfully called github for repo: {repo.get('name')}")
        else:
            logger_github.info(f"- unable to get info for {repo.get('name')}")
    
    df_new = pd.DataFrame(socialc_repo_list_for_df)


    return df_new


def get_existing_commits():
    # Check for duplicate tweets to remove

    base_query = sess.query(SocialPosts).filter(SocialPosts.social_name =='Github')
    df_existing = pd.read_sql(str(base_query)[:-2] + str('= "Github"'), sess.bind)

    table_name = 'social_posts_'
    cols = list(df_existing.columns)
    for col in cols:
        if col[:len(table_name)] == table_name:
            df_existing = df_existing.rename(columns=({col: col[len(table_name):]}))

    return df_existing


def add_new_github_to_db(df_to_add):
    df_to_add['time_stamp_utc'] = datetime.utcnow()
    rows_added = df_to_add.to_sql('social_posts', con=engine, if_exists='append', index=False)
    print(f"- Successfully added {rows_added} commits to db! -")


def github_scheduler_update():
    logger_github.info(f"- github_scheduler_update -")
    headers={}
    headers['Accept'] ="application/vnd.github+json"
    headers['Authorization']="Bearer " + config.GITHUB_TOKEN
    headers['X-GitHub-Api-Version']="2022-11-28"

    recent_repo_list = call_github_api_repos(headers)

    df_new = call_github_api_commits(headers, recent_repo_list)

    df_existing = get_existing_commits()

    if len(df_existing) == 0:
        df_to_add = df_new
    elif len(df_existing) > 0:
        df_to_add = df_new[~df_new.network_post_id.isin(df_existing.network_post_id)]

    logger_github.info(f"- Adding {len(df_to_add)} commits to db -")
    logger_github.info(f"- github_scheduler_update completed -")
    add_new_github_to_db(df_to_add)
import os
import pandas as pd
import tweepy

from prefect import flatten, task, Flow
from prefect.tasks.notifications.email_task import EmailTask
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

TWEET = """Check out my latest blog {title}!

Medium: {medium_link}
My blog: {blog_link}

{hashtags}
"""

def get_blob_info():
    
    df = pd.read_csv("/mnt/shared/blogs.csv")

    df = df.iloc[-2,:]

    return df["Title"], df["MediumLink"], df["BlogLink"], df["Hashtags"]

@task
def send_tweet(title: str, medium_link: str, blog_link: str, hashtags: str):

    CONSUMER_KEY = os.environ.get('CONSUMER_KEY', None)
    CONSUMER_SECRET = os.environ.get('CONSUMER_SECRET', None)
    ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN', None)
    ACCESS_TOKEN_SECRET = os.environ.get('ACCESS_TOKEN_SECRET', None)

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    api.update_status(
        TWEET.format(
            title=title,
            medium_link=medium_link,
            blog_link=blog_link,
            hashtags=hashtags
        )
    )
        
# Monday at 8pm
schedule = Schedule(clocks=[CronClock("0 0 * * 2")])

with Flow("Send Tweet", schedule=schedule) as flow:

    title, medium_link, blog_link, hashtags = get_blob_info()

    send_tweet(title, medium_link, blog_link, hashtags)

flow.register(project_name="Blog-Tweeter")

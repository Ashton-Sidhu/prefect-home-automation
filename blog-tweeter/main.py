import os
import pandas as pd
import numpy as np
import tweepy

from prefect import flatten, task, Flow
from prefect.tasks.notifications.email_task import EmailTask
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

TWEET_ATS = """Check out my latest blog: "{title}"!

Medium: {medium_link}
My blog: {blog_link}

{ats}

{hashtags}
"""

TWEET_NO_ATS = """Check out my latest blog: "{title}"!

Medium: {medium_link}
My blog: {blog_link}

{hashtags}
"""

def get_blob_info():
    
    df = pd.read_csv("/mnt/shared/blogs.csv")

    df = df.iloc[-1,:]

    return df["Title"], df["MediumLink"], df["BlogLink"], df["Hashtags"], df["Ats"]

@task
def send_tweet(title: str, medium_link: str, blog_link: str, hashtags: str, ats: str):

    CONSUMER_KEY = os.environ.get('CONSUMER_KEY', None)
    CONSUMER_SECRET = os.environ.get('CONSUMER_SECRET', None)
    ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN', None)
    ACCESS_TOKEN_SECRET = os.environ.get('ACCESS_TOKEN_SECRET', None)

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    if ats is not np.nan:
        tweet = TWEET_ATS.format(
            title=title,
            medium_link=medium_link,
            blog_link=blog_link,
            ats=ats.strip('"'),
            hashtags=hashtags
        )
    else:
        tweet = TWEET_NO_ATS.format(
            title=title,
            medium_link=medium_link,
            blog_link=blog_link,
            hashtags=hashtags
        )

    api.update_status(
        tweet
    )
        
# Tuesday at 8pm
schedule = Schedule(clocks=[CronClock("0 0 * * 3")])

with Flow("Send Tweet", schedule=schedule) as flow:

    title, medium_link, blog_link, hashtags, ats = get_blob_info()

    send_tweet(title, medium_link, blog_link, hashtags, ats)

flow.register(project_name="Blog-Tweeter")

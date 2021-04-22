import tweepy
import os
import prefect
from datetime import datetime, timedelta

from elasticsearch_dsl import Document, Text, connections
from prefect import task, Flow, Parameter, case


class Tweet(Document):

    tweet = Text()
    user = Text()
    tweet_url = Text()
    doc_type = Text()

    class Index:
        name = "tweet-index"


@task
def get_cursor():

    CONSUMER_KEY = os.environ.get("CONSUMER_KEY", None)
    CONSUMER_SECRET = os.environ.get("CONSUMER_SECRET", None)
    ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN", None)
    ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET", None)

    if (
        not CONSUMER_KEY
        and not CONSUMER_SECRET
        and not ACCESS_TOKEN
        and not ACCESS_TOKEN_SECRET
    ):
        raise ValueError(
            "Consumer and Access keys and secrets must be set as environment variables."
        )

    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    curs = tweepy.Cursor(api.favorites, tweet_mode="extended")

    return curs


@task
def get_tweets(curs):

    return curs.items()


@task
def tweets_to_process(tweets, start_date, end_date):

    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    return [
        tweet
        for tweet in tweets
        if tweet.created_at.date() >= start_date and tweet.created_at.date() <= end_date
    ]


@task
def any_tweets(tweets):
    return len(tweets) > 0


@task
def upload_tweets_elastic(tweets):

    logger = prefect.context.get("logger")

    tweets = [
        Tweet(
            tweet=tweet.full_text,
            user=tweet.user.name,
            doc_type="favourite",
            tweet_url=f"https://twitter.com/twitter/statuses/{tweet.id}",
        )
        for tweet in tweets
    ]

    connections.create_connection(hosts=["dev.lan:9200"])

    Tweet.init()

    logger.info("Index created!")

    logger.info("Loading Tweets...")
    for tweet in tweets:
        tweet.save()

    logger.info("Tweets loaded!")


with Flow("load_tweets") as flow:

    start_date = Parameter(
        "start_date",
        default=(datetime.today() - timedelta(days=1)).date().strftime("%Y-%m-%d"),
    )
    end_date = Parameter(
        "end_date",
        default=(datetime.today() - timedelta(days=1)).date().strftime("%Y-%m-%d"),
    )

    curs = get_cursor()

    tweets = get_tweets(curs)
    tweets = tweets_to_process(tweets, start_date, end_date)

    with case(any_tweets(tweets), True):
        upload_tweets_elastic(tweets)

flow.register(project_name="twitter-favs")

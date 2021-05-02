import tweepy
import os
import prefect

from elasticsearch_dsl import Document, Text, connections, Index
from prefect import task, Flow, Parameter


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
def create_global_connection():
    connections.create_connection(hosts=["dev.lan:9200"])

@task
def get_tweets(curs):

    return curs.items()


@task
def delete_index(index):
    i = Index(index)

    i.delete()


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

    Tweet.init()

    logger.info("Index created!")

    logger.info("Loading Tweets...")
    for tweet in tweets:
        tweet.save()

    logger.info("Tweets loaded!")


with Flow("load_tweets") as flow:

    index = Parameter(
        "index",
        default="tweet-index"
    )

    curs = get_cursor()

    tweets = get_tweets(curs)

    create_conn_task = create_global_connection()

    del_task = delete_index(index)

    upload_task = upload_tweets_elastic(tweets)

    flow.add_edge(create_conn_task, del_task)
    flow.add_edge(del_task, upload_task)


flow.register(project_name="twitter-favs")

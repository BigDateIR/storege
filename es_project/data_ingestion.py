import json
from connection import get_connection

def insert_tweet(es, index_name, tweet):
    response = es.index(index=index_name, document=tweet)
    print(f"Tweet indexed with ID: {response['_id']}")

def insert_tweets(es, index_name, tweets):
    for tweet in tweets:
        insert_tweet(es, index_name, tweet)

if __name__ == "__main__":
    es = get_connection()
    index_name = "tweets"

    with open("data/tweets.json", "r") as file:
        tweets = json.load(file)

    insert_tweets(es, index_name, tweets)

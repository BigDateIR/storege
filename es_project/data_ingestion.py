from kafka import KafkaConsumer
from elasticsearch.helpers import bulk
import json
import os
import datetime


def save_to_json_bulk(tweets, file_path="tweets.json"):
    """Save a batch of tweets to a JSON file."""
    try:
        if not os.path.exists(file_path):
            with open(file_path, "w") as file:
                json.dump([], file)

        with open(file_path, "r+") as file:
            data = json.load(file)
            data.extend(tweets)
            file.seek(0)
            json.dump(data, file, indent=4)
        print(f"Batch of {len(tweets)} tweets saved to JSON file: {file_path}")
    except Exception as e:
        print(f"Error saving tweets to JSON file: {e}")


def bulk_insert_tweets(es, index_name, tweets):
    """Index a batch of tweets into Elasticsearch."""
    actions = []
    for tweet in tweets:
        tweet["@timestamp"] = tweet.get("timestamp", datetime.datetime.utcnow().isoformat())

        if "location" in tweet:
            location = tweet["location"]
            if isinstance(location, dict) and "latitude" in location and "longitude" in location:
                tweet["location"] = {"lat": location["latitude"], "lon": location["longitude"]}
            else:
                print(f"Invalid location format, skipping tweet: {tweet.get('tweet_id')}")
                continue

        actions.append({
            "_index": index_name,
            "_id": tweet["tweet_id"],
            "_source": tweet
        })

    if actions:
        try:
            bulk(es, actions)
            print(f"Indexed {len(actions)} tweets in bulk.")
        except Exception as e:
            print(f"Error bulk indexing tweets: {e}")


def consume_from_kafka(es, index_name, topic_name, bootstrap_servers, batch_size=500):
    """Consume messages from Kafka and process them in batches."""
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='tweet-ingestion-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        max_poll_records=batch_size,
        consumer_timeout_ms=1000 
    )

    print(f"Connected to Kafka topic: {topic_name}")
    tweets_batch = []

    for message in consumer:
        try:
            tweet = message.value
            tweets_batch.append(tweet)

            if len(tweets_batch) >= batch_size:
                save_to_json_bulk(tweets_batch)
                bulk_insert_tweets(es, index_name, tweets_batch)
                tweets_batch = []
        except Exception as e:
            print(f"Error processing message: {e}")

    if tweets_batch:
        save_to_json_bulk(tweets_batch)
        bulk_insert_tweets(es, index_name, tweets_batch)

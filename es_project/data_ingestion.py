from kafka import KafkaConsumer
from connection import get_connection
import json
import os

def save_to_json(tweet, file_path="tweets.json"):
    """Save tweet to a JSON file."""
    try:
        if not os.path.exists(file_path):
            with open(file_path, "w") as file:
                json.dump([], file)

        with open(file_path, "r+") as file:
            data = json.load(file)
            data.append(tweet)
            file.seek(0)
            json.dump(data, file, indent=4)
        print(f"Tweet saved to JSON file: {file_path}")
    except Exception as e:
        print(f"Error saving tweet to JSON file: {e}")

def insert_tweet(es, index_name, tweet):
    """Index tweet into Elasticsearch and save to JSON."""
    save_to_json(tweet)

    try:
        response = es.index(index=index_name, document=tweet)
        print(f"Tweet indexed with ID: {response['_id']}")
    except Exception as e:
        print(f"Error indexing tweet: {e}")

def consume_from_kafka(es, index_name, topic_name, bootstrap_servers):
    """Consume messages from Kafka and process them."""
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='tweet-ingestion-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Connected to Kafka topic: {topic_name}")

    for message in consumer:
        tweet = message.value
        print(f"Consumed tweet: {tweet}")
        insert_tweet(es, index_name, tweet)

if __name__ == "__main__":
    es = get_connection()
    index_name = "tweets"

    topic_name_tweets = "send_consumer_data"
    bootstrap_servers = ["localhost:9092"]

    consume_from_kafka(es, index_name, topic_name_tweets, bootstrap_servers)

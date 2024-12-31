from kafka import KafkaConsumer
from connection import get_connection
import json

def insert_tweet(es, index_name, tweet):
    response = es.index(index=index_name, document=tweet)
    print(f"Tweet indexed with ID: {response['_id']}")

def consume_from_kafka(es, index_name, topic_name, bootstrap_servers):
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

    topic_name_tweets = "tweets-topic"
    bootstrap_servers = ["localhost:9092"]

    consume_from_kafka(es, index_name, topic_name_tweets, bootstrap_servers)

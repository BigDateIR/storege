from connection import get_connection
from index_management import create_index
from data_ingestion import insert_tweets
from queries import search_tweets, geo_search, sentiment_search
import json

def main():

    es = get_connection()
    index_name = "tweets"

    if not es.indices.exists(index=index_name):
        create_index(es, index_name)

    with open("data/tweets.json", "r") as file:
        tweets = json.load(file)
    insert_tweets(es, index_name, tweets)

    while True:
        print("\nSelect an option:")
        print("1. Search by text")
        print("2. Geo search")
        print("3. Sentiment search")
        print("4. Exit")

        choice = input("\nEnter your choice (1/2/3/4): ").strip()

        if choice == "1":
            query = input("Enter text to search: ").strip()
            print("\nSearch results:")
            search_tweets(es, index_name, query)

        elif choice == "2":
            lat = float(input("Enter latitude: ").strip())
            lon = float(input("Enter longitude: ").strip())
            distance = input("Enter distance (e.g., 50km): ").strip()
            print("\nGeo search results:")
            geo_search(es, index_name, lat, lon, distance)

        elif choice == "3":
            sentiment = input("Enter sentiment (Positive/Neutral/Negative): ").strip()
            print("\nSentiment search results:")
            sentiment_search(es, index_name, sentiment)

        elif choice == "4":
            print("Exiting the program. Goodbye!")
            break

        else:
            print("Invalid choice. Please try again.")

    es = get_connection()
    index_name = "tweets"

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Index '{index_name}' deleted.")
    create_index(es, index_name)

    with open("data/tweets.json", "r") as file:
        tweets = json.load(file)
    insert_tweets(es, index_name, tweets)

    while True:
        print("\nSelect an option:")
        print("1. Search by text")
        print("2. Geo search")
        print("3. Sentiment search")
        print("4. Exit")

        choice = input("\nEnter your choice (1/2/3/4): ").strip()

        if choice == "1":
            query = input("Enter text to search: ").strip()
            print("\nSearch results:")
            search_tweets(es, index_name, query)

        elif choice == "2":
            lat = float(input("Enter latitude: ").strip())
            lon = float(input("Enter longitude: ").strip())
            distance = input("Enter distance (e.g., 50km): ").strip()
            print("\nGeo search results:")
            geo_search(es, index_name, lat, lon, distance)

        elif choice == "3":
            sentiment = input("Enter sentiment (Positive/Neutral/Negative): ").strip()
            print("\nSentiment search results:")
            sentiment_search(es, index_name, sentiment)

        elif choice == "4":
            print("Exiting the program. Goodbye!")
            break

        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()

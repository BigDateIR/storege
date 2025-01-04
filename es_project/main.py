from connection import get_connection
from index_management import create_index
from data_ingestion import consume_from_kafka

def main():
    es = get_connection()
    index_name = "tweetsone"

    topic_name_tweets = "send_consumer_data"
    bootstrap_servers = ["localhost:9092"]

    # Ensure the index exists
    if not es.indices.exists(index=index_name):
        create_index(es, index_name)

    print("Starting Kafka consumer...")
    consume_from_kafka(es, index_name, topic_name_tweets, bootstrap_servers)

if __name__ == "__main__":
    main()

"""
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
            """


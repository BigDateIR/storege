def search_tweets(es, index_name, query):
    response = es.search(index=index_name, query={"match": {"text": query}})
    print("Search results:")
    for hit in response["hits"]["hits"]:
        print(hit["_source"])

def geo_search(es, index_name, lat, lon, distance="50km"):
    query = {
        "geo_distance": {
            "distance": distance,
            "location": {"lat": lat, "lon": lon}
        }
    }
    response = es.search(index=index_name, query=query)
    print("Geo search results:")
    for hit in response["hits"]["hits"]:
        print(hit["_source"])

    query = {
        "geo_distance": {
            "distance": distance,
            "location": {"lat": lat, "lon": lon}
        }
    }
    response = es.search(index=index_name, query=query)
    print("Geo search results:")
    for hit in response["hits"]["hits"]:
        print(hit["_source"])

def sentiment_search(es, index_name, sentiment):
    query = {"match": {"sentiment": sentiment}}
    response = es.search(index=index_name, query=query)
    print("Sentiment search results:")
    for hit in response["hits"]["hits"]:
        print(hit["_source"])

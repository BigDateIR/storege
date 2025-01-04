def create_index(es, index_name="tweetsone"):
    settings = {
        "mappings": {
            "properties": {
                "tweet_id": {"type": "keyword"},
                "text": {"type": "text"},
                "hashtags": {"type": "keyword"},
                "timestamp": {"type": "date"},
                "location": {"type": "geo_point"},
                "sentiment": {"type": "keyword"},
                "user": {
                    "properties": {
                        "user_id": {"type": "keyword"},
                        "name": {"type": "text"}
                    }
                }
            }
        }
    }
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=settings)
        print(f"Index '{index_name}' created successfully.")
    else:
        print(f"Index '{index_name}' already exists.")

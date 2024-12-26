from elasticsearch import Elasticsearch

def get_connection():
    es = Elasticsearch("http://localhost:9200")
    if es.ping():
        print("Connected to Elasticsearch")
    else:
        print("Failed to connect to Elasticsearch")
    return es

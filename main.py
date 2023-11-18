import os
import redis
import pymongo
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import certifi
import json

print("Starting app")
# Connection Redis
try:
    print("Connecting to Redis")
    redis_host = os.environ.get('uri_redis', 'localhost')
    redis_port = os.environ.get('port_redis', '18734')
    redis_password = os.environ.get('REDIS_PASSWORD', None)
    redis_db = "db0"

    if redis_password:
        redis_client = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        print("Connected to Redis")
    else:
        print("Supply a password for Redis")
except Exception as e:
    print("Error connecting to Redis: {}".format(e))

# Connection MongoDB
try:
    print("Connecting to MongoDB")
    mongo_uri = os.environ.get('uri_mongo', None)
    mongo_client = pymongo.MongoClient(mongo_uri, tlsCAFile=certifi.where())
    mongo_db = mongo_client['sample_mflix']
    print("Connected to MongoDB")
except Exception as e:
    print("Error connecting to MongoDB: {}".format(e))

app = FastAPI(
    title="POC ASSIGNMENT 2",
    description="API for Mongo & Redis",
    version="0.1.0",
    openapi_url="/openapi.json",
    docs_url="/",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def read_redis(key):
    try:
        value = redis_client.get(key)
        if value:
            return value
        else:
            return None
    except Exception as err_mongo:
        print("Error reading from Redis: {}".format(err_mongo))
        raise HTTPException(status_code=500, detail="Error reading from Redis: {}".format(err_mongo))


def write_redis(key, value):
    try:
        redis_client.set(key, value)
    except Exception as err_redis:
        print("Error writing to Redis: {}".format(err_redis))
        raise HTTPException(status_code=500, detail="Error writing to Redis: {}".format(err_redis))


def clean_json(data):
    data = data.replace("'", '"')
    data = data.replace("\\", "")
    return data


@app.post("/query_top_n")
async def query_top_n(n: int, from_year: int, to_year: int):
    try:
        redis_key = f"top-{n}-{from_year}-{to_year}"
        value = read_redis(redis_key)
        if value:
            # decode utf-8
            value = value.decode('utf-8')
            return {"message": "Returning from Redis", "result": json.loads(clean_json(value))}
        else:
            print("Querying MongoDB")
            pipeline = [
                {"$match": {"year": {"$gte": from_year, "$lte": to_year}}},
                {"$group": {"_id": "$title", "comment_count": {"$sum": 1}}},
                {"$sort": {"comment_count": -1}},
                {"$limit": n}
            ]
            cursor = mongo_db.movies.aggregate(pipeline)
            result = json.dumps(list(cursor))
            write_redis(redis_key, result)
            if result:
                return {"message": "Returning from Mongo", "result": json.loads(clean_json(result))}
            else:
                return {"message": "No results found", "result": []}


    except Exception as err:
        print("Error querying MongoDB: {}".format(err))
        raise HTTPException(status_code=500, detail="Error querying MongoDB: {}".format(err))

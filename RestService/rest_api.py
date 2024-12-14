"""
File: rest_api.py
Date: Oct 3, 2024
Description: rest api service for movie app
"""

from filecmp import clear_cache
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
import redis
import pickle
from redis.asyncio import Redis  # Async Redis client
from contextlib import asynccontextmanager
from contextlib import asynccontextmanager
import logging
from SPARQLWrapper import SPARQLWrapper, JSON

from Backend.db_crud import MovieDatabase

graphdb_endpoint = "http://localhost:7200/repositories/MoviesRepo"
sparql = SPARQLWrapper(graphdb_endpoint)


DO_LOGS = False
if DO_LOGS:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

def write_log(message, type="info"):
    if DO_LOGS:
        if "error" in type:
            logger.error(message)
        else:
            logger.info(message)

def get_redis_cache():
    return redis.Redis(
        host='redis-cache',
        port='6379',
    )

redis_client = Redis(host="redis-cache", port=6379)
FastAPICache.init(RedisBackend(redis_client), prefix="fastapi-cache")


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Lifespan context manager invoked.", flush=True)
    if not db_crud.is_connected():
        print("Connecting to the database.", flush=True)
        await db_crud.connect()
    yield
    if db_crud.is_connected():
        await db_crud.disconnect()

app = FastAPI(lifespan=lifespan, title="cec-assignment FastAPI Service")

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get('/movies')
@cache(expire=300)
async def get_movies_titles(title: str = Query(..., alias="movieLabel"), redis_client: cache = Depends(get_redis_cache)):
    try:
        write_log(f"Getting movies that contain {title} in their label", "info")
        var_name = f"movie_{title}"

        if (cached_answer := redis_client.get(var_name)) is not None:
            write_log(f"Found movie query {title} in cache")

            return pickle.loads(cached_answer)

        results = await MovieDatabase.fetch_movies_by_title(title)  # Use CRUD operation
        redis_client.set(var_name, pickle.dumps(results))
        write_log(f"Written movie query {title} into cache")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"The following error occurred during the operation: {str(e)}")

    return results

@app.get('/ping')
async def ping():
    return {"message": "I am alive :)"}

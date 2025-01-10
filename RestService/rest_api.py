"""
File: rest_api.py
Date: 10-01-2025
Description: rest api service for movie app
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from typing import Optional, List
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache
import redis
import pickle
from redis.asyncio import Redis  # Async Redis client
from contextlib import asynccontextmanager
import logging
from SPARQLWrapper import SPARQLWrapper, JSON
from db_crud import MovieDatabase

movieDatabase = MovieDatabase()

DO_LOGS = True
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

app = FastAPI(lifespan=lifespan, title="Knowledge and Data Engineer assignment FastAPI Service")

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get('/movies')
@cache(expire=300)
async def get_movies_titles(title: Optional[List[str]] = Query(None, alias="movieLabel"),
                            genre: Optional[List[str]] = Query(None, alias="genre"),
                            actor: Optional[List[str]] = Query(None, alias="actor"),
                            director: Optional[List[str]] = Query(None, alias="director"),
                            distributor: Optional[List[str]] = Query(None, alias="distributor"),
                            writer: Optional[List[str]] = Query(None, alias="writer"),
                            producer: Optional[List[str]] = Query(None, alias="producer"),
                            composer: Optional[List[str]] = Query(None, alias="composer"),
                            cinematographer: Optional[List[str]] = Query(None, alias="cinematographer"),
                            production_company: Optional[List[str]] = Query(None, alias="productionCompany"),
                            redis_client: cache = Depends(get_redis_cache)):
    try:
        write_log(f"Getting movies with provided filters", "info")
        params = {
            "title": title,
            "genre": genre,
            "actor": actor,
            "director": director,
            "distributor": distributor,
            "writer": writer,
            "producer": producer,
            "composer": composer,
            "cinematographer": cinematographer,
            "production_company": production_company
        }
        filtered_params = {k: v for k, v in params.items() if v}
        
        # Generate a cache key based on the filtered parameters
        var_name = "movie_" + "_".join(f"{k}_{'_'.join(v)}" for k, v in filtered_params.items())
        if (cached_answer := redis_client.get(var_name)) is not None:
            write_log(f"Found movie query in cache")
            return pickle.loads(cached_answer)

        # Adjust the query logic to handle lists of values
        # query_conditions = []
        # for key, values in filtered_params.items():
        #     if isinstance(values, list):
        #         query_conditions.append(f"{key} IN ({', '.join(map(repr, values))})")
        #     else:
        #         query_conditions.append(f"{key} = {repr(values)}")

        # query_string = " AND ".join(query_conditions)
        results = await movieDatabase.fetch_movies_by_properties(**params)
        
        redis_client.set(var_name, pickle.dumps(results))
        write_log(f"Written movie query into cache")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"The following error occurred during the operation: {str(e)}")

    return results

@app.get('/genres')
@cache(expire=300)
async def get_genres_by_name(name: Optional[str] = Query(None, alias="genreName"), redis_client: cache = Depends(get_redis_cache)):
    try:
        write_log(f"Getting genres with name {name}", "info")
        var_name = f"genre_{name}"
        if (cached_answer := redis_client.get(var_name)) is not None:
            write_log(f"Found genre query in cache")
            return pickle.loads(cached_answer)

        results = await movieDatabase.fetch_genres_by_name(name)
        redis_client.set(var_name, pickle.dumps(results))
        write_log(f"Written genre query into cache")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"The following error occurred during the operation: {str(e)}")

    return results

@app.get('/actors')
@cache(expire=300)
async def get_actors_by_name(name: Optional[str] = Query(None, alias="actorName"), redis_client: cache = Depends(get_redis_cache)):
    try:
        write_log(f"Getting actors with name {name}", "info")
        var_name = f"actor_{name}"
        if (cached_answer := redis_client.get(var_name)) is not None:
            write_log(f"Found actor query in cache")
            return pickle.loads(cached_answer)

        results = await movieDatabase.fetch_actors_by_name(name)
        redis_client.set(var_name, pickle.dumps(results))
        write_log(f"Written actor query into cache")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"The following error occurred during the operation: {str(e)}")

    return results

@app.get('/directors')
@cache(expire=300)
async def get_directors_by_name(name: Optional[str] = Query(None, alias="directorName"), redis_client: cache = Depends(get_redis_cache)):
    try:
        write_log(f"Getting directors with name {name}", "info")
        var_name = f"director_{name}"
        if (cached_answer := redis_client.get(var_name)) is not None:
            write_log(f"Found director query in cache")
            return pickle.loads(cached_answer)

        results = await movieDatabase.fetch_directors_by_name(name)
        redis_client.set(var_name, pickle.dumps(results))
        write_log(f"Written director query into cache")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"The following error occurred during the operation: {str(e)}")

    return results

@app.get('/distributors')
@cache(expire=300)
async def get_distributors_by_name(name: Optional[str] = Query(None, alias="distributorName"), redis_client: cache = Depends(get_redis_cache)):
    try:
        write_log(f"Getting distributors with name {name}", "info")
        var_name = f"distributor_{name}"
        if (cached_answer := redis_client.get(var_name)) is not None:
            write_log(f"Found distributor query in cache")
            return pickle.loads(cached_answer)

        results = await movieDatabase.fetch_distributors_by_name(name)
        redis_client.set(var_name, pickle.dumps(results))
        write_log(f"Written distributor query into cache")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"The following error occurred during the operation: {str(e)}")

    return results

@app.get('/writers')
@cache(expire=300)
async def get_writers_by_name(name: Optional[str] = Query(None, alias="writerName"), redis_client: cache = Depends(get_redis_cache)):
    try:
        write_log(f"Getting writers with name {name}", "info")
        var_name = f"writer_{name}"
        if (cached_answer := redis_client.get(var_name)) is not None:
            write_log(f"Found writer query in cache")
            return pickle.loads(cached_answer)

        results = await movieDatabase.fetch_writers_by_name(name)
        redis_client.set(var_name, pickle.dumps(results))
        write_log(f"Written writer query into cache")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"The following error occurred during the operation: {str(e)}")

    return results

@app.get('/producers')
@cache(expire=300)
async def get_producers_by_name(name: Optional[str] = Query(None, alias="producerName"), redis_client: cache = Depends(get_redis_cache)):
    try:
        write_log(f"Getting producers with name {name}", "info")
        var_name = f"producer_{name}"
        if (cached_answer := redis_client.get(var_name)) is not None:
            write_log(f"Found producer query in cache")
            return pickle.loads(cached_answer)

        results = await movieDatabase.fetch_producers_by_name(name)
        redis_client.set(var_name, pickle.dumps(results))
        write_log(f"Written producer query into cache")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"The following error occurred during the operation: {str(e)}")

    return results

@app.get('/composers')
@cache(expire=300)
async def get_composers_by_name(name: Optional[str] = Query(None, alias="composerName"), redis_client: cache = Depends(get_redis_cache)):
    try:
        write_log(f"Getting composers with name {name}", "info")
        var_name = f"composer_{name}"
        if (cached_answer := redis_client.get(var_name)) is not None:
            write_log(f"Found composer query in cache")
            return pickle.loads(cached_answer)

        results = await movieDatabase.fetch_composers_by_name(name)
        redis_client.set(var_name, pickle.dumps(results))
        write_log(f"Written composer query into cache")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"The following error occurred during the operation: {str(e)}")

    return results

@app.get('/cinematographers')
@cache(expire=300)
async def get_cinematographers_by_name(name: Optional[str] = Query(None, alias="cinematographerName"), redis_client: cache = Depends(get_redis_cache)):
    try:
        write_log(f"Getting cinematographers with name {name}", "info")
        var_name = f"cinematographer_{name}"
        if (cached_answer := redis_client.get(var_name)) is not None:
            write_log(f"Found cinematographer query in cache")
            return pickle.loads(cached_answer)

        results = await movieDatabase.fetch_cinematographers_by_name(name)
        redis_client.set(var_name, pickle.dumps(results))
        write_log(f"Written cinematographer query into cache")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"The following error occurred during the operation: {str(e)}")

    return results

@app.get('/production_companies')
@cache(expire=300)
async def get_production_companies_by_name(name: Optional[str] = Query(None, alias="productionCompanyName"), redis_client: cache = Depends(get_redis_cache)):
    try:
        write_log(f"Getting production companies with name {name}", "info")
        var_name = f"production_company_{name}"
        if (cached_answer := redis_client.get(var_name)) is not None:
            write_log(f"Found production company query in cache")
            return pickle.loads(cached_answer)

        results = await movieDatabase.fetch_productionCompanies_by_name(name)
        redis_client.set(var_name, pickle.dumps(results))
        write_log(f"Written production company query into cache")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"The following error occurred during the operation: {str(e)}")

    return results

@app.get('/countries')
@cache(expire=300)
async def get_countries_by_name(name: Optional[str] = Query(None, alias="country"), redis_client: cache = Depends(get_redis_cache)):
    try:
        write_log(f"Getting countries with name {name}", "info")
        var_name = f"country_{name}"
        if (cached_answer := redis_client.get(var_name)) is not None:
            write_log(f"Found country_ query in cache")
            return pickle.loads(cached_answer)

        results = await movieDatabase.fetch_countries_by_name(name)
        redis_client.set(var_name, pickle.dumps(results))
        write_log(f"Written country_ query into cache")
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"The following error occurred during the operation: {str(e)}")

    return results

@app.get('/clear_cache')
async def clear_cache(redis_client: cache = Depends(get_redis_cache)):
    try:
        redis_client.flushall()
        write_log("Cleared all cache", "info")
        return {"message": "Cache cleared successfully"}
    except Exception as e:
        print(f"Error clearing cache: {e}")
        raise HTTPException(status_code=500, detail=f"The following error occurred during the operation: {str(e)}")


@app.get('/ping')
async def ping():
    return {"message": "I am alive :)"}
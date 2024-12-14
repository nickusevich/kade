from SPARQLWrapper import SPARQLWrapper, JSON
import logging
import asyncio
import numpy as np
import os

def is_running_in_docker():
    """Check if the code is running inside a Docker container."""
    path = '/proc/1/cgroup'
    if os.path.exists(path):
        with open(path, 'r') as f:
            return 'docker' in f.read()
    return False

GRAPHDB_ENDPOINT = "http://localhost:7200/repositories/MoviesRepo"
if is_running_in_docker():
    GRAPHDB_ENDPOINT = "http://host.docker.internal:7200/repositories/MoviesRepo"

class MovieDatabase:

    def __init__(self):
        self.sparql = SPARQLWrapper(GRAPHDB_ENDPOINT)
        self.limit = 5000

    def close(self):
        self.sparql = None

    def is_connected(self):
        try:
            self.sparql.queryType = 'SELECT'
            self.sparql.setQuery("ASK WHERE { ?s ?p ?o }")
            self.sparql.setReturnFormat(JSON)
            result = self.sparql.query().convert()
            return result.get('boolean', False)
        except Exception as e:
            logging.error(f"Database connection check failed: {e}")
            return False

    async def fetch_objects_by_title(self, object_type: str, title: str = None):
        return_data = []

        # Check if connected to the database
        if not self.is_connected():
            logging.info("Not connected to the database. Attempting to reconnect.")
            self.sparql = SPARQLWrapper(GRAPHDB_ENDPOINT)
            if not self.is_connected():
                logging.error("Failed to reconnect to the database.")
                raise Exception("Failed to reconnect to the database.")

        # Generate case-insensitive filters
        name_filter = 'LANG(?label) = "en"'
        if title:
            name_filter += f'&& CONTAINS(LCASE(STR(?label)), "{title.lower()}")'

        # Construct the SPARQL query
        query = f"""
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?object ?label
        WHERE {{
        ?object a dbo:{object_type} .
        ?object rdfs:label ?label .
        FILTER ({name_filter})
        }}
        LIMIT {self.limit}
        """

        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)

        # Execute the query and process results
        try:
            logging.info(f"Executing SPARQL query: {query}")
            results = self.sparql.query().convert()
            if "results" in results and "bindings" in results["results"]:
                return_data = [
                    {
                        "object_uri": result["object"]["value"],
                        "label": result["label"]["value"]
                    }
                    for result in results["results"]["bindings"]
                ]
            else:
                logging.warning("No results found in SPARQL query response.")
        except Exception as e:
            logging.error(f"fetch_objects_by_title - Failed: {e}")
            raise

        return return_data

    async def fetch_movies_by_title(self, title: str = None):
        return await self.fetch_objects_by_title("Film", title)

    async def fetch_actors_by_name(self, name: str = None):
        return await self.fetch_objects_by_title("Actor", name)
    
    async def fetch_directors_by_name(self, name: str = None):
        return await self.fetch_objects_by_title("Director", name)
    
    async def fetch_distributor_by_name(self, name: str = None):
        return await self.fetch_objects_by_title("Distributor", name)
    
    async def fetch_writer_by_name(self, name: str = None):
        return await self.fetch_objects_by_title("Writer", name)
    
    async def fetch_producer_by_name(self, name: str = None):
        return await self.fetch_objects_by_title("Producer", name)



async def main():
    db = MovieDatabase()
    movie_results = await db.fetch_movies_by_title("matrix")
    print(f"Movies found: {len(movie_results)}")
    print(movie_results[:10])

    actor_results = await db.fetch_actors_by_name()
    print(f"Actors found: {len(actor_results)}")
    print(actor_results[:10])

    director_results = await db.fetch_directors_by_name()
    print(f"Directors found: {len(director_results)}")
    print(director_results[:10])

    distributor_results = await db.fetch_distributor_by_name()
    print(f"Directors found: {len(distributor_results)}")
    print(distributor_results[:10])

    writer_results = await db.fetch_writer_by_name()
    print(f"Writers found: {len(writer_results)}")
    print(writer_results[:10])

    producer_results = await db.fetch_producer_by_name()
    print(f"Producers found: {len(producer_results)}")
    print(producer_results[:10])

if __name__ == "__main__":
    asyncio.run(main())
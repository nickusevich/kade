"""
This module provides a class for interacting with a SPARQL endpoint to fetch various types of objects
such as movies, actors, directors, etc., from a GraphDB repository.
"""

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
    """
    A class to interact with a SPARQL endpoint to fetch various types of objects.
    """

    def __init__(self):
        """
        Initialize the MovieDatabase with the SPARQL endpoint.
        """
        self.sparql = SPARQLWrapper(GRAPHDB_ENDPOINT)
        self.limit = 5000

    def close(self):
        """
        Close the connection to the SPARQL endpoint.
        """
        self.sparql = None

    def is_connected(self):
        """
        Check if the connection to the SPARQL endpoint is active.

        Returns:
            bool: True if connected, False otherwise.
        """
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
        """
        Fetch objects by title from the SPARQL endpoint.

        Args:
            object_type (str): The type of object to fetch (e.g., "Film", "Actor").
            title (str, optional): The title to search for. Defaults to None.

        Returns:
            list: A list of dictionaries containing object URIs and labels.
        """
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

    async def fetch_movies_by_name(self, title: str = None):
        """
        Fetch movies by title from the SPARQL endpoint.

        Args:
            title (str, optional): The title to search for. Defaults to None.

        Returns:
            list: A list of dictionaries containing movie URIs and labels.
        """
        return await self.fetch_objects_by_title("Film", title)
    
    async def fetch_genres_by_name(self, title: str = None):
        """
        Fetch genres by title from the SPARQL endpoint.

        Args:
            title (str, optional): The genre name to search for. Defaults to None.

        Returns:
            list: A list of dictionaries containing genres URIs and labels.
        """
        return await self.fetch_objects_by_title("Genre", title)

    async def fetch_actors_by_name(self, name: str = None):
        """
        Fetch actors by name from the SPARQL endpoint.

        Args:
            name (str, optional): The name to search for. Defaults to None.

        Returns:
            list: A list of dictionaries containing actor URIs and labels.
        """
        return await self.fetch_objects_by_title("Actor", name)
    
    async def fetch_directors_by_name(self, name: str = None):
        """
        Fetch directors by name from the SPARQL endpoint.

        Args:
            name (str, optional): The name to search for. Defaults to None.

        Returns:
            list: A list of dictionaries containing director URIs and labels.
        """
        return await self.fetch_objects_by_title("Director", name)
    
    async def fetch_distributors_by_name(self, name: str = None):
        """
        Fetch distributors by name from the SPARQL endpoint.

        Args:
            name (str, optional): The name to search for. Defaults to None.

        Returns:
            list: A list of dictionaries containing distributor URIs and labels.
        """
        return await self.fetch_objects_by_title("Distributor", name)
    
    async def fetch_writers_by_name(self, name: str = None):
        """
        Fetch writers by name from the SPARQL endpoint.

        Args:
            name (str, optional): The name to search for. Defaults to None.

        Returns:
            list: A list of dictionaries containing writer URIs and labels.
        """
        return await self.fetch_objects_by_title("Writer", name)
    
    async def fetch_producers_by_name(self, name: str = None):
        """
        Fetch producers by name from the SPARQL endpoint.

        Args:
            name (str, optional): The name to search for. Defaults to None.

        Returns:
            list: A list of dictionaries containing producer URIs and labels.
        """
        return await self.fetch_objects_by_title("Producer", name)
    
    async def fetch_composers_by_name(self, name: str = None):
        """
        Fetch composers by name from the SPARQL endpoint.

        Args:
            name (str, optional): The name to search for. Defaults to None.

        Returns:
            list: A list of dictionaries containing composer URIs and labels.
        """
        return await self.fetch_objects_by_title("Composer", name)
    
    async def fetch_cinematographers_by_name(self, name: str = None):
        """
        Fetch cinematographers by name from the SPARQL endpoint.

        Args:
            name (str, optional): The name to search for. Defaults to None.

        Returns:
            list: A list of dictionaries containing cinematographer URIs and labels.
        """
        return await self.fetch_objects_by_title("Cinematographer", name)
    
    async def fetch_productionCompanies_by_name(self, name: str = None):
        """
        Fetch production companies by name from the SPARQL endpoint.

        Args:
            name (str, optional): The name to search for. Defaults to None.

        Returns:
            list: A list of dictionaries containing production company URIs and labels.
        """
        return await self.fetch_objects_by_title("productionCompany", name)

    async def fetch_movies_by_properties(self, title: str = None, genre:str = None, actor: str = None, director: str = None, distributor: str = None, writer: str = None, producer: str = None, composer: str = None, cinematographer: str = None, production_company: str = None):
        """
        Fetch movies by various properties from the SPARQL endpoint.

        Args:
            title (str, optional): The title to search for. Defaults to None.
            genre (str, optional): The genre to search for. Defaults to None.
            actor (str, optional): The actor to search for. Defaults to None.
            director (str, optional): The director to search for. Defaults to None.
            distributor (str, optional): The distributor to search for. Defaults to None.
            writer (str, optional): The writer to search for. Defaults to None.
            producer (str, optional): The producer to search for. Defaults to None.
            composer (str, optional): The composer to search for. Defaults to None.
            cinematographer (str, optional): The cinematographer to search for. Defaults to None.
            production_company (str, optional): The production company to search for. Defaults to None.

        Returns:
            list: A list of dictionaries containing movie URIs and labels.
        """
        return_data = []

        # Check if connected to the database
        if not self.is_connected():
            logging.info("Not connected to the database. Attempting to reconnect.")
            self.sparql = SPARQLWrapper(GRAPHDB_ENDPOINT)
            if not self.is_connected():
                logging.error("Failed to reconnect to the database.")
                raise Exception("Failed to reconnect to the database.")

        # Construct the SPARQL query
        query = """
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?movie ?movieLabel
        WHERE {
          ?movie a dbo:Film .
          ?movie rdfs:label ?movieLabel .
        """

        # Add filters based on provided properties
        filters = []
        if title:
            filters.append(f'FILTER (CONTAINS(LCASE(STR(?movieLabel)), "{title.lower()}"))')
        if genre:
            filters.append(f'?movie dbo:genre ?genre . ?genre rdfs:label ?genreLabel . FILTER (CONTAINS(LCASE(STR(?genreLabel)), "{genre.lower()}")) . ')
        if actor:
            query += f'?movie dbo:starring ?actor . ?actor rdfs:label ?actorLabel . FILTER (CONTAINS(LCASE(STR(?actorLabel)), "{actor.lower()}")) . '
        if director:
            query += f'?movie dbo:director ?director . ?director rdfs:label ?directorLabel . FILTER (CONTAINS(LCASE(STR(?directorLabel)), "{director.lower()}")) . '
        if distributor:
            query += f'?movie dbo:distributor ?distributor . ?distributor rdfs:label ?distributorLabel . FILTER (CONTAINS(LCASE(STR(?distributorLabel)), "{distributor.lower()}")) . '
        if writer:
            query += f'?movie dbo:writer ?writer . ?writer rdfs:label ?writerLabel . FILTER (CONTAINS(LCASE(STR(?writerLabel)), "{writer.lower()}")) . '
        if producer:
            query += f'?movie dbo:producer ?producer . ?producer rdfs:label ?producerLabel . FILTER (CONTAINS(LCASE(STR(?producerLabel)), "{producer.lower()}")) . '
        if composer:
            query += f'?movie dbo:musicComposer ?composer . ?composer rdfs:label ?composerLabel . FILTER (CONTAINS(LCASE(STR(?composerLabel)), "{composer.lower()}")) . '
        if cinematographer:
            query += f'?movie dbo:cinematography ?cinematographer . ?cinematographer rdfs:label ?cinematographerLabel . FILTER (CONTAINS(LCASE(STR(?cinematographerLabel)), "{cinematographer.lower()}")) . '
        if production_company:
            query += f'?movie dbo:productionCompany ?productionCompany . ?productionCompany rdfs:label ?productionCompanyLabel . FILTER (CONTAINS(LCASE(STR(?productionCompanyLabel)), "{production_company.lower()}")) . '

        query += " ".join(filters)
        query += """
        FILTER (LANG(?movieLabel) = "en")
        }"""
        query += f"""
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
                        "movie_uri": result["movie"]["value"],
                        "movie": result["movieLabel"]["value"]
                    }
                    for result in results["results"]["bindings"]
                ]
            else:
                logging.warning("No results found in SPARQL query response.")
        except Exception as e:
            logging.error(f"fetch_movies_by_properties - Failed: {e}")
            raise

        return return_data


async def main():
    """
    Main function to test the MovieDatabase class methods.
    """
    db = MovieDatabase()
    properties_to_test = ["movies", "genres", "actors", "directors", "distributors", "writers", "producers", "composers", "cinematographers", "productionCompanies"]
    for property_to_test in properties_to_test:
        print(f"Testing fetch_movies_by_properties with {property_to_test}")
        function_name = f"fetch_{property_to_test}_by_name"
        results = await getattr(db, function_name)()
        if results:
            print(f"{property_to_test} found: {len(results)}")
        else:
            print(f"No {property_to_test} found.")

    movies = await db.fetch_movies_by_properties(actor="Lauren Graham")
    print(f"Movies by Lauren Graham found: {len(movies)}")
    print(movies)

    movies = await db.fetch_movies_by_properties(genre="Drama")
    print(f"Movies found by genre Drama: {len(movies)}")
    print(movies[:10])

if __name__ == "__main__":
    asyncio.run(main())
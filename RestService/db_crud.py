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
        self.limit = 50000

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
        ORDER BY ASC(?label)
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
    
    async def fetch_countries_by_name(self, name: str = None):
        """
        Fetch countries by name from the SPARQL endpoint.

        Args:
            name (str, optional): The name to search for. Defaults to None.

        Returns:
            list: A list of dictionaries containing country URIs and labels.
        """
        return await self.fetch_objects_by_title("Country", name)

    async def fetch_movies_by_properties(self, title: list = None, genre: list = None, actor: list = None, director: list = None, distributor: list = None, writer: list = None, producer: list = None, composer: list = None, cinematographer: list = None, production_company: list = None):
        """
        Fetch movies by various properties from the SPARQL endpoint.

        Args:
            title (list, optional): The titles to search for. Defaults to None.
            genre (list, optional): The genres to search for. Defaults to None.
            actor (list, optional): The actors to search for. Defaults to None.
            director (list, optional): The directors to search for. Defaults to None.
            distributor (list, optional): The distributors to search for. Defaults to None.
            writer (list, optional): The writers to search for. Defaults to None.
            producer (list, optional): The producers to search for. Defaults to None.
            composer (list, optional): The composers to search for. Defaults to None.
            cinematographer (list, optional): The cinematographers to search for. Defaults to None.
            production_company (list, optional): The production companies to search for. Defaults to None.

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

        SELECT DISTINCT ?movie ?title
        WHERE {
        ?movie a dbo:Film .
        ?movie rdfs:label ?title .
        """

        # Add filters based on provided properties
        filters = []

        def add_filters(param_name, param_values, sparql_property, use_or=False):
            if param_values:
                if use_or:
                    conditions = [f'CONTAINS(LCASE(STR(?{param_name})), "{value.lower()}")' for value in param_values]
                    filter_condition = " || ".join(conditions)
                    filters.append(f'FILTER ({filter_condition})')
                else:
                    for value in param_values:
                        filters.append(f'?movie {sparql_property} ?{param_name} . ?{param_name} rdfs:label ?{param_name}Label . FILTER (CONTAINS(LCASE(STR(?{param_name}Label)), "{value.lower()}")) .')

        add_filters('title', title, 'rdfs:label', use_or=True)
        add_filters('genre', genre, 'dbo:genre')
        add_filters('actor', actor, 'dbo:starring')
        add_filters('director', director, 'dbo:director')
        add_filters('distributor', distributor, 'dbo:distributor')
        add_filters('writer', writer, 'dbo:writer')
        add_filters('producer', producer, 'dbo:producer')
        add_filters('composer', composer, 'dbo:musicComposer')
        add_filters('cinematographer', cinematographer, 'dbo:cinematography')
        add_filters('production_company', production_company, 'dbo:productionCompany')

        query += " ".join(filters)
        query += """
        FILTER (LANG(?title) = "en")
        }"""
        query += f"""
        LIMIT {self.limit}
        """

        logging.info(f"SPARQL query: {query}")
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)

        # Execute the query and process results
        try:
            logging.info(f"Executing SPARQL query: {query}")
            results = self.sparql.query().convert()
            if "results" in results and "bindings" in results["results"]:
                return_data = [
                    {
                        "object_uri": result["movie"]["value"],
                        "label": result["title"]["value"]
                    }
                    for result in results["results"]["bindings"]
                ]
            else:
                logging.warning("No results found in SPARQL query response.")
        except Exception as e:
            logging.error(f"fetch_movies_by_properties - Failed: {e}")
            raise

        return return_data
    

    async def fetch_movies_by_properties_dev(self, **kwargs):
        """
        Fetch movies by various properties from the SPARQL endpoint.

        Returns:
            list: A list of dictionaries containing movie URIs and labels.
        """
        return_data = []

        # Extract relevant key/values from the submitted fields
        title = kwargs.get('title', None)
        genres = kwargs.get('genres', None)
        actors = kwargs.get('actors', None)
        director = kwargs.get('director', None)

        # if title:
        #     print(f"It works!!!!{title[0]}")

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
            filters.append(f'FILTER (CONTAINS(LCASE(STR(?movieLabel)), "{title[0].lower()}"))')
        if genres:
            filters.append(f'?movie dbo:genre ?genre . ?genre rdfs:label ?genreLabel . FILTER (CONTAINS(LCASE(STR(?genreLabel)), "{genres.lower()}")) . ')
        if actors:
            query += f'?movie dbo:starring ?actor . ?actor rdfs:label ?actorLabel . FILTER (CONTAINS(LCASE(STR(?actorLabel)), "{actors.lower()}")) . '
        if director:
            query += f'?movie dbo:director ?director . ?director rdfs:label ?directorLabel . FILTER (CONTAINS(LCASE(STR(?directorLabel)), "{director.lower()}")) . '


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
                        "object_uri": result["movie"]["value"],
                        "label": result["movieLabel"]["value"]
                    }
                    for result in results["results"]["bindings"]
                ]
            else:
                logging.warning("No results found in SPARQL query response.")
        except Exception as e:
            logging.error(f"fetch_movies_by_properties - Failed: {e}")
            raise

        return return_data

    async def fetch_similar_movies(self, target_movie: str, limit: int = 10):
        """
        fetch movies similar to the target movie based on genre/release year/actors/directors

        Returns:
        list of dicts containing movie URIs and similarity score (similarity score is descending, the movie with highest score has more common properties with the target)
        """
        return_data = []

                # Check if connected to the database
        if not self.is_connected():
            logging.info("Not connected to the database. Attempting to reconnect.")
            self.sparql = SPARQLWrapper(GRAPHDB_ENDPOINT)
            if not self.is_connected():
                logging.error("Failed to reconnect to the database.")
                raise Exception("Failed to reconnect to the database.")
        query = f"""
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX dbr: <http://dbpedia.org/resource/>

        SELECT ?movie
            (COUNT(?sharedGenre) +
             COUNT(?sharedYear) +
             COUNT(?sharedActor) +
             COUNT(?sharedDirector) AS ?similarityScore)
        WHERE{{
            #target movie
            BIND(dbr:{target_movie.replace(' ','_')} AS ?targetMovie)

            #properties from target movie
            OPTIONAL {{ ?targetMovie dbo:genre ?targetGenre. }}
            OPTIONAL {{ ?targetMovie dbo:releaseYear ?targetYear. }}
            OPTIONAL {{ ?targetMovie dbo:starring ?targetActor. }}
            OPTIONAL {{ ?targetMovie dbo:director ?targetDirector. }}

            #chech not the same movie
            ?movie a dbo:Film.
            FILTER(?movie != ?targetMovie)

            #matching
            OPTIONAL{{
                ?movie dbo:genre ?sharedGenre.
                FILTER(?sharedGenre = ?targetGenre)
            }}

            OPTIONAL{{
                ?movie dbo:releaseYear ?sharedYear.
                FILTER(?sharedYear = ?targetYear)
            }}
            
            OPTIONAL{{
                ?movie dbo:starring ?sharedActor.
                
            }}
            FILTER(?sharedActor = ?targetActor)
            
            OPTIONAL{{
                ?movie dbo:director ?sharedDirector.
                
            }}
               FILTER(?sharedDirector = ?targetDirector) 
        }}
        GROUP BY ?movie
        ORDER BY DESC(?similarityScore)
        LIMIT {limit}
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
                        "object_uri": result["movie"]["value"],
                        "similarity_score": result["similarityScore"]["value"]
                    }
                    for result in results["results"]["bindings"]
                ]
            else:
                logging.warning("No results found in SPARQL query response.")
        except Exception as e:
            logging.error(f"fetch_similar_movies - Failed: {e}")
            raise

        return return_data

async def main():
    """
    Main function to test the MovieDatabase class methods.
    """
    db = MovieDatabase()

    # Test fetching actors by name
    actors = await db.fetch_actors_by_name("Ali")
    print(f"Actors found: {len(actors)}")

    #test fetching similar movies
    user_limit = 5
    similar_movies = await db.fetch_similar_movies("%27Til_We_Meet_Again", limit=user_limit)
    print(similar_movies)

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


    movies = await db.fetch_movies_by_properties()
    print(movies[:10])

    params = {
            "title": "",
            "genre": "",
            "actor": "",
            "director": "",
            "distributor": "",
            "writer": "",
            "producer": "",
            "composer": "",
            "cinematographer": "",
            "production_company": ""
        }
    filtered_params = {k: v for k, v in params.items() if v}
    var_name = "movie_" + "_".join(f"{k}_{v}" for k, v in filtered_params.items())

    print(var_name)
    

if __name__ == "__main__":
    asyncio.run(main())
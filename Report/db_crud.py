"""
file: db_crud.py
date: 10-01-2025
description: This module provides a class for interacting with a SPARQL endpoint to fetch various types of objects
such as movies, actors, directors, etc., from a GraphDB repository.
"""

from SPARQLWrapper import SPARQLWrapper, JSON
import logging
import asyncio
import numpy as np
import os
import pandas as pd


def is_running_in_docker():
    """Check if the code is running inside a Docker container."""
    path = '/proc/1/cgroup'
    if os.path.exists(path):
        with open(path, 'r') as f:
            return 'docker' in f.read()
    return False


def add_filters(filters, param_name, param_values, sparql_property, use_or=False):
    if param_values:
        if use_or:
            conditions = [f'CONTAINS(LCASE(STR(?{param_name})), "{value.lower()}")' for value in param_values]
            filter_condition = " || ".join(conditions)
            filters.append(f'FILTER ({filter_condition})')
        else:
            for value in param_values:
                filters.append(f'?movie {sparql_property} ?{param_name} . ?{param_name} rdfs:label ?{param_name}Label . FILTER (CONTAINS(LCASE(STR(?{param_name}Label)), "{value.lower()}")) .')
    return filters

GRAPHDB_ENDPOINT = "http://localhost:7200/repositories/MoviesRepo"
if is_running_in_docker():
    GRAPHDB_ENDPOINT = "http://graphdb:7200/repositories/MoviesRepo"

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
                unique_data = {}
                for result in results["results"]["bindings"]:
                    label = result["label"]["value"]
                    label_cap = label.capitalize()
                    if label_cap not in unique_data:
                        unique_data[label_cap] = {
                            "object_uri": result["object"]["value"],
                            "label": label_cap
                        }
                return_data = list(unique_data.values())
            else:
                logging.warning("No results found in SPARQL query response.")
        except Exception as e:
            logging.error(f"fetch_objects_by_title - Failed: {e}")
            raise

        return return_data

    # async def fetch_movies_by_name(self, title: str = None):
    #     """
    #     Fetch movies by title from the SPARQL endpoint.

    #     Args:
    #         title (str, optional): The title to search for. Defaults to None.

    #     Returns:
    #         list: A list of dictionaries containing movie URIs and labels.
    #     """
    #     return await self.fetch_objects_by_title("Film", title)
    
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
    
    async def fetch_movies_by_name(self, title: str = None):
        """
        Fetch movies by title from the SPARQL endpoint.
        
        Args:
            title (str, optional): The title to search for. Defaults to None.
    
        Returns:
            list: A list of dictionaries containing movie URIs and labels.
        """
        return await self.fetch_objects_by_title("Film", title)

    async def fetch_movies_by_properties(self, title: list = None, genre: list = None, start_year: int = None, end_year: int = None, actor: list = None, director: list = None, description: str = "", number_of_results: int = 10, distributor: list = None, writer: list = None, producer: list = None, composer: list = None, cinematographer: list = None, production_company: list = None,
                                         get_similar_movies=False):
        """
        Fetch movies by various properties from the SPARQL endpoint.

        Args:
            title (list, optional): The titles to search for. Defaults to None.
            genre (list, optional): The genres to search for. Defaults to None.
            start_year (int, optional): The start year to search for. Defaults to None.
            end_year (int, optional): The end year to search for. Defaults to None.
            actor (list, optional): The actors to search for. Defaults to None.
            director (list, optional): The directors to search for. Defaults to None.
            description (str, optional): The description to search for. Defaults to "".
            number_of_results (int, optional): The number of results to return. Defaults to 10.
            distributor (list, optional): The distributors to search for. Defaults to None.
            writer (list, optional): The writers to search for. Defaults to None.
            producer (list, optional): The producers to search for. Defaults to None.
            composer (list, optional): The composers to search for. Defaults to None.
            cinematographer (list, optional): The cinematographers to search for. Defaults to None.
            production_company (list, optional): The production companies to search for. Defaults to None.
            get_similar_movies (bool, optional): Whether to fetch similar movies. Defaults to False.

        Returns:
            list: A list of dictionaries containing movie URIs and labels.
        """
        # Check if connected to the database
        if not self.is_connected():
            logging.info("Not connected to the database. Attempting to reconnect.")
            self.sparql = SPARQLWrapper(GRAPHDB_ENDPOINT)
            if not self.is_connected():
                logging.error("Failed to reconnect to the database.")
                raise Exception("Failed to reconnect to the database.")

        return_data = []

        if get_similar_movies and title: # if title is given fetch similar movies
            logging.info("Fetching similar movies based on properties. - fetch_movies_by_properties")
            params = {
                "title": title,
                "genre": genre,
                "actors": actor,
                "director": director,
                "year_range": [start_year, end_year],
                "number_of_results": number_of_results
            }
            similar_movies = await self.fetch_similar_movies(params)
            return similar_movies
        else:  # fetch movies based on properties
            # Construct the SPARQL query
            logging.info("Fetching movies based on properties. - fetch_movies_by_properties")
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
            add_filters(filters, 'title', title, 'rdfs:label', use_or=True)
            add_filters(filters, 'genre', genre, 'dbo:genre')
            add_filters(filters, 'actor', actor, 'dbo:starring')
            add_filters(filters, 'director', director, 'dbo:director')
            add_filters(filters, 'distributor', distributor, 'dbo:distributor')
            add_filters(filters, 'writer', writer, 'dbo:writer')
            add_filters(filters, 'producer', producer, 'dbo:producer')
            add_filters(filters, 'composer', composer, 'dbo:musicComposer')
            add_filters(filters, 'cinematographer', cinematographer, 'dbo:cinematography')
            add_filters(filters, 'production_company', production_company, 'dbo:productionCompany')

            # Nikita we need to add the description filter logic here

            if start_year or end_year:
                query += "?movie dbo:releaseYear ?releaseYear ."
                if start_year:
                    filters.append(f'FILTER (?releaseYear >= "{start_year}"^^xsd:gYear)')
                if end_year:
                    filters.append(f'FILTER (?releaseYear <= "{end_year}"^^xsd:gYear)')

            query += " ".join(filters)
            query += """
            FILTER (LANG(?title) = "en")
            }"""
            query += f"""
            LIMIT {number_of_results}
            """

            logging.info(f"SPARQL query: {query} - fetch_movies_by_properties")
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
    

    async def fetch_movies_details(self, movies):
        """
        Fetch movies details from the SPARQL endpoint.

        Args:
            movies (list): The movies to get details for.

        Returns:
            list: A list of dictionaries containing movie URIs and their details.
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
        movies_filter = " ".join([f"<{movie['object_uri']}>" for movie in movies])

        query = f"""
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?movie ?title ?abstract ?runtime ?budget ?boxOffice ?releaseYear ?country_label 
            (GROUP_CONCAT(DISTINCT ?genre_label; separator=", ") AS ?genres)
            (GROUP_CONCAT(DISTINCT ?starring_label; separator=", ") AS ?starring)
            (GROUP_CONCAT(DISTINCT ?director_label; separator=", ") AS ?directors)
            (GROUP_CONCAT(DISTINCT ?producer_label; separator=", ") AS ?producers)
            (GROUP_CONCAT(DISTINCT ?writer_label; separator=", ") AS ?writers)
            (GROUP_CONCAT(DISTINCT ?composer_label; separator=", ") AS ?composers)
            (GROUP_CONCAT(DISTINCT ?cinematographer_label; separator=", ") AS ?cinematographers)
        WHERE {{
            VALUES ?movie {{ {movies_filter} }}
            ?movie rdfs:label ?title .

            OPTIONAL {{ ?movie dbo:abstract ?abstract . }}
            OPTIONAL {{ ?movie dbo:runtime ?runtime . }}
            OPTIONAL {{ ?movie dbo:budget ?budget . }}
            OPTIONAL {{ ?movie dbo:boxOffice ?boxOffice . }}
            OPTIONAL {{ ?movie dbo:releaseYear ?releaseYear . }}
            OPTIONAL {{ ?movie dbo:country ?country .
                    ?country rdfs:label ?country_label .
                    FILTER (lang(?country_label) = 'en')
                    }}        
            OPTIONAL {{ ?movie dbo:genre ?genre .
                    ?genre rdfs:label ?genre_label .
                    FILTER (lang(?genre_label) = 'en')
                    }}
            OPTIONAL {{ ?movie dbo:starring ?starring .
                    ?starring rdfs:label ?starring_label .
                    FILTER (lang(?starring_label) = 'en')
                    }}
            OPTIONAL {{ ?movie dbo:director ?director .
                    ?director rdfs:label ?director_label .
                    FILTER (lang(?director_label) = 'en')
                    }}
            OPTIONAL {{ ?movie dbo:producer ?producer .
                    ?producer rdfs:label ?producer_label .
                    FILTER (lang(?producer_label) = 'en')
                    }}
            OPTIONAL {{ ?movie dbo:writer ?writer .
                    ?writer rdfs:label ?writer_label .
                    FILTER (lang(?writer_label) = 'en')
                    }}
            OPTIONAL {{ ?movie dbo:musicComposer ?composer .
                    ?composer rdfs:label ?composer_label .
                    FILTER (lang(?composer_label) = 'en')
                    }}
            OPTIONAL {{ ?movie dbo:cinematography ?cinematographer .
                    ?cinematographer rdfs:label ?cinematographer_label .
                    FILTER (lang(?cinematographer_label) = 'en')
                    }}
        }}
        GROUP BY ?movie ?title ?abstract ?runtime ?budget ?boxOffice ?releaseYear ?country_label
        """
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()

        # Use a dictionary to remove duplicates based on the movie URI
        unique_movies = {}
        for result in results["results"]["bindings"]:
            movie_uri = result["movie"]["value"]
            if movie_uri not in unique_movies:
                unique_movies[movie_uri] = {
                    "movie": movie_uri,
                    "title": result["title"]["value"],
                    "abstract": result.get("abstract", {}).get("value", ""),
                    "runtime": result.get("runtime", {}).get("value", ""),
                    "budget": result.get("budget", {}).get("value", ""),
                    "boxOffice": result.get("boxOffice", {}).get("value", ""),
                    "releaseYear": result.get("releaseYear", {}).get("value", ""),
                    "country": result.get("country_label", {}).get("value", ""),
                    "genres": result.get("genres", {}).get("value", ""),
                    "starring": result.get("starring", {}).get("value", ""),
                    "directors": result.get("directors", {}).get("value", ""),
                    "producers": result.get("producers", {}).get("value", ""),
                    "writers": result.get("writers", {}).get("value", ""),
                    "composers": result.get("composers", {}).get("value", ""),
                    "cinematographers": result.get("cinematographers", {}).get("value", "")
                }

        return list(unique_movies.values())
    

    async def generate_sparql_query(self, params):
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
        
        title = params.get('title', None)
        if type(title) == list:
            title = title[0]
        genres = params.get('genres', [])
        actors = params.get('actors', [])
        director = params.get('director', None)
        year = params.get('year_range', None)
        limit = params.get('number_of_results', 10)

        query = f"""
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX dbr: <http://dbpedia.org/resource/>

        SELECT ?movie
                   (COUNT(?sharedGenre) +
        COUNT(?sharedYear) +
        COUNT(?sharedActor) +
        COUNT(?sharedDirector) AS ?similarityScore)
        WHERE {{
        """
        if title:
            query += f"""
            # target movie
            BIND(dbr:{title.replace(' ', '_')} AS ?targetMovie)

            # properties from target movie
            OPTIONAL {{ ?targetMovie dbo:genre ?targetGenre. }}
            OPTIONAL {{ ?targetMovie dbo:releaseYear ?targetYear. }}
            OPTIONAL {{ ?targetMovie dbo:starring ?targetActor. }}
            OPTIONAL {{ ?targetMovie dbo:director ?targetDirector. }}

            # check not the same movie
            ?movie a dbo:Film.
            FILTER(?movie != ?targetMovie)
            FILTER(?similarityScore > 0)
            """
        
        # Add filters using the existing add_filters method
        # filters = []
        # if genres:
        #     add_filters(filters, 'genre', genres, 'dbo:genre')
        # if actors:
        #     add_filters(filters, 'actor', actors, 'dbo:starring')
        # if director:
        #     add_filters(filters, 'director', [director], 'dbo:director')
        # if year is not None and len(year) == 2 and year[0] is not None and year[1] is not None:
        #     filters.append(f'OPTIONAL {{ ?movie dbo:releaseYear ?movieYear . FILTER(?movieYear >= {year[0]} && ?movieYear <= {year[1]}) }}')

        # query += "\n".join(filters)
       
        query +=""""
        OPTIONAL {
        ?movie dbo:genre ?sharedGenre.
        FILTER(?targetGenre = ?SharedGenre)
        }
        OPTIONAL {
        ?movie dbo:releaseYear ?sharedYear.
        FILTER(?sharedYear = ?targetYear)
         }
        OPTIONAL {
        ?movie dbo:starring ?sharedActor.
        FILTER(?sharedActor = ?targetActor)
        }   

        OPTIONAL {
        ?movie dbo:director ?sharedDirector.
        FILTER(?sharedDirector = ?targetDirector)
        }   
        }
        FILTER(?similarityScore>0)
        """
        query +=f"""
        GROUP BY ?movie
        ORDER BY DESC(?similarityScore)
        LIMIT {int(limit)}
        """
        logging.info(f"Generated SPARQL Query: {query}")
        return query
    
    async def fetch_similar_movies(self, params):
        limit = params.get('limit', 10)
        genre_filter = params.get('genre_filter', [])
        title_filter = params.get('title_filter', [])

    # Initialize SPARQL query
        query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?movie ?title (COUNT(?similarity) AS ?similarityScore)
        WHERE {
            ?movie a <http://www.w3.org/2001/XMLSchema#movie> ;
                rdfs:label ?title .
            
            # Additional filters go here, such as genre_filter and title_filter.
        }
        GROUP BY ?movie
        ORDER BY DESC(?similarityScore)
        """

        # Adding the LIMIT clause correctly
        try:
            query += f" LIMIT {int(limit)}"  # Ensure limit is an integer
        except ValueError as e:
            logging.error(f"Invalid limit value: {limit}")
            raise ValueError(f"Invalid limit value: {limit}")

        # Log the generated query for debugging
        logging.info(f"Generated SPARQL Query: {query}")

        # Execute the query
        try:
            results = self.sparql.query().convert()
        except SPARQLWrapper.SPARQLExceptions.QueryBadFormed as e:
            logging.error(f"Failed to execute query: {e}")
            raise e
        return results

        # query = await self.generate_sparql_query(params)
        # logging.info(f"SPARQL query: {query}")
        
        # self.sparql.setQuery(query)
        # self.sparql.setReturnFormat(JSON)

        # # Execute the query and process results
        # try:
        #     logging.info(f"Executing SPARQL query: {query}")
        #     results = self.sparql.query().convert()
        #     if "results" in results and "bindings" in results["results"]:
        #         return_data = [
        #             {
        #                 "object_uri": result["movie"]["value"],
        #                 "similarity_score": result["similarityScore"]["value"]
        #             }
        #             for result in results["results"]["bindings"]
        #         ]
        #         return return_data
        #     else:
        #         logging.warning("No results found in SPARQL query response.")
        # except Exception as e:
        #     logging.error(f"fetch_similar_movies - Failed: {e}")
        #     raise

        # return []

async def main():
    """
    Main function to test the MovieDatabase class methods.
    """
    db = MovieDatabase()

    # movies = await db.fetch_movies_by_properties(start_year=2000, end_year=2020, number_of_results=10)
    # movies = await db.fetch_movies_by_properties(title=["Shrek"], get_similar_movies=True)

    # Test fetching actors by name
    actors = await db.fetch_actors_by_name("Lauren Graham")
    print(f"Actors found: {len(actors)}")

    results = await db.fetch_movies_by_properties(title=["shrek"], number_of_results=5000)

    #test fetching similar movies
    params = {
        "title": "%27Til_We_Meet_Again",
        "genres":["Drama","Action"],
        "actors":["Ali","Smith"],
        "director":"James Cameron",
        "year_range":[1990,2023],
        "similar_movies":5
    }
    similar_movies = await db.fetch_similar_movies(params)
    print(f"similar movies found: {len(similar_movies)}")
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

    movies = await db.fetch_movies_by_properties(actor=["Lauren Graham"])
    print(f"Movies by Lauren Graham found: {len(movies)}")
    print(movies)

    # Fetch detailed information for these movies using fetch_movies_details
    movie_details = await db.fetch_movies_details(movies)
    print(f"Movie details found: {len(movie_details)}")
    df_movie_details = pd.DataFrame(movie_details)
    df_movie_details.to_csv("movie_details_test.csv", index=False)


    movies = await db.fetch_movies_by_properties(genre=["Drama"])
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
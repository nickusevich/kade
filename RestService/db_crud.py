from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import pandas as pd
import json
import numpy as np

GRAPHDB_ENDPOINT = "http://localhost:7200/repositories/MoviesRepo"


class MovieDatabase:

    def __init__(self, uri, user, password):
        self.sparql = SPARQLWrapper(GRAPHDB_ENDPOINT)
        self.limit = 500

    def close(self):
        self.sparql = None

    def fetch_movies_by_title(self, title: str = None):
        return_data = []
            
        # Generate case-insensitive filters
        name_filter = 'LANG(?movieLabel) = "en"'
        if title:
            name_filter += f'&& CONTAINS(LCASE(STR(?movieLabel)), "{title.lower()}")'

        
        # Construct the SPARQL query
        query = f"""
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?movie ?movieLabel
        WHERE {{
        ?movie a dbo:Film .
        ?movie rdfs:label ?movieLabel .
        FILTER ({name_filter})
        }}
        LIMIT {self.limit}
        """

        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)

        # Execute the query and process results
        try:
            results = sparql.query().convert()            
            return_data = [
                {
                    "movie_uri": result["movie"]["value"],
                    "movie": result["movieLabel"]["value"]
                }
                for result in results["results"]["bindings"]
            ]
        except Exception as e:
            print(f"fetch_movies_by_title - Failed: {e}")

        return pd.DataFrame(return_data)
import requests
from requests.utils import unquote


graphdb_endpoint = "http://localhost:7200/repositories/MoviesRepo"




def get_all_directors():
    list_of_directors = []

    query = """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    
    SELECT DISTINCT ?director
    WHERE {
      ?movie a dbo:Film .
      ?movie dbo:director ?director .
    }
    """
    response = requests.get(
        graphdb_endpoint,
        params={"query": query},
        headers={"Accept": "application/sparql-results+json"}
    )
    
    if response.status_code == 200:
        results = response.json()
        directors =  [result["director"]["value"] for result in results["results"]["bindings"]]

        for director in directors:
            director_name = director.split("/")[-1].replace("_", " ")

            list_of_directors.append(director_name)

        return list_of_directors

    else:
        print(f"Query failed with status code {response.status_code}")
        return []
    


def get_all_actors():
    unique_actors = set()

    query = """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    
    SELECT DISTINCT ?actor ?actorLabel
    WHERE {
      ?movie a dbo:Film .
      ?movie dbo:starring ?actor .
      ?actor rdfs:label ?actorLabel .
      FILTER (LANG(?actorLabel) = "en")
      FILTER (?actor != <http://dbpedia.org/resource/N/A>)
    }
    """
    response = requests.get(
        graphdb_endpoint,
        params={"query": query},
        headers={"Accept": "application/sparql-results+json"}
    )
    
    if response.status_code == 200:
        results = response.json()
        actors = [result["actor"]["value"] for result in results["results"]["bindings"]]
        actors_names = [result["actorLabel"]["value"] for result in results["results"]["bindings"]]

        # for actor in actors:
         

        #     actor_name = actor.split("/")[-1].replace("_", " ").strip()
        #     unique_actors.add(actor_name)

        return list(set(actors_names))
    else:
        print(f"Query failed with status code {response.status_code}")
        return []




graphdb_endpoint = "http://localhost:7200/repositories/MoviesRepo"

def get_all_genres():
    unique_genres = set()  

    query = """
    PREFIX dbo: <http://dbpedia.org/ontology/>
    
    SELECT DISTINCT ?genre
    WHERE {
      ?movie a dbo:Film .
      ?movie dbo:genre ?genre .
    }
    """
    response = requests.get(
        graphdb_endpoint,
        params={"query": query},
        headers={"Accept": "application/sparql-results+json"}
    )
    
    if response.status_code == 200:
        results = response.json()
        genres = [result["genre"]["value"] for result in results["results"]["bindings"]]

        for genre in genres:
            # Extract and clean genre names
            genre_name = genre.split("/")[-1].replace("_", " ")
            genre_name = unquote(genre_name)  # Decode URL-encoded characters
            unique_genres.add(genre_name)  # Add to set to ensure uniqueness

        return list(unique_genres)  # Convert set back to list for display

    else:
        print(f"Query failed with status code {response.status_code}")
        return []


print(get_all_actors())
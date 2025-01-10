"""
File: get_data_from_dbpedia.py
Date: 09-12-2024
Description: Get movies data from dbpedia
"""

from SPARQLWrapper import SPARQLWrapper, JSON, POST
import pandas as pd
import time
import os

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
CHUNK_SIZE = 100  # Process movies in chunks of 100

# Define the SPARQL endpoint
endpoint_url = "http://dbpedia.org/sparql"


DIR_PATH = "DB/Datasets"

# Step 1: Fetch mandatory information for a specific date range
def fetch_mandatory_information(start_date, end_date, limit=5000):
    sparql = SPARQLWrapper(endpoint_url)
    query = f"""
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX dbr: <http://dbpedia.org/resource/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT DISTINCT ?movie ?movieLabel
    WHERE {{
      ?movie a dbo:Film .
      OPTIONAL {{ ?movie dbo:releaseDate ?releaseDate . }}
      OPTIONAL {{ ?movie dbo:releaseYear ?releaseYear . }}

      FILTER(
        (?releaseDate >= "{start_date}"^^xsd:date && ?releaseDate <= "{end_date}"^^xsd:date) ||
        (STR(?releaseYear) >= "{start_date[:4]}" && STR(?releaseYear) <= "{end_date[:4]}")
      )

      ?movie rdfs:label ?movieLabel .
      FILTER(LANG(?movieLabel) = "en")
      FILTER(BOUND(?releaseDate) || BOUND(?releaseYear))
    }}
    GROUP BY ?movie ?movieLabel
    LIMIT {limit}
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    try:
        print(f"Fetching mandatory movie information from {start_date} to {end_date}...")
        results = sparql.query().convert()
        data = [
            {
                "movie_uri": result["movie"]["value"],
                "movie": result["movieLabel"]["value"]
            }
            for result in results["results"]["bindings"]
        ]
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Failed to fetch mandatory data for {start_date} to {end_date}: {e}")
        return pd.DataFrame()

# Step 2: Fetch optional single-valued attributes
def fetch_single_valued_attributes(movie_uris, attribute_label, optional_fields=[], filter_lang=False):
    """
    Fetch single-valued attributes for a list of movie URIs.

    Args:
        movie_uris (list): List of movie URIs.
        attribute_label (str): The label for the main attribute.
        optional_fields (list): List of optional fields, each a tuple (prefix, prefix_url, attribute_property, attribute_label).
        filter_lang (bool): Whether to filter by language (English).

    Returns:
        dict: A dictionary mapping movie URIs to their attributes.
    """
    sparql = SPARQLWrapper(endpoint_url)
    results = []
    if filter_lang:
        print(f"Fetching {attribute_label} with English language filter...")
    for i in range(0, len(movie_uris), CHUNK_SIZE):
        chunk = movie_uris[i:i + CHUNK_SIZE]
        values = " ".join([f"<{uri}>" for uri in chunk])
        query = f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        """
        for opt_prefix, opt_prefix_url, _ in optional_fields:
            query += f"PREFIX {opt_prefix}: <{opt_prefix_url}>\n"

        query += f"""
        SELECT DISTINCT ?movie ?{attribute_label}
        """

        query += f"""
        WHERE {{
          VALUES ?movie {{ {values} }}
        """
        for opt_prefix, _, opt_attr_property in optional_fields:
            query += f" OPTIONAL {{ ?movie {opt_prefix}:{opt_attr_property} ?{attribute_label}. }}\n"
        
        if filter_lang:
            query += f" FILTER(isURI(?{attribute_label}) || LANG(?{attribute_label}) = 'en' || LANG(?{attribute_label}) = '')\n"

        query += "}"

        for attempt in range(MAX_RETRIES):
            try:
                print(f"Fetching {attribute_label} for chunk {i // CHUNK_SIZE + 1} (attempt {attempt + 1})...")
                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                chunk_results = sparql.query().convert()
                results.extend(chunk_results["results"]["bindings"])
                break
            except Exception as e:
                print(f"Error fetching {attribute_label} for chunk {i // CHUNK_SIZE + 1} (attempt {attempt + 1}): {e}")
                time.sleep(RETRY_DELAY)
    return {
        result["movie"]["value"]: result.get(attribute_label, {}).get("value", "N/A")
        for result in results
    }

# Step 3: Fetch Rotten Tomatoes ID
def fetch_rotten_tomatoes_id(movie_uris):
    sparql = SPARQLWrapper(endpoint_url)
    results = []
    for i in range(0, len(movie_uris), CHUNK_SIZE):
        chunk = movie_uris[i:i + CHUNK_SIZE]
        values = " ".join([f"<{uri}>" for uri in chunk])
        query = f"""
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?movie ?externalLink
        WHERE {{
          VALUES ?movie {{ {values} }}
          ?movie dbo:wikiPageExternalLink ?externalLink .
          FILTER(CONTAINS(LCASE(STR(?externalLink)), "rottentomatoes"))
        }}
        """
        for attempt in range(MAX_RETRIES):
            try:
                print(f"Fetching RottenTomatoesID for chunk {i // CHUNK_SIZE + 1} (attempt {attempt + 1})...")
                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                chunk_results = sparql.query().convert()
                results.extend(chunk_results["results"]["bindings"])
                break
            except Exception as e:
                print(f"Error fetching RottenTomatoesID for chunk {i // CHUNK_SIZE + 1} (attempt {attempt + 1}): {e}")
                time.sleep(RETRY_DELAY)
    return {
        result["movie"]["value"]: result.get("externalLink", {}).get("value", "N/A")
        for result in results
    }

# Step 4: Fetch grouped attributes (e.g., genres, actors, directors, distributors)
def fetch_grouped_attributes(movie_uris, attribute_property, attribute_label, prefix, prefix_url, include_uri=False):
    sparql = SPARQLWrapper(endpoint_url)
    results = []
    for i in range(0, len(movie_uris), CHUNK_SIZE):
        chunk = movie_uris[i:i + CHUNK_SIZE]
        values = " ".join([f"<{uri}>" for uri in chunk])
        query = f"""
        PREFIX {prefix}: <{prefix_url}>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?movie 
               (GROUP_CONCAT(DISTINCT ?{attribute_label}; separator="; ") AS ?{attribute_label}s)
               {'(GROUP_CONCAT(DISTINCT ?attribute; separator="; ") AS ?attributeURIs)' if include_uri else ''}
        WHERE {{
          VALUES ?movie {{ {values} }}
          OPTIONAL {{
            ?movie {prefix}:{attribute_property} ?attribute.
            ?attribute rdfs:label ?{attribute_label}.
            FILTER(LANG(?{attribute_label}) = "en")
          }}
        }}
        GROUP BY ?movie
        """
        for attempt in range(MAX_RETRIES):
            try:
                print(f"Fetching {attribute_label} for chunk {i // CHUNK_SIZE + 1} (attempt {attempt + 1})...")
                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                chunk_results = sparql.query().convert()
                results.extend(chunk_results["results"]["bindings"])
                break
            except Exception as e:
                print(f"Error fetching {attribute_label} for chunk {i // CHUNK_SIZE + 1} (attempt {attempt + 1}): {e}")
                time.sleep(RETRY_DELAY)
    return {
        result["movie"]["value"]: {
            f"{attribute_label}s": result.get(f"{attribute_label}s", {}).get("value", "N/A"),
            "attributeURIs": result.get("attributeURIs", {}).get("value", "N/A") if include_uri else "N/A"
        }
        for result in results
    }

# Fetch movies based on a list of names
def fetch_movies_by_names(names, limit=5000, chunk_size=50):
    """
    Fetch movies containing specific names from the DBpedia endpoint.

    Args:
        names (list): List of movie name substrings to search for.
        limit (int): Maximum number of results to fetch.
        chunk_size (int): Number of names to process in each chunk.

    Returns:
        pd.DataFrame: DataFrame with movie URIs and labels.
    """
    # DBpedia SPARQL endpoint
    endpoint_url = "http://dbpedia.org/sparql"
    sparql = SPARQLWrapper(endpoint_url)
    all_data = []

    # Process names in chunks
    for i in range(0, len(names), chunk_size):
        print(f"Processing chunk {i // chunk_size + 1} of {len(names) // chunk_size + 1}...")
        chunk = names[i:i + chunk_size]
        
        # Generate case-insensitive filters
        name_filters = " || ".join([f'CONTAINS(LCASE(STR(?movieLabel)), "{name.lower()}")' for name in chunk])
        
        # Construct the SPARQL query
        query = f"""
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?movie ?movieLabel
        WHERE {{
          ?movie a dbo:Film .
          ?movie rdfs:label ?movieLabel .
          FILTER (LANG(?movieLabel) = "en" && ({name_filters}))
        }}
        LIMIT {limit}
        """

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        # Execute the query and process results
        try:
            results = sparql.query().convert()            
            data = [
                {
                    "movie_uri": result["movie"]["value"],
                    "movie": result["movieLabel"]["value"]
                }
                for result in results["results"]["bindings"]
            ]
            all_data.extend(data)
        except Exception as e:
            print(f"fetch_movies_by_names - Failed to fetch movies for chunk {i // chunk_size + 1}: {e}")

    return pd.DataFrame(all_data)

def fetch_top_companies_and_movies(limit=100):
    """
    Fetch top production companies and their associated movies.

    Args:
        limit (int): Maximum number of companies to fetch.

    Returns:
        pd.DataFrame: DataFrame with company URIs, labels, and their associated movies.
    """
    # DBpedia SPARQL endpoint
    endpoint_url = "http://dbpedia.org/sparql"
    sparql = SPARQLWrapper(endpoint_url)
    
    # Step 1: Fetch top production companies
    top_companies_query = f"""
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?company ?companyLabel (COUNT(DISTINCT ?movie) AS ?movieCount)
    WHERE {{
      ?movie a dbo:Film .
      ?movie (dbo:producer | dbo:productionCompany) ?company .
      ?company a dbo:Company .
      ?company rdfs:label ?companyLabel .
      FILTER (LANG(?companyLabel) = "en")
    }}
    GROUP BY ?company ?companyLabel
    ORDER BY DESC(?movieCount)
    LIMIT {limit}
    """
    
    sparql.setQuery(top_companies_query)
    sparql.setReturnFormat(JSON)
    
    try:
        # Fetch top production companies
        print(f"Fetching top {limit} production companies...")
        results = sparql.query().convert()
        companies = [
            {
                "company_uri": result["company"]["value"],
                "company": result["companyLabel"]["value"],
                "movie_count": int(result["movieCount"]["value"])
            }
            for result in results["results"]["bindings"]
        ]
        
        # Convert to DataFrame
        companies_df = pd.DataFrame(companies)
        print(f"Fetched {len(companies_df)} production companies.")
        
        # Step 2: Fetch movies for each company
        movies_data = []
        for company in companies_df["company_uri"]:
            movies_query = f"""
            PREFIX dbo: <http://dbpedia.org/ontology/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

            SELECT DISTINCT ?movie ?movieLabel
            WHERE {{
              ?movie a dbo:Film .
              ?movie (dbo:producer | dbo:productionCompany) <{company}> .
              ?movie rdfs:label ?movieLabel .
              FILTER (LANG(?movieLabel) = "en")
            }}
            """
            sparql.setQuery(movies_query)
            sparql.setReturnFormat(JSON)

            try:
                print(f"Fetching movies for company: {company}")
                movie_results = sparql.query().convert()
                movie_data = [
                    {
                        "movie_uri": result["movie"]["value"],
                        "movie": result["movieLabel"]["value"]
                    }
                    for result in movie_results["results"]["bindings"]
                ]
                movies_data.extend(movie_data)
            except Exception as e:
                print(f"Failed to fetch movies for company {company}: {e}")
        
        # Convert movie data to DataFrame
        movies_df = pd.DataFrame(movies_data)
        return companies_df, movies_df
    
    except Exception as e:
        print(f"Failed to fetch production companies: {e}")
        return pd.DataFrame(), pd.DataFrame()

# Main function to fetch movies for multiple years and save as CSV
def main():
    start_year = 1990
    end_year = 2024
    all_data = []
    attribute_summary = {}

    # Fetch movies based on names
    movie_names = [
    'shrek', 'titanic', 'inception', 'joker', 'parasite', 'avengers',
    'harry potter', 'star wars', 'jurassic park', 'the godfather', 'batman', 'spiderman', 
    'the matrix', 'lord of the rings', 'back to the future', 'forrest gump', 'the lion king',
    'et the extra-terrestrial', 'the dark knight', 'pulp fiction', 'the shawshank redemption',
    'fight club', 'the silence of the lambs', 'the sixth sense', 'the green mile', 'gladiator',
    'the departed', 'the prestige', 'the dark knight rises', 'the wolf of wall street', 'interstellar',
    'the revenant', 'the irishman', '1917', 'tenet', 'black panther', 'the shape of water', 'get out',
    'dunkirk', 'la la land', 'moonlight', 'birdman', 'spotlight', 'the artist', 'argo', '12 years a slave',
    'the king speech', 'slumdog millionaire', 'no country for old', 'casablanca', 'gone with the wind',
    'schindler\'s list', 'goodfellas', 'braveheart', 'saving private ryan', 'the sound of music', 'citizen kane',
    'a beautiful mind', 'rocky', 'jaws', 'the exorcist', 'the shining', 'blade runner', 'alien', 'terminator',
    'die hard', 'indiana jones', 'the breakfast club', 'ferris bueller\'s day off', 'ghostbusters', 'top gun',
    'dirty dancing', 'pretty woman', 'the princess bride', 'the big lebowski', 'fargo', 'american beauty',
    'memento', 'requiem for a dream', 'amelie', 'pan\'s labyrinth', 'her', 'whiplash', 'mad max: fury road',
    'the grand budapest hotel', 'the social network', 'the imitation game', 'mission impossible', 'fast and furious',
    'pirates of the caribbean', 'transformers', 'x-men', 'star trek', 'hunger games', 'twilight', 'james bond',
    'bourne', 'planet of the apes', 'toy story', 'ice age', 'despicable me', 'how to train your dragon',
    'kung fu panda', 'hotel transylvania', 'lego movie', 'minions', 'cars', 'finding nemo', 'incredibles',
    'monsters inc', 'wreck-it ralph', 'zootopia', 'frozen', 'moana', 'coco', 'ralph breaks the internet',
    'big hero 6', 'tangled', 'brave', 'inside out', 'up', 'wall-e', 'ratatouille', 'a bug\'s life', 'ant-man',
    'doctor strange', 'guardians of the galaxy', 'thor', 'iron man', 'captain america', 'hulk', 'black widow',
    'eternals', 'shang-chi', 'venom', 'deadpool', 'logan', 'fantastic four', 'blade', 'ghost rider', 'punisher',
    'daredevil', 'electra', 'hellboy', 'kick-ass', 'kingsman', 'john wick', 'rambo', 'expendables', 'lethal weapon',
    'rush hour', 'bad boys', 'men in black', 'jack ryan', 'jack reacher', 'taken', 'equalizer', 'mad max', 'rambo',
    'expendables', 'lethal weapon', 'die hard', 'rush hour', 'bad boys', 'men in black', 'mission impossible'
    ]
    print("Fetching movies by name...")
    movies_by_names = fetch_movies_by_names(movie_names)
    print(f"We fetched {len(movies_by_names)} movies by name")

    
    companies_df, movies_by_companies = fetch_top_companies_and_movies(limit=100)
    print(f"we fetched {len(companies_df)} comapnies and {len(movies_by_companies)} movies")
    movies_by_years = []
    # Fetch movies by years
    for year in range(start_year, end_year + 1):
        print(f"Processing year {year}...")
        # Fetch mandatory information
        basic_info = fetch_mandatory_information(f"{year}-01-01", f"{year}-12-31", limit=5000)
        print(f"Fetched {len(basic_info)} movies from {year}")
        if not basic_info.empty:
            movies_by_years.extend(basic_info.to_dict('records'))

    movies_by_years = pd.DataFrame(movies_by_years)

    # Remove duplicates based on movie_uri
    all_movies_df = pd.concat([movies_by_names, movies_by_companies, movies_by_years], ignore_index=True)
    print(f"Before dropping duplicates: all_movies_df has {len(all_movies_df)} rows and {len(all_movies_df.columns)} columns")
    all_movies_df = pd.DataFrame(all_movies_df).drop_duplicates(subset=["movie_uri"])

    print(f"After dropping duplicates: all_movies_df has {len(all_movies_df)} rows and {len(all_movies_df.columns)} columns")
 
    # Fetch optional single-valued attributes
    single_valued_attributes = {
        "releaseDate": {"property": "releaseDate",
                         "attributes": [("dbo", "http://dbpedia.org/ontology/", "releaseDate")]},
        "country": {"property": "country", "filter_lang": True,
                     "attributes": [("dbo", "http://dbpedia.org/ontology/", "country"), ("dbp", "http://dbpedia.org/property/", "country")]},
        "language": {"property": "language", "filter_lang": True,
                      "attributes": [("dbo", "http://dbpedia.org/ontology/", "language"), ("dbp", "http://dbpedia.org/property/", "language")]},
        "runtime": {"property": "runtime",
                     "attributes": [("dbo", "http://dbpedia.org/ontology/", "runtime"), ("dbp", "http://dbpedia.org/property/", "runtime")]},
        "budget": {"property": "budget",
                    "attributes": [("dbo", "http://dbpedia.org/ontology/", "budget"), ("dbp", "http://dbpedia.org/property/", "budget")]},
        "boxOffice": {"property": "gross",
                       "attributes": [("dbo", "http://dbpedia.org/ontology/", "gross"), ("dbp", "http://dbpedia.org/property/", "gross")]},
        "basedOn": {"property": "basedOn", "filter_lang": True,
                     "attributes": [("dbo", "http://dbpedia.org/ontology/", "basedOn")]},
        "IMDbID": {"property": "imdbId",
                    "attributes": [("dbo", "http://dbpedia.org/ontology/", "imdbId")]},
        "plot": {"property": "abstract", "filter_lang": True,
                  "attributes": [("dbo", "http://dbpedia.org/ontology/", "abstract")]},
        "franchise": {"property": "franchise", "filter_lang": True,
                       "attributes": [("dbo", "http://dbpedia.org/ontology/", "franchise")]},
        "depiction": {"property": "depiction", "filter_lang": True,
                       "attributes": [("dbo", "http://dbpedia.org/ontology/", "depiction")]},
        "wikiPageWikiLink": {"property": "wikiPageWikiLink",
                              "attributes": [("dbo", "http://dbpedia.org/ontology/", "wikiPageWikiLink")]},
        "primaryTopic": {"property": "primaryTopic", "filter_lang": True,
                          "attributes": [("foaf", "http://xmlns.com/foaf/0.1/", "primaryTopic")]},
        "wasDerivedFrom": {"property": "wasDerivedFrom", "filter_lang": True,
                            "attributes": [("prov", "http://www.w3.org/ns/prov#", "wasDerivedFrom")]}
    }

    movie_uris = all_movies_df["movie_uri"].tolist()

    for label, details in single_valued_attributes.items():
        print(f"Fetching {label}...")
        single_data = fetch_single_valued_attributes(movie_uris, details["property"], details["attributes"], details.get("filter_lang", False))
        all_movies_df[label] = all_movies_df["movie_uri"].map(lambda uri: single_data.get(uri, "N/A"))
        count = len(all_movies_df[all_movies_df[label] != 'N/A'])
        if label not in attribute_summary:
            attribute_summary[label] = {"status": "none", "count": 0}
        attribute_summary[label]["count"] += count
        if count > 0:
            if count < len(all_movies_df):
                attribute_summary[label]["status"] = "some"
            else:
                attribute_summary[label]["status"] = "all"

    # Fetch Rotten Tomatoes ID separately
    print("Fetching RottenTomatoesID...")
    rotten_tomatoes_data = fetch_rotten_tomatoes_id(movie_uris)
    all_movies_df["RottenTomatoesID"] = all_movies_df["movie_uri"].map(lambda uri: rotten_tomatoes_data.get(uri, "N/A"))
    count = len(all_movies_df[all_movies_df["RottenTomatoesID"] != 'N/A'])
    if "RottenTomatoesID" not in attribute_summary:
        attribute_summary["RottenTomatoesID"] = {"status": "none", "count": 0}
    attribute_summary["RottenTomatoesID"]["count"] += count
    if count > 0:
        if count < len(all_movies_df):
            attribute_summary["RottenTomatoesID"]["status"] = "some"
        else:
            attribute_summary["RottenTomatoesID"]["status"] = "all"

    # Fetch grouped attributes
    grouped_attributes = {
        "genres": {"property": "genre", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
        "actors": {"property": "starring", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
        "directors": {"property": "director", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
        "distributors": {"property": "distributor", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
        "writer": {"property": "writer", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
        "producers": {"property": "producer", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
        "composers": {"property": "musicComposer", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
        "cinematographers": {"property": "cinematography", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
        "filmingLocations": {"property": "filmingLocation", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
        "productionCompanies": {"property": "productionCompany", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
        "mainSubjects": {"property": "subject", "prefix": "dcterms", "url": "http://purl.org/dc/terms/"},
        "series": {"property": "series", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"}
    }

    grouped_data = {}
    for label, details in grouped_attributes.items():
        print(f"Fetching {label}...")
        grouped_data[label] = fetch_grouped_attributes(movie_uris, details["property"], f"{label[:-1]}Label", details["prefix"], details["url"], include_uri=True)

    for label in grouped_attributes.keys():
        all_movies_df[label] = all_movies_df["movie_uri"].map(
            lambda uri: grouped_data[label].get(uri, {}).get(f"{label[:-1]}Labels", "N/A") if grouped_data[label].get(uri, {}).get(f"{label[:-1]}Labels") else "N/A"
        )
        all_movies_df[f"{label}_URIs"] = all_movies_df["movie_uri"].map(
            lambda uri: grouped_data[label].get(uri, {}).get("attributeURIs", "N/A") if grouped_data[label].get(uri, {}).get("attributeURIs") else "N/A"
        )

        count = len(all_movies_df[all_movies_df[label] != 'N/A'])
        if label not in attribute_summary:
            attribute_summary[label] = {"status": "none", "count": 0}
        attribute_summary[label]["count"] += count
        if count > 0:
            if count < len(all_movies_df):
                attribute_summary[label]["status"] = "some"
            else:
                attribute_summary[label]["status"] = "all"

    # Identify columns with only N/A values
    na_columns = all_movies_df.columns[all_movies_df.isna().all()].tolist()
    print(f"Columns with only N/A values: {na_columns}")

    # Drop columns that are entirely N/A
    all_movies_df = all_movies_df.dropna(axis=1, how="all")

    print(f"all_movies_df has {len(all_movies_df)} rows and {len(all_movies_df.columns)} columns")
    
    # Ensure output directory exists
    time_str = time.strftime("%Y_%m_%d_%H_%M_%S")
    output_file = f"{DIR_PATH}/CSVs/dbpedia_movies_{time_str}.csv"
    all_movies_df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")

    # Save attribute summary to CSV
    attribute_summary_df = pd.DataFrame([
        {"attribute": k, "status": v["status"], "count": v["count"]}
        for k, v in attribute_summary.items()
    ])
    attribute_summary_file = f"{DIR_PATH}/CSVs/attribute_summary_{time_str}.csv"
    attribute_summary_df.to_csv(attribute_summary_file, index=False)
    print(f"Attribute summary saved to {attribute_summary_file}")

if __name__ == "__main__":
    main()
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
import os

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Define the SPARQL endpoint
endpoint_url = "https://query.wikidata.org/sparql"

# Step 1: Fetch basic information for a specific date range
def fetch_basic_information(start_date, end_date, limit=1000):
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setQuery(f"""
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?movie ?movieLabel (MIN(?releaseDate) AS ?earliestReleaseDate) 
           ?runtime ?duration ?languageLabel ?budget ?boxOffice ?basedOn ?basedOnLabel ?IMDbID ?RottenTomatoesID ?dbpediaURI
    WHERE {{
      ?movie wdt:P31 wd:Q11424;              # Instance of film
             wdt:P577 ?releaseDate;          # Release date
             wdt:P364 wd:Q1860.              # Original language is English

      OPTIONAL {{ ?movie wdt:P2047 ?runtime. }}                # Runtime (optional)
      OPTIONAL {{ ?movie wdt:P2047 ?duration. }}               # Duration (optional)
      OPTIONAL {{ ?movie wdt:P2130 ?budget. }}                 # Budget (optional)
      OPTIONAL {{ ?movie wdt:P2142 ?boxOffice. }}              # Box Office Revenue (optional)
      OPTIONAL {{ ?movie wdt:P144 ?basedOn. }}                 # Based On (optional)
      OPTIONAL {{ ?movie wdt:P345 ?IMDbID. }}                  # IMDb ID (optional)
      OPTIONAL {{ ?movie wdt:P1258 ?RottenTomatoesID. }}       # Rotten Tomatoes ID (optional)
      OPTIONAL {{ ?movie wdt:P1628 ?dbpediaURI. }}             # DBpedia URI (optional)

                    
      FILTER((?releaseDate >= "{start_date}T00:00:00Z"^^xsd:dateTime) &&
             (?releaseDate <= "{end_date}T23:59:59Z"^^xsd:dateTime))

      # Label service to fetch human-readable labels for entities
      SERVICE wikibase:label {{ 
        bd:serviceParam wikibase:language "en". 
      }}
    }}
    GROUP BY ?movie ?movieLabel ?runtime ?duration ?languageLabel ?budget ?boxOffice 
             ?basedOn ?basedOnLabel ?IMDbID ?RottenTomatoesID ?dbpediaURI
    LIMIT {limit}
    """)
    sparql.setReturnFormat(JSON)

    try:
        print(f"Fetching movies from {start_date} to {end_date}...")
        results = sparql.query().convert()
        data = [
            {
                "movie_uri": result["movie"]["value"],
                "movie": result["movieLabel"]["value"],
                "releaseDate": result["earliestReleaseDate"]["value"],
                "runtime": result.get("runtime", {}).get("value", None),
                "duration": result.get("duration", {}).get("value", None),
                "language": "English",  # Filter ensures all movies are in English
                "budget": result.get("budget", {}).get("value", None),
                "boxOffice": result.get("boxOffice", {}).get("value", None),
                "basedOn": result.get("basedOnLabel", {}).get("value", None),
                "basedOn_uri": result.get("basedOn", {}).get("value", None),
                "IMDbID": result.get("IMDbID", {}).get("value", None),
                "RottenTomatoesID": result.get("RottenTomatoesID", {}).get("value", None),
                "dbpediaURI": result.get("dbpediaURI", {}).get("value", None),
            }
            for result in results["results"]["bindings"]
        ]
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Failed to fetch data for {start_date} to {end_date}: {e}")
        return pd.DataFrame()


# Step 2: Fetch grouped attributes (e.g., genres, actors, directors, distributors)
def fetch_grouped_attributes(movie_uris, attribute_property, attribute_label, include_uri=False):
    sparql = SPARQLWrapper(endpoint_url)
    results = []
    chunk_size = 100  # Adjust chunk size for query stability
    for i in range(0, len(movie_uris), chunk_size):
        chunk = movie_uris[i:i + chunk_size]
        values = " ".join([f"<{uri}>" for uri in chunk])
        query = f"""
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?movie 
               (GROUP_CONCAT(DISTINCT ?{attribute_label}; separator=", ") AS ?{attribute_label}s)
               {'(GROUP_CONCAT(DISTINCT ?attribute; separator=", ") AS ?attributeURIs)' if include_uri else ''}
        WHERE {{
          VALUES ?movie {{ {values} }}
          OPTIONAL {{
            ?movie wdt:{attribute_property} ?attribute.
            ?attribute rdfs:label ?{attribute_label}.
            FILTER(LANG(?{attribute_label}) = "en")
          }}
        }}
        GROUP BY ?movie
        """
        for attempt in range(MAX_RETRIES):
            try:
                print(f"Fetching {attribute_label} for chunk {i // chunk_size + 1} (attempt {attempt + 1})...")
                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                chunk_results = sparql.query().convert()
                results.extend(chunk_results["results"]["bindings"])
                break
            except Exception as e:
                print(f"Error fetching {attribute_label} for chunk {i // chunk_size + 1} (attempt {attempt + 1}): {e}")
                time.sleep(RETRY_DELAY)
    return {
        result["movie"]["value"]: {
            f"{attribute_label}s": result.get(f"{attribute_label}s", {}).get("value", "N/A"),
            "attributeURIs": result.get("attributeURIs", {}).get("value", "N/A") if include_uri else "N/A"
        }
        for result in results
    }

# Main function to fetch movies for multiple years and save as CSV
def main():
    start_year = 2024
    end_year = 2024
    all_data = []

    # Fetch grouped attributes
    grouped_attributes = {
        "genres": "P136",
        "actors": "P161",
        "directors": "P57",
        "distributors": "P750",
        "producers": "P162",
        "composers": "P86",
        "cinematographers": "P344",
        "filmingLocations": "P915",
        "productionCompanies": "P272",
        "mainSubjects": "P921",
        "series": "P179"
    }

    for year in range(start_year, end_year + 1):
        basic_info = fetch_basic_information(f"{year}-01-01", f"{year}-12-31", limit=1000)
        movie_uris = basic_info["movie_uri"].tolist()
        info = basic_info
        grouped_data = {}
        for label, prop in grouped_attributes.items():
            print(f"Fetching {label}...")
            grouped_data[label] = fetch_grouped_attributes(movie_uris, prop, f"{label[:-1]}Label", include_uri=True)

        # Add grouped attributes to the dataframe
        for label in grouped_attributes.keys():
            info[label] = info["movie_uri"].map(
                lambda uri: grouped_data[label].get(uri, {}).get(f"{label[:-1]}Labels", "N/A")
            )
            info[f"{label}_URIs"] = info["movie_uri"].map(
                lambda uri: grouped_data[label].get(uri, {}).get("attributeURIs", "N/A")
            )

        # Deduplicate rows
        info = info.drop_duplicates(subset=["movie", "releaseDate"])
        all_data.append(info)
        time.sleep(1)  # Add a delay to avoid overloading the server

    # Combine all fetched data
    combined_data = pd.concat(all_data, ignore_index=True)

    # Drop columns that are entirely N/A
    combined_data = combined_data.dropna(axis=1, how="all")

    print(f"Combined data has {len(combined_data)} rows and {len(combined_data.columns)} columns")
    
    # Ensure output directory exists
    os.makedirs("Datasets", exist_ok=True)
    output_file = f"Datasets/english_movies_{start_year}_{end_year}_detailed.csv"
    combined_data.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")

if __name__ == "__main__":
    main()
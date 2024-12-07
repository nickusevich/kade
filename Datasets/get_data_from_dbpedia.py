from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time
import os

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
CHUNK_SIZE = 100  # Process movies in chunks of 100

# Define the SPARQL endpoint
endpoint_url = "http://dbpedia.org/sparql"

# Step 1: Fetch mandatory information for a specific date range
def fetch_mandatory_information(start_date, end_date, limit=5000):
    sparql = SPARQLWrapper(endpoint_url)
    query = f"""
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX dbr: <http://dbpedia.org/resource/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?movie ?movieLabel (MIN(?releaseDate) AS ?releaseDate)
    WHERE {{
      ?movie a dbo:Film ;
             dbo:releaseDate ?releaseDate .

      FILTER((?releaseDate >= "{start_date}"^^xsd:date) &&
             (?releaseDate <= "{end_date}"^^xsd:date))

      ?movie rdfs:label ?movieLabel .
      FILTER(LANG(?movieLabel) = "en")
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
                "movie": result["movieLabel"]["value"],
                "releaseDate": result["releaseDate"]["value"]
            }
            for result in results["results"]["bindings"]
        ]
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Failed to fetch mandatory data for {start_date} to {end_date}: {e}")
        return pd.DataFrame()

# Step 2: Fetch optional single-valued attributes
def fetch_single_valued_attributes(movie_uris, attribute_property, attribute_label, prefix, prefix_url, filter_lang=False):
    sparql = SPARQLWrapper(endpoint_url)
    results = []
    if filter_lang:
        print(f"Fetching {attribute_label} with English language filter...")
    for i in range(0, len(movie_uris), CHUNK_SIZE):
        chunk = movie_uris[i:i + CHUNK_SIZE]
        values = " ".join([f"<{uri}>" for uri in chunk])
        query = f"""
        PREFIX {prefix}: <{prefix_url}>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?movie ?{attribute_label}
        WHERE {{
          VALUES ?movie {{ {values} }}
          OPTIONAL {{ ?movie {prefix}:{attribute_property} ?{attribute_label}. }}
          {f"FILTER(LANG(?{attribute_label}) = 'en')" if filter_lang else ""}
        }}
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

# Main function to fetch movies for multiple years and save as CSV
def main():
    start_year = 1990
    end_year = 2024
    all_data = []
    attribute_summary = {}

    for year in range(start_year, end_year + 1):
        print(f"Processing year {year}...")
        # Fetch mandatory information
        basic_info = fetch_mandatory_information(f"{year}-01-01", f"{year}-12-31", limit=1000)
        print(f"Fetched {len(basic_info)} movies from {year}")
        if basic_info.empty:
            continue
        movie_uris = basic_info["movie_uri"].tolist()
        info = basic_info

        # Fetch optional single-valued attributes
        single_valued_attributes = {
            "country": {"property": "country", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
            "language": {"property": "language", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
            "runtime": {"property": "runtime", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
            "budget": {"property": "budget", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
            "boxOffice": {"property": "gross", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
            "basedOn": {"property": "basedOn", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
            "IMDbID": {"property": "imdbId", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
            "plot": {"property": "abstract", "prefix": "dbo", "url": "http://dbpedia.org/ontology/", "filter_lang": True},
            "franchise": {"property": "franchise", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
            "depiction": {"property": "depiction", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
            "wikiPageWikiLink": {"property": "wikiPageWikiLink", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
            "primaryTopic": {"property": "primaryTopic", "prefix": "foaf", "url": "http://xmlns.com/foaf/0.1/"},
            "wasDerivedFrom": {"property": "wasDerivedFrom", "prefix": "prov", "url": "http://www.w3.org/ns/prov#"}
        }

        for label, details in single_valued_attributes.items():
            print(f"Fetching {label}...")
            single_data = fetch_single_valued_attributes(movie_uris, details["property"], label, details["prefix"], details["url"], details.get("filter_lang", False))
            info[label] = info["movie_uri"].map(lambda uri: single_data.get(uri, "N/A"))
            count = len(info[info[label] != 'N/A'])
            if label not in attribute_summary:
                attribute_summary[label] = {"status": "none", "count": 0}
            attribute_summary[label]["count"] += count
            if count > 0:
                if count < len(info):
                    attribute_summary[label]["status"] = "some"
                else:
                    attribute_summary[label]["status"] = "all"

        # Fetch Rotten Tomatoes ID separately
        print("Fetching RottenTomatoesID...")
        rotten_tomatoes_data = fetch_rotten_tomatoes_id(movie_uris)
        info["RottenTomatoesID"] = info["movie_uri"].map(lambda uri: rotten_tomatoes_data.get(uri, "N/A"))
        count = len(info[info["RottenTomatoesID"] != 'N/A'])
        if "RottenTomatoesID" not in attribute_summary:
            attribute_summary["RottenTomatoesID"] = {"status": "none", "count": 0}
        attribute_summary["RottenTomatoesID"]["count"] += count
        if count > 0:
            if count < len(info):
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
            info[label] = info["movie_uri"].map(
                lambda uri: grouped_data[label].get(uri, {}).get(f"{label[:-1]}Labels", "N/A") if grouped_data[label].get(uri, {}).get(f"{label[:-1]}Labels") else "N/A"
            )
            info[f"{label}_URIs"] = info["movie_uri"].map(
                lambda uri: grouped_data[label].get(uri, {}).get("attributeURIs", "N/A") if grouped_data[label].get(uri, {}).get("attributeURIs") else "N/A"
            )

            count = len(info[info[label] != 'N/A'])
            if label not in attribute_summary:
                attribute_summary[label] = {"status": "none", "count": 0}
            attribute_summary[label]["count"] += count
            if count > 0:
                if count < len(info):
                    attribute_summary[label]["status"] = "some"
                else:
                    attribute_summary[label]["status"] = "all"

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
    output_file = f"Datasets/dbpedia_movies_{start_year}_{end_year}.csv"
    combined_data.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")

    # Save attribute summary to CSV
    attribute_summary_df = pd.DataFrame([
        {"attribute": k, "status": v["status"], "count": v["count"]}
        for k, v in attribute_summary.items()
    ])
    attribute_summary_file = f"Datasets/attribute_summary_{start_year}_{end_year}.csv"
    attribute_summary_df.to_csv(attribute_summary_file, index=False)
    print(f"Attribute summary saved to {attribute_summary_file}")

if __name__ == "__main__":
    main()
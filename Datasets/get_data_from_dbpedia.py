from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import time

# Define the SPARQL endpoint for DBpedia
endpoint_url = "https://dbpedia.org/sparql"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
BATCH_SIZE = 100  # Process movies in batches of 100


def fetch_dbpedia_uris_from_wikidata(movie_uris):
    """
    Fetch DBpedia URIs for given Wikidata URIs using owl:sameAs property.
    Handles large lists by splitting into manageable batches.
    """
    results = []
    sparql = SPARQLWrapper(endpoint_url)

    for i in range(0, len(movie_uris), BATCH_SIZE):
        batch = movie_uris[i:i + BATCH_SIZE]
        values = " ".join([f"<{uri}>" for uri in batch])
        sparql.setQuery(f"""
        PREFIX owl: <http://www.w3.org/2002/07/owl#>

        SELECT DISTINCT ?dbpedia_uri ?wikidata_uri
        WHERE {{
          ?dbpedia_uri owl:sameAs ?wikidata_uri .
          VALUES ?wikidata_uri {{ {values} }}
        }}
        """)
        sparql.setReturnFormat(JSON)

        for attempt in range(MAX_RETRIES):
            try:
                print(f"Mapping Wikidata URIs to DBpedia URIs for batch {i // BATCH_SIZE + 1} (attempt {attempt + 1})...")
                batch_results = sparql.query().convert()
                results.extend(batch_results["results"]["bindings"])
                break
            except Exception as e:
                print(f"Error mapping URIs (batch {i // BATCH_SIZE + 1}, attempt {attempt + 1}): {e}")
                time.sleep(RETRY_DELAY)

    return {
        result["wikidata_uri"]["value"]: result["dbpedia_uri"]["value"]
        for result in results
    }


def fetch_dbpedia_uris_by_label(movie_labels):
    """
    Fetch DBpedia URIs for movies using their labels as a fallback method.
    """
    results = []
    sparql = SPARQLWrapper(endpoint_url)

    for i in range(0, len(movie_labels), BATCH_SIZE):
        batch = movie_labels[i:i + BATCH_SIZE]
        values = " ".join([f'"{label}"@en' for label in batch])
        sparql.setQuery(f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?dbpedia_uri ?label
        WHERE {{
          ?dbpedia_uri rdfs:label ?label .
          VALUES ?label {{ {values} }}
        }}
        """)
        sparql.setReturnFormat(JSON)

        for attempt in range(MAX_RETRIES):
            try:
                print(f"Mapping movie labels to DBpedia URIs for batch {i // BATCH_SIZE + 1} (attempt {attempt + 1})...")
                batch_results = sparql.query().convert()
                results.extend(batch_results["results"]["bindings"])
                break
            except Exception as e:
                print(f"Error mapping labels (batch {i // BATCH_SIZE + 1}, attempt {attempt + 1}): {e}")
                time.sleep(RETRY_DELAY)

    return {
        result["label"]["value"]: result["dbpedia_uri"]["value"]
        for result in results
    }

def fetch_single_valued_features(movie_uris):
    """
    Fetch single-valued features for movies.
    """
    results = []
    for i in range(0, len(movie_uris), BATCH_SIZE):
        batch = movie_uris[i:i + BATCH_SIZE]
        sparql = SPARQLWrapper(endpoint_url)
        values = " ".join([f"<{uri}>" for uri in batch])
        sparql.setQuery(f"""
        PREFIX dbo: <http://dbpedia.org/ontology/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        
        SELECT DISTINCT ?movie ?boxOffice ?plot ?franchise 
                        ?abstract ?depiction ?wikiPageWikiLink ?primaryTopic ?wasDerivedFrom
        WHERE {{
          VALUES ?movie {{ {values} }}
          OPTIONAL {{ ?movie dbo:gross ?boxOffice. }}
          OPTIONAL {{ ?movie dbo:abstract ?plot. FILTER(LANG(?plot) = "en") }}
          OPTIONAL {{ ?movie dbo:franchise ?franchise. }}
          OPTIONAL {{ ?movie foaf:depiction ?depiction. }}
          OPTIONAL {{ ?movie foaf:wikiPageWikiLink ?wikiPageWikiLink. }}
          OPTIONAL {{ ?movie foaf:primaryTopic  ?primaryTopic. }}
          OPTIONAL {{ ?movie foaf:wasDerivedFrom  ?wasDerivedFrom. }}
        }}
        """)
        sparql.setReturnFormat(JSON)

        for attempt in range(MAX_RETRIES):
            try:
                print(f"Fetching single-valued features for batch {i // BATCH_SIZE + 1} (attempt {attempt + 1})...")
                batch_results = sparql.query().convert()
                results.extend(batch_results["results"]["bindings"])
                break
            except Exception as e:
                print(f"Error fetching single-valued features (batch {i // BATCH_SIZE + 1}, attempt {attempt + 1}): {e}")
                time.sleep(RETRY_DELAY)
    print(f"From {len(movie_uris)} URIs, found {len(results)} in DBpedia.")
    return [
        {
            "movie_uri": result["movie"]["value"],
            "boxOffice": result.get("boxOffice", {}).get("value", None),
            "abstract": result.get("abstract", {}).get("value", None),
            "plot": result.get("plot", {}).get("value", None),
            "franchise": result.get("franchise", {}).get("value", None),
            "depiction": result.get("depiction", {}).get("value", None),
            "wikiPageWikiLink": result.get("wikiPageWikiLink", {}).get("value", None),
            "primaryTopic": result.get("primaryTopic", {}).get("value", None),
            "wasDerivedFrom": result.get("wasDerivedFrom", {}).get("value", None),
        }
        for result in results
    ]


def fetch_multi_valued_features(movie_uris, property_label, property_uri, prefix, prefix_url):
    """
    Fetch multi-valued features for movies.
    """
    results = []
    for i in range(0, len(movie_uris), BATCH_SIZE):
        batch = movie_uris[i:i + BATCH_SIZE]
        sparql = SPARQLWrapper(endpoint_url)
        values = " ".join([f"<{uri}>" for uri in batch])
        sparql.setQuery(f"""
        PREFIX {prefix}: <{prefix_url}>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?movie 
               (GROUP_CONCAT(DISTINCT ?{property_label}; separator=", ") AS ?{property_label}s)
               (GROUP_CONCAT(DISTINCT ?property_uri; separator=", ") AS ?{property_label}URIs)
        WHERE {{
          VALUES ?movie {{ {values} }}
          ?movie {prefix}:{property_uri} ?property_uri.
          ?property_uri rdfs:label ?{property_label}.
          FILTER (LANG(?{property_label}) = "en")
        }}
        GROUP BY ?movie
        """)
        sparql.setReturnFormat(JSON)

        for attempt in range(MAX_RETRIES):
            try:
                print(f"Fetching {property_label} for batch {i // BATCH_SIZE + 1} (attempt {attempt + 1})...")
                batch_results = sparql.query().convert()
                results.extend(batch_results["results"]["bindings"])
                break
            except Exception as e:
                print(f"Error fetching {property_label} (batch {i // BATCH_SIZE + 1}, attempt {attempt + 1}): {e}")
                time.sleep(RETRY_DELAY)

    return {
        result["movie"]["value"]: {
            f"{property_label}s": result.get(f"{property_label}s", {}).get("value", "N/A"),
            f"{property_label}URIs": result.get(f"{property_label}URIs", {}).get("value", "N/A"),
        }
        for result in results
    }

def main():
    # Load movies fetched from Wikidata
    wikidata_movies_file = "Datasets/english_movies_2024_2024_detailed.csv"  # Adjust to your file path
    wikidata_movies = pd.read_csv(wikidata_movies_file)
    wikidata_uris = wikidata_movies["movie_uri"].tolist()
    movie_labels = wikidata_movies["movie"].tolist()

    # Map Wikidata URIs to DBpedia URIs
    print("Mapping Wikidata URIs to DBpedia URIs...")
    uri_mapping = fetch_dbpedia_uris_from_wikidata(wikidata_uris)

    # Fallback: Map movie labels to DBpedia URIs for unmatched movies
    unmatched_movies = wikidata_movies[~wikidata_movies["movie_uri"].isin(uri_mapping.keys())]

    # Fetch DBpedia URIs using labels
    print("Fallback: Mapping movie labels to DBpedia URIs...")
    label_mapping = fetch_dbpedia_uris_by_label(unmatched_movies["movie"].tolist())

    # Combine mappings
    combined_mapping = uri_mapping.copy()
    for index, row in unmatched_movies.iterrows():
        movie_label = row["movie"]
        if movie_label in label_mapping:
            combined_mapping[row["movie_uri"]] = label_mapping[movie_label]

    print(f"len of combined_mapping: {len(combined_mapping)}")

    # Map combined DBpedia URIs to the original Wikidata dataframe
    wikidata_movies["dbpedia_uri"] = wikidata_movies["movie_uri"].map(combined_mapping)


    # Filter out movies without a corresponding DBpedia URI
    dbpedia_movies = wikidata_movies.dropna(subset=["dbpedia_uri"]).reset_index(drop=True)

    # Fetch single-valued features from DBpedia
    print("Fetching single-valued features...")
    single_features = fetch_single_valued_features(dbpedia_movies["dbpedia_uri"].tolist())
    df_single = pd.DataFrame(single_features)

    # Fetch multi-valued features from DBpedia
    multi_valued_properties = {
        "subject": {"property": "subject", "prefix": "dcterms", "url": "http://purl.org/dc/terms/"},
        "wikiPageWikiLink": {"property": "wikiPageWikiLink", "prefix": "dbo", "url": "http://dbpedia.org/ontology/"},
    }

    for label, details in multi_valued_properties.items():
        print(f"Fetching {label}...")
        grouped_data = fetch_multi_valued_features(
            dbpedia_movies["dbpedia_uri"].tolist(),
            label,
            details["property"],
            details["prefix"],
            details["url"],
        )
        df_single[label] = df_single["movie_uri"].map(lambda uri: grouped_data.get(uri, {}).get(f"{label}s", "N/A"))
        df_single[f"{label}_URIs"] = df_single["movie_uri"].map(lambda uri: grouped_data.get(uri, {}).get(f"{label}URIs", "N/A"))

    # Combine Wikidata and DBpedia data
    combined_df = pd.merge(dbpedia_movies, df_single, on="movie_uri", how="left")

    # Drop columns that are entirely N/A
    combined_df = combined_df.dropna(axis=1, how="all")

    # Save to CSV
    output_file = "Datasets/combined_movies_detailed.csv"
    combined_df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")


if __name__ == "__main__":
    main()

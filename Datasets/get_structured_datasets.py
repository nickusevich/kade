from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd

# Define the SPARQL endpoint
endpoint_url = "https://query.wikidata.org/sparql"

# Step 1: Fetch basic information
def fetch_basic_information():
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setQuery("""
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?movie ?movieLabel (MIN(?releaseDate) AS ?earliestReleaseDate) 
           ?runtime ?duration ?languageLabel (GROUP_CONCAT(DISTINCT ?countryLabel; separator=", ") AS ?countries)
           ?rating 
    WHERE {
      ?movie wdt:P31 wd:Q11424;              # Instance of film
             wdt:P577 ?releaseDate;          # Release date
             wdt:P2047 ?runtime;             # Runtime
             wdt:P2769 ?rating;              # Audience rating
             wdt:P364 wd:Q1860;              # Original language is English
             wdt:P495 ?country;              # Country of origin
    
      OPTIONAL { ?movie wdt:P2047 ?duration. } # Duration (optional)

      FILTER((?releaseDate >= "2017-01-01T00:00:00Z"^^xsd:dateTime) &&
             (?releaseDate <= "2017-12-31T00:00:00Z"^^xsd:dateTime))

      ?movie rdfs:label ?movieLabel.
      FILTER(LANG(?movieLabel) = "en")

      ?country rdfs:label ?countryLabel.
      FILTER(LANG(?countryLabel) = "en")
    }
    GROUP BY ?movie ?movieLabel ?runtime ?duration ?languageLabel ?rating
    LIMIT 100
    """)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    data = [
        {
            "movie_uri": result["movie"]["value"],
            "movie": result["movieLabel"]["value"],
            "releaseDate": result["earliestReleaseDate"]["value"],
            "runtime": result.get("runtime", {}).get("value", None),
            "duration": result.get("duration", {}).get("value", None),
            "language": "English",  # Filter ensures all movies are in English
            "countries": result.get("countries", {}).get("value", None),
            "rating": result.get("rating", {}).get("value", None),
        }
        for result in results["results"]["bindings"]
    ]
    return pd.DataFrame(data)

# Step 2: Fetch grouped attributes (e.g., genres, actors, directors, distributors)
def fetch_grouped_attributes(movie_uris, attribute_property, attribute_label, include_uri=False):
    sparql = SPARQLWrapper(endpoint_url)
    values = " ".join([f"<{uri}>" for uri in movie_uris])
    query = f"""
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?movie 
           (GROUP_CONCAT(DISTINCT ?{attribute_label}; separator=", ") AS ?{attribute_label}s)
           {'(GROUP_CONCAT(DISTINCT ?attribute; separator=", ") AS ?attributeURIs)' if include_uri else ''}
    WHERE {{
      VALUES ?movie {{ {values} }}
      ?movie wdt:{attribute_property} ?attribute.
      ?attribute rdfs:label ?{attribute_label}.
      FILTER(LANG(?{attribute_label}) = "en")
    }}
    GROUP BY ?movie
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return {
        result["movie"]["value"]: {
            f"{attribute_label}s": result.get(f"{attribute_label}s", {}).get("value", "N/A"),
            "attributeURIs": result.get("attributeURIs", {}).get("value", "N/A") if include_uri else "N/A"
        }
        for result in results["results"]["bindings"]
    }

# Main function to combine data and save as CSV
def main():
    # Fetch basic information
    print("Fetching basic movie information...")
    basic_info = fetch_basic_information()
    movie_uris = basic_info["movie_uri"].tolist()

    # Fetch grouped attributes
    print("Fetching genres...")
    genres = fetch_grouped_attributes(movie_uris, "P136", "genreLabel", include_uri=True)
    print("Fetching actors...")
    actors = fetch_grouped_attributes(movie_uris, "P161", "actorLabel", include_uri=True)
    print("Fetching directors...")
    directors = fetch_grouped_attributes(movie_uris, "P57", "directorLabel", include_uri=True)
    print("Fetching distributors...")
    distributors = fetch_grouped_attributes(movie_uris, "P750", "distributorLabel", include_uri=True)

    # Add grouped attributes to the dataframe
    basic_info["genres"] = basic_info["movie_uri"].map(lambda uri: genres.get(uri, {}).get("genreLabels", "N/A"))
    basic_info["genres_URIs"] = basic_info["movie_uri"].map(lambda uri: genres.get(uri, {}).get("attributeURIs", "N/A"))
    basic_info["actors"] = basic_info["movie_uri"].map(lambda uri: actors.get(uri, {}).get("actorLabels", "N/A"))
    basic_info["actors_URIs"] = basic_info["movie_uri"].map(lambda uri: actors.get(uri, {}).get("attributeURIs", "N/A"))
    basic_info["directors"] = basic_info["movie_uri"].map(lambda uri: directors.get(uri, {}).get("directorLabels", "N/A"))
    basic_info["directors_URIs"] = basic_info["movie_uri"].map(lambda uri: directors.get(uri, {}).get("attributeURIs", "N/A"))
    basic_info["distributors"] = basic_info["movie_uri"].map(lambda uri: distributors.get(uri, {}).get("distributorLabels", "N/A"))
    basic_info["distributors_URIs"] = basic_info["movie_uri"].map(lambda uri: distributors.get(uri, {}).get("attributeURIs", "N/A"))

    # Deduplicate rows
    basic_info = basic_info.drop_duplicates(subset=["movie", "releaseDate"])

    # Save to CSV (retain the URI column for reference)
    output_file = "Datasets/english_movies_2017_detailed.csv"
    basic_info.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")

if __name__ == "__main__":
    main()
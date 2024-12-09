"""
File: get_reviews.py
Date: 09-12-2024
Description: Preprocess the movie csv data and convert it to RDF format (ttl) to feed into GraphDB
"""

import csv
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD
import urllib.parse

# Function to sanitize URIs
def clean_uri(uri):
    """Sanitize URIs by encoding special characters."""
    return urllib.parse.quote(uri, safe=':/')

def csv_to_rdf(csv_file, rdf_file, minimal=False):
    """
    Convert a CSV file to RDF (Turtle format).
    
    Parameters:
        csv_file (str): Path to the input CSV file.
        rdf_file (str): Path to save the output Turtle file.
        minimal (bool): If True, include minimal properties; otherwise, include all properties.
    """
    # Define namespaces
    DBR = Namespace("http://dbpedia.org/resource/")
    DBO = Namespace("http://dbpedia.org/ontology/")
    DCT = Namespace("http://purl.org/dc/terms/")

    # Create a new RDF graph
    g = Graph()
    g.bind("dbr", DBR)
    g.bind("dbo", DBO)
    g.bind("dct", DCT)

    processed_count = 0  # Count rows processed
    skipped_count = 0  # Count rows skipped
    movie_count = 0  # Count unique movies added to the graph

    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            movie_title = row.get('movie')
            if not movie_title:  # Skip rows without a movie title
                skipped_count += 1
                print(f"Skipped row due to missing movie title: {row}")
                continue

            # Sanitize and create a unique URI for the movie
            try:
                movie_uri = URIRef(clean_uri(f"{DBR}{movie_title.replace(' ', '_')}"))
            except Exception as e:
                skipped_count += 1
                print(f"Skipped row due to URI error: {row}, Error: {e}")
                continue

            # Check if the movie is already added
            if (movie_uri, RDF.type, DBO.Film) not in g:
                g.add((movie_uri, RDF.type, DBO.Film))
                movie_count += 1  # Increment unique movie count

            processed_count += 1

            if minimal:
                # Add movie title
                g.add((movie_uri, RDFS.label, Literal(movie_title, datatype=XSD.string)))

                # Add release year
                release_date = row.get('releaseDate')
                if release_date:
                    release_year = release_date.split("-")[0]  # Extract the year
                    g.add((movie_uri, DBO.releaseYear, Literal(release_year, datatype=XSD.gYear)))

                # Add genres
                genres = row.get('genres')
                if genres:
                    for genre in genres.split('; '):
                        g.add((movie_uri, DBO.genre, Literal(genre, datatype=XSD.string)))

                # Add actors
                actors = row.get('actors')
                if actors:
                    for actor in actors.split('; '):
                        actor_uri = URIRef(clean_uri(f"{DBR}{actor.replace(' ', '_')}"))
                        g.add((movie_uri, DBO.starring, actor_uri))

                # Add directors
                directors = row.get('directors')
                if directors:
                    for director in directors.split('; '):
                        director_uri = URIRef(clean_uri(f"{DBR}{director.replace(' ', '_')}"))
                        g.add((movie_uri, DBO.director, director_uri))

                # Add language
                language = row.get('language')
                if language:
                    g.add((movie_uri, DBO.language, Literal(language, datatype=XSD.string)))
            else:
                # Include all properties for full mode
                for key, value in row.items():
                    if value is not None and value != '' and value != 'N/A':
                        try:
                            predicate = DBO[key] if key != 'movie' else RDFS.label
                            # Split the value by ';' and add each part separately
                            values = value.split(';')
                            for val in values:
                                val = val.strip()  # Remove any leading/trailing whitespace
                                g.add((movie_uri, predicate, Literal(val, datatype=XSD.string)))
                        except Exception as e:
                            print(f"Error adding property: {key} -> {value}, Error: {e}")

    # Serialize the graph to RDF (Turtle format)
    try:
        g.serialize(destination=rdf_file, format='turtle')
        print(f"RDF saved to {rdf_file}")
    except Exception as e:
        print(f"Error during serialization: {e}")

    # Print summary
    print(f"Total rows processed: {processed_count}")
    print(f"Total rows skipped: {skipped_count}")
    print(f"Total unique movies added: {movie_count}")

csv_file="Datasets\CSVs\dbpedia_movies_1990_2024.csv"

csv_to_rdf(
    csv_file,  # Adjust the path to your CSV file
    rdf_file="Datasets\TTLs\full3.ttl",  # Path to save the full Turtle file
    minimal=False  # Set to True for minimal output
)

csv_to_rdf(
    csv_file,  # Adjust the path to your CSV file
    rdf_file="Datasets\TTLs\minimal3.ttl",  # Path to save the minimal Turtle file
    minimal=True  # Minimal output
)
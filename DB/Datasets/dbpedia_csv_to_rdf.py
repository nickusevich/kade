import csv
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD
import urllib.parse

# Function to sanitize URIs
def clean_uri(uri):
    """Sanitize URIs by encoding special characters."""
    return urllib.parse.quote(uri, safe=':/')

def csv_to_rdf(csv_file, rdf_file):
    """
    Convert a CSV file to RDF (Turtle format).
    
    Parameters:
        csv_file (str): Path to the input CSV file.
        rdf_file (str): Path to save the output Turtle file.
    """
    # Define namespaces
    DBR = Namespace("http://dbpedia.org/resource/")
    DBO = Namespace("http://dbpedia.org/ontology/")
    DCT = Namespace("http://purl.org/dc/terms/")
    RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

    # Create a new RDF graph
    g = Graph()
    g.bind("dbr", DBR)
    g.bind("dbo", DBO)
    g.bind("dct", DCT)

    processed_count = 0  # Count rows processed
    skipped_count = 0  # Count rows skipped
    movie_count = 0  # Count unique movies added to the graph

    with open(csv_file, 'r', encoding='utf-8') as f:
        print(f"Reading CSV file: {csv_file}...")
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

            # Map attributes to RDF properties
            str_attributes = {
                'movie': RDFS.label,
                'runtime': DBO.runtime,
                'budget': DBO.budget,
                'boxOffice': DBO.boxOffice,
                'country': DBO.country,
                'language': DBO.language,
                'IMDbID': DBO.imdbID,
                'plot': DBO.abstract,
                'wikiPageWikiLink': DCT.subject,
                'RottenTomatoesID': DBO.rottenTomatoesID,
                'mainSubjects': DBO.mainSubject,
            }

            num_attributes = {
                'runtime': DBO.runtime,
                'budget': DBO.budget,
                'boxOffice': DBO.boxOffice,
            }

            object_attributes = {
                'genres' :DBO.genre,
                'actors': DBO.starring,
                'directors': DBO.director,
                'distributors': DBO.distributor,
                'writer': DBO.writer,
                'producers': DBO.producer,
                'composers': DBO.composer,
                'cinematographers': DBO.cinematographer,
                'productionCompanies': DBO.productionCompany,
                'wasDerivedFrom': DBO.wasDerivedFrom,
                'series': DBO.series,
            }
            # Handle literal date attributes
            release_date = row.get('releaseDate')
            if release_date is not None and release_date != '' and release_date != 'N/A':
                release_year = release_date.split("-")[0]  # Extract the year
                g.add((movie_uri, DBO.releaseYear, Literal(release_year, datatype=XSD.gYear)))

            # Handle literal string attributes
            for attr, predicate in str_attributes.items():
                value = row.get(attr)
                if value is not None and value != '' and value.strip() != 'N/A':
                    for val in value.split('; '):
                        g.add((movie_uri, predicate, Literal(val.strip(), lang="en")))
            
            # # Handle literal numeric attributes
            # for attr, predicate in num_attributes.items():
            #     value = row.get(attr)
            #     if value is not None and value != '' and value != 'N/A':
            #         for val in value:
            #             g.add((movie_uri, predicate, Literal(val, datatype=XSD.integer)))
            #     else:
            #         g.add((movie_uri, predicate, Literal(value, datatype=XSD.string)))
            for attr, predicate in num_attributes.items():
                value = row.get(attr)
                if value is not None and value != '' and value.strip() != 'N/A':
                # Split by semicolon for multiple values
                    for val in value.split(';'):
                        val = val.strip()  # Remove leading/trailing whitespace
                        if val.isdigit():  # Check if the value is numeric
                            g.add((movie_uri, predicate, Literal(int(val), datatype=XSD.integer)))
                        else:
                            g.add((movie_uri, predicate, Literal(val.strip(), datatype=XSD.string)))

            # Handle object attributes
            for attr, predicate in object_attributes.items():
                values = row.get(attr)
                uri_values = row.get(f"{attr}_URIs")
                if uri_values is None or uri_values == '' or uri_values == 'N/A':
                    uri_values = ""

                if values is not None and values != '' and values.strip() != 'N/A':
                    for value, uri in  zip(values.split('; '), uri_values.split('; ')):
                        if uri is not None and uri != '' and uri.strip() != 'N/A':
                            object_uri = URIRef(clean_uri(uri))
                        else:
                            object_uri = URIRef(clean_uri(f"{DBR}{value.replace(' ', '_')}"))

                        g.add((movie_uri, predicate, object_uri))
                        
                        if value is not None and value != '' and value.strip() != 'N/A':
                            g.add((object_uri, RDFS.label, Literal(value.strip(), lang="en")))

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


# File paths Datasets\CSVs\actors_URIs.csv
folder_path = "DB/Datasets"
csv_file = f"{folder_path}/CSVs/dbpedia_movies.csv"  # Adjust the path to your CSV file
rdf_file = f"{folder_path}/TTLs/dbpedia_movies.ttl"  # Path to save the Turtle file

# Convert CSV to RDF
csv_to_rdf(csv_file, rdf_file)

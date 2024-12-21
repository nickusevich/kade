import csv
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD
import urllib.parse
import requests
import re
import pandas as pd

# Function to sanitize URIs
def clean_uri(uri):
    """Sanitize URIs by encoding special characters."""
    return urllib.parse.quote(uri, safe=':/')

# Function to clean and standardize country literals
def clean_country_value(country_value):
    """Clean country values to remove trailing slashes, excess spaces, and incorrect formatting."""
    if country_value.startswith("http://") or country_value.startswith("https://"):
        return country_value
    country_value = re.sub(r'/$', '', country_value)  # Remove trailing slashes
    country_value = re.sub(r'ref\|.*', '', country_value)  # Remove references
    country_value = re.sub(r'Umited States', 'United States', country_value) # Fix typo
    country_value = re.sub(r'United Satates', 'United States', country_value) # Fix typo
    country_value = re.sub(r'United States United States', 'United States', country_value) # Fix typo
    country_value = re.sub(r'United Statyes', 'United States', country_value) # Fix typo
    country_value = re.sub(r'Pennsylvania', 'United States', country_value) # Fix typo
    country_value = re.sub(r'^\bAmerican\b$', 'United States', country_value) # Fix typo
    country_value = re.sub(r'Pittsburgh', 'United States', country_value) # Fix typo
    country_value = re.sub(r'United Statesi', 'United States', country_value) # Fix typo
    country_value = re.sub(r'United States04', 'United States', country_value) # Fix typo
    country_value = re.sub(r'Phoenix Arizona', 'United States', country_value) # Fix typo
    country_value = re.sub(r'^\bUSA\b$', 'United States', country_value) # Fix typo
    country_value = re.sub(r'^\bU.S.\b$', 'United States', country_value) # Fix typo
    country_value = re.sub(r'^\bU.S.A.\b$', 'United States', country_value) # Fix typo
    country_value = re.sub(r'^\bUK\b$', 'United Kingdom', country_value) # Fix typo
    country_value = re.sub(r'^\bU.K.\b$', 'United Kingdom', country_value) # Fix typo
    country_value = re.sub(r'Great Britain', 'United Kingdom', country_value) # Fix typo
    country_value = re.sub(r'British Hong Kong', 'United Kingdom , China', country_value)  # Only replace if it is the only word
    country_value = re.sub(r'British India', 'United Kingdom , India', country_value)  # Only replace if it is the only word
    country_value = re.sub(r'British Raj', 'United Kingdom , India', country_value)  # Only replace if it is the only word
    country_value = re.sub(r'Made on location in England and Scotland', 'United Kingdom', country_value) # Fix typo
    country_value = re.sub(r'^\bBritain\b$', 'United Kingdom', country_value) # Fix typo
    country_value = re.sub(r'English', 'United States', country_value) # Fix typo
    country_value = re.sub(r'East Germany', 'Germany', country_value) # Fix typo
    country_value = re.sub(r'West Germany', 'Germany', country_value) # Fix typo
    country_value = re.sub(r'Nazi Germany', 'Germany', country_value) # Fix typo
    country_value = re.sub(r'German Democratic Republic', 'Germany', country_value) # Fix typo
    country_value = re.sub(r'German Empire', 'Germany', country_value) # Fix typo
    country_value = re.sub(r'Hong Kong Stock Exchange', 'China', country_value) # Fix typo
    country_value = re.sub(r'Hong Kong action cinema', 'China', country_value) # Fix typo
    country_value = re.sub(r'Mainland China', 'China', country_value) # Fix typo
    country_value = re.sub(r'Hong Kong people', 'China', country_value) # Fix typo
    country_value = re.sub(r"People's Republic of China", 'China', country_value) # Fix typo
    country_value = re.sub(r'Hong Kong S.A.R.', 'China', country_value) # Fix typo
    country_value = re.sub(r'Hong Kong', 'China', country_value) # Fix typo
    country_value = re.sub(r'^\bAustro\b$', 'Austria Hungary', country_value) # Fix typo
    country_value = re.sub(r'Hungarian Empire', 'Hungary', country_value) # Fix typo
    country_value = re.sub(r'^\blagos Nigeria\b$', 'Nigeria', country_value) # Fix typo
    country_value = re.sub(r'^\bGeorgia\b$', 'Georgia (country)', country_value) # Fix typo
    country_value = re.sub(r'^\bCzechia\b$', 'Czech Republic', country_value) # Fix typo
    country_value = re.sub(r'^\bCongo\b$', 'Republic of the Congo', country_value) # Fix typo
    country_value = re.sub(r'^\bFrench\b$', 'France', country_value) # Fix typo
    country_value = re.sub(r'^\bSwedish\b$', 'sweden', country_value) # Fix typo
    country_value = re.sub(r'^\bIndian\b$', 'India', country_value) # Fix typo
    country_value = re.sub(r'India cricket team', 'India', country_value) # Fix typo
    country_value = re.sub(r'India national cricket team', 'India', country_value) # Fix typo
    country_value = re.sub(r'Indian cinema', 'India', country_value) # Fix typo
    country_value = re.sub(r'Irish Free State', 'Ireland', country_value) # Fix typo
    country_value = re.sub(r'Lithuanian SSR', 'Lithuania', country_value) # Fix typo
    country_value = re.sub(r'INDIA', 'India', country_value) # Fix typo
    country_value = re.sub(r'Imperial Russia', 'Russia', country_value) # Fix typo
    country_value = re.sub(r'^\bDominion of India\b$', 'India', country_value) # Fix typo
    country_value = re.sub(r'^\bJapanese\b$', 'Japan', country_value) # Fix typo
    country_value = re.sub(r'^\bFRANCE\b$', 'France', country_value) # Fix typo
    country_value = re.sub(r'French Third Republic', 'France', country_value) # Fix typo
    country_value = re.sub(r'^\bEmpire of Japan\b$', 'Japan', country_value) # Fix typo
    country_value = re.sub(r'^\bLuxembourgh\b$', 'Luxembourg', country_value) # Fix typo
    country_value = re.sub(r'CanadaChina', 'Canada , China', country_value) # Fix typo
    country_value = re.sub(r'Dutch_East_Indies', 'Netherlands', country_value) # Fix typo
    country_value = re.sub(r'United States. France & German', 'United States, France, Germany', country_value) # Fix typo
    country_value = re.sub(r'United Kingdom Germany', 'United Kingdom, Germany', country_value) # Fix typo
    country_value = re.sub(r'Türk', 'Turkey', country_value) # Fix typo
    country_value = re.sub(r'Turkeyiye', 'Turkey', country_value) # Fix typo
    country_value = re.sub(r'British China', 'United Kingdom , China', country_value) # Fix typo
    country_value = re.sub(r'Palestine', 'Israel', country_value) # Fix typo
    country_value = re.sub(r'FranceUnited Kingdom', 'France, United Kingdom', country_value) # Fix typo
    country_value = re.sub(r'^\bMacedonia\b$', 'North Macedonia', country_value)  # Only replace if it is the only word
    country_value = re.sub(r'Cinema of', '', country_value)  # Only replace if it is the only word
    country_value = re.sub(r'Bosnia and Herzegovina', 'Bosnia , Herzegovina', country_value)  # Only replace if it is the only word
    country_value = re.sub(r'Canada Arts Council', 'Canada', country_value)  # Only replace if it is the only word
    country_value = re.sub(r'History of Australia (1851\–1900)', 'Australia', country_value)  # Only replace if it is the only word
    country_value = re.sub(r'Nepali language', 'Nepal', country_value)  # Only replace if it is the only word
    country_value = re.sub(r'PAKISTAN', 'Pakistan', country_value)  # Only replace if it is the only word
    country_value = re.sub(r'Philippine cinema', 'Philippines', country_value)  # Only replace if it is the only word
    country_value = re.sub(r"Polish People's Republic", 'Poland', country_value)  # Only replace if it is the only word
    country_value = country_value.strip()  # Remove extra spaces
    return country_value

# Function to resolve country literals or URIs to DBpedia URIs and labels
def resolve_country_uri(country_literal_or_uri):
    """Resolve a country literal, URI, or a comma-separated list to its corresponding DBpedia URIs and English labels."""
    DBPEDIA_SPARQL_ENDPOINT = "http://dbpedia.org/sparql"
    headers = {"Accept": "application/sparql-results+json"}
    if country_literal_or_uri.startswith("http://") or country_literal_or_uri.startswith("https://"):
        countries = [country_literal_or_uri]
    else:
        countries = [clean_country_value(c.strip()) for c in re.split(r'[;,&\-\n/]', clean_country_value(str(country_literal_or_uri)))] 
    
    resolved_countries = []

    for country in countries:
        if country in ['', 'N/A', 'R.O.C.', 'among many other locations', 'Worldwide', 'Ibadan', 'Oyo state', 'Stuntman',
                       'Dustin DeMont', 'Terrebonne', '87.0', 'Participants in World War II']:
            continue
        
        if country.startswith("http://") or country.startswith("https://"):
            # If input is already a URI, fetch the English label
            query = f"""
            SELECT ?label WHERE {{
                <{country}> rdfs:label ?label .
                FILTER (lang(?label) = 'en')
            }}
            """
            uri = country
        else:
            # If input is a literal, resolve it to a DBpedia resource and fetch the URI and label
            query = f"""
                SELECT ?country ?label WHERE {{
                    ?country a dbo:Country ;
                            rdfs:label ?label .
                    FILTER (lang(?label) = 'en' && 
                            LCASE(STR(?label)) = LCASE("{country}"))
                }}
            """
            uri = None

        try:
            response = requests.get(DBPEDIA_SPARQL_ENDPOINT, headers=headers, params={"query": query})
            response.raise_for_status()
            results = response.json().get("results", {}).get("bindings", [])

            if results:
                # Fetch the first match
                uri = results[0].get("country", {}).get("value", uri) or uri
                label = results[0].get("label", {}).get("value", "Unknown")
                resolved_countries.append((URIRef(uri), label))
            else:
                print(f"Country '{country}' not found in DBpedia.")
        except Exception as e:
            print(f"Error resolving country '{country}': {e}")
            resolved_countries.append((Literal(country), None))

    return resolved_countries

def fetch_all_unique_countries(csv_file):
    """Fetch all unique country values from the CSV file."""
    df = pd.read_csv(csv_file)
    unique_countries = df['country'].dropna().unique()
    return unique_countries

def resolve_all_countries(unique_countries):
    """Resolve all unique country values to their corresponding DBpedia URIs and labels."""
    resolved_countries = {}
    for country in unique_countries:
        resolved_countries[country] = resolve_country_uri(country)
    return resolved_countries

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
    RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
    PROV = Namespace("http://www.w3.org/ns/prov#")

    # Create a new RDF graph
    g = Graph()
    g.bind("dbr", DBR)
    g.bind("dbo", DBO)
    g.bind("dct", DCT)

    processed_count = 0  # Count rows processed
    skipped_count = 0  # Count rows skipped
    movie_count = 0  # Count unique movies added to the graph

    # Fetch and resolve all unique country values
    unique_countries = fetch_all_unique_countries(csv_file)
    resolved_countries_dict = resolve_all_countries(unique_countries)
    
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
                'genres' :(DBO.genre, DBO.Genre, RDF.Property),
                'actors': (DBO.starring, DBO.Actor, DBO.Person),
                'directors': (DBO.director, DBO.Director, DBO.Person),
                'distributors': (DBO.distributor, DBO.Distributor, DBO.Person),
                'writer': (DBO.writer, DBO.Writer, DBO.Person),
                'producers': (DBO.producer, DBO.Producer, DBO.Person),
                'composers': (DBO.composer, DBO.Composer, DBO.Person),
                'cinematographers': (DBO.cinematographer, DBO.Cinematographer, DBO.Person),
                'productionCompanies': (DBO.productionCompany, DBO.productionCompany, RDF.Property),
                'wasDerivedFrom': PROV.wasDerivedFrom,
                'series': DBO.series,
            }

            # Handle literal date attributes
            release_date = row.get('releaseDate')
            if release_date is not None and release_date != '' and release_date != 'N/A':
                release_year = release_date.split("-")[0]  # Extract the year
                g.add((movie_uri, DBO.releaseYear, Literal(release_year, datatype=XSD.gYear)))

            # Handle country attribute
            country = row.get('country')
            if country is not None and country != '' and country != 'N/A':
                resolved_countries = resolved_countries_dict.get(country, [])
                if resolved_countries:
                    for country_uri, country_label in resolved_countries:
                        g.add((movie_uri, DBO.country, URIRef(country_uri)))
                        g.add((URIRef(country_uri), RDF.type, DBO.Country))

                        if country_label is not None:                    
                            g.add((URIRef(country_uri), RDFS.label, Literal(country_label, lang="en")))
                else:
                    g.add((movie_uri, DBO.country, Literal(country, lang="en")))

            # Handle literal string attributes
            for attr, predicate in str_attributes.items():
                value = row.get(attr)
                if value is not None and value != '' and value.strip() != 'N/A':
                    for val in value.split('; '):
                        g.add((movie_uri, predicate, Literal(val.strip(), lang="en")))
            
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

                        if isinstance(predicate, tuple):
                            tuple_tmp = predicate
                            predicate = tuple_tmp[0] 
                            classType = tuple_tmp[1]
                            type = tuple_tmp[2]
                            g.add((object_uri, RDF.type, type))
                            g.add((object_uri, RDF.type, classType))
                            
                        g.add((movie_uri, predicate, object_uri))                        
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
csv_file = f"{folder_path}/CSVs/dbpedia_movies_2024_12_15_12_10_36.csv"  # Adjust the path to your CSV file
rdf_file = f"{folder_path}/TTLs/dbpedia_movies.ttl"  # Path to save the Turtle file

# Convert CSV to RDF
csv_to_rdf(csv_file, rdf_file)
import csv
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD
import urllib.parse
import requests
import re
import pandas as pd
import time
import os
import pickle


# Define namespaces
DBR = Namespace("http://dbpedia.org/resource/")
DBO = Namespace("http://dbpedia.org/ontology/")
DCT = Namespace("http://purl.org/dc/terms/")
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
PROV = Namespace("http://www.w3.org/ns/prov#")

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
    country_value = re.sub(r'Umited States', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'United Satates', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'United States United States', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'United Statyes', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Pennsylvania', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'^\bAmerican\b$', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Pittsburgh', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'United Statesi', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'United States04', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Phoenix Arizona', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'^\bUSA\b$', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'^\bU\.S\.\b$', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'^\bU\.S\.A\.\b$', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'^\bUK\b$', 'United Kingdom', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'^\bU\.K\.\b$', 'United Kingdom', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Great Britain', 'United Kingdom', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'British Hong Kong', 'United Kingdom , China', country_value, flags=re.IGNORECASE)  # Only replace if it is the only word
    country_value = re.sub(r'British India', 'United Kingdom , India', country_value, flags=re.IGNORECASE)  # Only replace if it is the only word
    country_value = re.sub(r'British Raj', 'United Kingdom , India', country_value, flags=re.IGNORECASE)  # Only replace if it is the only word
    country_value = re.sub(r'Made on location in England and Scotland', 'United Kingdom', country_value) # Fix typo
    country_value = re.sub(r'^\bBritain\b$', 'United Kingdom', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'English', 'United States', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'East Germany', 'Germany', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'West Germany', 'Germany', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Nazi Germany', 'Germany', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'German Democratic Republic', 'Germany', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'German Empire', 'Germany', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Hong Kong Stock Exchange', 'China', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Hong Kong action cinema', 'China', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Mainland China', 'China', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Hong Kong people', 'China', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r"People's Republic of China", 'China', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Hong Kong S.A.R.', 'China', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Hong Kong', 'China', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'^\bAustro\b$', 'Austria Hungary', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Hungarian Empire', 'Hungary', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'^\blagos Nigeria\b$', 'Nigeria', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'^\bGeorgia\b$', 'Georgia (country)', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'^\bCzechia\b$', 'Czech Republic', country_value) # Fix typo
    country_value = re.sub(r'^\bCongo\b$', 'Republic of the Congo', country_value) # Fix typo
    country_value = re.sub(r'^\bFrench\b$', 'France', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'^\bSwedish\b$', 'sweden', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'^\bIndian\b$', 'India', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'India cricket team', 'India', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'India national cricket team', 'India', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Indian cinema', 'India', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Irish Free State', 'Ireland', country_value) # Fix typo
    country_value = re.sub(r'Lithuanian SSR', 'Lithuania', country_value) # Fix typo
    country_value = re.sub(r'INDIA', 'India', country_value, flags=re.IGNORECASE) # Fix typo
    country_value = re.sub(r'Imperial Russia', 'Russia', country_value, flags=re.IGNORECASE) # Fix typo
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
            time.sleep(1) # Delay to avoid overloading the DBpedia endpoint

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

def resolve_all_countries(unique_countries):
    """Resolve all unique country values to their corresponding DBpedia URIs and labels."""
    resolved_countries = {}
    output_pickle_path = "DB/Datasets/CSVs/countries_dic.pkl"

    # Check if the pickle file exists
    if os.path.exists(output_pickle_path):
        with open(output_pickle_path, mode='rb') as pickle_file:
            resolved_countries = pickle.load(pickle_file)
        return resolved_countries
    else:
        print(f"File {output_pickle_path} does not exist. Resolving countries...")

    # Resolve countries if the pickle file doesn't exist
    for country in unique_countries:
        resolved_countries[country] = resolve_country_uri(country)

    # Export the resolved_countries dictionary to a pickle file
    with open(output_pickle_path, mode='wb') as pickle_file:
        pickle.dump(resolved_countries, pickle_file)

    return resolved_countries

def clean_genre_value(genre_value):
    """Clean genre values to remove trailing slashes, excess spaces, and incorrect formatting."""
    genre_value = re.sub(r'/$', '', genre_value)  # Remove trailing slashes
    genre_value = re.sub(r"film", '', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"\(genre\)", '', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Comedy \(drama\)", 'Comedy, Drama', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r'\(.*?\)', '', genre_value)
    genre_value = re.sub(r"Syfy", 'Science Fiction', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Sci-Fi", 'Science Fiction', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Docufiction", 'Documentary, Fiction,', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Docudrama", 'Documentary Drama', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Dramedy", 'Drama, Comedy', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Comedy-drama", 'Comedy, Drama', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Satire \( and television\)", 'Satire', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Action \(fiction\)", 'Action, Fiction', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"List of reality television programs", 'Reality-TV', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Reality TV", 'Reality-TV', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Reality TV", 'Reality-TV', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Crime thriller", 'Crime, Thriller', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Comedy thriller", 'Comedy, Thriller', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Comedy drama", 'Crime, Drama', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r"Comedy horror", 'Crime, Horror', genre_value, flags=re.IGNORECASE)  # Only replace if it is the only word    
    genre_value = re.sub(r'\bgenre\b', '', genre_value)
    genre_value = re.sub(r'\bgenre\b', '', genre_value)


    genre_value = genre_value.strip()  # Remove extra spaces
    genre_value = genre_value.capitalize()  # Capitalize the first letter
    return genre_value

def resolve_genre(genre_literal):
    """Resolve a genre literal."""
    genres = [clean_genre_value(c.strip()) for c in re.split(r'[;,&\n/]', clean_genre_value(str(genre_literal)))] 
    return genres
    # return [Literal(genre) for genre in genres]

def resolve_all_genres(unique_genres):
    """ Function to resolve genre literals to DBpedia URIs and labels """
    resolved_genres = {}
    output_pickle_path = "DB/Datasets/CSVs/genres_dic.pkl"

    # Check if the pickle file exists
    if os.path.exists(output_pickle_path):
        with open(output_pickle_path, mode='rb') as pickle_file:
            resolved_genres = pickle.load(pickle_file)
        return resolved_genres
    else:
        print(f"File {output_pickle_path} does not exist. Resolving genres...")

    # Resolve genres if the pickle file doesn't exist
    for genre in unique_genres:
        resolved_genres[genre] = resolve_genre(genre)

    # Export the resolved_genres dictionary to a pickle file
    with open(output_pickle_path, mode='wb') as pickle_file:
        pickle.dump(resolved_genres, pickle_file)

    return resolved_genres



def preprocess_genres(g, resolved_genres_dict):
    # Define the super genres and their sub-genres
    super_genres = {
        "Comedy": ["Romantic Comedy", "Dark Comedy", "Slapstick", "Satire", "Sitcom", "Mockumentary", "Surreal humour",
                "Black Comedy", "Parody", "Comedy of manners", "Comedy Thriller", "Comedy-drama", "Comedy horror",
                "Toilet humour",
                "Comedy of errors", "Comedy of humours", "Comedy of intrigue", "Comedy of situation"],
        "Drama": ["Romance", "Historical Drama", "Crime Drama", "Legal Drama", "Melodrama", "Thriller", "Erotica",
                "Psychological thriller", "Political Drama", "Family Drama", "Teen Drama", "Biographical", "Biopic",
                "Political", "Psychological", "Romantic", "LGBT",
                "Anthology", "Soap opera", "Telenovela", "Serial (radio and television)", "Suspense", "Mystery",
                "Crime thriller", "Political thriller", "Detective fiction", "Historical fiction", "Tragedy"],
        "Action": ["Martial Arts", "Superhero", "Spy", "Adventure", "Adventure fiction", "Disaster", "War", "Western",
                    "Adventure", "Crime", "Crime fiction", "Crime thriller", "Police procedural", "Military", "Heist", "Survival"],
        "Animation": ["Anime", "Stop Motion", "Computer Animation", "Clay Animation", "Animated series", "Animated cartoon"],
        "Horror": ["Slasher", "Supernatural Horror", "Psychological Horror", "Monster", "Gothic Horror", "Body Horror",
                "Zombie", "Vampire", "Werewolf", "Occult", "Paranormal", "Haunted House"],
        "Science Fiction": ["Superhero fiction", "Supernatural", "Fantasy", "Cyberpunk", "Space Opera", "Time Travel",
                            "Time-travel",
                            "Dystopian", "Alternate History", "Science fantasy", "Apocalyptic and post-apocalyptic fiction",
                            "Steampunk", "Speculative fiction", "Alien invasion", "Space exploration", "Utopian"],
        "Documentary": ["Biography", "Biographical Documentary", "Nature Documentary", "Travel Documentary", "Music Documentary",
                        "History",
                        "Political Documentary", "Social Documentary", "Science Documentary", "History Documentary",
                        "First-person shooter", "History", "Science", "Mental health", "Biblical", "Infotainment",
                        "Sports Documentary", "War Documentary", "True Crime", "Docufiction", "Mockumentary", "Docudrama"],
        "Musical": ["Musical theatre", "Musical film", "Music", "Opera", "Rock opera", "Concert film", "Dance film"],
        "Fantasy": ["Urban fantasy", "Dark fantasy", "High fantasy", "Sword and sorcery", "Fairy tale", "Mythopoeia",
                    "Time-travel", "Dystopian fiction",
                    "Magical realism", "Paranormal", "Folklore", "Wuxia", "Supernatural fiction", "Epic fantasy"],
        "Romance": ["Romantic Comedy", "Romantic Drama", "Chick flick", "Romantic thriller", "Romantic fantasy"],
        "Sports": ["Sports film", "Sports drama", "Sports comedy", "Sports documentary", "Martial arts", "Professional wrestling"],
        "Crime": ["Crime Drama", "Crime Thriller", "Police Procedural", "Heist", "Gangster", "Detective fiction", "Noir",
                "Mystery fiction", "Legal drama", "Courtroom drama"],
        "War": ["War film", "Anti-war film", "Military", "Historical war", "War drama", "War documentary"],
        "Music": ["Music", "Musical film", "Concert film", "Music documentary", "Music video", "Music biopic", "Funk metal", "Progressive rock",
                    "Industrial hip hop", "Improvisational", "Eurodance", "Eurohouse",
                "Rockumentary", "Music of Bollywood", "Music of Bollywood", "Music of Bollywood", "Music of Bollywood", "Music of Bollywood", "Concert film"],
        "Western": ["Spaghetti Western", "Revisionist Western", "Contemporary Western", "Acid Western", "Space Western"],
        "Reality-TV": ["Reality television", "Reality show", "Chat show", "Reality Television", "Adventure game",
                     "Reality competition", "Reality game show", "Reality legal show", "Reality medical show", "Game show"],
        "Other": []  # This will be used for genres not listed above
    }

    for super_genre in super_genres:
        super_genre_uri = URIRef(clean_uri(f"{DBR}{super_genre.replace(' ', '_')}"))
        g.add((super_genre_uri, RDF.type, DBO.Genre))
        g.add((super_genre_uri, RDFS.label, Literal(super_genre.capitalize(), lang="en")))

        for sub_genre in super_genres[super_genre]:
            sub_genre_uri = URIRef(clean_uri(f"{DBR}{sub_genre.replace(' ', '_')}"))
            g.add((sub_genre_uri, RDF.type, DBO.Genre))
            g.add((sub_genre_uri, RDFS.subClassOf, super_genre_uri))
            g.add((sub_genre_uri, RDFS.label, Literal(sub_genre.capitalize(), lang="en")))

    for genre in resolved_genres_dict:
        resolved_genres = resolved_genres_dict.get(genre, [])
        for genre_literal in resolved_genres:
                # Determine the super genres
                super_genres_for_genre = set()
                for super_genre, sub_genres in super_genres.items():
                    if genre_literal in sub_genres or re.search(super_genre, genre_literal, re.IGNORECASE) \
                        or re.search(genre_literal, super_genre, re.IGNORECASE):
                            super_genres_for_genre.add(super_genre)

                # If no super genre is found, classify as "Other"
                if not super_genres_for_genre:
                    print(f"No super genre found for '{genre_literal}', classifying as 'Other'")
                    super_genres_for_genre.add("Other")


                # Add the super genre triples
                for super_genre in super_genres_for_genre:
                    super_genre_uri = URIRef(clean_uri(f"{DBR}{super_genre.replace(' ', '_')}"))

                    # If the genre is not already a super genre, add the sub-genre relationship
                    if genre_literal not in super_genres:
                        genre_uri = URIRef(clean_uri(f"{DBR}{genre_literal.replace(' ', '_')}"))
                        g.add((genre_uri, RDF.type, DBO.Genre))
                        g.add((genre_uri, RDFS.label, Literal(genre_literal.capitalize(), lang="en")))
                        g.add((genre_uri, RDFS.subClassOf, super_genre_uri))
        
    return g
def csv_to_rdf(csv_file, rdf_file):
    """
    Convert a CSV file to RDF (Turtle format).
    
    Parameters:
        csv_file (str): Path to the input CSV file.
        rdf_file (str): Path to save the output Turtle file.
    """

    # Create a new RDF graph
    g = Graph()
    g.bind("dbr", DBR)
    g.bind("dbo", DBO)
    g.bind("dct", DCT)

    processed_count = 0  # Count rows processed
    skipped_count = 0  # Count rows skipped
    movie_count = 0  # Count unique movies added to the graph

    df = pd.read_csv(csv_file, encoding='utf-8')
    print("Resolving unique countries...")
    unique_countries = df['country'].dropna().unique()
    resolved_countries_dict = resolve_all_countries(unique_countries)
    # resolved_countries_dict = dict()

    # Fetch and resolve all unique genre values
    print("Resolving unique genres...")
    unique_genres = df['genres'].dropna().unique()
    resolved_genres_dict = resolve_all_genres(unique_genres)
    g = preprocess_genres(g, resolved_genres_dict)
    
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
                # 'genres' :(DBO.genre, DBO.Genre, RDF.Property),
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

            # Handle genre attribute
            genres = row.get('genres')
            if genres is not None and genres != '' and genres != 'N/A':
                resolved_genres = resolved_genres_dict.get(genres, [])
                if resolved_genres:
                    for genre in resolved_genres:
                        genre_uri = URIRef(clean_uri(f"{DBR}{genre.replace(' ', '_')}"))
                        g.add((movie_uri, DBO.genre, genre_uri))
                else:
                    g.add((movie_uri, DBO.genre, Literal(genres, lang="en")))

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
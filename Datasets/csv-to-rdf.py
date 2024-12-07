import csv
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD

def csv_to_rdf(csv_file, rdf_file):
    # Define Wikidata namespaces
    WD = Namespace("http://www.wikidata.org/entity/")
    WDT = Namespace("http://www.wikidata.org/prop/direct/")
    RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

    # Create a new RDF graph
    g = Graph()
    g.bind("wd", WD)
    g.bind("wdt", WDT)
    g.bind("rdfs", RDFS)

    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            movie_uri = row['movie_uri']
            if not movie_uri:  # Skip rows without a movie URI
                continue
            
            subject = URIRef(movie_uri)
            
            # Add type of entity (movie)
            g.add((subject, RDF.type, WD["Q11424"]))  # Q11424 is the entity for "film"
            
            # Add movie name
            if row['movie']:
                g.add((subject, RDFS.label, Literal(row['movie'], datatype=XSD.string)))
            
            # Add release date
            if row['releaseDate']:
                g.add((subject, WDT["P577"], Literal(row['releaseDate'], datatype=XSD.dateTime)))
            
            # Add language
            if row['language']:
                g.add((subject, WDT["P364"], Literal(row['language'], datatype=XSD.string)))
            
            # Add genre
            if row['genres']:
                genres = row['genres'].split(', ')  # Assuming genres are comma-separated
                for genre in genres:
                    g.add((subject, WDT["P136"], Literal(genre, datatype=XSD.string)))

            # Add actors
            if row['actors']:
                actors = row['actors'].split(', ')  # Assuming URIs are comma-separated
                for actor in actors:
                    if actor:
                        g.add((subject, WDT["P161"], Literal(actor, datatype=XSD.string)))
            
            # Add directors
            if row['directors']:
                directors = row['directors'].split(', ')  # Assuming URIs are comma-separated
                for director in directors:
                    if director:
                        g.add((subject, WDT["P57"], Literal(director, datatype=XSD.string)))

    # Serialize the graph to RDF
    g.serialize(destination=rdf_file, format='turtle')
    print(f"RDF saved to {rdf_file}")

# Example usage
csv_to_rdf(
    csv_file="/Users/kostas/Documents/Data Science/p2/knowledge/project/english_movies_2024_2024_detailed.csv",
    rdf_file="/Users/kostas/Documents/Data Science/p2/knowledge/project/english_movies_2024_2024_detailed.ttl"
)

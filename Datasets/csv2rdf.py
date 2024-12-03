import pandas as pd  # For handling CSV and CSV contents
from rdflib import Graph, Literal, RDF, URIRef, Namespace  # Basic RDF handling
from rdflib.namespace import FOAF, XSD  # Most common namespaces
import urllib.parse  # For parsing strings to URI's

# Load the CSV file
url = 'https://raw.githubusercontent.com/KRontheWeb/csv2rdf-tutorial/master/example.csv'
df = pd.read_csv(url, sep=";", quotechar='"')

# Clean the DataFrame (fill NaN values with empty strings)
df = df.fillna('')

# Initialize the RDF graph
g = Graph()

# Define Namespaces
ppl = Namespace('http://example.org/people/')
loc = Namespace('http://mylocations.org/addresses/')
schema = Namespace('http://schema.org/')

# Bind Namespaces to the Graph
g.bind('foaf', FOAF)
g.bind('ppl', ppl)
g.bind('loc', loc)
g.bind('schema', schema)

# Iterate through the CSV and populate the RDF graph
for index, row in df.iterrows():
    person_uri = URIRef(ppl + urllib.parse.quote(row['Name']))
    address_uri = URIRef(loc + urllib.parse.quote(row['Address']))

    # Add triples for the person
    g.add((person_uri, RDF.type, FOAF.Person))
    g.add((person_uri, URIRef(schema + 'name'), Literal(row['Name'], datatype=XSD.string)))
    g.add((person_uri, FOAF.age, Literal(int(row['Age']), datatype=XSD.integer)))
    g.add((person_uri, URIRef(schema + 'address'), address_uri))

    # Add triples for the address
    g.add((address_uri, RDF.type, URIRef(schema + 'Place')))
    g.add((address_uri, URIRef(schema + 'name'), Literal(row['Address'], datatype=XSD.string)))

# Serialize the graph into Turtle format and print
output_file = "output_data.ttl"  # Specify the file name
g.serialize(destination=output_file, format='turtle')

import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import urllib.error

# SPARQL endpoint setup
GRAPHDB_ENDPOINT = "http://localhost:7200/repositories/MoviesRepo"
sparql = SPARQLWrapper(GRAPHDB_ENDPOINT)

# Function to execute a SPARQL query and return the results
def execute_sparql_query(query):
    try:
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        return results['results']['bindings']
    except urllib.error.URLError as e:
        print(f"Failed to reach the server. Reason: {e.reason}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

# Query to count the number of triples
query_num_triples = """
SELECT (COUNT(*) AS ?num_triples)
WHERE {
    ?subject ?predicate ?object .
}
"""

# Query to count the number of unique subjects
query_num_subjects = """
SELECT (COUNT(DISTINCT ?subject) AS ?num_subjects)
WHERE {
    ?subject ?predicate ?object .
}
"""

# Query to count the number of unique predicates
query_num_predicates = """
SELECT (COUNT(DISTINCT ?predicate) AS ?num_predicates)
WHERE {
    ?subject ?predicate ?object .
}
"""

# Query to count the number of unique objects
query_num_objects = """
SELECT (COUNT(DISTINCT ?object) AS ?num_objects)
WHERE {
    ?subject ?predicate ?object .
}
"""

# Query to count the number of unique films
query_num_films = """
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT (COUNT(DISTINCT ?film) AS ?num_films)
WHERE {
    ?film rdf:type dbo:Film .
}
"""

# Query to count the unique counts for each predicate and fetch descriptions
query_unique_counts_per_predicate = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?predicate (COUNT(DISTINCT ?subject) AS ?num_subjects) (COUNT(DISTINCT ?object) AS ?num_objects)
WHERE {
    ?subject ?predicate ?object .
}
GROUP BY ?predicate
"""

# Execute the queries and fetch the results
num_triples = int(execute_sparql_query(query_num_triples)[0]['num_triples']['value'])
num_subjects = int(execute_sparql_query(query_num_subjects)[0]['num_subjects']['value'])
num_predicates = int(execute_sparql_query(query_num_predicates)[0]['num_predicates']['value'])
num_objects = int(execute_sparql_query(query_num_objects)[0]['num_objects']['value'])
num_films = int(execute_sparql_query(query_num_films)[0]['num_films']['value'])
unique_counts_per_predicate_results = execute_sparql_query(query_unique_counts_per_predicate)

# Convert the unique counts per predicate results to a DataFrame
unique_counts_per_predicate = pd.DataFrame([
    {
        'predicate': result['predicate']['value'],
        'num_subjects': int(result['num_subjects']['value']),
        'num_objects': int(result['num_objects']['value'])
    }
    for result in unique_counts_per_predicate_results
])

# Print dataset statistics
print(f"Number of triples: {num_triples}")
print(f"Number of subjects: {num_subjects}")
print(f"Number of predicates: {num_predicates}")
print(f"Number of objects: {num_objects}")
print(f"Number of films: {num_films}")

# Add the statistics to a DataFrame
statistics_df = pd.DataFrame({
    'Statistic': ['Number of triples', 'Number of subjects', 'Number of predicates', 'Number of objects', 'Number of films'],
    'Value': [num_triples, num_subjects, num_predicates, num_objects, num_films]
})

# Save the statistics and unique counts to CSV files
statistics_df.to_csv('dataset_statistics.csv', index=False)
unique_counts_per_predicate.to_csv('unique_counts_per_predicate.csv', index=False)

# Print unique counts for each predicate
print("\nUnique counts for each predicate:")
print(unique_counts_per_predicate)

# Generate LaTeX table for dataset statistics
statistics_latex = statistics_df.to_latex(index=False, caption='Dataset Statistics', label='tab:dataset_statistics')

# Generate LaTeX table for unique counts per predicate
unique_counts_latex = unique_counts_per_predicate.to_latex(index=False, caption='Unique Counts per Predicate', label='tab:unique_counts_per_predicate')

# Save the LaTeX tables to .tex files
with open('dataset_statistics.tex', 'w') as f:
    f.write(statistics_latex)

with open('unique_counts_per_predicate.tex', 'w') as f:
    f.write(unique_counts_latex)

print("LaTeX tables generated and saved to 'dataset_statistics.tex' and 'unique_counts_per_predicate.tex'")
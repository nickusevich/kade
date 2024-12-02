from SPARQLWrapper import SPARQLWrapper, JSON
import csv

def fetch_data_from_wikidata():
    # Set up the SPARQL endpoint
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.setQuery("""
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?movie ?movieLabel ?directorLabel ?releaseDate
    WHERE {
    ?movie wdt:P31 wd:Q11424;       
            wdt:P57 ?director;      
            wdt:P577 ?releaseDate.  
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    LIMIT 100
    """)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    # Extract results
    movies_data = []
    for result in results["results"]["bindings"]:
        movie = result['movieLabel']['value']
        director = result['directorLabel']['value']
        release_date = result['releaseDate']['value']
        movies_data.append((movie, director, release_date))
    
    return movies_data

def save_to_csv(data, filename="movies_data.csv"):
    # Save data to a CSV file
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Movie", "Director", "Release Date"])  # Write headers
        writer.writerows(data)
    print(f"Data successfully saved to {filename}")

if __name__ == "__main__":
    print("Fetching data from Wikidata...")
    movies_data = fetch_data_from_wikidata()
    
    if movies_data:
        print(f"Fetched {len(movies_data)} records.")
        save_to_csv(movies_data)
    else:
        print("No data found.")

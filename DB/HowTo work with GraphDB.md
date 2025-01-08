## GraphDB

## How to run the GraphDB?
1. Install docker on your local computer
2. Open CMD and CD to the current folder
3. Run the following command:
    docker-compose -f graphdb.yaml up -d


## How to access the GraphDB?
You can access it through the following URL:
    http://localhost:7200/graphs


## How to put down the DB?
docker-compose -f graphdb.yaml down


## How to uplaod and play around with DB

1. Login to the your local GraphDB instance (running on Docker) via the following link:
http://localhost:7200/

![GraphDB First Screen](<Screenshots/GraphDB first screen.png>)

### After accessing the DB a repository should be created
1. Navigation side bar -> Setup -> Create new repository
![alt text](<Screenshots/Create repository.png>)
2. Select the opton of GraphDB repository (the first one from the left)
![alt text](<Screenshots/Create repository 2.png>)
3. Repository ID (any name you want)
![alt text](<Screenshots/Create repository 3.png>)
4. Leave all othe detailes as deafult and press Create button on the bottm right of the screen
![alt text](<Screenshots/Create repository 4.png>)
5. Now you will see the repository and will be able to select it on the combobox in the upper right corner of the screen
![alt text](<Screenshots/Create repository 5.png>)

### Importing files
1. Navigation side bar -> Import -> Upload RDF files
![alt text](<Screenshots/import file 1.png>)
2. Select TTL File (e.g. DB/Datasets/TTLs/dbpedia_movies.ttl)
Leave default values as is
![alt text](<Screenshots/import file 2.png>)
3. Press the Import button in the dialog and then you will see the data been loaded
![alt text](<Screenshots/import file 3.png>)


### Exploration 
1. Navigation side bar -> Explore -> Graph overview
2. the default graph (it's the same name every time and updated every time the imported file changes-no need for delete)
![alt text](<Screenshots/Exploration 1.png>)

3. Depending the situation you can go through subject-predicate-object-content to look data in detail
4. After selecting one you can see also the visualization through visual graph up right


### Sparql
1. First we have to turn on autocomplete
2. Navigation side bar -> Setup -> Autocomplete -> on
3. Navigation side bar -> Sparql to write queries-possibility of saving for future use


### Support
1. Through help you can find documentation and step-by-step tutorial
2. Except for that Interactive Guides have 2 examples that demonstrate all that mentioned step-by-step


### Example SPARQL queries

1. **Retrieve all movies and their abstracts:**
```sparql
SELECT ?movie ?abstract
WHERE {
  ?movie a <http://dbpedia.org/ontology/Film> ;
         <http://dbpedia.org/ontology/abstract> ?abstract .
}
```

2. **Retrieve all movies directed by a specific director (e.g., "Conrad Vernon"):**
```sparql
SELECT ?movie
WHERE {
  ?movie a <http://dbpedia.org/ontology/Film> ;
         <http://dbpedia.org/ontology/director> <http://dbpedia.org/resource/Conrad_Vernon> .
}
```

3. **Retrieve movies and their genres (exclude N/A):**
```sparql
SELECT ?movie ?genre
WHERE {
  ?movie a <http://dbpedia.org/ontology/Film> ;
         <http://dbpedia.org/ontology/genre> ?genre .
  FILTER (?genre != <http://dbpedia.org/resource/N/A>)
} limit 500
```

4. **Retrieve movies released in a specific year (e.g., 2001):**
```sparql
PREFIX dbo: <http://dbpedia.org/ontology/>

SELECT ?movie ?releaseYear
WHERE {
  ?movie a dbo:Film .
  ?movie dbo:releaseYear ?releaseYear .
  FILTER(?releaseYear = "2001"^^<http://www.w3.org/2001/XMLSchema#gYear>)
}
LIMIT 500
```

5. **Retrieve movies and their main subjects:**
```sparql
SELECT ?movie ?mainSubject
WHERE {
  ?movie a <http://dbpedia.org/ontology/Film> ;
         <http://dbpedia.org/ontology/mainSubject> ?mainSubject .
} LIMIT 500
```

6. **Retrieve movies and their runtimes:**
```sparql
SELECT ?movie ?runtime
WHERE {
  ?movie a <http://dbpedia.org/ontology/Film> ;
         <http://dbpedia.org/ontology/runtime> ?runtime .
    FILTER(?runtime != "N/A")
} limit 500
```

7. **Retrieve movies and their starring actors:**
```sparql
SELECT ?movie ?actor
WHERE {
  ?movie a <http://dbpedia.org/ontology/Film> ;
         <http://dbpedia.org/ontology/starring> ?actor .
  FILTER (?actor != <http://dbpedia.org/resource/N/A>)
} limit 500
```

8. Retrieve movies, their directors, and the number of genres they belong to:
```sparql
PREFIX dbo: <http://dbpedia.org/ontology/>

SELECT ?movie ?director (COUNT(?genre) AS ?genreCount)
WHERE {
  ?movie a dbo:Film ;
         dbo:director ?director ;
         dbo:genre ?genre .
 FILTER (?genre != <http://dbpedia.org/resource/N/A>)
 FILTER (?director != <http://dbpedia.org/resource/N/A>)
}
GROUP BY ?movie ?director
LIMIT 500
```

9. Count the number of movies for each country:
```sparql
PREFIX dbo: <http://dbpedia.org/ontology/>

SELECT ?country (COUNT(?movie) AS ?movieCount)
WHERE {
  ?movie a dbo:Film ;
         dbo:country ?country .
}
GROUP BY ?country
ORDER BY DESC(?movieCount)
```

10. Count the number of movies for each production company:
```sparql
PREFIX dbo: <http://dbpedia.org/ontology/>

SELECT ?productionCompany (COUNT(?movie) AS ?movieCount)
WHERE {
  ?movie a dbo:Film ;
         dbo:productionCompany ?productionCompany .
   FILTER (?productionCompany != <http://dbpedia.org/resource/N/A>)

}
GROUP BY ?productionCompany
HAVING (COUNT(?movie) >= 5)
ORDER BY DESC(?movieCount)
```

11. movies based on input

SELECT DISTINCT ?movie ?genre ?actor ?director
WHERE {
  
    OPTIONAL {
      ?inputMovie :movie ?inputTitle;
                  :genres ?inputGenre;
                  :actors ?inputActor;
                  :directors ?inputDirector.
    }
  ?film :title ?title.
  OPTIONAL {?film :genres ?genre.}
  OPTIONAL {?film :actors ?actor.}
  OPTIONAL {?film :directors ?director.}

  FILTER(
    (?genre = ?inputGenre || !BOUND(?inputGenre)) && 
    (?actor = ?inputActor || !BOUND(?inputActor)) &&
    (?director = ?inputDirector || !BOUND(?inputDirector)) && (?title !=inputTitle)
  )
}

12. Similarity
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX dbr: <http://dbpedia.org/resource/>

SELECT ?movie 
#       (COUNT(DISTINCT ?sharedGenre) +
        (COUNT(?sharedYear) +
        COUNT(?sharedActor) +
        COUNT(?sharedDirector) AS ?similarityScore)
WHERE {
  # Define the target movie
  BIND(dbr:%27Til_We_Meet_Again AS ?targetMovie)

  # Retrieve the properties of the target movie
#  OPTIONAL { ?targetMovie dbo:genre ?targetGenre. }
  OPTIONAL { ?targetMovie dbo:releaseYear ?targetYear. }
  OPTIONAL { ?targetMovie dbo:starring ?targetActor. }
  OPTIONAL { ?targetMovie dbo:director ?targetDirector. }

  # Find other movies (filter out the target movie itself)
  ?movie a dbo:Film . 
  FILTER(?movie != ?targetMovie)
#
  
  # Match shared genres
#  OPTIONAL {
#    ?movie dbo:genre ?sharedGenre.
#    FILTER(?sharedGenre = ?targetGenre)
#  }

  # Match shared release year
  OPTIONAL {
    ?movie dbo:releaseYear ?sharedYear.
    FILTER(?sharedYear = ?targetYear)
  }

  # Match shared actors
  OPTIONAL {
    ?movie dbo:starring ?sharedActor.

  }
    FILTER(?sharedActor = ?targetActor)
  # Match shared directors
  OPTIONAL {
    ?movie dbo:director ?sharedDirector.

  }
    FILTER(?sharedDirector = ?targetDirector)
}
GROUP BY ?movie
ORDER BY DESC(?similarityScore)
LIMIT 10

13. Subclass creation they are implemented 2 ways--
    1. create all the subclasses of class Person
 
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

INSERT DATA {

  dbo:actor rdfs:subClassOf dbo:Person .
  dbo:director rdfs:subClassOf dbo:Person .
  dbo:distributor rdfs:subClassOf dbo:Person .
  dbo:producer rdfs:subClassOf dbo:Person .
  dbo:writer rdfs:subClassOf dbo:Person .
  dbo:cinematographer rdfs:subClassOf dbo:Person .
  dbo:composer rdfs:subClassOf dbo:Person .
  
       

  # Optionally, define Person as a class if not already defined
  dbo:Person a rdfs:Class .
}
   2. create subclass person of human and then for person we do equivalentclass for each one with union from owl
      1. first query
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

INSERT DATA {
  dbo:Person rdfs:subClassOf dbo:Human .
}

    2. second query   

PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:	<http://www.w3.org/2002/07/owl#>

INSERT DATA {
    dbo:person owl:equivalentClass [
            owl:unionOf (
            dbo:actor 
            dbo:director 
            dbo:distributor 
            dbo:producer 
            dbo:writer 
            dbo:cinematographer 
            dbo:composer)].
}


# Get plot and plot embedding
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?movie ?plot ?plotEmbedding
WHERE {
  ?movie a dbo:Film ;
         dbo:abstract ?plot ;
    	 dbo:plotEmbedding ?plotEmbedding .
  FILTER (?plot != <http://dbpedia.org/resource/N/A>)
}
LIMIT 500
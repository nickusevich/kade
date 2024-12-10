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
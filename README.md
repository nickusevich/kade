# Knowledge and Data Engineer Porject

## Group #4 Members
1. Nikita Aksjonov
2. Nikos Kessidis
3. Bar Melinarskiy
4. Konstantinos Zavantias

## Introduction
This project is designed for the Knowledge and Data Engineer assignment. It involves setting up a database, a REST service, and a user interface using Docker containers for movie recommandiotn system of data fetched from DBpedia.

## Prerequisites
- Docker
- Docker Compose
- Python 3.10.15

## Installation and Setup

### Step 1: Clone the Project
First, clone the project repository from GitHub:
```sh
git clone https://github.com/nickusevich/kade.git
cd kade
```


Step 2: Download the TTL File: 
Download the TTL file named dbpedia_movies.ttl from OneDrive using the following link: [Download TTL File](https://solisservices-my.sharepoint.com/:u:/g/personal/b_melinarskiy_students_uu_nl/EaqdYIc4zk1AvOkxGF7ILnABd5fYw6zsBbQS4M7lvKhCGw?e=uLF3Z7)

Place the downloaded TTL file (dbpedia_movies.ttl) in the TTLs directory: DB\Datasets\TTLs.

Step 3: Run the Setup Script
Run the run_script.py script to create the three Docker containers: DB, Rest Service, and UI.


```
python run_script.py setup DB/Datasets/TTLs/dbpedia_movies.ttl
```

or just -
```
python run_script.py
```
and it will take the default paramters

**Please note that the installation process may take a significant amount of time (20-30 minutes), especially the upload of data to the database and the creation of the REST Service due to the heavy requirements.**

This script will:

Create a Docker network if it doesn't already exist.
Create and start the GraphDB container.
Wait for the GraphDB service to be available.
Create the GraphDB repository and upload the TTL file.
Create and start the Rest Service container.
Create and start the UI container.
Step 4: Verify the Setup
After running the script, you should have three Docker containers running:

DB: The GraphDB container.
Rest Service: The REST API service container.
UI: The user interface container.
You can verify the status of the containers using the following command:
```
docker ps or via Docker Desktop -
```

![Docker](<Screenshots4ReadMe/dockers.png>)

Usage
Once the setup is complete, you can access the services as follows:

GraphDB: http://localhost:7200
![DB](<Screenshots4ReadMe/DB.png>)
REST Service: http://localhost:80/docs
![Rest Service](<Screenshots4ReadMe/RestService.png>)
UI: http://localhost:8050/
![UI](<Screenshots4ReadMe/UI.png>)


Cleanup
To remove all Docker resources created by this project, run the following command:

```
python run_script.py delete_all
```


GitHub Copilot
Here is the updated content for your README.md file:

Step 2: Download the TTL File
Download the TTL file from OneDrive using the following link: Download TTL File

Place the downloaded TTL file in the TTLs directory: DB\Datasets\TTLs.

Step 3: Run the Setup Script
Run the run_script.py script to create the three Docker containers: DB, Rest Service, and UI.

This script will:

Create a Docker network if it doesn't already exist.
Create and start the GraphDB container.
Wait for the GraphDB service to be available.
Create the GraphDB repository and upload the TTL file.
Create and start the Rest Service container.
Create and start the UI container.
Step 4: Verify the Setup
After running the script, you should have three Docker containers running:

DB: The GraphDB container.
Rest Service: The REST API service container.
UI: The user interface container.
You can verify the status of the containers using the following command:

Usage
Once the setup is complete, you can access the services as follows:

GraphDB: http://localhost:7200
REST Service: http://localhost:80
UI: http://localhost:3000
Cleanup
To remove all Docker resources created by this project, run the following command:


### Troubleshooting
If `run_script.py` did not work for you (it has only been tested on Windows, so compatibility with macOS is uncertain), you can start each container manually. Each part of the project has its own README file, but here are the main steps:

1. From the `DB` folder, run:
    ```sh
    docker-compose -f graphdb.yaml up -d
    ```

2. Then, via [http://localhost:7200](http://localhost:7200), create a repository named 'MoviesRepo' and upload the TTL file to a named graph called 'MoviesGraph'.

3. From the [RestService](http://_vscodecontentref_/0) folder, run:
    ```sh
    docker-compose -f rest_service.yml up -d --build
    ```

4. Once the service is up, create the final container for the UI:
    From the [UI](http://_vscodecontentref_/1) folder, run:
    ```sh
    docker-compose -f ui.yml up -d --build
    ```

License
This project is licensed under the MIT License.
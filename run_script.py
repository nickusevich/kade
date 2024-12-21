import os
import subprocess
import time
import requests
import sys
import docker
import pandas as pd
import urllib.parse


def info(message):
    print(f"[INFO] {message}")

def error(message):
    print(f"[ERROR] {message}")

def success(message):
    print(f"[SUCCESS] {message}")

def container_exists(container_name_substring):
    result = subprocess.run(
        ["docker", "ps", "-a", "--format", "{{.Names}}"],
        capture_output=True,
        text=True
    )
    return container_name_substring in result.stdout

def wait_for_service(url, timeout=300, interval=10):
    elapsed = 0
    headers = {"Accept": "application/json"}
    stop_status_codes = {406, 404, 403}  # Add any other status codes that should stop the loop
    while elapsed < timeout:
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                info(f"Service at {url} is available.")
                return True
            elif response.status_code in stop_status_codes:
                error(f"Received status code {response.status_code} from {url}. Stopping attempts.")
                return False
            else:
                error(f"Received status code {response.status_code} from {url}")
        except requests.ConnectionError:
            pass
        info(f"Waiting for service at {url} to be available...")
        time.sleep(interval)
        elapsed += interval
    error(f"Timed out waiting for service at {url} to be available.")
    return False

def repository_exists(repo_id):
    GRAPHDB_URL = "http://localhost:7200/rest/repositories"
    response = requests.get(GRAPHDB_URL)
    if response.status_code == 200:
        repositories = response.json()
        for repo in repositories:
            if repo['id'] == repo_id:
                return True
    return False

def delete_existing_data(repo_id):
    GRAPHDB_URL = f"http://localhost:7200/repositories/{repo_id}/statements"
    response = requests.delete(GRAPHDB_URL)
    if response.status_code == 204:
        success("Existing data deleted successfully.")
    else:
        error(f"Failed to delete existing data. Response: {response.content}")

def setup_environment(ttl_file_path):
    # Navigate to the src directory
    kade_dir = os.path.abspath(os.path.dirname(__file__))
    os.chdir(kade_dir)
    success("Successfully navigated to kade.")

    # Check if the GraphDB container already exists
    if not container_exists("graphdb"):
        # Create Data folder in DB folder if it does not already exist
        data_folder_path = os.path.join(kade_dir, "DB", "Data")
        os.makedirs(data_folder_path, exist_ok=True)
        success(f"Data folder created at {data_folder_path} (if it did not already exist).")
        # Create the GraphDB container
        print("Creating the GraphDB container...")
        subprocess.run(["docker-compose", "-f", "./DB/graphdb.yaml", "up", "-d"])
    else:
        info("GraphDB container already exists.")

    # Wait for GraphDB service to be available
    GRAPHDB_URL = "http://localhost:7200/rest/repositories"
    if not wait_for_service(GRAPHDB_URL):
        return

    REPO_ID = "MoviesRepo"

    # Check if the repository already exists
    if not repository_exists(REPO_ID):
        # Upload repository configuration file
        repo_config_path = "DB/Datasets/TTLs/repo-config.ttl"
        with open(repo_config_path, 'rb') as repo_config_file:
            files = {'config': repo_config_file}
            response = requests.post(f"{GRAPHDB_URL}", files=files)
        if response.status_code == 201:
            success("GraphDB repository created successfully.")
        else:
            error(f"Failed to create GraphDB repository. Response: {response.content}")
            return
    else:
        info("GraphDB repository already exists.")
        # Delete existing data in the repository
        delete_existing_data(REPO_ID)

    # GraphDB server URL and repository ID
    server_url = "http://localhost:7200"
    repo_id = "MoviesRepo"

    with open(ttl_file_path, 'rb') as f:
        file_content = f.read()
    headers = {'Content-Type': 'application/x-turtle', 'Accept': 'application/json'}

    # upload_url = f'{server_url}/repositories/{repo_id}/rdf-graphs/service?default'
    named_graph = "http://example.org/graph/MoviesGraph"
    upload_url = f'{server_url}/repositories/{repo_id}/rdf-graphs/service?graph={urllib.parse.quote(named_graph)}'
    response = requests.post(upload_url, headers=headers, data=file_content)

    if response.status_code == 204:
        print("Import data successfully into GraphDB.")
    else:
        print(f"Failed to import file: {response.status_code}, {response.text} in GraphDB.")

    # Create the Rest Service container
     # Get all the container names to monitor their status
    print("Waiting for the DB to be up before moving on...")
    container_names = ["graphdb"]
    wait_for_containers(container_names)
    print("Creating the Rest Service container...")
    subprocess.run(["docker-compose", "-f", "./RestService/rest_service.yml", "up", "-d", "--build"])

    # Create the UI container
    print("Waiting for the DB and rest service to be up before moving on...")
    container_names = ["graphdb", "restservice"]
    wait_for_containers(container_names)
    print("Creating the UI container...")
    subprocess.run(["docker-compose", "-f", "./UI/ui.yml", "up", "-d", "--build"])

    # Get all the container names to monitor their status
    container_names = ["graphdb", "restservice", "ui"]

    # Wait for the containers to be in a valid state
    print("Waiting for the containers to be in a valid state...")
    wait_for_containers(container_names)

def wait_for_containers(container_names, timeout=300, interval=10):
    client = docker.from_env()
    elapsed = 0
    while elapsed < timeout:
        all_containers_valid = True
        not_running_containers = []

        # Get all running containers
        containers = client.containers.list()

        # Define the data for the DataFrame
        container_data = []
        for container in containers:
            container_info = {
                'ID': container.id,
                'Name': container.name,
                'Status': container.status,
                'Image': container.image.tags[0] if container.image.tags else 'N/A',  # Get the first image tag
                'StartedAt': container.attrs['State']['StartedAt']
            }
            container_data.append(container_info)

        # Create a pandas DataFrame
        df = pd.DataFrame(container_data)

        for container_name in container_names:
            container_info = df[df['Name'].str.contains(container_name)]
            if container_info.empty:
                all_containers_valid = False
                not_running_containers.append(container_name)
                not_running_containers = list(set(not_running_containers))
                error(f"Container '{container_name}' not found.")
                continue
            
            container_info = container_info.iloc[0]
            container_status = container_info['Status']

            if container_status == "running":
                info(f"Container '{container_name}' is Running.")
            else:
                all_containers_valid = False
                not_running_containers.append(container_name)
                if container_status in ["exited", "dead"]:
                    error(f"Container '{container_name}' has failed with status '{container_status}'.")
                    subprocess.run(["docker", "logs", container_name])

        if all_containers_valid:
            success("All specified containers are now in a valid status.")
            return True

        if not_running_containers:
            time.sleep(interval)
            elapsed += interval
        else:
            success("All containers are already running.")
            return True

    error("Timed out waiting for specified containers to reach a valid status.")
    return False

def force_remove_container(container_id):
    client = docker.from_env()
    try:
        container = client.containers.get(container_id)
        container.kill()
        container.remove(force=True)
        success(f"Forcefully removed container {container_id}")
    except docker.errors.NotFound:
        error(f"Container {container_id} not found")
    except docker.errors.APIError as e:
        error(f"Failed to remove container {container_id}: {e}")

def delete_all_resources():
    info("Clearing existing Docker resources...")

    # Stop and remove all containers, networks, and volumes defined in docker-compose files
    try:
        subprocess.run(["docker-compose", "-f", "./DB/graphdb.yaml", "down", "--volumes", "--remove-orphans"], check=True)
        subprocess.run(["docker-compose", "-f", "./RestService/rest_service.yml", "down", "--volumes", "--remove-orphans"], check=True)
        subprocess.run(["docker-compose", "-f", "./UI/ui.yml", "down", "--volumes", "--remove-orphans"], check=True)
    except subprocess.CalledProcessError as e:
        error(f"docker-compose down failed: {e}")

    # Force stop and remove any remaining containers
    containers = subprocess.run("docker ps -aq", shell=True, capture_output=True, text=True).stdout.split()
    for container in containers:
        try:
            subprocess.run(["docker", "stop", container], check=True)
            subprocess.run(["docker", "rm", "-f", container], check=True)
        except subprocess.CalledProcessError as e:
            error(f"Failed to stop/remove container {container}: {e}")
            force_remove_container(container)

    # Remove all images
    images = subprocess.run("docker images -q", shell=True, capture_output=True, text=True).stdout.split()
    for image in images:
        try:
            subprocess.run(["docker", "rmi", "-f", image], check=True)
        except subprocess.CalledProcessError as e:
            error(f"Failed to remove image {image}: {e}")

    # Remove all volumes
    volumes = subprocess.run("docker volume ls -q", shell=True, capture_output=True, text=True).stdout.split()
    for volume in volumes:
        try:
            subprocess.run(["docker", "volume", "rm", volume], check=True)
        except subprocess.CalledProcessError as e:
            error(f"Failed to remove volume {volume}: {e}")

    # Remove all networks
    networks = subprocess.run("docker network ls -q", shell=True, capture_output=True, text=True).stdout.split()
    for network in networks:
        if network not in ["bridge", "host", "none"]:
            try:
                subprocess.run(["docker", "network", "rm", network], check=True)
            except subprocess.CalledProcessError as e:
                error(f"Failed to remove network {network}: {e}")

    success("All existing Docker resources cleared.")

if __name__ == "__main__":
    ttl_file_path = "DB\Datasets\TTLs\dbpedia_movies.ttl"
    task = "setup"
    if len(sys.argv) > 1:   
        task = sys.argv[1]
    if len(sys.argv) > 2:   
        ttl_file_path = sys.argv[2]

    task = str(task).lower()

    if task == "setup":
        setup_environment(ttl_file_path)
    elif task == "delete_all":
        delete_all_resources()
    else:
        print("Usage: python run_script.py setup [path_to_ttl_file] | delete_all")

    print("Done :)")
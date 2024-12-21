#!/bin/bash

# Define constants
# Define color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color


# Function for printing success messages
success() {
    echo -e "${GREEN}SUCCESS:${NC} $1"
}

# Function for printing error messages
error() {
    echo -e "${RED}ERROR:${NC} $1"
}

# Function for printing warning messages
warning() {
    echo -e "${YELLOW}WARNING:${NC} $1"
}

# Function for printing info messages
info() {
    echo -e "${YELLOW}INFO:${NC} $1"
}

# Function to find the absolute path to the src directory
find_src_directory() {
    # Get the absolute path of the directory where the script is located
    script_dir="$(dirname "$(realpath "$0")")"

    # Check if src exists in the parent directory of the script
    src_dir="$(realpath "$script_dir/../kade")"

    if [[ -d "$src_dir" ]]; then
        echo "$src_dir"
    else
        return 1
    fi
}
# Function to check the status of containers
check_container_status() {
    docker ps -a --format '{{.Names}} {{.Status}}' | awk '$2 != "Up" { print $1 }'
}

# Function to wait for specified containers to be in a valid state
wait_for_containers() {
    local container_name_pattern="$1"  # Pattern to match container names
    local timeout=300  # Total wait time in seconds
    local interval=10   # Interval to check container status in seconds
    local elapsed=0

    while [ $elapsed -lt $timeout ]; do
        all_containers_valid=true
        not_running_containers=()

        # Use pattern matching to find containers that match the given name pattern
        containers=$(docker ps -a --format '{{.Names}}' | grep "$container_name_pattern")

        # Check if any containers were found for the pattern
        if [ -n "$containers" ]; then
            for container_name in $containers; do
                container_status=$(docker inspect -f '{{.State.Status}}' "$container_name")

                if [ "$container_status" == "running" ]; then
                    info "Container '$container_name' is Running."
                else
                    all_containers_valid=false
                    not_running_containers+=("$container_name")  # Add to list of not running containers
                    if [ -n "$container_status" ]; then
                        info "Container '$container_name' is in '$container_status' state. Waiting..."
                        if [ "$container_status" == "exited" ] || [ "$container_status" == "dead" ]; then
                            error "Container '$container_name' has failed with status '$container_status'."
                            docker logs "$container_name" || { error "Failed to get logs for container: $container_name"; }
                        fi
                    else
                        all_containers_valid=false
                        info "Container '$container_name' found but status could not be determined. Waiting..."
                    fi
                fi
            done
        else
            all_containers_valid=false
            info "No containers found for pattern '$container_name_pattern'. Waiting for them to be created..."
        fi

        if [ "$all_containers_valid" == true ]; then
            success "All specified containers are now in a valid status."
            return 0  # Successful exit for the function
        fi

        # Only check for containers that are not running
        if [ ${#not_running_containers[@]} -gt 0 ]; then
            sleep $interval
            elapsed=$((elapsed + interval))
        else
            success "All containers are already running."
            return 0
        fi
    done

    # If we reach here, it means we've timed out
    error "Timed out waiting for specified containers to reach a valid status."
    return 1  # Indicate an error occurred
}

# Function to clear existing docker resources
clear_docker_resources() {
    info "Clearing existing Docker resources..."
    
    # Stop and remove all containers
    docker ps -aq | xargs docker stop || { error "Failed to stop containers."; exit 1; }
    docker ps -aq | xargs docker rm || { error "Failed to remove containers."; exit 1; }
    
    # Remove all images
    docker images -q | xargs docker rmi -f || { error "Failed to remove images."; exit 1; }
    
    # Remove all volumes
    docker volume ls -q | xargs docker volume rm || { error "Failed to remove volumes."; exit 1; }
    
    # Remove all networks
    docker network ls -q | grep -v 'bridge\|host\|none' | xargs docker network rm || { error "Failed to remove networks."; exit 1; }
    
    success "All existing Docker resources cleared."
}

# Function to check if a container with a name containing a specific substring exists
container_exists() {
    local container_name_substring="$1"
    docker ps -a --format '{{.Names}}' | grep -q "$container_name_substring"
}

# Argument parsing for clear_all
if [[ "$1" == "X" ]]; then
    clear_docker_resources
fi

# Find the src directory
src_directory=$(find_src_directory)
if [ $? -ne 0 ]; then
    error "Failed to find the kade directory."
    exit 1
fi

# Navigate to the src directory
info "Navigating to the app kade directory"
cd "$kade" || { error "Failed to navigate to kade directory."; exit 1; }
success "Successfully navigated to kade."



# Check if the GraphDB container already exists
if ! container_exists "graphdb"; then
    # Create the GraphDB container
    docker-compose -f "./DB/graphdb.yaml" up -d
else
    echo "GraphDB container already exists."
fi



# create the Rest Serivce container
docker-compose -f "./RestService/rest_service.yml" up -d --build

# create the UI container
docker-compose -f "./UI/ui.yml" up -d --build


# Get all the pod names to monitor their status
containers_names=("graphdb" "restservice", "ui")

# Wait for the pods to be in a valid state
wait_for_containers "${containers_names[@]}"

success "All steps completed successfully!!!!"
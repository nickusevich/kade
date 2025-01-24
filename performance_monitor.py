import psutil
import time
import requests
import logging

#logging info
logging.basicConfig(
    filename="performance_monitor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def monitor_process(pid, name, duration=60, interval=1):
    try:
        process = psutil.Process(pid)
        start_time = time.time()
        logging.info(f"Monitoring {name} (PID: {pid}) for {duration} seconds.")

        while time.time() - start_time < duration:
            cpu_usage = process.cpu_percent(interval=0.1)  # CPU usage as a percentage
            memory_usage = process.memory_info().rss / (1024 * 1024)  # Memory usage in MB
            logging.info(f"{name} - CPU: {cpu_usage:.2f}%, Memory: {memory_usage:.2f} MB")
            time.sleep(interval)
    except psutil.NoSuchProcess:
        logging.error(f"Process with PID {pid} not found.")
    except Exception as e:
        logging.error(f"Error monitoring process {name} (PID: {pid}): {e}")

def measure_response_time(endpoint, duration=60, interval=1):
    start_time = time.time()
    logging.info(f"Measuring response time for {endpoint} for {duration} seconds.")

    while time.time() - start_time < duration:
        try:
            start_request = time.time()
            response = requests.get(endpoint, timeout=5)
            response.raise_for_status()
            duration = time.time() - start_request
            logging.info(f"Response time for {endpoint}: {duration:.2f} seconds (Status: {response.status_code})")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error measuring response time for {endpoint}: {e}")
        time.sleep(interval)

if __name__ == "__main__":
    
    rest_api_pid = 5001
    ui_pid = 7001

    rest_api_endpoint = "http://localhost:80"  
    ui_endpoint = "http://localhost:8050"            

    # Monitor processes
    monitor_process(rest_api_pid, "REST API", duration=60)
    monitor_process(ui_pid, "UI", duration=60)

    # Measure response times
    measure_response_time(rest_api_endpoint, duration=60)
    measure_response_time(ui_endpoint, duration=60)

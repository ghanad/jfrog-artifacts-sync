import subprocess
import json
import time
import logging
import logging.handlers
import concurrent.futures
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

# Constants
METRICS_FILE = "/mnt/baloot_bot/file_metrics.prom"
LOG_FILE = "/var/log/jfrog_downloader/jfrog_sync.log"
MAX_RETRIES = 3

REPOSITORIES = [
    {
        "name": "npm",
        "command": [
            "jf", "rt", "download",
            "--server-id=source-server",
            "npm-cache",
            "/mnt/baloot_bot/npm/",
            "--recursive=true",
            "--flat=false",
            "--include-dirs=true",
            "--detailed-summary=true",
            "--retries=3"
        ]
    },
    {
        "name": "pypi",
        "command": [
            "jf", "rt", "download",
            "--server-id=source-server",
            "pypi-cache",
            "/mnt/baloot_bot/pypi/",
            "--flat=false",
            "--retries=3"
        ]
    },
    {
        "name": "maven",
        "command": [
            "jf", "rt", "download",
            "--server-id=source-server",
            "maven-cache",
            "/mnt/baloot_bot/maven/",
            "--flat=false",
            "--retries=3"
        ]
    }
]

def setup_logging():
    """Configure logging with rotation"""
    # Create logger
    logger = logging.getLogger('jfrog_sync')
    logger.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create rotating file handler (10MB per file, keep 5 backup files)
    handler = logging.handlers.RotatingFileHandler(
        LOG_FILE,
        maxBytes=10*1024*1024,
        backupCount=5
    )
    
    # Set formatter for handler
    handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(handler)
    
    # Also add console handler for immediate feedback
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def execute_command(repo: Dict, logger: logging.Logger) -> Tuple[str, float, Dict, bool]:
    """Execute JFrog command with retries and return metrics"""
    start_time = time.time()
    success = False
    result = {"status": "failure", "totals": {"success": 0, "failure": 0}}
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Starting download for {repo['name']} repository (attempt {attempt + 1})")
            process = subprocess.run(
                repo['command'],
                capture_output=True,
                text=True,
                check=True
            )
            
            # Parse JSON output
            try:
                result = json.loads(process.stdout)
                success = result["status"] == "success"
                if success:
                    break
            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON output for {repo['name']}")
                if process.stdout:
                    logger.error(f"Output was: {process.stdout}")
                
        except subprocess.CalledProcessError as e:
            logger.error(f"Error executing command for {repo['name']}: {str(e)}")
            logger.error(f"Command output: {e.stderr}")
            
        if attempt < MAX_RETRIES - 1:
            logger.info(f"Retrying {repo['name']} in 5 seconds...")
            time.sleep(5)
    
    duration = time.time() - start_time
    return repo['name'], duration, result, success

def write_metrics(metrics: List[Tuple], total_duration: float, logger: logging.Logger):
    """Write metrics in Prometheus format"""
    timestamp = int(time.time())
    
    try:
        with open(METRICS_FILE, 'w') as f:
            # Write repository-specific metrics
            for repo_name, duration, result, success in metrics:
                f.write(f'jfrog_download_duration_seconds{{repository="{repo_name}", jfrog="internet"}} {duration}\n')
                f.write(f'jfrog_download_files_total{{repository="{repo_name}",status="success", jfrog="internet"}} {result["totals"]["success"]}\n')
                f.write(f'jfrog_download_files_total{{repository="{repo_name}",status="failure", jfrog="internet"}} {result["totals"]["failure"]}\n')
                f.write(f'jfrog_download_success{{repository="{repo_name}", jfrog="internet"}} {1 if success else 0}\n')
            
            # Write global metrics
            f.write(f'jfrog_script_duration_seconds{{jfrog="internet"}} {total_duration}\n')
            f.write(f'jfrog_last_update_timestamp {timestamp}\n')
        
        logger.info(f"Metrics written to {METRICS_FILE}")
    except Exception as e:
        logger.error(f"Error writing metrics: {str(e)}")

def main():
    # Setup logging
    logger = setup_logging()
    logger.info(f"Starting JFrog sync at {datetime.now()}")
    
    total_start_time = time.time()
    
    # Create metrics directory if it doesn't exist
    Path(METRICS_FILE).parent.mkdir(parents=True, exist_ok=True)
    
    # Execute commands in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(REPOSITORIES)) as executor:
        future_to_repo = {
            executor.submit(execute_command, repo, logger): repo 
            for repo in REPOSITORIES
        }
        
        metrics = []
        for future in concurrent.futures.as_completed(future_to_repo):
            repo = future_to_repo[future]
            try:
                metrics.append(future.result())
                logger.info(f"Completed download for {repo['name']}")
            except Exception as e:
                logger.error(f"Error processing {repo['name']}: {str(e)}")
    
    total_duration = time.time() - total_start_time
    
    # Write metrics
    write_metrics(metrics, total_duration, logger)
    
    logger.info(f"JFrog sync completed in {total_duration:.2f} seconds")

if __name__ == "__main__":
    main()

import asyncio
import logging
import logging.handlers
import os
import json
import gzip
import shutil
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple
from dataclasses import dataclass
from time import perf_counter
from pathlib import Path

@dataclass
class DownloadMetrics:
    success: bool
    duration: float
    file_count: int
    successful_downloads: int
    failed_downloads: int

class LogManager:
    def __init__(self, log_dir: str = "/var/log/jfrog_downloader"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = self.log_dir / "jfrog_download.log"
        self.archive_dir = self.log_dir / "archive"
        self.archive_dir.mkdir(exist_ok=True)
        
        # Configure TimedRotatingFileHandler for daily log rotation
        self.handler = logging.handlers.TimedRotatingFileHandler(
            self.log_file,
            when='midnight',
            interval=1,
            backupCount=30,  # Keep logs for the last 30 days
            encoding='utf-8'
        )
        
        # Set log format
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.handler.setFormatter(formatter)
        
        # Configure logger
        self.logger = logging.getLogger('JFrogDownloader')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(self.handler)
        
        # Setup monthly archive management
        self.setup_monthly_archive()

    def setup_monthly_archive(self):
        """Initialize monthly archive management and cleanup old files"""
        self.archive_old_logs()
        
    def archive_old_logs(self):
        """Archive logs older than one month"""
        current_date = datetime.now()
        previous_month = current_date - timedelta(days=30)
        
        # Check all log files
        for log_file in self.log_dir.glob("jfrog_download.log.*"):
            try:
                # Extract date from filename
                file_date_str = log_file.name.split('.')[-1]
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                
                # If file is older than one month
                if file_date < previous_month:
                    self.compress_log_file(log_file)
            except (ValueError, IndexError):
                continue

    def compress_log_file(self, log_file: Path):
        """Compress a log file using gzip"""
        archive_file = self.archive_dir / f"{log_file.name}.gz"
        try:
            with log_file.open('rb') as f_in:
                with gzip.open(archive_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            log_file.unlink()  # Remove original file after compression
            self.logger.info(f"Compressed and archived {log_file} to {archive_file}")
        except Exception as e:
            self.logger.error(f"Failed to compress {log_file}: {e}")

class JFrogDownloader:
    def __init__(self):
        self.log_manager = LogManager()
        self.logger = self.log_manager.logger
        self.metric_file = "/mnt/baloot_bot/file_metrics.prom"
        self.start_time = perf_counter()
        self.download_metrics: Dict[str, DownloadMetrics] = {}
        
        self.logger.info("Initializing JFrog Downloader")
        self.logger.info(f"Metric file: {self.metric_file}")

    async def run_command(self, cmd: list) -> tuple[int, str, str]:
        """Execute a system command and return its output"""
        self.logger.info(f"Executing command: {' '.join(cmd)}")
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            stdout_str = stdout.decode().strip()
            stderr_str = stderr.decode().strip()
            
            if process.returncode == 0:
                self.logger.info(f"Command executed successfully")
            else:
                self.logger.error(f"Command failed with return code {process.returncode}")
                if stderr_str:
                    self.logger.error(f"stderr: {stderr_str}")
            
            return process.returncode, stdout_str, stderr_str
        except Exception as e:
            self.logger.error(f"Command execution failed: {e}")
            return 1, "", str(e)

    def count_files(self, dir_path: str) -> int:
        """Count total number of files in directory and subdirectories"""
        self.logger.info(f"Counting files in directory: {dir_path}")
        try:
            file_count = sum(len(files) for _, _, files in os.walk(dir_path))
            self.logger.info(f"Found {file_count} files in {dir_path}")
            return file_count
        except Exception as e:
            self.logger.error(f"Error counting files in {dir_path}: {e}")
            return 0

    def parse_download_output(self, stdout: str) -> Tuple[int, int]:
        """Parse JFrog download output JSON and extract success/failure counts"""
        self.logger.info("Parsing download output")
        if not stdout:
            self.logger.warning("Empty download output received")
            return 0, 0
            
        try:
            data = json.loads(stdout)
            totals = data.get('totals', {})
            success_count = totals.get('success', 0)
            failure_count = totals.get('failure', 0)
            self.logger.info(f"Parse results - Success: {success_count}, Failure: {failure_count}")
            return success_count, failure_count
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JFrog download output JSON: {e}")
            self.logger.error(f"Raw output: {stdout}")
            return 0, 0

    async def check_jf_exists(self) -> bool:
        """Check if JFrog CLI is installed"""
        exit_code, _, _ = await self.run_command(["which", "jf"])
        exists = exit_code == 0
        if not exists:
            self.logger.error("JFrog CLI (jf) not found")
        return exists

    async def download_artifacts(self, repo: str, path: str, cleanup_path: Optional[str] = None) -> bool:
        """Download artifacts from specified repository"""
        self.logger.info(f"Starting download for repository: {repo}")
        self.logger.info(f"Download path: {path}")
        if cleanup_path:
            self.logger.info(f"Cleanup path: {cleanup_path}")
        
        start_time = perf_counter()
        
        if not await self.check_jf_exists():
            return False

        if not os.path.exists(os.path.dirname(path)):
            self.logger.info(f"Creating directory: {path}")
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                self.logger.info(f"Successfully created directory: {path}")
            except Exception as e:
                self.logger.error(f"Failed to create directory {path}: {e}")
                return False

        successful_downloads = failed_downloads = 0
        success = False
        
        for attempt in range(3):
            self.logger.info(f"Download attempt {attempt + 1}/3 for {repo}")
            
            if attempt > 0:
                wait_time = 5 * attempt
                self.logger.info(f"Waiting {wait_time} seconds before retry")
                await asyncio.sleep(wait_time)
                
            cmd = [
                "jf", "rt", "download",
                "--server-id=source-server",
                repo, path,
                "--flat=false",
                "--retries=3"
            ]
            
            exit_code, stdout, stderr = await self.run_command(cmd)
            
            if exit_code == 0:
                successful_downloads, failed_downloads = self.parse_download_output(stdout)
                success = True
                self.logger.info(f"Download successful for {repo} on attempt {attempt + 1}")
                break
            else:
                self.logger.warning(f"Download attempt {attempt + 1} failed for {repo}")
                if stderr:
                    self.logger.warning(f"Error message: {stderr}")

        if cleanup_path and os.path.exists(cleanup_path):
            self.logger.info(f"Starting cleanup of {cleanup_path}")
            try:
                os.system(f"rm -rf {cleanup_path}")
                self.logger.info(f"Successfully cleaned up {cleanup_path}")
            except Exception as e:
                self.logger.error(f"Cleanup failed for {cleanup_path}: {e}")

        duration = perf_counter() - start_time
        self.logger.info(f"Download operation took {duration:.2f} seconds")
        
        file_count = self.count_files(path) if success else 0
        
        self.download_metrics[repo] = DownloadMetrics(
            success=success,
            duration=duration,
            file_count=file_count,
            successful_downloads=successful_downloads,
            failed_downloads=failed_downloads
        )
        
        self.logger.info(f"Download metrics for {repo}: {self.download_metrics[repo]}")
        return success

    def write_metrics(self) -> None:
        """Write Prometheus metrics to file"""
        self.logger.info("Starting to write metrics")
        metrics = [
            ("jfrog_file_count", "Number of files in directory", "gauge",
             lambda m: m.file_count),
            ("jfrog_download_duration_seconds", "Download duration for each repository", "gauge",
             lambda m: m.duration),
            ("jfrog_download_success", "Download success status (1 for success, 0 for failure)", "gauge",
             lambda m: 1 if m.success else 0),
            ("jfrog_successful_downloads", "Number of files successfully downloaded", "gauge",
             lambda m: m.successful_downloads),
            ("jfrog_failed_downloads", "Number of files that failed to download", "gauge",
             lambda m: m.failed_downloads)
        ]

        try:
            self.logger.info(f"Writing metrics to {self.metric_file}")
            with open(self.metric_file, 'w') as f:
                for metric_name, help_text, metric_type, value_func in metrics:
                    f.write(f"# HELP {metric_name} {help_text}\n")
                    f.write(f"# TYPE {metric_name} {metric_type}\n")
                    for repo, metric in self.download_metrics.items():
                        value = value_func(metric)
                        f.write(f'{metric_name}{{repository="{repo}", jfrog="internet"}} {value}\n')
                        self.logger.info(f"Wrote metric {metric_name} for {repo}: {value}")

                total_duration = perf_counter() - self.start_time
                f.write("# HELP jfrog_script_duration_seconds Total script execution time in seconds\n")
                f.write("# TYPE jfrog_script_duration_seconds gauge\n")
                f.write(f'jfrog_script_duration_seconds{{jfrog="internet"}} {total_duration}\n')
                self.logger.info(f"Total script duration: {total_duration:.2f} seconds")
            
            self.logger.info("Successfully wrote all metrics")
        except Exception as e:
            self.logger.error(f"Failed to write metrics: {e}")

    async def main(self) -> bool:
        """Main execution function"""
        self.logger.info("Starting JFrog download script")
        
        repos = [
            ("npm-cache", "/mnt/baloot_bot/npm/", "/mnt/baloot_bot/npm/.npm"),
            ("pypi-cache", "/mnt/baloot_bot/pypi/", "/mnt/baloot_bot/pypi/.pypi/"),
            ("maven-cache", "/mnt/baloot_bot/maven/", "/mnt/baloot_bot/maven/.maven/")
        ]
        
        self.logger.info(f"Processing {len(repos)} repositories")
        tasks = [self.download_artifacts(repo, path, cleanup) for repo, path, cleanup in repos]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success = all(isinstance(r, bool) and r for r in results)
        
        if not success:
            self.logger.error("One or more downloads failed")
        else:
            self.logger.info("All downloads completed successfully")
        
        self.write_metrics()
        return success

if __name__ == "__main__":
    downloader = JFrogDownloader()
    asyncio.run(downloader.main())

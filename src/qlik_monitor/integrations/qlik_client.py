"""
Qlik API client for interacting with Qlik Replicate REST API.
"""
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..config import config
from ..core.models import ReplicationTask, TaskStatus, TaskMetrics

logger = logging.getLogger(__name__)


class QlikAPIClient:
    """Client for Qlik Replicate REST API."""

    def __init__(
        self,
        api_endpoint: Optional[str] = None,
        api_token: Optional[str] = None,
        timeout: int = 30
    ):
        """
        Initialize Qlik API client.

        Args:
            api_endpoint: Qlik API endpoint URL
            api_token: API authentication token
            timeout: Request timeout in seconds
        """
        self.api_endpoint = api_endpoint or config.qlik.api_endpoint
        self.api_token = api_token or config.qlik.api_token
        self.timeout = timeout

        # Setup session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set default headers
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        })

    def get_all_tasks(self) -> List[ReplicationTask]:
        """
        Retrieve all replication tasks.

        Returns:
            List of ReplicationTask objects
        """
        try:
            response = self.session.get(
                f"{self.api_endpoint}/tasks",
                timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            tasks = []
            for task_data in data.get("tasks", []):
                task = self._parse_task(task_data)
                tasks.append(task)

            logger.info(f"Retrieved {len(tasks)} tasks from Qlik API")
            return tasks

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve tasks from Qlik API: {e}")
            return []

    def get_task_details(self, task_id: str) -> Optional[ReplicationTask]:
        """
        Get detailed information about a specific task.

        Args:
            task_id: Task identifier

        Returns:
            ReplicationTask object or None if not found
        """
        try:
            response = self.session.get(
                f"{self.api_endpoint}/tasks/{task_id}",
                timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            task = self._parse_task(data)
            logger.debug(f"Retrieved task details for {task_id}")
            return task

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve task {task_id}: {e}")
            return None

    def get_task_metrics(self, task_id: str) -> Optional[TaskMetrics]:
        """
        Get metrics for a specific task.

        Args:
            task_id: Task identifier

        Returns:
            TaskMetrics object or None if not found
        """
        try:
            response = self.session.get(
                f"{self.api_endpoint}/tasks/{task_id}/metrics",
                timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            metrics = TaskMetrics(
                lag_seconds=data.get("lag_seconds"),
                records_processed=data.get("records_processed"),
                records_per_second=data.get("records_per_second"),
                last_update_time=self._parse_datetime(data.get("last_update_time")),
                error_count=data.get("error_count", 0)
            )

            return metrics

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve metrics for task {task_id}: {e}")
            return None

    def restart_task(self, task_id: str) -> bool:
        """
        Restart a replication task.

        Args:
            task_id: Task identifier

        Returns:
            True if restart was successful, False otherwise
        """
        try:
            response = self.session.post(
                f"{self.api_endpoint}/tasks/{task_id}/restart",
                timeout=self.timeout
            )
            response.raise_for_status()

            logger.info(f"Successfully restarted task {task_id}")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to restart task {task_id}: {e}")
            return False

    def stop_task(self, task_id: str) -> bool:
        """
        Stop a replication task.

        Args:
            task_id: Task identifier

        Returns:
            True if stop was successful, False otherwise
        """
        try:
            response = self.session.post(
                f"{self.api_endpoint}/tasks/{task_id}/stop",
                timeout=self.timeout
            )
            response.raise_for_status()

            logger.info(f"Successfully stopped task {task_id}")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to stop task {task_id}: {e}")
            return False

    def _parse_task(self, data: Dict[str, Any]) -> ReplicationTask:
        """Parse task data from API response."""
        status_map = {
            "RUNNING": TaskStatus.RUNNING,
            "STOPPED": TaskStatus.STOPPED,
            "ERROR": TaskStatus.ERROR,
            "ABORTED": TaskStatus.ABORTED
        }

        metrics_data = data.get("metrics", {})
        metrics = TaskMetrics(
            lag_seconds=metrics_data.get("lag_seconds"),
            records_processed=metrics_data.get("records_processed"),
            records_per_second=metrics_data.get("records_per_second"),
            last_update_time=self._parse_datetime(metrics_data.get("last_update_time")),
            error_count=metrics_data.get("error_count", 0)
        )

        return ReplicationTask(
            task_id=data["task_id"],
            task_name=data.get("task_name", data["task_id"]),
            status=status_map.get(data.get("status", "UNKNOWN"), TaskStatus.UNKNOWN),
            source_endpoint=data.get("source_endpoint", ""),
            target_endpoint=data.get("target_endpoint", ""),
            metrics=metrics,
            restart_attempts=data.get("restart_attempts", 0),
            metadata=data.get("metadata", {}),
            created_at=self._parse_datetime(data.get("created_at")),
            updated_at=self._parse_datetime(data.get("updated_at"))
        )

    @staticmethod
    def _parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string to datetime object."""
        if not dt_str:
            return None
        try:
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return None

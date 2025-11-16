"""
Main monitoring agent orchestrator.
"""
import logging
from typing import List, Dict, Any
from datetime import datetime

from ..config import config
from ..core.models import ReplicationTask, TaskAnomaly, RestartProposal, MonitoringState
from ..core.monitor import TaskMonitor
from ..integrations.qlik_client import QlikAPIClient
from ..integrations.openai_analyzer import OpenAIAnalyzer

logger = logging.getLogger(__name__)


class MonitoringAgent:
    """Main agent that orchestrates monitoring, analysis, and actions."""

    def __init__(
        self,
        qlik_client: QlikAPIClient = None,
        openai_analyzer: OpenAIAnalyzer = None,
        task_monitor: TaskMonitor = None
    ):
        """
        Initialize monitoring agent.

        Args:
            qlik_client: Qlik API client instance
            openai_analyzer: OpenAI analyzer instance
            task_monitor: Task monitor instance
        """
        self.qlik_client = qlik_client or QlikAPIClient()
        self.openai_analyzer = openai_analyzer or OpenAIAnalyzer()
        self.task_monitor = task_monitor or TaskMonitor()

    def run_monitoring_cycle(self) -> Dict[str, Any]:
        """
        Run a complete monitoring cycle.

        Returns:
            Dictionary with monitoring results and proposed actions
        """
        logger.info("Starting monitoring cycle")

        # Step 1: Retrieve all tasks from Qlik
        tasks = self.qlik_client.get_all_tasks()
        if not tasks:
            logger.warning("No tasks retrieved from Qlik API")
            return {
                "status": "no_tasks",
                "timestamp": datetime.utcnow().isoformat(),
                "tasks_checked": 0
            }

        # Step 2: Detect anomalies in all tasks
        all_anomalies = {}
        for task in tasks:
            anomalies = self.task_monitor.detect_anomalies(task)
            if anomalies:
                all_anomalies[task.task_id] = anomalies

        # Step 3: Assess overall health
        health_state = self.task_monitor.assess_overall_health(tasks)

        # Step 4: Generate restart proposals using AI
        proposals = []
        for task_id, anomalies in all_anomalies.items():
            task = next((t for t in tasks if t.task_id == task_id), None)
            if not task:
                continue

            # Use AI to analyze critical anomalies
            critical_anomalies = [
                a for a in anomalies
                if a.severity in ["high", "critical"]
            ]

            for anomaly in critical_anomalies:
                proposal = self.openai_analyzer.analyze_task_anomaly(task, anomaly)
                if proposal:
                    proposals.append(proposal)

        logger.info(
            f"Monitoring cycle complete: {len(tasks)} tasks checked, "
            f"{len(all_anomalies)} with anomalies, {len(proposals)} proposals generated"
        )

        return {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "tasks_checked": len(tasks),
            "health_state": health_state.model_dump(),
            "anomalies_count": len(all_anomalies),
            "proposals": [p.model_dump() for p in proposals],
            "task_details": [
                {
                    "task_id": t.task_id,
                    "task_name": t.task_name,
                    "status": t.status.value,
                    "lag_seconds": t.metrics.lag_seconds,
                    "anomalies": [a.model_dump() for a in all_anomalies.get(t.task_id, [])]
                }
                for t in tasks
            ]
        }

    def execute_restart_proposal(
        self,
        proposal: RestartProposal,
        approved: bool = False
    ) -> Dict[str, Any]:
        """
        Execute a restart proposal.

        Args:
            proposal: The restart proposal to execute
            approved: Whether user has approved the restart

        Returns:
            Dictionary with execution results
        """
        if proposal.requires_approval and not approved:
            logger.info(
                f"Restart proposal for {proposal.task_id} requires approval but not approved"
            )
            return {
                "status": "awaiting_approval",
                "task_id": proposal.task_id,
                "task_name": proposal.task_name,
                "reason": "User approval required"
            }

        logger.info(
            f"Executing restart for task {proposal.task_id}: {proposal.reason}"
        )

        # Attempt to restart the task
        success = self.qlik_client.restart_task(proposal.task_id)

        result = {
            "status": "success" if success else "failed",
            "task_id": proposal.task_id,
            "task_name": proposal.task_name,
            "action": proposal.suggested_action,
            "timestamp": datetime.utcnow().isoformat()
        }

        if success:
            logger.info(f"Successfully restarted task {proposal.task_id}")
        else:
            logger.error(f"Failed to restart task {proposal.task_id}")
            result["error"] = "Restart operation failed"

        return result

    def get_task_details(self, task_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific task.

        Args:
            task_id: Task identifier

        Returns:
            Dictionary with task details and analysis
        """
        task = self.qlik_client.get_task_details(task_id)
        if not task:
            return {
                "status": "not_found",
                "task_id": task_id
            }

        anomalies = self.task_monitor.detect_anomalies(task)

        return {
            "status": "success",
            "task": task.model_dump(),
            "anomalies": [a.model_dump() for a in anomalies],
            "health_status": "healthy" if not anomalies else "unhealthy"
        }

    def evaluate_all_tasks(self) -> Dict[str, Any]:
        """
        Run comprehensive evaluation of all tasks using AI.

        Returns:
            Dictionary with AI assessment and recommendations
        """
        logger.info("Running comprehensive task evaluation with AI")

        tasks = self.qlik_client.get_all_tasks()
        if not tasks:
            return {
                "status": "no_tasks",
                "assessment": "No tasks found to evaluate"
            }

        # Get AI-powered batch analysis
        ai_assessment = self.openai_analyzer.batch_analyze_tasks(tasks)

        # Get statistical health state
        health_state = self.task_monitor.assess_overall_health(tasks)

        return {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "health_state": health_state.model_dump(),
            "ai_assessment": ai_assessment,
            "total_tasks": len(tasks)
        }

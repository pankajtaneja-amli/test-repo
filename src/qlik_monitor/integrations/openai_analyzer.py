"""
OpenAI integration for intelligent anomaly detection and decision-making.
"""
import logging
import json
from typing import List, Optional
from openai import OpenAI

from ..config import config
from ..core.models import ReplicationTask, TaskAnomaly, RestartProposal

logger = logging.getLogger(__name__)


class OpenAIAnalyzer:
    """OpenAI-powered analyzer for task anomalies and restart decisions."""

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        """
        Initialize OpenAI analyzer.

        Args:
            api_key: OpenAI API key
            model: OpenAI model to use (e.g., 'gpt-4')
        """
        self.api_key = api_key or config.openai.api_key
        self.model = model or config.openai.model
        self.client = OpenAI(api_key=self.api_key)

    def analyze_task_anomaly(
        self,
        task: ReplicationTask,
        anomaly: TaskAnomaly
    ) -> Optional[RestartProposal]:
        """
        Analyze a task anomaly and propose action using OpenAI.

        Args:
            task: The replication task with anomaly
            anomaly: Detected anomaly details

        Returns:
            RestartProposal if action is recommended, None otherwise
        """
        try:
            prompt = self._build_anomaly_analysis_prompt(task, anomaly)

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are an expert database replication analyst. "
                            "Your role is to analyze replication task anomalies and "
                            "provide actionable recommendations. Be precise and data-driven."
                        )
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1000
            )

            analysis_text = response.choices[0].message.content
            proposal = self._parse_analysis_response(task, anomaly, analysis_text)

            logger.info(
                f"OpenAI analysis completed for task {task.task_id} "
                f"with confidence {proposal.confidence if proposal else 0}"
            )
            return proposal

        except Exception as e:
            logger.error(f"Failed to analyze anomaly with OpenAI: {e}")
            return None

    def evaluate_restart_decision(
        self,
        task: ReplicationTask,
        historical_restarts: int
    ) -> dict:
        """
        Evaluate whether a task should be restarted based on its state.

        Args:
            task: The replication task to evaluate
            historical_restarts: Number of recent restart attempts

        Returns:
            Dictionary with decision and reasoning
        """
        try:
            prompt = self._build_restart_evaluation_prompt(task, historical_restarts)

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are an expert in database replication operations. "
                            "Evaluate whether a task should be restarted based on its "
                            "current state and history. Provide clear reasoning."
                        )
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=500
            )

            decision_text = response.choices[0].message.content
            return self._parse_restart_decision(decision_text)

        except Exception as e:
            logger.error(f"Failed to evaluate restart decision: {e}")
            return {"should_restart": False, "reason": f"Error: {str(e)}"}

    def batch_analyze_tasks(
        self,
        tasks: List[ReplicationTask]
    ) -> dict:
        """
        Analyze multiple tasks and provide overall health assessment.

        Args:
            tasks: List of replication tasks

        Returns:
            Dictionary with overall assessment and recommendations
        """
        try:
            prompt = self._build_batch_analysis_prompt(tasks)

            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a database replication monitoring expert. "
                            "Analyze multiple replication tasks and provide an overall "
                            "health assessment with prioritized recommendations."
                        )
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1500
            )

            assessment_text = response.choices[0].message.content
            return {
                "assessment": assessment_text,
                "total_tasks": len(tasks),
                "analyzed_at": "now"
            }

        except Exception as e:
            logger.error(f"Failed to batch analyze tasks: {e}")
            return {"assessment": f"Error: {str(e)}", "total_tasks": len(tasks)}

    def _build_anomaly_analysis_prompt(
        self,
        task: ReplicationTask,
        anomaly: TaskAnomaly
    ) -> str:
        """Build prompt for anomaly analysis."""
        return f"""Analyze the following replication task anomaly:

Task Information:
- Task ID: {task.task_id}
- Task Name: {task.task_name}
- Status: {task.status.value}
- Source: {task.source_endpoint}
- Target: {task.target_endpoint}

Anomaly Details:
- Type: {anomaly.anomaly_type}
- Severity: {anomaly.severity}
- Description: {anomaly.description}

Metrics:
- Lag: {task.metrics.lag_seconds} seconds
- Records Processed: {task.metrics.records_processed}
- Processing Rate: {task.metrics.records_per_second} rec/sec
- Error Count: {task.metrics.error_count}
- Last Update: {task.metrics.last_update_time}

Previous Restart Attempts: {task.restart_attempts}

Based on this information:
1. Should this task be restarted? (yes/no)
2. What is your confidence level? (0.0 to 1.0)
3. What specific action should be taken?
4. Provide detailed analysis and reasoning.
5. Does this require user approval? (yes/no)

Format your response as JSON:
{{
    "should_restart": true/false,
    "confidence": 0.0-1.0,
    "action": "specific action to take",
    "analysis": "detailed reasoning",
    "requires_approval": true/false
}}"""

    def _build_restart_evaluation_prompt(
        self,
        task: ReplicationTask,
        historical_restarts: int
    ) -> str:
        """Build prompt for restart evaluation."""
        return f"""Evaluate if this replication task should be restarted:

Task: {task.task_name} (ID: {task.task_id})
Status: {task.status.value}
Lag: {task.metrics.lag_seconds} seconds
Error Count: {task.metrics.error_count}
Last Update: {task.metrics.last_update_time}
Recent Restarts: {historical_restarts}
Max Allowed Restarts: {config.monitoring.max_restart_attempts}

Should this task be restarted? Provide reasoning in JSON format:
{{
    "should_restart": true/false,
    "confidence": 0.0-1.0,
    "reason": "detailed explanation"
}}"""

    def _build_batch_analysis_prompt(self, tasks: List[ReplicationTask]) -> str:
        """Build prompt for batch task analysis."""
        task_summaries = []
        for task in tasks[:20]:  # Limit to 20 tasks to avoid token limits
            task_summaries.append(
                f"- {task.task_name}: {task.status.value}, "
                f"lag={task.metrics.lag_seconds}s, "
                f"errors={task.metrics.error_count}"
            )

        return f"""Analyze these {len(tasks)} replication tasks:

{chr(10).join(task_summaries)}

Provide:
1. Overall health assessment
2. Critical issues requiring immediate attention
3. Tasks that should be prioritized for restart
4. Recommendations for improving replication health"""

    def _parse_analysis_response(
        self,
        task: ReplicationTask,
        anomaly: TaskAnomaly,
        response_text: str
    ) -> Optional[RestartProposal]:
        """Parse OpenAI response into RestartProposal."""
        try:
            # Try to extract JSON from response
            start_idx = response_text.find("{")
            end_idx = response_text.rfind("}") + 1
            if start_idx >= 0 and end_idx > start_idx:
                json_str = response_text[start_idx:end_idx]
                data = json.loads(json_str)

                if data.get("should_restart", False):
                    return RestartProposal(
                        task_id=task.task_id,
                        task_name=task.task_name,
                        reason=anomaly.description,
                        confidence=float(data.get("confidence", 0.5)),
                        suggested_action=data.get("action", "Restart task"),
                        analysis=data.get("analysis", response_text),
                        requires_approval=data.get("requires_approval", True)
                    )
            return None

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"Failed to parse OpenAI response: {e}")
            return None

    def _parse_restart_decision(self, response_text: str) -> dict:
        """Parse restart decision from OpenAI response."""
        try:
            start_idx = response_text.find("{")
            end_idx = response_text.rfind("}") + 1
            if start_idx >= 0 and end_idx > start_idx:
                json_str = response_text[start_idx:end_idx]
                return json.loads(json_str)
            return {"should_restart": False, "reason": "Could not parse response"}

        except json.JSONDecodeError:
            return {"should_restart": False, "reason": "Invalid response format"}

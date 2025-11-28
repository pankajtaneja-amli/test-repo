"""
AWS Lambda function for evaluating restart proposals.
This function uses OpenAI to analyze proposals and make decisions.
"""
import json
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from qlik_monitor.core.agent import MonitoringAgent
from qlik_monitor.core.models import RestartProposal
from qlik_monitor.config import config

logger = logging.getLogger()
logger.setLevel(getattr(logging, config.log_level))


def lambda_handler(event, context):
    """
    Lambda handler for evaluating restart proposals.

    Args:
        event: Lambda event containing proposal data
        context: Lambda context

    Returns:
        Evaluation results with decision
    """
    try:
        logger.info(f"Evaluating restart proposal: {json.dumps(event)}")

        # Extract proposal from event
        proposal_data = event.get("proposal", {})
        task_id = proposal_data.get("task_id")
        
        if not task_id:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "status": "error",
                    "error": "Missing task_id in proposal"
                })
            }

        # Initialize agent
        agent = MonitoringAgent()

        # Get task details
        task_info = agent.get_task_details(task_id)

        # Check if we should auto-approve based on severity
        auto_approve = event.get("auto_approve", False)
        requires_approval = proposal_data.get("requires_approval", True)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "task_info": task_info,
                "requires_approval": requires_approval and not auto_approve,
                "recommendation": "approve" if task_info.get("anomalies") else "reject"
            })
        }

    except Exception as e:
        logger.error(f"Error evaluating proposal: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "error": str(e)
            })
        }

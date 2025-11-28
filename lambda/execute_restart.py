"""
AWS Lambda function for executing task restarts.
This function performs the actual restart operation.
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
    Lambda handler for executing task restart.

    Args:
        event: Lambda event containing restart parameters
        context: Lambda context

    Returns:
        Restart execution results
    """
    try:
        logger.info(f"Executing restart: {json.dumps(event)}")

        # Extract parameters
        proposal_data = event.get("proposal", {})
        approved = event.get("approved", False)

        # Create proposal object
        proposal = RestartProposal(**proposal_data)

        # Initialize agent
        agent = MonitoringAgent()

        # Execute restart
        result = agent.execute_restart_proposal(proposal, approved=approved)

        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }

    except Exception as e:
        logger.error(f"Error executing restart: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "error": str(e)
            })
        }

"""
AWS Lambda function for monitoring Qlik replication tasks.
This function is triggered by AWS Step Functions.
"""
import json
import logging
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from qlik_monitor.core.agent import MonitoringAgent
from qlik_monitor.config import config

# Setup logging
logger = logging.getLogger()
logger.setLevel(getattr(logging, config.log_level))


def lambda_handler(event, context):
    """
    Lambda handler for monitoring cycle.

    Args:
        event: Lambda event containing monitoring parameters
        context: Lambda context

    Returns:
        Monitoring results
    """
    try:
        logger.info(f"Starting monitoring cycle with event: {json.dumps(event)}")

        # Initialize agent
        agent = MonitoringAgent()

        # Run monitoring cycle
        results = agent.run_monitoring_cycle()

        logger.info(f"Monitoring cycle completed successfully")
        return {
            "statusCode": 200,
            "body": json.dumps(results)
        }

    except Exception as e:
        logger.error(f"Error in monitoring cycle: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "error": str(e)
            })
        }


if __name__ == "__main__":
    # For local testing
    test_event = {}
    test_context = {}
    result = lambda_handler(test_event, test_context)
    print(json.dumps(result, indent=2))

#!/usr/bin/env python3
"""
Example script: Run a monitoring cycle locally for testing.
"""
import sys
import os
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.qlik_monitor.core.agent import MonitoringAgent
from src.qlik_monitor.config import config
from src.qlik_monitor.utils.helpers import setup_logging


def main():
    """Run a monitoring cycle."""
    # Setup logging
    setup_logging(config.log_level)

    print("=" * 60)
    print("Qlik Replication Monitoring Agent - Local Test")
    print("=" * 60)
    print()

    # Display configuration
    print("Configuration:")
    print(f"  Qlik API: {config.qlik.api_endpoint}")
    print(f"  OpenAI Model: {config.openai.model}")
    print(f"  Lag Threshold: {config.monitoring.lag_threshold_seconds}s")
    print(f"  Stuck Threshold: {config.monitoring.stuck_task_threshold_seconds}s")
    print()

    # Create agent
    print("Initializing monitoring agent...")
    agent = MonitoringAgent()

    # Run monitoring cycle
    print("Running monitoring cycle...")
    print("-" * 60)
    
    try:
        results = agent.run_monitoring_cycle()
        
        print()
        print("Monitoring Results:")
        print("-" * 60)
        print(json.dumps(results, indent=2, default=str))
        print()
        
        # Summary
        health_state = results.get("health_state", {})
        print("Summary:")
        print(f"  Total Tasks: {results.get('tasks_checked', 0)}")
        print(f"  Healthy: {health_state.get('healthy_tasks', 0)}")
        print(f"  Lagging: {health_state.get('lagging_tasks', 0)}")
        print(f"  Stuck: {health_state.get('stuck_tasks', 0)}")
        print(f"  Aborted: {health_state.get('aborted_tasks', 0)}")
        print(f"  Errors: {health_state.get('error_tasks', 0)}")
        print()
        
        # Proposals
        proposals = results.get("proposals", [])
        if proposals:
            print(f"Restart Proposals: {len(proposals)}")
            for i, proposal in enumerate(proposals, 1):
                print(f"  {i}. {proposal.get('task_name')} - "
                      f"Confidence: {proposal.get('confidence', 0):.2f}")
        else:
            print("No restart proposals generated")
        
        print()
        print("=" * 60)
        print("Monitoring cycle completed successfully!")
        print("=" * 60)
        
        return 0

    except Exception as e:
        print()
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

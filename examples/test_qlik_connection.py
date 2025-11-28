#!/usr/bin/env python3
"""
Example script: Test Qlik API connectivity.
"""
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.qlik_monitor.integrations.qlik_client import QlikAPIClient
from src.qlik_monitor.config import config


def main():
    """Test Qlik API connectivity."""
    print("=" * 60)
    print("Qlik API Connectivity Test")
    print("=" * 60)
    print()

    print(f"API Endpoint: {config.qlik.api_endpoint}")
    print(f"Testing connection...")
    print()

    try:
        client = QlikAPIClient()
        
        # Test retrieving tasks
        print("Fetching task list...")
        tasks = client.get_all_tasks()
        
        print(f"✓ Successfully connected to Qlik API")
        print(f"✓ Retrieved {len(tasks)} tasks")
        print()

        if tasks:
            print("Sample Tasks:")
            for i, task in enumerate(tasks[:5], 1):
                print(f"  {i}. {task.task_name} ({task.task_id})")
                print(f"     Status: {task.status.value}")
                print(f"     Lag: {task.metrics.lag_seconds}s")
                print()
        
        return 0

    except Exception as e:
        print(f"✗ Connection failed: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

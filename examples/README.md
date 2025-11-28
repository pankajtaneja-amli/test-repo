# Examples

This directory contains example scripts for testing and running the Qlik Monitoring Agent locally.

## Available Examples

### 1. Test Qlik Connection (`test_qlik_connection.py`)

Test connectivity to the Qlik Replicate API.

**Usage:**
```bash
# Set environment variables
export QLIK_API_ENDPOINT="https://your-qlik-server.com/api"
export QLIK_API_TOKEN="your-token"

# Run the test
python examples/test_qlik_connection.py
```

**What it does:**
- Validates API credentials
- Tests connection to Qlik server
- Lists available replication tasks
- Shows basic task information

### 2. Run Monitoring Cycle (`run_monitoring.py`)

Execute a complete monitoring cycle locally.

**Usage:**
```bash
# Set all required environment variables
export QLIK_API_ENDPOINT="https://your-qlik-server.com/api"
export QLIK_API_TOKEN="your-token"
export OPENAI_API_KEY="your-openai-key"
export OPENAI_MODEL="gpt-4"

# Run the monitoring cycle
python examples/run_monitoring.py
```

**What it does:**
- Retrieves all replication tasks
- Detects anomalies in each task
- Generates AI-powered restart proposals
- Displays comprehensive monitoring results

## Configuration

All examples read configuration from environment variables. You can also use a `.env` file:

```bash
# Copy the example
cp .env.example .env

# Edit with your values
nano .env

# Run examples (they will automatically load .env)
python examples/run_monitoring.py
```

## Prerequisites

Install dependencies before running examples:

```bash
pip install -r requirements.txt
```

## Testing Without Real Services

For testing without access to Qlik or OpenAI:

1. **Mock Qlik API**: Use a mock server or modify the client to return sample data
2. **Skip OpenAI**: Comment out OpenAI analysis calls in the monitoring cycle
3. **Use Test Data**: Create sample `ReplicationTask` objects directly

Example with test data:

```python
from src.qlik_monitor.core.models import ReplicationTask, TaskStatus, TaskMetrics

# Create test task
test_task = ReplicationTask(
    task_id="test-1",
    task_name="Test Replication Task",
    status=TaskStatus.RUNNING,
    source_endpoint="test-source",
    target_endpoint="test-target",
    metrics=TaskMetrics(lag_seconds=1200)  # High lag
)

# Test anomaly detection
from src.qlik_monitor.core.monitor import TaskMonitor
monitor = TaskMonitor()
anomalies = monitor.detect_anomalies(test_task)
print(f"Detected {len(anomalies)} anomalies")
```

## Troubleshooting

### Connection Errors

If you get connection errors:
- Verify `QLIK_API_ENDPOINT` is correct and accessible
- Check firewall/network rules
- Validate API token is active and has proper permissions

### Import Errors

If you get import errors:
- Ensure you're in the project root directory
- Verify all dependencies are installed: `pip install -r requirements.txt`
- Check Python version is 3.9+

### OpenAI Errors

If OpenAI calls fail:
- Verify API key is valid
- Check you have sufficient API credits
- Ensure model name is correct (e.g., "gpt-4", not "GPT-4")

## Next Steps

After testing locally:
1. Review the [Deployment Guide](../docs/DEPLOYMENT.md)
2. Deploy Lambda functions to AWS
3. Configure Step Functions workflow
4. Set up scheduled monitoring

# Configuration Guide

## Overview

The Qlik Monitoring Agent is configured through environment variables. This guide explains all available configuration options.

## Configuration File

The agent loads configuration from a `.env` file. Copy the template:

```bash
cp .env.example .env
```

## Configuration Sections

### Qlik API Configuration

```bash
# Qlik Replicate REST API endpoint
QLIK_API_ENDPOINT=https://your-qlik-server.com/api

# API authentication token
# Obtain from Qlik Replicate UI: Tools > Manage API Keys
QLIK_API_TOKEN=your-api-token-here
```

**Notes:**
- Endpoint should be the base URL without trailing slash
- API token requires permissions: Task Read, Task Control
- Use HTTPS for production environments

### OpenAI Configuration

```bash
# OpenAI API key from https://platform.openai.com/api-keys
OPENAI_API_KEY=sk-...

# Model to use for analysis
# Options: gpt-4, gpt-4-turbo, gpt-3.5-turbo
OPENAI_MODEL=gpt-4
```

**Model Selection:**
- `gpt-4`: Best accuracy, higher cost
- `gpt-4-turbo`: Good balance of speed and accuracy
- `gpt-3.5-turbo`: Faster, lower cost, less accurate

**Cost Considerations:**
- GPT-4: ~$0.03/1K tokens (input), ~$0.06/1K tokens (output)
- GPT-3.5-turbo: ~$0.001/1K tokens
- Average analysis: 500-1000 tokens per task

### AWS Configuration

```bash
# AWS region for deployment
AWS_REGION=us-east-1

# AWS account ID
AWS_ACCOUNT_ID=123456789012

# Step Function ARN (after deployment)
STEP_FUNCTION_ARN=arn:aws:states:us-east-1:123456789012:stateMachine:QlikMonitoringAgent
```

### Monitoring Configuration

```bash
# Monitoring interval in seconds (for scheduled runs)
MONITORING_INTERVAL_SECONDS=300

# Lag threshold in seconds
# Tasks with lag above this trigger high_lag anomaly
LAG_THRESHOLD_SECONDS=600

# Stuck task threshold in seconds
# Tasks with no updates for this duration are considered stuck
STUCK_TASK_THRESHOLD_SECONDS=1800

# Maximum restart attempts per task
# Prevents infinite restart loops
MAX_RESTART_ATTEMPTS=3
```

**Threshold Guidelines:**

| Workload Type | Lag Threshold | Stuck Threshold |
|---------------|---------------|-----------------|
| Real-time CDC | 300-600s      | 1800s           |
| Batch loads   | 1800-3600s    | 7200s           |
| Low priority  | 3600s+        | 14400s          |

### Logging Configuration

```bash
# Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL=INFO
```

**Log Levels:**
- `DEBUG`: Detailed information for debugging
- `INFO`: General informational messages (recommended)
- `WARNING`: Warning messages only
- `ERROR`: Error messages only
- `CRITICAL`: Critical errors only

## AWS Secrets Manager Configuration

For production deployments, store sensitive values in AWS Secrets Manager:

### Store Secrets

```bash
# Qlik API Token
aws secretsmanager create-secret \
  --name QlikMonitor/QlikAPIToken \
  --secret-string "your-qlik-api-token"

# OpenAI API Key
aws secretsmanager create-secret \
  --name QlikMonitor/OpenAIKey \
  --secret-string "your-openai-api-key"
```

### Update Lambda Configuration

Modify Lambda environment to reference secrets:

```bash
aws lambda update-function-configuration \
  --function-name QlikMonitor-MonitorTasks \
  --environment Variables="{
    QLIK_API_ENDPOINT=https://your-qlik-server.com/api,
    QLIK_API_TOKEN_SECRET=QlikMonitor/QlikAPIToken,
    OPENAI_KEY_SECRET=QlikMonitor/OpenAIKey,
    OPENAI_MODEL=gpt-4
  }"
```

### Update Code to Read Secrets

Add secret retrieval logic in Lambda:

```python
import boto3
from botocore.exceptions import ClientError

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return response['SecretString']
    except ClientError as e:
        raise e

# Use in Lambda
if 'QLIK_API_TOKEN_SECRET' in os.environ:
    qlik_token = get_secret(os.environ['QLIK_API_TOKEN_SECRET'])
else:
    qlik_token = os.environ.get('QLIK_API_TOKEN')
```

## Advanced Configuration

### Custom Anomaly Detection Rules

Edit `src/qlik_monitor/core/monitor.py` to add custom rules:

```python
# Example: Custom error rate threshold
if task.metrics.error_count > CUSTOM_ERROR_THRESHOLD:
    anomalies.append(TaskAnomaly(
        task_id=task.task_id,
        task_name=task.task_name,
        anomaly_type="custom_high_errors",
        severity="high",
        description=f"Custom error threshold exceeded"
    ))
```

### OpenAI Prompt Customization

Modify prompts in `src/qlik_monitor/integrations/openai_analyzer.py`:

```python
def _build_anomaly_analysis_prompt(self, task, anomaly):
    return f"""
    [Custom prompt template]
    
    Task: {task.task_name}
    Anomaly: {anomaly.description}
    
    [Custom instructions]
    """
```

### Step Functions Workflow Customization

Edit `step_functions/state_machine.json` to:
- Add custom approval steps
- Include notification states
- Add conditional branching
- Integrate with other services

## Environment-Specific Configurations

### Development

```bash
# .env.dev
QLIK_API_ENDPOINT=https://dev-qlik.example.com/api
OPENAI_MODEL=gpt-3.5-turbo  # Lower cost for testing
LOG_LEVEL=DEBUG
MONITORING_INTERVAL_SECONDS=600  # Less frequent
MAX_RESTART_ATTEMPTS=1  # Conservative in dev
```

### Production

```bash
# .env.prod
QLIK_API_ENDPOINT=https://prod-qlik.example.com/api
OPENAI_MODEL=gpt-4  # Best accuracy
LOG_LEVEL=INFO
MONITORING_INTERVAL_SECONDS=300
MAX_RESTART_ATTEMPTS=3
```

## Configuration Validation

Validate configuration before deployment:

```python
from qlik_monitor.config import config

# Check required values
assert config.qlik.api_endpoint, "QLIK_API_ENDPOINT required"
assert config.qlik.api_token, "QLIK_API_TOKEN required"
assert config.openai.api_key, "OPENAI_API_KEY required"

# Check thresholds
assert config.monitoring.lag_threshold_seconds > 0
assert config.monitoring.stuck_task_threshold_seconds > 0

print("Configuration valid!")
```

## Troubleshooting

### Common Configuration Issues

**Issue**: "Failed to connect to Qlik API"
- Verify `QLIK_API_ENDPOINT` is correct
- Check network/firewall rules
- Validate API token is active

**Issue**: "OpenAI API rate limit exceeded"
- Increase `MONITORING_INTERVAL_SECONDS`
- Use `gpt-3.5-turbo` for lower-priority tasks
- Request rate limit increase from OpenAI

**Issue**: "Too many false positives"
- Increase threshold values
- Adjust anomaly severity levels
- Customize detection rules

## Best Practices

1. **Use Secrets Manager** for production credentials
2. **Start conservative** with thresholds, adjust based on data
3. **Monitor costs** for OpenAI API usage
4. **Test configuration** in dev before production
5. **Version control** configuration templates
6. **Document changes** to custom rules
7. **Regular reviews** of threshold effectiveness

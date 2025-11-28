# Deployment Guide

## Prerequisites

- AWS Account with appropriate permissions
- Python 3.9 or later
- AWS CLI configured
- Qlik Replicate instance with REST API enabled
- OpenAI API key

## Step 1: Environment Setup

### 1.1 Clone the Repository

```bash
git clone https://github.com/pankajtaneja-amli/test-repo.git
cd test-repo
```

### 1.2 Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 1.3 Configure Environment Variables

```bash
cp .env.example .env
# Edit .env with your configuration
```

Required configuration:
- `QLIK_API_ENDPOINT`: Your Qlik Replicate API endpoint
- `QLIK_API_TOKEN`: API authentication token
- `OPENAI_API_KEY`: OpenAI API key
- `AWS_REGION`: AWS region for deployment
- `AWS_ACCOUNT_ID`: Your AWS account ID

## Step 2: AWS IAM Setup

### 2.1 Create Lambda Execution Role

```bash
aws iam create-role \
  --role-name QlikMonitorLambdaRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'
```

### 2.2 Attach Policies

```bash
# CloudWatch Logs access
aws iam attach-role-policy \
  --role-name QlikMonitorLambdaRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Secrets Manager access (for API keys)
aws iam attach-role-policy \
  --role-name QlikMonitorLambdaRole \
  --policy-arn arn:aws:iam::aws:policy/SecretsManagerReadWrite
```

### 2.3 Create Step Functions Execution Role

```bash
aws iam create-role \
  --role-name QlikMonitorStepFunctionsRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "states.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach policy to invoke Lambda functions
aws iam put-role-policy \
  --role-name QlikMonitorStepFunctionsRole \
  --policy-name LambdaInvokePolicy \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:*:*:function:QlikMonitor-*"
    }]
  }'
```

## Step 3: Store Secrets in AWS Secrets Manager

```bash
# Store Qlik API credentials
aws secretsmanager create-secret \
  --name QlikMonitor/QlikAPIToken \
  --secret-string "your-qlik-api-token"

# Store OpenAI API key
aws secretsmanager create-secret \
  --name QlikMonitor/OpenAIKey \
  --secret-string "your-openai-api-key"
```

## Step 4: Package and Deploy Lambda Functions

### 4.1 Create Deployment Package

```bash
# Create deployment directory
mkdir -p deployment
cd deployment

# Copy Lambda functions
cp -r ../lambda .
cp -r ../src .
cp ../requirements.txt .

# Install dependencies
pip install -r requirements.txt -t .

# Create ZIP package
zip -r ../qlik-monitor-lambda.zip .
cd ..
```

### 4.2 Deploy Lambda Functions

```bash
# Monitor Tasks Lambda
aws lambda create-function \
  --function-name QlikMonitor-MonitorTasks \
  --runtime python3.9 \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/QlikMonitorLambdaRole \
  --handler lambda.monitor_tasks.lambda_handler \
  --zip-file fileb://qlik-monitor-lambda.zip \
  --timeout 300 \
  --memory-size 512 \
  --environment Variables="{
    QLIK_API_ENDPOINT=https://your-qlik-server.com/api,
    OPENAI_MODEL=gpt-4,
    AWS_REGION=us-east-1,
    LOG_LEVEL=INFO
  }"

# Evaluate Proposal Lambda
aws lambda create-function \
  --function-name QlikMonitor-EvaluateProposal \
  --runtime python3.9 \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/QlikMonitorLambdaRole \
  --handler lambda.evaluate_proposal.lambda_handler \
  --zip-file fileb://qlik-monitor-lambda.zip \
  --timeout 120 \
  --memory-size 256

# Execute Restart Lambda
aws lambda create-function \
  --function-name QlikMonitor-ExecuteRestart \
  --runtime python3.9 \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/QlikMonitorLambdaRole \
  --handler lambda.execute_restart.lambda_handler \
  --zip-file fileb://qlik-monitor-lambda.zip \
  --timeout 60 \
  --memory-size 256
```

## Step 5: Deploy Step Functions State Machine

### 5.1 Update State Machine Definition

```bash
cd step_functions
# Replace placeholders with your values
sed -i "s/\${AWS_REGION}/us-east-1/g" state_machine.json
sed -i "s/\${AWS_ACCOUNT_ID}/YOUR_ACCOUNT_ID/g" state_machine.json
```

### 5.2 Create State Machine

```bash
aws stepfunctions create-state-machine \
  --name QlikMonitoringAgent \
  --definition file://state_machine.json \
  --role-arn arn:aws:iam::YOUR_ACCOUNT_ID:role/QlikMonitorStepFunctionsRole
```

## Step 6: Configure Scheduled Execution

### 6.1 Create EventBridge Rule

```bash
aws events put-rule \
  --name QlikMonitoringSchedule \
  --schedule-expression "rate(5 minutes)" \
  --state ENABLED
```

### 6.2 Add Step Functions as Target

```bash
# Get state machine ARN
STATE_MACHINE_ARN=$(aws stepfunctions list-state-machines \
  --query "stateMachines[?name=='QlikMonitoringAgent'].stateMachineArn" \
  --output text)

# Create EventBridge role for Step Functions
aws iam create-role \
  --role-name EventBridgeStepFunctionsRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "events.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

aws iam put-role-policy \
  --role-name EventBridgeStepFunctionsRole \
  --policy-name StartStepFunctions \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [{
      \"Effect\": \"Allow\",
      \"Action\": \"states:StartExecution\",
      \"Resource\": \"${STATE_MACHINE_ARN}\"
    }]
  }"

# Add target to EventBridge rule
aws events put-targets \
  --rule QlikMonitoringSchedule \
  --targets "Id"="1","Arn"="${STATE_MACHINE_ARN}","RoleArn"="arn:aws:iam::YOUR_ACCOUNT_ID:role/EventBridgeStepFunctionsRole"
```

## Step 7: Verification

### 7.1 Test Lambda Functions Locally

```bash
# Test monitor function
python lambda/monitor_tasks.py
```

### 7.2 Trigger Manual Execution

```bash
aws stepfunctions start-execution \
  --state-machine-arn ${STATE_MACHINE_ARN} \
  --name manual-test-$(date +%s)
```

### 7.3 View Execution Results

```bash
# List recent executions
aws stepfunctions list-executions \
  --state-machine-arn ${STATE_MACHINE_ARN} \
  --max-results 5

# Get execution details
aws stepfunctions describe-execution \
  --execution-arn <execution-arn>
```

### 7.4 Check CloudWatch Logs

```bash
# View Lambda logs
aws logs tail /aws/lambda/QlikMonitor-MonitorTasks --follow
```

## Step 8: Monitoring and Alerts

### 8.1 Create CloudWatch Alarms

```bash
# Alarm for failed executions
aws cloudwatch put-metric-alarm \
  --alarm-name QlikMonitor-ExecutionFailures \
  --alarm-description "Alert on Step Functions execution failures" \
  --metric-name ExecutionsFailed \
  --namespace AWS/States \
  --statistic Sum \
  --period 300 \
  --threshold 1 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 1 \
  --dimensions Name=StateMachineArn,Value=${STATE_MACHINE_ARN}
```

### 8.2 Setup SNS for Notifications

```bash
# Create SNS topic
aws sns create-topic --name QlikMonitorAlerts

# Subscribe email
aws sns subscribe \
  --topic-arn arn:aws:sns:REGION:ACCOUNT:QlikMonitorAlerts \
  --protocol email \
  --notification-endpoint your-email@example.com
```

## Troubleshooting

### Lambda Function Issues
- Check CloudWatch Logs for errors
- Verify IAM role permissions
- Ensure secrets are accessible
- Check Lambda timeout settings

### Step Functions Issues
- Review execution history in AWS Console
- Check Lambda function invocation errors
- Verify state machine IAM role

### API Connection Issues
- Test Qlik API endpoint accessibility
- Verify API token validity
- Check network/VPC configuration

## Cleanup

To remove all deployed resources:

```bash
# Delete EventBridge rule
aws events remove-targets --rule QlikMonitoringSchedule --ids 1
aws events delete-rule --name QlikMonitoringSchedule

# Delete State Machine
aws stepfunctions delete-state-machine --state-machine-arn ${STATE_MACHINE_ARN}

# Delete Lambda functions
aws lambda delete-function --function-name QlikMonitor-MonitorTasks
aws lambda delete-function --function-name QlikMonitor-EvaluateProposal
aws lambda delete-function --function-name QlikMonitor-ExecuteRestart

# Delete IAM roles
aws iam delete-role --role-name QlikMonitorLambdaRole
aws iam delete-role --role-name QlikMonitorStepFunctionsRole
aws iam delete-role --role-name EventBridgeStepFunctionsRole

# Delete secrets
aws secretsmanager delete-secret --secret-id QlikMonitor/QlikAPIToken
aws secretsmanager delete-secret --secret-id QlikMonitor/OpenAIKey
```

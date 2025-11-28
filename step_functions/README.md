# AWS Step Functions State Machine for Qlik Monitoring

This directory contains the AWS Step Functions state machine definition for orchestrating the Qlik replication task monitoring workflow.

## State Machine Overview

The state machine implements the following workflow:

1. **MonitorTasks**: Executes Lambda function to monitor all Qlik replication tasks
2. **CheckAnomalies**: Evaluates if any anomalies were detected
3. **ProcessProposals**: Processes each restart proposal (parallel execution)
   - **EvaluateProposal**: AI-powered evaluation of the proposal
   - **CheckApproval**: Determines if human approval is needed
   - **WaitForApproval**: Waits for human decision (if required)
   - **ExecuteRestart**: Performs the task restart
4. **GenerateReport**: Creates final monitoring report
5. **Success**: Completes workflow successfully

## Deployment

To deploy this state machine:

```bash
# Replace placeholders in state_machine.json
sed -i 's/${AWS_REGION}/us-east-1/g' state_machine.json
sed -i 's/${AWS_ACCOUNT_ID}/your-account-id/g' state_machine.json

# Create the state machine
aws stepfunctions create-state-machine \
  --name QlikMonitoringAgent \
  --definition file://state_machine.json \
  --role-arn arn:aws:iam::your-account-id:role/StepFunctionsExecutionRole
```

## Triggering the Workflow

### Manual Execution

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:region:account:stateMachine:QlikMonitoringAgent \
  --input '{}'
```

### Scheduled Execution (EventBridge)

```bash
aws events put-rule \
  --name QlikMonitoringSchedule \
  --schedule-expression "rate(5 minutes)"

aws events put-targets \
  --rule QlikMonitoringSchedule \
  --targets "Id"="1","Arn"="arn:aws:states:region:account:stateMachine:QlikMonitoringAgent","RoleArn"="arn:aws:iam::account:role/EventBridgeRole"
```

## Human Approval Integration

When a restart proposal requires approval, the state machine will pause at the `WaitForApproval` state and wait for a callback. You can approve or deny using:

```bash
# Approve
aws stepfunctions send-task-success \
  --task-token <token-from-waiting-state> \
  --task-output '{"approved": true}'

# Deny
aws stepfunctions send-task-failure \
  --task-token <token-from-waiting-state> \
  --error "ApprovalDenied" \
  --cause "User denied restart approval"
```

## Monitoring Executions

View execution history:

```bash
aws stepfunctions list-executions \
  --state-machine-arn arn:aws:states:region:account:stateMachine:QlikMonitoringAgent

aws stepfunctions describe-execution \
  --execution-arn <execution-arn>
```

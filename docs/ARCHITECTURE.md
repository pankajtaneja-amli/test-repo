# Architecture Overview

## System Architecture

The Qlik Replication Monitoring Agent is a cloud-native, AI-powered monitoring solution built on AWS services.

### High-Level Components

```
┌─────────────────────────────────────────────────────────────────┐
│                      AWS Step Functions                         │
│                   (Orchestration Layer)                         │
└────────────┬────────────────────────────────────┬───────────────┘
             │                                    │
             ▼                                    ▼
┌────────────────────────┐           ┌────────────────────────┐
│   AWS Lambda Functions │           │   EventBridge/         │
│   - Monitor Tasks      │           │   CloudWatch           │
│   - Evaluate Proposals │           │   (Scheduling/Logs)    │
│   - Execute Restarts   │           │                        │
└────────┬───────────────┘           └────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Core Monitoring Agent                        │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────┐  │
│  │ Task Monitor │  │ Qlik API     │  │ OpenAI Analyzer     │  │
│  │ (Anomaly     │  │ Client       │  │ (Intelligence)      │  │
│  │  Detection)  │  │              │  │                     │  │
│  └──────────────┘  └──────────────┘  └─────────────────────┘  │
└────────┬─────────────────────┬─────────────────────┬───────────┘
         │                     │                     │
         ▼                     ▼                     ▼
┌────────────────┐   ┌──────────────────┐   ┌─────────────────┐
│ Qlik Replicate │   │ OpenAI API       │   │ AWS Services    │
│ REST API       │   │ (GPT-4)          │   │ (SNS, SQS, etc) │
└────────────────┘   └──────────────────┘   └─────────────────┘
```

## Core Components

### 1. Monitoring Agent (`src/qlik_monitor/core/agent.py`)
The main orchestrator that coordinates all monitoring activities:
- Retrieves tasks from Qlik API
- Detects anomalies using TaskMonitor
- Analyzes anomalies using OpenAI
- Generates restart proposals
- Executes approved restarts

### 2. Task Monitor (`src/qlik_monitor/core/monitor.py`)
Rule-based anomaly detection system that identifies:
- **Aborted tasks**: Tasks in ABORTED state
- **Stuck tasks**: No updates for extended period
- **High lag**: Replication lag exceeds threshold
- **High errors**: Error count above threshold
- **Low performance**: Processing rate below acceptable level

### 3. Qlik API Client (`src/qlik_monitor/integrations/qlik_client.py`)
REST API client for Qlik Replicate:
- Retrieves task lists and details
- Fetches real-time metrics
- Executes restart/stop operations
- Includes retry logic and error handling

### 4. OpenAI Analyzer (`src/qlik_monitor/integrations/openai_analyzer.py`)
AI-powered decision engine using GPT-4:
- Analyzes detected anomalies
- Proposes intelligent remediation actions
- Evaluates restart decisions
- Provides confidence scores
- Generates detailed reasoning

### 5. AWS Lambda Functions (`lambda/`)
Serverless execution handlers:
- **monitor_tasks.py**: Runs monitoring cycle
- **evaluate_proposal.py**: Evaluates restart proposals
- **execute_restart.py**: Executes approved restarts

### 6. AWS Step Functions (`step_functions/state_machine.json`)
Orchestration workflow that:
- Triggers monitoring on schedule
- Processes proposals in parallel
- Handles human approval workflow
- Generates final reports

## Data Flow

1. **Monitoring Trigger**: EventBridge triggers Step Functions on schedule
2. **Task Retrieval**: Lambda fetches all tasks from Qlik API
3. **Anomaly Detection**: Rule-based system identifies issues
4. **AI Analysis**: OpenAI evaluates each anomaly and proposes actions
5. **Approval Workflow**: Critical actions require human approval
6. **Execution**: Approved restarts are executed via Qlik API
7. **Reporting**: Results logged to CloudWatch

## Key Features

### Intelligent Decision Making
- **AI-Powered Analysis**: Uses GPT-4 to analyze complex scenarios
- **Confidence Scoring**: Every proposal includes confidence level
- **Context-Aware**: Considers task history and metrics

### Automated Monitoring
- **Continuous Monitoring**: Runs on configurable schedule
- **Multi-Metric Analysis**: Lag, errors, performance, updates
- **Threshold-Based**: Configurable thresholds for each metric

### Human-in-the-Loop
- **Approval Workflow**: Critical actions require approval
- **Task Callbacks**: Step Functions waits for human decision
- **Audit Trail**: All actions logged

### Scalability & Reliability
- **Serverless**: Auto-scales with workload
- **Parallel Processing**: Multiple proposals processed simultaneously
- **Retry Logic**: Built-in retry for transient failures
- **Error Handling**: Comprehensive exception handling

## Configuration

All components are configured via environment variables:
- Qlik API credentials and endpoints
- OpenAI API key and model selection
- AWS region and resource ARNs
- Monitoring thresholds and intervals
- Logging levels

See `.env.example` for complete configuration options.

## Security Considerations

1. **API Keys**: Stored in AWS Secrets Manager or Parameter Store
2. **IAM Roles**: Least-privilege access for Lambda functions
3. **Network**: VPC endpoints for private API access
4. **Encryption**: In-transit and at-rest encryption
5. **Audit**: CloudTrail logging for all API calls

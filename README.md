# Qlik Replication Monitoring Agent

An intelligent, cloud-native agent for monitoring Qlik replication tasks with automated anomaly detection, AI-powered decision-making, and orchestrated remediation workflows.

## 🎯 Overview

This agent provides automated, smart monitoring of Qlik replication tasks by:
- **Continuously monitoring** all replication activities in real-time
- **Detecting anomalies** such as stuck, lagging, or aborted tasks
- **Intelligent analysis** using OpenAI models for decision-making
- **Automated remediation** with smart restart proposals
- **Human-in-the-loop** approval workflow for critical actions
- **Orchestration** via AWS Step Functions for reliability and scalability

## 🏗️ Architecture

The solution consists of three main layers:

1. **Orchestration Layer**: AWS Step Functions manages the workflow
2. **Intelligence Layer**: OpenAI GPT-4 provides intelligent analysis and decision-making
3. **Integration Layer**: Qlik API client for task monitoring and control

```
EventBridge (Schedule) → Step Functions → Lambda Functions → Qlik API
                              ↓
                          OpenAI API (Decision Engine)
                              ↓
                        Restart Proposals → Human Approval → Execute
```

See [Architecture Documentation](docs/ARCHITECTURE.md) for detailed information.

## ✨ Features

### Intelligent Monitoring
- **Multi-metric analysis**: Monitors lag, error rates, processing speed, and task health
- **Configurable thresholds**: Customize when alerts and actions trigger
- **Real-time detection**: Identifies issues as they occur

### AI-Powered Decision Making
- **Contextual analysis**: OpenAI evaluates task state with full context
- **Confidence scoring**: Every proposal includes a confidence level
- **Detailed reasoning**: Understand why actions are recommended

### Automated Remediation
- **Smart restarts**: Automatically restarts tasks when appropriate
- **Approval workflows**: Critical actions require human approval
- **Retry limits**: Prevents restart loops

### Production-Ready
- **Serverless**: Auto-scales with AWS Lambda
- **Reliable**: Built-in retry logic and error handling
- **Observable**: Comprehensive logging to CloudWatch
- **Secure**: API keys stored in AWS Secrets Manager

## 🚀 Quick Start

### Prerequisites
- AWS Account with CLI configured
- Python 3.9+
- Qlik Replicate with REST API enabled
- OpenAI API key

### Installation

1. Clone the repository:
```bash
git clone https://github.com/pankajtaneja-amli/test-repo.git
cd test-repo
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment:
```bash
cp .env.example .env
# Edit .env with your configuration
```

4. Deploy to AWS:
```bash
# See detailed deployment guide
cat docs/DEPLOYMENT.md
```

### Local Testing

Test the monitoring agent locally:

```bash
# Set environment variables
export QLIK_API_ENDPOINT="https://your-qlik-server.com/api"
export QLIK_API_TOKEN="your-token"
export OPENAI_API_KEY="your-openai-key"

# Run monitoring cycle
python lambda/monitor_tasks.py
```

## 📖 Documentation

- **[Architecture](docs/ARCHITECTURE.md)**: System design and components
- **[Deployment Guide](docs/DEPLOYMENT.md)**: Step-by-step deployment instructions
- **[Configuration](docs/CONFIGURATION.md)**: Configuration options and best practices
- **[API Reference](docs/API.md)**: Lambda function interfaces and data models

## 🔧 Configuration

Key configuration options in `.env`:

```bash
# Qlik Configuration
QLIK_API_ENDPOINT=https://your-qlik-server.com/api
QLIK_API_TOKEN=your-token

# OpenAI Configuration  
OPENAI_API_KEY=your-key
OPENAI_MODEL=gpt-4

# Monitoring Thresholds
MONITORING_INTERVAL_SECONDS=300
LAG_THRESHOLD_SECONDS=600
STUCK_TASK_THRESHOLD_SECONDS=1800
MAX_RESTART_ATTEMPTS=3
```

See `.env.example` for all available options.

## 📊 Monitoring Dashboard

Access monitoring data through:

1. **AWS Step Functions Console**: View workflow executions
2. **CloudWatch Logs**: Detailed execution logs
3. **CloudWatch Metrics**: Execution statistics
4. **SNS Notifications**: Alert on failures

## 🔐 Security

- API credentials stored in AWS Secrets Manager
- Least-privilege IAM roles for all components
- Encrypted data in transit and at rest
- Audit logging via CloudTrail
- VPC endpoints for private API access

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙋 Support

For issues, questions, or feature requests:
- Open an [Issue](https://github.com/pankajtaneja-amli/test-repo/issues)
- Check [Documentation](docs/)
- Review [Architecture](docs/ARCHITECTURE.md)

## 🎯 Roadmap

- [ ] Dashboard UI for monitoring and approvals
- [ ] Multi-region support
- [ ] Custom alerting rules
- [ ] Integration with other monitoring tools
- [ ] Machine learning for predictive maintenance
- [ ] Historical trend analysis

## 👏 Acknowledgments

Built with:
- [AWS Step Functions](https://aws.amazon.com/step-functions/)
- [AWS Lambda](https://aws.amazon.com/lambda/)
- [OpenAI API](https://openai.com/api/)
- [Qlik Replicate](https://www.qlik.com/us/products/qlik-replicate)
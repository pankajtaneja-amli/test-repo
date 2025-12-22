"""
Configuration module for Qlik Replication Monitoring Agent.
Handles loading and validation of environment variables and settings.
"""
import os
from typing import Optional
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class QlikConfig(BaseModel):
    """Qlik API configuration."""
    api_endpoint: str = Field(default=os.getenv("QLIK_API_ENDPOINT", ""))
    api_token: str = Field(default=os.getenv("QLIK_API_TOKEN", ""))


class OpenAIConfig(BaseModel):
    """OpenAI configuration."""
    api_key: str = Field(default=os.getenv("OPENAI_API_KEY", ""))
    model: str = Field(default=os.getenv("OPENAI_MODEL", "gpt-4"))


class AWSConfig(BaseModel):
    """AWS configuration."""
    region: str = Field(default=os.getenv("AWS_REGION", "us-east-1"))
    account_id: str = Field(default=os.getenv("AWS_ACCOUNT_ID", ""))
    step_function_arn: str = Field(default=os.getenv("STEP_FUNCTION_ARN", ""))


class MonitoringConfig(BaseModel):
    """Monitoring configuration."""
    interval_seconds: int = Field(
        default=int(os.getenv("MONITORING_INTERVAL_SECONDS", "300"))
    )
    lag_threshold_seconds: int = Field(
        default=int(os.getenv("LAG_THRESHOLD_SECONDS", "600"))
    )
    stuck_task_threshold_seconds: int = Field(
        default=int(os.getenv("STUCK_TASK_THRESHOLD_SECONDS", "1800"))
    )
    max_restart_attempts: int = Field(
        default=int(os.getenv("MAX_RESTART_ATTEMPTS", "3"))
    )


class AppConfig(BaseModel):
    """Main application configuration."""
    qlik: QlikConfig = Field(default_factory=QlikConfig)
    openai: OpenAIConfig = Field(default_factory=OpenAIConfig)
    aws: AWSConfig = Field(default_factory=AWSConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    log_level: str = Field(default=os.getenv("LOG_LEVEL", "INFO"))


# Global configuration instance
config = AppConfig()

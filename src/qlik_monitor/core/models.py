"""
Data models for Qlik replication tasks and monitoring states.
"""
from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    """Qlik replication task status."""
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    ERROR = "ERROR"
    STUCK = "STUCK"
    LAGGING = "LAGGING"
    ABORTED = "ABORTED"
    UNKNOWN = "UNKNOWN"


class TaskMetrics(BaseModel):
    """Metrics for a replication task."""
    lag_seconds: Optional[int] = Field(default=None, description="Replication lag in seconds")
    records_processed: Optional[int] = Field(default=None, description="Total records processed")
    records_per_second: Optional[float] = Field(default=None, description="Processing rate")
    last_update_time: Optional[datetime] = Field(default=None, description="Last update timestamp")
    error_count: Optional[int] = Field(default=0, description="Number of errors")


class ReplicationTask(BaseModel):
    """Qlik replication task model."""
    task_id: str = Field(..., description="Unique task identifier")
    task_name: str = Field(..., description="Human-readable task name")
    status: TaskStatus = Field(..., description="Current task status")
    source_endpoint: str = Field(..., description="Source database endpoint")
    target_endpoint: str = Field(..., description="Target database endpoint")
    metrics: TaskMetrics = Field(default_factory=TaskMetrics, description="Task metrics")
    restart_attempts: int = Field(default=0, description="Number of restart attempts")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    created_at: Optional[datetime] = Field(default=None)
    updated_at: Optional[datetime] = Field(default=None)


class TaskAnomaly(BaseModel):
    """Detected anomaly in a replication task."""
    task_id: str
    task_name: str
    anomaly_type: str = Field(..., description="Type of anomaly (e.g., 'high_lag', 'stuck', 'aborted')")
    severity: str = Field(..., description="Severity level: 'low', 'medium', 'high', 'critical'")
    description: str = Field(..., description="Human-readable description")
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    metrics_snapshot: Dict[str, Any] = Field(default_factory=dict)


class RestartProposal(BaseModel):
    """Proposal to restart a task."""
    task_id: str
    task_name: str
    reason: str = Field(..., description="Reason for restart proposal")
    confidence: float = Field(..., ge=0.0, le=1.0, description="AI confidence in the proposal")
    suggested_action: str = Field(..., description="Suggested action to take")
    analysis: str = Field(..., description="Detailed analysis from AI")
    requires_approval: bool = Field(default=True, description="Whether user approval is required")
    created_at: datetime = Field(default_factory=datetime.utcnow)


class MonitoringState(BaseModel):
    """Overall monitoring state."""
    total_tasks: int = 0
    healthy_tasks: int = 0
    lagging_tasks: int = 0
    stuck_tasks: int = 0
    aborted_tasks: int = 0
    error_tasks: int = 0
    last_check_time: Optional[datetime] = None

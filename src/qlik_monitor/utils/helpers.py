"""
Utility functions for the Qlik monitoring agent.
"""
import logging
import structlog
from typing import Optional


def setup_logging(log_level: str = "INFO") -> None:
    """
    Configure structured logging for the application.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Configure standard logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Configure structlog for structured logging
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def format_seconds(seconds: Optional[int]) -> str:
    """
    Format seconds into human-readable time string.

    Args:
        seconds: Number of seconds

    Returns:
        Formatted string (e.g., "2h 30m", "45s")
    """
    if seconds is None:
        return "N/A"

    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes}m"
    elif seconds < 86400:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"
    else:
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        return f"{days}d {hours}h"


def format_rate(rate: Optional[float]) -> str:
    """
    Format processing rate to human-readable string.

    Args:
        rate: Records per second

    Returns:
        Formatted string (e.g., "1.5K rec/s")
    """
    if rate is None:
        return "N/A"

    if rate < 1000:
        return f"{rate:.1f} rec/s"
    elif rate < 1000000:
        return f"{rate/1000:.1f}K rec/s"
    else:
        return f"{rate/1000000:.1f}M rec/s"


def calculate_health_score(
    healthy: int,
    lagging: int,
    stuck: int,
    aborted: int,
    errors: int
) -> float:
    """
    Calculate overall health score based on task states.

    Args:
        healthy: Number of healthy tasks
        lagging: Number of lagging tasks
        stuck: Number of stuck tasks
        aborted: Number of aborted tasks
        errors: Number of error tasks

    Returns:
        Health score between 0.0 and 1.0
    """
    total = healthy + lagging + stuck + aborted + errors
    if total == 0:
        return 1.0

    # Weight different issues
    score = (
        healthy * 1.0 +
        lagging * 0.7 +
        stuck * 0.3 +
        aborted * 0.0 +
        errors * 0.0
    ) / total

    return max(0.0, min(1.0, score))

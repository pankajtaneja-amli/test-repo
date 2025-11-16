"""
Pytest configuration file.
"""
import pytest
import os


@pytest.fixture
def mock_env():
    """Fixture to set up mock environment variables."""
    original_env = dict(os.environ)
    
    os.environ.update({
        'QLIK_API_ENDPOINT': 'https://test-qlik.com/api',
        'QLIK_API_TOKEN': 'test-token',
        'OPENAI_API_KEY': 'test-key',
        'OPENAI_MODEL': 'gpt-4',
        'AWS_REGION': 'us-east-1',
        'AWS_ACCOUNT_ID': '123456789012',
        'LAG_THRESHOLD_SECONDS': '600',
        'STUCK_TASK_THRESHOLD_SECONDS': '1800',
        'MAX_RESTART_ATTEMPTS': '3',
        'LOG_LEVEL': 'INFO'
    })
    
    yield
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)

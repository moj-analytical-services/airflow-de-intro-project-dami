import logging
import os
import pytest
from scripts.functions import setup_logging  # Import the setup_logging function from your module


@pytest.fixture
def temp_log_file(tmp_path):
    return str(tmp_path/"test_log.log")

def test_setup_logging_creates_logger(temp_log_file):
    logger = setup_logging(temp_log_file)
    assert isinstance(logger, logging.Logger)

def test_setup_logging_sets_correct_level(temp_log_file):
    logger = setup_logging(temp_log_file, log_level=logging.DEBUG)
    assert logger.level == logging.DEBUG

def test_setup_logging_creates_file(temp_log_file):
    setup_logging(temp_log_file)
    assert os.path.exists(temp_log_file)

def test_logger_writes_to_file(temp_log_file):
    logger = setup_logging(temp_log_file)
    test_message = "Test log message"
    logger.info(test_message)
    
    with open(temp_log_file, 'r') as log_file:
        content = log_file.read()
        assert test_message in content
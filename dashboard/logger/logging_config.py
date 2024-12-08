import logging
import os


def setup_logger():
    # Create logs directory if it doesn't exist
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # Create a logger
    logger = logging.getLogger("PostgresDashboard")
    logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all levels of logs

    # Check if handlers are already added to avoid duplicate logging
    if not logger.handlers:
        # Console handler for displaying logs in the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)  # Log everything to the console

        # File handler for writing logs to a file
        file_handler = logging.FileHandler("logs/dashboard.log")
        file_handler.setLevel(logging.DEBUG)  # Log everything to the file

        # Define a common log format
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Set the formatter for both handlers
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger


# Initialize the logger
logger = setup_logger()

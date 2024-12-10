import logging
import os
import inspect

import inspect
import logging


class ClassNameFilter(logging.Filter):
    def filter(self, record):
        # Start from the filter frame and move outward.
        frame = inspect.currentframe()
        # Move one frame back to exit the current filter() call stack
        frame = frame.f_back

        while frame:
            # Identify the module this frame belongs to
            module_name = frame.f_globals.get("__name__", "")

            # We want to skip frames that are clearly from the logging or inspect modules
            # or from our ClassNameFilter itself.
            if (not module_name.startswith("logging") and
                    module_name != "inspect" and
                    not (module_name == __name__ and frame.f_code.co_name == 'filter')):

                # Check if 'self' is in locals
                if "self" in frame.f_locals:
                    record.classname = frame.f_locals["self"].__class__.__name__
                    break
            frame = frame.f_back
        else:
            # If we never broke out of the loop, no suitable 'self' was found
            record.classname = 'N/A'

        return True


def setup_logger():
    if not os.path.exists("logs"):
        os.makedirs("logs")

    logger = logging.getLogger("PostgresDashboard")
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        file_handler = logging.FileHandler("logs/dashboard.log")
        file_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(classname)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        # Add the class name filter to the logger
        logger.addFilter(ClassNameFilter())

    return logger


# Initialize the logger once
logger = setup_logger()

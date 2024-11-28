import json


def load_config(file_path):
    """Load a JSON configuration file."""
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Configuration file not found: {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Invalid JSON format in configuration file: {file_path}")
        return None

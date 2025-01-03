import os

def get_version():
    version = os.getenv("VERSION")
    if not version:
        raise ValueError("Environment variable VERSION is not set.")
    return version

__VERSION__="1.0.0"
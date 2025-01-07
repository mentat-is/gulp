"""
reads requirements from requirements.txt, then uses pyproject.toml to setup
"""

from setuptools import setup
import os

def get_version():
    version = os.getenv("VERSION")
    if not version:
        raise ValueError("Environment variable VERSION is not set.")
    return version


def get_requirements():
    with open("requirements.txt") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]


setup(
    name="gulp",
    install_requires=get_requirements(),
)

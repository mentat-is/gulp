"""
reads requirements from requirements.txt, then uses pyproject.toml to setup
"""

from setuptools import setup


def get_requirements():
    with open("requirements.txt") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]


setup(
    name="gulp",
    install_requires=get_requirements(),
)

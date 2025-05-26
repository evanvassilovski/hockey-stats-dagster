from setuptools import setup, find_packages

setup(
    name="hockey_stats_project",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-cloud",
        # Add your dependencies
        "numpy",
        "pandas",
        "sqlalchemy",
        "lxml",
        "python-dotenv"
    ],
)

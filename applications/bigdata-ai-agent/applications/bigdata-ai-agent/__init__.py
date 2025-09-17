"""
BigData AI Agent Package
========================

This package provides a set of utilities to profile and tune Apache Spark
jobs running on Hadoop/YARN clusters.  It collects job metadata from the
YARN ResourceManager API and detailed executor metrics from the Spark
History Server REST API.  The agent stores metadata in a MySQL database,
analyses resource usage to categorise jobs, generates tuning
recommendations using the OpenAI ChatCompletion API and notifies
stakeholders via Slack.  It can also commit configuration changes to a
Git repository for version control.

Note: This package contains the building blocks of an automated
monitoring and optimisation system.  It does not perform any actions
automatically on import.  Use the CLI defined in ``main.py`` or call
individual functions programmatically.

The REST endpoints used by the fetchers are documented by Apache.  For
example, the YARN ResourceManager exposes a list of applications at
``/ws/v1/cluster/apps`` which returns JSON records containing memory
seconds, vcore seconds, start and finish times and queue usage
statistics【762643672411904†L4727-L4851】.  The Spark History Server exposes
application-specific metrics under ``/api/v1``; for example, executor
information is available via ``/applications/[app‑id]/executors``【383517666583736†L417-L423】.

"""

from .config import Config
from .database import Database
from .job_fetcher import YarnFetcher, SparkFetcher
from .analyzer import JobAnalyzer
from .recommendation import RecommendationEngine
from .slack_notifier import SlackNotifier
from .git_integration import GitHandler

__all__ = [
    "Config",
    "Database",
    "YarnFetcher",
    "SparkFetcher",
    "JobAnalyzer",
    "RecommendationEngine",
    "SlackNotifier",
    "GitHandler",
]
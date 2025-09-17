"""
Job Fetchers
============

This module provides classes to fetch job and executor metrics from
YARN's ResourceManager API and Spark's History Server API.  The
ResourceManager exposes a list of applications via its web services at
``/ws/v1/cluster/apps`` which returns details about each job such as
memorySeconds, vcoreSeconds and queue usage percentage【762643672411904†L4727-L4851】.
The Spark history server exposes additional metrics per application under
``/api/v1``; for example, a list of executors is available at
``/applications/[app‑id]/executors``【383517666583736†L417-L423】.  These
fetchers encapsulate HTTP interactions and provide Python objects for
further processing.

The fetchers do not cache results; callers should implement caching if
they expect high call volumes.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

import requests

from .config import Config

LOGGER = logging.getLogger(__name__)


class YarnFetcher:
    """Fetch application metadata from the YARN ResourceManager REST API."""

    def __init__(self, config: Config, session: Optional[requests.Session] = None) -> None:
        self.base_url = config.yarn_api_url.rstrip("/")
        self.session = session or requests.Session()

    def list_applications(
        self,
        states: Optional[Sequence[str]] = None,
        application_types: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve a list of applications from the ResourceManager.

        :param states: Optional list of application states to filter on,
            e.g. ['RUNNING', 'FINISHED'].  According to the YARN API, the
            ``states`` parameter accepts a comma‑separated list【762643672411904†L4745-L4764】.
        :param application_types: Optional list of application types (e.g.
            ['SPARK']).  When not specified, all types are returned.
        :param limit: Optional maximum number of records to return (``limit``
            query parameter).  If ``None``, the API returns all records.
        :returns: List of application dictionaries.  The keys follow the
            YARN app object schema【762643672411904†L4727-L4851】.
        :raises requests.HTTPError: If the HTTP request fails.
        """
        params: Dict[str, str] = {}
        if states:
            params["states"] = ",".join(states)
        if application_types:
            params["applicationTypes"] = ",".join(application_types)
        if limit is not None:
            params["limit"] = str(limit)
        url = f"{self.base_url}/ws/v1/cluster/apps"
        LOGGER.debug("Fetching applications from %s with params %s", url, params)
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        apps = data.get("apps", {}).get("app", [])  # type: ignore
        LOGGER.info("Received %d applications from YARN", len(apps))
        return apps


class SparkFetcher:
    """Fetch metrics from the Spark History Server REST API."""

    def __init__(self, config: Config, session: Optional[requests.Session] = None) -> None:
        self.base_url = config.spark_history_url.rstrip("/")
        self.session = session or requests.Session()

    def list_applications(self) -> List[Dict[str, Any]]:
        """Return a list of Spark applications available on the history server.

        The endpoint ``/api/v1/applications`` returns a JSON array of
        application summaries【383517666583736†L318-L351】.
        """
        url = f"{self.base_url}/api/v1/applications"
        LOGGER.debug("Fetching Spark applications from %s", url)
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        apps: List[Dict[str, Any]] = resp.json()  # type: ignore
        LOGGER.info("Received %d applications from Spark history server", len(apps))
        return apps

    def list_executors(self, app_id: str) -> List[Dict[str, Any]]:
        """Return executor metrics for a specific application.

        According to the Spark documentation, the endpoint
        ``/applications/[app‑id]/executors`` returns metrics per executor,
        including memory and CPU usage【383517666583736†L417-L423】.

        :param app_id: Application ID as returned by YARN.  For jobs
            running in YARN cluster mode, the ID must be in the form
            ``[base-app-id]/[attempt-id]``【383517666583736†L328-L334】.
        :returns: List of executor dictionaries.
        """
        url = f"{self.base_url}/api/v1/applications/{app_id}/executors"
        LOGGER.debug("Fetching executors for %s from %s", app_id, url)
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        executors: List[Dict[str, Any]] = resp.json()  # type: ignore
        LOGGER.info("Received %d executors for %s", len(executors), app_id)
        return executors

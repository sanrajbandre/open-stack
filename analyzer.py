"""
Job Analysis and Categorisation
==============================

The ``JobAnalyzer`` class is responsible for interpreting raw metrics
collected from YARN and Spark and deriving useful insights.  It
computes average CPU and memory usage for each job, determines an SLA
category (hourly or daily) based on elapsed time and flags jobs that
appear over‑allocated or under‑utilised.  The analysis results are
later fed into the recommendation engine.

The calculation of average CPU and memory usage is based on YARN's
``memorySeconds`` and ``vcoreSeconds`` statistics.  These counters
represent the aggregate number of megabyte‑seconds and vcore‑seconds
consumed by the application【762643672411904†L4727-L4851】.  Dividing by
the elapsed time in seconds yields the average megabytes (MB) and
virtual cores used per second over the entire run.  Note that these
figures indicate actual consumption, not just requested container
allocations.

When an elapsed time is not available (e.g. the job is still running),
the analyzer will skip that job.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

LOGGER = logging.getLogger(__name__)


@dataclass
class AnalysisResult:
    cluster_name: str
    app_id: str
    category: str
    avg_cpu: Optional[float]
    avg_memory: Optional[float]
    status: str
    notes: str


class JobAnalyzer:
    """Analyse job statistics and compute SLA categories and utilisation.

    The analyser assigns jobs to one of several categories based on their
    execution time:

    * ``hourly`` – jobs completing in under an hour (<= 3.6e6 ms)
    * ``two_hours`` – jobs running longer than an hour but no more than 2 hours
      (> 3.6e6 ms and <= 7.2e6 ms)
    * ``daily`` – jobs running longer than 2 hours but within a day (<= 86.4e6 ms)
    * ``weekly`` – jobs running longer than a day but within a week (<= 604.8e6 ms)
    * ``monthly`` – jobs running longer than a week but within roughly a month
      (<= 2.592e9 ms)
    * ``custom`` – jobs explicitly defined in a custom SLA file (see
      ``custom_sla_file`` in ``Config``).  Custom categories override
      computed thresholds and allow special SLAs to be assigned per job.

    Jobs exceeding the monthly threshold are still labelled ``monthly``.

    The analyser also flags over‑ or under‑utilisation based on average CPU
    and memory usage.
    """

    # Duration thresholds in milliseconds
    HOURLY_THRESHOLD_MS: int = 60 * 60 * 1000  # 1 hour
    TWO_HOURS_THRESHOLD_MS: int = 2 * 60 * 60 * 1000  # 2 hours
    DAILY_THRESHOLD_MS: int = 24 * 60 * 60 * 1000  # 1 day
    WEEKLY_THRESHOLD_MS: int = 7 * 24 * 60 * 60 * 1000  # 1 week
    MONTHLY_THRESHOLD_MS: int = 30 * 24 * 60 * 60 * 1000  # ~30 days

    def __init__(self, custom_sla: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
        """
        :param custom_sla: Optional mapping of job identifiers (IDs or names)
            to custom SLA definitions.  Each value may contain a
            ``category`` key to force a specific category and/or a
            ``threshold_ms`` key specifying a custom maximum duration.  If a
            job matches an entry in this mapping, the ``category`` will be
            used instead of the standard thresholds.  See README for examples.
        """
        self.custom_sla: Dict[str, Dict[str, Any]] = custom_sla or {}

    def analyse_jobs(self, jobs: Iterable[Dict[str, Any]]) -> List[AnalysisResult]:
        """Analyse a list of jobs and assign SLA categories.

        :param jobs: Iterable of dictionaries as returned by
          ``YarnFetcher.list_applications``.
        :returns: List of ``AnalysisResult`` objects with categorisation and
          averages.
        """
        results: List[AnalysisResult] = []
        for job in jobs:
            cluster_name = job.get("cluster_name", "")
            app_id = job.get("id")
            app_name = job.get("name")
            elapsed = job.get("elapsedTime")  # in ms
            mem_seconds = job.get("memorySeconds")  # MB * s
            vcore_seconds = job.get("vcoreSeconds")  # vcore * s
            if not app_id or elapsed is None or elapsed <= 0:
                LOGGER.debug("Skipping job %s due to missing elapsed time", app_id)
                continue
            # Convert elapsed time to seconds for average calculations
            elapsed_sec = elapsed / 1000.0
            avg_memory = mem_seconds / elapsed_sec if mem_seconds is not None else None
            avg_cpu = vcore_seconds / elapsed_sec if vcore_seconds is not None else None

            # Determine if job has a custom SLA entry
            custom_entry: Optional[Dict[str, Any]] = None
            if app_id in self.custom_sla:
                custom_entry = self.custom_sla[app_id]
            elif app_name and app_name in self.custom_sla:
                custom_entry = self.custom_sla[app_name]

            if custom_entry:
                # Override category and optionally apply custom threshold in notes
                category = custom_entry.get("category", "custom")
                notes = f"Custom SLA applied (category={category})"
            else:
                # Assign category based on elapsed time thresholds
                if elapsed <= self.HOURLY_THRESHOLD_MS:
                    category = "hourly"
                elif elapsed <= self.TWO_HOURS_THRESHOLD_MS:
                    category = "two_hours"
                elif elapsed <= self.DAILY_THRESHOLD_MS:
                    category = "daily"
                elif elapsed <= self.WEEKLY_THRESHOLD_MS:
                    category = "weekly"
                elif elapsed <= self.MONTHLY_THRESHOLD_MS:
                    category = "monthly"
                else:
                    category = "monthly"
                notes = ""

            # Determine utilisation status (simplistic heuristics)
            status = "normal"
            # If already set by custom entry, keep notes; else compute notes
            if avg_cpu is not None and avg_cpu < 0.1:
                status = "underutilised"
                if not notes:
                    notes = "Low CPU usage; consider reducing executors or cores"
            if avg_memory is not None and avg_memory < 256:
                status = "underutilised"
                if not notes:
                    notes = "Low memory usage; consider reducing executor memory"
            if avg_cpu is not None and avg_cpu > 4:
                status = "overutilised"
                if not notes:
                    notes = "High CPU usage; job may be starved or need more cores"
            if avg_memory is not None and avg_memory > 16384:
                status = "overutilised"
                if not notes:
                    notes = "High memory usage; job may require additional memory"

            results.append(
                AnalysisResult(
                    cluster_name=cluster_name,
                    app_id=app_id,
                    category=category,
                    avg_cpu=avg_cpu,
                    avg_memory=avg_memory,
                    status=status,
                    notes=notes,
                )
            )
        return results

"""
AI Agent CLI
============

This script exposes a command‑line interface for orchestrating the job
profiling workflow.  It can be executed directly, or the functions
inside can be imported into other code.  Typical usage::

    python -m bigdata_ai_agent.main profile   # fetch metadata and store in DB
    python -m bigdata_ai_agent.main analyse   # compute averages and categories
    python -m bigdata_ai_agent.main recommend # generate tuning recommendations
    python -m bigdata_ai_agent.main full      # perform all steps

The actions are defined as separate functions for modularity.  The
``profile`` action pulls metadata from YARN and Spark and writes it to
MySQL.  The ``analyse`` action reads job stats from the DB, computes
average resource usage and SLA categories using the ``JobAnalyzer`` and
writes results into the ``recommendations`` table (without actual
recommendations yet).  The ``recommend`` action invokes the
recommendation engine on all analysed jobs, stores the suggestions in
the database, commits them to a Git repository (if configured) and
sends a summary to Slack.

Logging output can be increased with the ``--verbose`` flag.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

from .config import Config, Cluster
from .database import Database
from .job_fetcher import YarnFetcher, SparkFetcher
from .analyzer import JobAnalyzer
from .recommendation import RecommendationEngine
from .slack_notifier import SlackNotifier
from .git_integration import GitHandler


def profile_jobs(cfg: Config) -> None:
    """Fetch job metadata from YARN and Spark for all configured clusters and store it in the database."""
    db = Database(cfg)
    db.connect()
    db.init_schema()
    # Determine clusters: if cfg.clusters is empty, treat cfg as a single cluster
    clusters: List[Cluster]
    if cfg.clusters:
        clusters = cfg.clusters
    else:
        # Construct a default cluster using top‑level URLs
        default_name = "default"
        clusters = [
            Cluster(
                name=default_name,
                yarn_api_url=cfg.yarn_api_url,
                spark_history_url=cfg.spark_history_url,
            )
        ]
    for cluster in clusters:
        # Build a cluster‑specific configuration for the fetchers
        cluster_cfg = Config(
            yarn_api_url=cluster.yarn_api_url,
            spark_history_url=cluster.spark_history_url,
            mysql_host=cfg.mysql_host,
            mysql_port=cfg.mysql_port,
            mysql_user=cfg.mysql_user,
            mysql_password=cfg.mysql_password,
            mysql_database=cfg.mysql_database,
            openai_api_key=cfg.openai_api_key,
            slack_webhook_url=cfg.slack_webhook_url,
            git_repo_path=cfg.git_repo_path,
            git_remote_url=cfg.git_remote_url,
            custom_sla_file=cfg.custom_sla_file,
            clusters=[cluster],
        )
        yarn = YarnFetcher(cluster_cfg)
        # Fetch both running and finished Spark applications
        try:
            apps = yarn.list_applications(states=["RUNNING", "FINISHED"], application_types=["SPARK"])
        except Exception as exc:
            logging.error("Failed to fetch applications for cluster %s: %s", cluster.name, exc)
            continue
        db.insert_jobs(cluster.name, apps)
        spark = SparkFetcher(cluster_cfg)
        # Collect basic executor metrics; due to variability across Spark versions,
        # we'll store only a small subset of fields.  Additional metrics can be
        # added easily by extending this loop.
        for app in apps:
            app_id = app.get("id")
            if not app_id:
                continue
            try:
                executors = spark.list_executors(app_id)
            except Exception as exc:
                logging.error("Failed to fetch executors for %s in cluster %s: %s", app_id, cluster.name, exc)
                continue
            metrics = []
            for ex in executors:
                executor_id = ex.get("id") or ex.get("executorId") or ex.get("hostPort")
                # Example metrics: total duration and peak memory (if present)
                # These keys may vary between Spark versions.
                if "totalDuration" in ex:
                    metrics.append({"executor_id": executor_id, "metric_name": "totalDuration", "metric_value": ex["totalDuration"]})
                if "maxMemory" in ex:
                    metrics.append({"executor_id": executor_id, "metric_name": "maxMemory", "metric_value": ex["maxMemory"]})
                if "memoryUsed" in ex:
                    metrics.append({"executor_id": executor_id, "metric_name": "memoryUsed", "metric_value": ex["memoryUsed"]})
                if "totalGCTime" in ex:
                    metrics.append({"executor_id": executor_id, "metric_name": "totalGCTime", "metric_value": ex["totalGCTime"]})
            if metrics:
                db.insert_spark_metrics(cluster.name, app_id, metrics)
    db.close()


def analyse_jobs(cfg: Config) -> None:
    """Compute average resource usage and categorise jobs for all clusters."""
    db = Database(cfg)
    db.connect()
    jobs = db.fetch_jobs()  # fetch all clusters
    # Load custom SLA definitions if provided
    custom_sla = None
    if cfg.custom_sla_file:
        try:
            from pathlib import Path
            import json
            custom_path = Path(cfg.custom_sla_file)
            if custom_path.suffix.lower() == ".json":
                custom_sla = json.loads(custom_path.read_text())
            elif custom_path.suffix.lower() in {".yaml", ".yml"}:
                try:
                    import yaml  # type: ignore
                except ImportError:
                    raise ImportError(
                        "PyYAML is required to load YAML custom SLA files."
                    )
                custom_sla = yaml.safe_load(custom_path.read_text())
            else:
                raise ValueError(
                    f"Unsupported custom SLA file format: {custom_path.suffix}"
                )
        except Exception as exc:
            logging.error("Failed to load custom SLA file %s: %s", cfg.custom_sla_file, exc)
            custom_sla = None
    analyzer = JobAnalyzer(custom_sla)
    results = analyzer.analyse_jobs(jobs)
    # Persist analysis results without recommendations (yet)
    for res in results:
        db.upsert_recommendation(
            cluster_name=res.cluster_name,
            app_id=res.app_id,
            category=res.category,
            avg_cpu=res.avg_cpu,
            avg_memory=res.avg_memory,
            status=res.status,
            recommended_executors=None,
            recommended_memory=None,
            recommended_cores=None,
            notes=res.notes,
        )
    db.close()


def generate_recommendations(cfg: Config) -> None:
    """Generate tuning recommendations using the OpenAI API or heuristics for all clusters."""
    db = Database(cfg)
    db.connect()
    jobs = db.fetch_jobs()
    # Load custom SLA definitions if provided
    custom_sla = None
    if cfg.custom_sla_file:
        try:
            from pathlib import Path
            import json
            custom_path = Path(cfg.custom_sla_file)
            if custom_path.suffix.lower() == ".json":
                custom_sla = json.loads(custom_path.read_text())
            elif custom_path.suffix.lower() in {".yaml", ".yml"}:
                try:
                    import yaml  # type: ignore
                except ImportError:
                    raise ImportError(
                        "PyYAML is required to load YAML custom SLA files."
                    )
                custom_sla = yaml.safe_load(custom_path.read_text())
            else:
                raise ValueError(
                    f"Unsupported custom SLA file format: {custom_path.suffix}"
                )
        except Exception as exc:
            logging.error("Failed to load custom SLA file %s: %s", cfg.custom_sla_file, exc)
            custom_sla = None
    analyzer = JobAnalyzer(custom_sla)
    analysis_results = analyzer.analyse_jobs(jobs)
    engine = RecommendationEngine(cfg)
    git_handler = GitHandler(cfg)
    slack_notifier = SlackNotifier(cfg)
    # Ensure git repository is initialised if configured
    try:
        git_handler.init_repo()
    except Exception as exc:
        logging.warning("Git initialisation failed: %s", exc)
    # Prepare summary for Slack
    slack_summary_lines = []
    for res in analysis_results:
        executors, mem_mb, cores, note = engine.recommend(res)
        db.upsert_recommendation(
            cluster_name=res.cluster_name,
            app_id=res.app_id,
            category=res.category,
            avg_cpu=res.avg_cpu,
            avg_memory=res.avg_memory,
            status=res.status,
            recommended_executors=executors,
            recommended_memory=mem_mb,
            recommended_cores=cores,
            notes=note,
        )
        # Write recommendation file into repo
        if cfg.git_repo_path:
            # Create cluster‑specific directory under recommendations
            rec_base = Path(cfg.git_repo_path) / "recommendations" / res.cluster_name
            rec_base.mkdir(parents=True, exist_ok=True)
            rec_file = rec_base / f"{res.app_id}.json"
            with rec_file.open("w", encoding="utf-8") as f:
                json.dump(
                    {
                        "cluster_name": res.cluster_name,
                        "app_id": res.app_id,
                        "category": res.category,
                        "avg_cpu": res.avg_cpu,
                        "avg_memory": res.avg_memory,
                        "status": res.status,
                        "recommended_executors": executors,
                        "recommended_memory_mb": mem_mb,
                        "recommended_cores": cores,
                        "notes": note,
                    },
                    f,
                    indent=2,
                )
            git_handler.commit([str(rec_file)], message=f"Update recommendation for {res.cluster_name}/{res.app_id}")
        slack_summary_lines.append(
            f"Job {res.cluster_name}/{res.app_id}: category={res.category}, "
            f"avg_cpu={res.avg_cpu:.2f if res.avg_cpu else 'N/A'}, "
            f"avg_mem={res.avg_memory:.2f if res.avg_memory else 'N/A'}, "
            f"rec_exe={executors}, rec_mem={mem_mb:.0f}MB, rec_cores={cores}"
        )
    # Push changes if remote configured
    try:
        git_handler.push()
    except Exception as exc:
        logging.warning("Git push failed: %s", exc)
    # Send Slack summary
    if slack_summary_lines:
        slack_text = "Job recommendations updated:\n" + "\n".join(slack_summary_lines)
        slack_notifier.send_message(slack_text)
    db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="BigData AI Agent utility")
    parser.add_argument(
        "action",
        choices=["profile", "analyse", "recommend", "full"],
        help="Action to perform",
    )
    parser.add_argument(
        "--config", "-c",
        help="Path to configuration file (JSON or YAML).  If omitted, environment variables are used.",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    cfg = Config.from_file(args.config) if args.config else Config.from_env()
    if args.action == "profile":
        profile_jobs(cfg)
    elif args.action == "analyse":
        analyse_jobs(cfg)
    elif args.action == "recommend":
        generate_recommendations(cfg)
    elif args.action == "full":
        profile_jobs(cfg)
        analyse_jobs(cfg)
        generate_recommendations(cfg)


if __name__ == "__main__":
    main()
"""
Configuration Management
========================

This module defines a simple configuration object for the AI agent.
Configuration can be loaded from environment variables or from a
YAML/JSON file.  Storing secrets (like database passwords or API
tokens) in environment variables is recommended for security.

The configuration fields include:

* ``yarn_api_url`` – Base URL for the YARN ResourceManager REST API.  The
  ResourceManager exposes details of running and completed applications
  via ``/ws/v1/cluster/apps`` and related endpoints【762643672411904†L4727-L4851】.
* ``spark_history_url`` – Base URL for the Spark History Server API.  The
  history server exposes job and executor metrics under ``/api/v1``
  endpoints, e.g. ``/applications/[app‑id]/executors``【383517666583736†L417-L423】.
* ``mysql_host``, ``mysql_port``, ``mysql_user``, ``mysql_password``,
  ``mysql_database`` – MySQL connection parameters for storing job
  metadata and analysis results.
* ``openai_api_key`` – API key for the OpenAI ChatCompletion API used
  by the recommendation engine.
* ``slack_webhook_url`` – Incoming webhook URL for posting notifications
  to Slack.
* ``git_repo_path`` – Local path to the Git repository containing job
  configuration files.  The agent can commit changes to this
  repository via ``GitHandler``.
* ``git_remote_url`` – (Optional) Remote URL for pushing commits.

The ``Config`` class will automatically read matching environment
variables if they exist.  It also provides a ``from_file`` class method
to load configuration from a JSON or YAML file.  Unsupported keys are
ignored.

```
YARN_API_URL=http://yarn-master:8088
SPARK_HISTORY_URL=http://history-server:18080
MYSQL_HOST=db.example.com
MYSQL_PORT=3306
MYSQL_USER=profiler
MYSQL_PASSWORD=secret
MYSQL_DATABASE=job_profile
OPENAI_API_KEY=sk-***
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
GIT_REPO_PATH=/opt/job-config-repo
GIT_REMOTE_URL=git@example.com:data/job-config.git
```

Note: If you store credentials in a file instead of environment variables,
ensure that file permissions are restricted.

"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, List


@dataclass
class Config:
    """Dataclass encapsulating all runtime configuration for the agent."""

    yarn_api_url: str = ""
    spark_history_url: str = ""
    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_user: str = ""
    mysql_password: str = ""
    mysql_database: str = ""
    openai_api_key: str = ""
    slack_webhook_url: str = ""
    git_repo_path: str = ""
    git_remote_url: Optional[str] = None
    # Optional path to a custom SLA definition file (JSON or YAML).  This file
    # can be used to assign special categories or execution time thresholds to
    # individual jobs.  See the README for details on the format.
    custom_sla_file: Optional[str] = None

    # List of cluster definitions.  When empty, the agent assumes a single
    # cluster using the top‑level ``yarn_api_url`` and ``spark_history_url``
    # values.  Each cluster is described by a ``Cluster`` dataclass defined
    # below.  See README for details.
    clusters: List["Cluster"] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> "Config":
        """Create a Config instance from environment variables.

        Each attribute looks up a corresponding environment variable using
        upper‑case keys (``YARN_API_URL``, ``SPARK_HISTORY_URL``, etc.).
        If a variable is not defined, the default value remains.
        """
        # Parse multi‑cluster configuration from the CLUSTERS environment variable.
        clusters_env: List["Cluster"] = []
        clusters_json = os.getenv("CLUSTERS")
        if clusters_json:
            # CLUSTERS can be a JSON string representing a list of cluster
            # definitions.  Each entry should include a name and either API
            # or UI URLs.  If API URLs are missing, the agent will fall
            # back to using the corresponding UI URLs.  Invalid entries
            # are skipped.
            try:
                data = json.loads(clusters_json)
                if isinstance(data, list):
                    for item in data:
                        try:
                            name = item.get("name")
                            # Prefer explicit API URLs; fall back to UI URLs if absent
                            yarn_api_url = item.get("yarn_api_url") or item.get("yarn_ui_url")
                            spark_history_url = item.get("spark_history_url") or item.get("spark_ui_url")
                            # Require a name and both API endpoints (after fallback) to register the cluster
                            if name and yarn_api_url and spark_history_url:
                                clusters_env.append(
                                    Cluster(
                                        name=name,
                                        yarn_api_url=yarn_api_url,
                                        spark_history_url=spark_history_url,
                                        namenode_namespace=item.get("namenode_namespace"),
                                        yarn_ui_url=item.get("yarn_ui_url"),
                                        spark_ui_url=item.get("spark_ui_url"),
                                    )
                                )
                        except Exception:
                            # Skip malformed cluster entries
                            continue
            except Exception:
                # Ignore parsing errors
                pass
        return cls(
            yarn_api_url=os.getenv("YARN_API_URL", ""),
            spark_history_url=os.getenv("SPARK_HISTORY_URL", ""),
            mysql_host=os.getenv("MYSQL_HOST", "localhost"),
            mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
            mysql_user=os.getenv("MYSQL_USER", ""),
            mysql_password=os.getenv("MYSQL_PASSWORD", ""),
            mysql_database=os.getenv("MYSQL_DATABASE", ""),
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL", ""),
            git_repo_path=os.getenv("GIT_REPO_PATH", ""),
            git_remote_url=os.getenv("GIT_REMOTE_URL"),
            custom_sla_file=os.getenv("CUSTOM_SLA_FILE"),
            clusters=clusters_env,
        )

    @classmethod
    def from_file(cls, path: str | Path) -> "Config":
        """Load configuration from a JSON or YAML file.

        :param path: Path to a JSON or YAML configuration file.  Unknown
            keys are ignored.  Environment variables override file
            values.
        :returns: Config instance.
        :raises ValueError: If the file extension is not ``.json`` or
            ``.yaml``/``.yml``.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")
        data: Dict[str, Any]
        if path.suffix.lower() == ".json":
            data = json.loads(path.read_text())
        elif path.suffix.lower() in {".yaml", ".yml"}:
            try:
                import yaml  # type: ignore
            except ImportError as exc:
                raise ImportError(
                    "To load YAML configuration, please install PyYAML"
                ) from exc
            data = yaml.safe_load(path.read_text())
        else:
            raise ValueError(
                "Unsupported configuration file format: expected .json or .yaml/.yml"
            )
        # Initialise with environment variables first
        cfg = cls.from_env()
        # Fill in missing values from the file
        for key, value in data.items():
            key_lower = key.lower()
            if key_lower == "clusters" and isinstance(value, list):
                clusters: List[Cluster] = []
                for item in value:
                    # Build a cluster from a dict.  Use UI URLs if API URLs are missing.
                    if not isinstance(item, dict):
                        continue
                    try:
                        name = item.get("name")
                        yarn_api_url = item.get("yarn_api_url") or item.get("yarn_ui_url")
                        spark_history_url = item.get("spark_history_url") or item.get("spark_ui_url")
                        if name and yarn_api_url and spark_history_url:
                            # Create a copy with the resolved API URLs so that Cluster() sees the required keys
                            cluster_data = item.copy()
                            cluster_data["yarn_api_url"] = yarn_api_url
                            cluster_data["spark_history_url"] = spark_history_url
                            clusters.append(Cluster(**cluster_data))
                    except Exception:
                        continue
                cfg.clusters = clusters
                continue
            # Preserve backwards compatibility: allow keys like "custom_sla_file"
            # to override the environment variable value.
            if hasattr(cfg, key_lower):
                setattr(cfg, key_lower, value)
        return cfg

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the configuration to a dictionary."""
        cfg_dict = {
            "yarn_api_url": self.yarn_api_url,
            "spark_history_url": self.spark_history_url,
            "mysql_host": self.mysql_host,
            "mysql_port": self.mysql_port,
            "mysql_user": self.mysql_user,
            "mysql_password": self.mysql_password,
            "mysql_database": self.mysql_database,
            "openai_api_key": self.openai_api_key,
            "slack_webhook_url": self.slack_webhook_url,
            "git_repo_path": self.git_repo_path,
            "git_remote_url": self.git_remote_url,
        }
        if self.custom_sla_file:
            cfg_dict["custom_sla_file"] = self.custom_sla_file
        if self.clusters:
            cfg_dict["clusters"] = [cluster.__dict__ for cluster in self.clusters]
        return cfg_dict


@dataclass
class Cluster:
    """Definition of a Hadoop/Spark cluster.

    Each cluster entry must include a ``name``, ``yarn_api_url`` and
    ``spark_history_url``.  Optional fields such as ``namenode_namespace``,
    ``yarn_ui_url`` and ``spark_ui_url`` may also be provided.
    """
    name: str
    yarn_api_url: str
    spark_history_url: str
    namenode_namespace: Optional[str] = None
    yarn_ui_url: Optional[str] = None
    spark_ui_url: Optional[str] = None
